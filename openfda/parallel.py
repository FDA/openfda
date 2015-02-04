#!/usr/bin/python

'''
Parallel processing utilities.

The main function of interest is `map_reduce`, which provides a local
version of mapreduce.
'''

from cPickle import loads, dumps
import collections
import cPickle
import glob
import logging
import multiprocessing
import os
import pprint
import simplejson as json
import sys
import traceback
import xmltodict

import leveldb
import types


def _wrap_process(fn, args, kw=None):
  if kw is None: kw = {}
  try:
    fn(*args, **kw)
  except:
    print >> sys.stderr, 'Caught exception in process/thread.'
    print >> sys.stderr, 'Arguments were: %s' % pprint.pformat(args)
    traceback.print_exc()
    return sys.exc_info()


class ShardedDB(object):
  def __init__(self, filebase, num_shards, create_if_missing):
    self.filebase = filebase
    self.num_shards = num_shards
    self._shards = []
    os.system('mkdir -p "%s"' % filebase)
    for i in range(num_shards):
      shard_file = '%s/shard-%05d-of-%05d.db' % (filebase, i, num_shards)
      self._shards.append(leveldb.LevelDB(shard_file, create_if_missing=create_if_missing))

  @staticmethod
  def create(filebase, num_shards):
    'Create a new ShardedDB with the given number of output shards.'
    return ShardedDB(filebase, num_shards, True)

  @staticmethod
  def open(filebase):
    'Open an existing ShardedDB.'
    files = glob.glob('%s/*.db' % filebase)
    return ShardedDB(filebase, len(files), False)

  def _shard_for(self, key):
    return self._shards[hash(key) % self.num_shards]

  def put(self, key, value):
    self._shard_for(key).Put(key, cPickle.dumps(value, -1))

  def get(self, key):
    return cPickle.loads(self._shard_for(key).Get(key))

  def range_iter(self, start_key, end_key):
    iters = [db.RangeIter(start_key, end_key) for db in self._shards]
    for i in iters:
      for key, value in i:
        yield key, cPickle.loads(value)

  def __iter__(self):
    for shard in self._shards:
      for key, value in shard.RangeIter():
        yield key, cPickle.loads(value)


class MapOutput(object):
  def __init__(self, shuffle_queues):
    self.shuffle_queues = shuffle_queues

  def add(self, k, v):
    queue = self.shuffle_queues[hash(k) % len(self.shuffle_queues)]
    queue.put((k, cPickle.dumps(v, -1)))


class MapInput(object):
  def __init__(self, filename):
    self.filename = filename


class LevelDBInput(MapInput):
  def __init__(self, filename):
    MapInput.__init__(self, filename)
    self.db = leveldb.LevelDB(filename)

  def __iter__(self):
    count = 0
    for key, value in self.db.RangeIter():
      count += 1
      if count % 1000 == 0:
        logging.info('MapInput(%s) [%d]: %s', self.filename, count, key)
      yield key, cPickle.loads(value)


class LevelDBOutput(object):
  def __init__(self, db):
    self.db = db

  def put(self, key, value):
    assert type(key) == types.StringType
    self.db.Put(key, cPickle.dumps(value, -1))


class FilenameInput(MapInput):
  def __init__(self, filename):
    MapInput.__init__(self, filename)

  def __iter__(self):
    yield (self.filename, '')


class JSONLineInput(MapInput):
  def __init__(self, filename):
    MapInput.__init__(self, filename)

  def __iter__(self):
    for idx, line in enumerate(open(self.filename)):
      yield str(idx), json.loads(line)

class XMLDictInput(MapInput):
  ''' Simple class for set XML records and streaming dictionaries.
      This input reads all of the records into memory; care should be taken
      when using it with large inputs.
  '''
  def __iter__(self):
    _item = []
    def _handler(_, ord_dict):
      _item.append(json.dumps(ord_dict))
      return True

    xml_file = open(self.filename).read()
    xmltodict.parse(xml_file, item_depth=2, item_callback=_handler)

    for idx, line in enumerate(_item):
      yield str(idx), line

class LineInput(MapInput):
  def __init__(self, filename):
    MapInput.__init__(self, filename)
    self._filename = filename

  def __iter__(self):
    for idx, line in enumerate(open(self._filename)):
      yield str(idx), line

class Collection(object):
  def __init__(self, filenames_or_glob, input_type):
    if isinstance(filenames_or_glob, list):
      self.filenames = filenames_or_glob
      for f in self.filenames:
        assert os.path.exists(f), 'Missing input: %s' % f
    else:
      self.filenames = glob.glob(filenames_or_glob)

    self.input_type = input_type

  @staticmethod
  def from_list(list, input_type=FilenameInput):
    return Collection(list, input_type)

  @staticmethod
  def from_glob(glob, input_type=FilenameInput):
    return Collection(glob, input_type)

  @staticmethod
  def from_sharded(prefix, input_type=LevelDBInput):
    return Collection(prefix + '/*-of-*.db', input_type)

  def __iter__(self):
    for filename in self.filenames:
      yield filename


class Counters(object):
  def __init__(self, master):
    self.master = master
    self._local_counts = collections.defaultdict(int)

  def increment(self, key):
    self._local_counts[key] += 1

  def add(self, key, value):
    self._local_counts[key] += value



def _run_mapper(mapper, shuffle_queues, input_type, shard_file):
  try:
    map_output = MapOutput(shuffle_queues)
    map_input = input_type(shard_file)
    mapper.map_shard(map_input, map_output)
  except:
    print >> sys.stderr, 'Error running mapper (%s, %s)' % (mapper, shard_file)
    traceback.print_exc()
    raise


class Mapper(object):
  def map(self, key, value, output):
    raise NotImplementedError

  def map_shard(self, map_input, map_output):
    logging.info('Starting mapper: input=%s', map_input)
    mapper = self.map
    for key, value in map_input:
      # logging.info('Mapping... %s, %s', key, value)
      mapper(key, value, map_output)


class IdentityMapper(Mapper):
  def map(self, key, value, output):
    output.add(key, value)


def group_by_key(iterator):
  '''Group identical keys together.

  Given a sorted iterator of (key, value) pairs, returns an iterator of
  (key1, values), (key2, values).
  '''
  last_key = None
  values = []
  for key, value in iterator:
    value = loads(value)
    user_key, _ = key.rsplit('.', 1)
    if user_key != last_key:
      if last_key is not None:
        yield last_key, values
      last_key = user_key
      values = [value]
    else:
      values.append(value)

  if last_key is not None:
    yield last_key, values


def _run_reducer(reducer, queue, output_prefix, i, num_shards):
  try:
    reducer.initialize(queue, output_prefix, i, num_shards)
    reducer.shuffle()
    return reducer.reduce_finished()
  except:
    print >> sys.stderr, 'Error running reducer (%s)' % reducer
    traceback.print_exc()
    raise


class Reducer(object):
  def initialize(self, input_queue, prefix, shard_idx, num_shards):
    self.input_queue = input_queue
    self.prefix = prefix
    self.shard_idx = shard_idx
    self.num_shards = num_shards

  def reduce_shard(self, input_db, output_db):
    for key, values in group_by_key(input_db.RangeIter()):
      self.reduce(key, values, output_db)

  def shuffle(self):
    os.system('mkdir -p "%s"' % self.prefix)
    shuffle_dir = self.prefix + '/shard-%05d-of-%05d.shuffle.db' % (self.shard_idx, self.num_shards)
    shuffle_db = leveldb.LevelDB(shuffle_dir)

    idx = 0
    while 1:
      next_entry = self.input_queue.get()
      if next_entry is None:
        break
      key, value_str = next_entry
      shuffle_db.Put(key + ('.%s' % idx), value_str)
      idx += 1

    output_db = leveldb.LevelDB(
      self.prefix + '/shard-%05d-of-%05d.db' % (self.shard_idx, self.num_shards))

    self.reduce_shard(shuffle_db, LevelDBOutput(output_db))
    del output_db
    del shuffle_db
    os.system('rm -rf "%s"' % shuffle_dir)

  def reduce(self, key, values, output):
    raise NotImplementedError

  def reduce_finished(self):
    '''Called after all values have been reduced.

    The result of this call is returned to the caller of `mapreduce`.
    '''
    pass



class IdentityReducer(Reducer):
  def reduce(self, key, values, output):
    for value in values:
      output.put(key, value)


class SumReducer(Reducer):
  def reduce(self, key, values, output):
    output.put(key, sum([float(v) for v in values]))


class NullReducer(Reducer):
  def reduce(self, key, values, output):
    return


def mapreduce(
  input_collection,
  mapper,
  reducer,
  output_prefix,
  num_shards=None,
  map_workers=None):

  if num_shards is None:
    num_shards = multiprocessing.cpu_count()

  # Managers "manage" the queues used by the mapper and reducers to communicate
  # A single manager ends up being a processing bottleneck; hence we shard the
  # queues across a number of managers.
  managers = [multiprocessing.Manager() for i in range(multiprocessing.cpu_count())]
  shuffle_queues = [managers[i % len(managers)].Queue(1000) for i in range(num_shards)]

  mapper_pool = multiprocessing.Pool(processes=map_workers)
  reducer_pool = multiprocessing.Pool(processes=num_shards)

  try:
    os.system('mkdir -p %s' % os.path.dirname(output_prefix))
    mapper_tasks = []
    for shard in input_collection:
      mapper_tasks.append(
        mapper_pool.apply_async(
          _run_mapper,
          args=(mapper, shuffle_queues, input_collection.input_type, shard)))

    reducer_tasks = []
    for i in range(num_shards):
      reducer_tasks.append(reducer_pool.apply_async(
        _run_reducer,
        args=(reducer, shuffle_queues[i], output_prefix, i, num_shards)))


    for i, task in enumerate(mapper_tasks):
      logging.info('Waiting for mappers... %d/%d', i, len(mapper_tasks))
      task.get()

    logging.info('All mappers finished.')

    # flush shuffle queues
    for q in shuffle_queues:
      q.put(None)

    reduce_outputs = []
    for i, task in enumerate(reducer_tasks):
      logging.info('Waiting for reducers... %d/%d', i, len(reducer_tasks))
      reduce_outputs.append(task.get())

    logging.info('All reducers finished.')
    return reduce_outputs
  except:
    logging.error('MapReduce run failed.', exc_info=1)
    os.system('rm -rf "%s"' % output_prefix)
    raise
  finally:
    # Ensure all the processes and queues we've allocated are cleaned up.
    #
    # This cleanup step shouldn't strictly be necessary: the finalizers for
    # pools and managers should destroy everything when the objects are
    # garbage collected; for some reason this isn't happening: it's
    # possible there is a reference loop somewhere that is preventing
    # collection.
    logging.debug('Closing mapper pool.')
    mapper_pool.close(); mapper_pool.join()

    logging.debug('Closing reducer pool.')
    reducer_pool.close(); reducer_pool.join()

    # Trying to close the queues fails, as they are actually proxy objects.
    # Quitting the managers should result in queues being destroyed as well.
    # [q.close() for q in shuffle_queues]

    logging.debug('Shutting down managers.')
    [m.shutdown() for m in managers]

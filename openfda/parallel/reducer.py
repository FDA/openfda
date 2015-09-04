from cPickle import loads
import collections
import logging
import os
import tempfile

import leveldb

logger = logging.getLogger('mapreduce')

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


class Reducer(object):
  def initialize(self, input_queue, tmp_prefix, output_class, output_prefix, shard_idx, num_shards):
    self.tmp_prefix = tmp_prefix
    self.output_prefix = output_prefix
    self.input_queue = input_queue
    self.output_class = output_class
    self.shard_idx = shard_idx
    self.num_shards = num_shards

  def reduce_shard(self, input_db, output_db):
    for idx, (key, values) in enumerate(group_by_key(input_db.RangeIter())):
      if idx % 1000 == 0:
        logger.info('Reducing records=%d key=%s shard=%d', idx, key, self.shard_idx)
      self.reduce(key, values, output_db)

  def shuffle(self):
    os.system('mkdir -p "%s"' % self.tmp_prefix)
    shuffle_dir = tempfile.mkdtemp(
      prefix='shard-%05d-of-%05d' % (self.shard_idx, self.num_shards),
      dir=self.tmp_prefix)
    shuffle_db = leveldb.LevelDB(shuffle_dir)

    idx = 0
    while 1:
      next_entry = self.input_queue.get()
      if next_entry is None:
        break
      key, value_str = next_entry
      shuffle_db.Put(key + ('.%s' % idx), value_str)
      idx += 1

      if idx % 1000 == 0:
        logger.info('Shuffling records=%d key=%s shard=%d', idx, key, self.shard_idx)

    output_db = self.output_class.create_writer(self.output_prefix, self.shard_idx, self.num_shards)
    logger.debug('Reducer: %s', output_db)
    self.reduce_shard(shuffle_db, output_db)
    output_db.flush()
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

class ListReducer(Reducer):
  def reduce(self, key, values, output):
    output.put(key, list(values))

class NullReducer(Reducer):
  def reduce(self, key, values, output):
    return

def pivot_values(value_list):
  ''' Takes a list of (name, value) tuples, and `pivots` them, returning
      a dictionary from name -> [values].

      This is frequently used when joining a number of inputs together,
      where each input is tagged with a table name.
  '''
  intermediate = collections.defaultdict(list)
  for row in value_list:
    table_name, val = row
    intermediate[table_name].append(val)
  return intermediate

class PivotReducer(Reducer):
  def reduce(self, key, values, output):
    val = pivot_values(values)
    output.put(key, val)

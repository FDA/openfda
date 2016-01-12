#!/usr/bin/python

'''
Parallel processing utilities.

The main function of interest is `map_reduce`, which provides a local
version of mapreduce.
'''

import arrow
import collections
import glob
import logging
import multiprocessing
import os
import subprocess
import traceback
import time

import cPickle
import sys

from .inputs import MRInput, LevelDBInput, FilenameInput
from .outputs import MROutput, LevelDBOutput
from .mapper import Mapper
from .reducer import Reducer
from . import watchdog

try:
  from setproctitle import setproctitle
except:
  def setproctitle(title):
    pass

logger = logging.getLogger('mapreduce')

class WrappedException(Exception):
  def __init__(self):
    self.exception_message = traceback.format_exc()


class MRException(Exception):
  def __init__(self, msg, child_message=None):
    Exception.__init__(self, msg)
    self._child_message = child_message

  def __str__(self):
    return '%s.  Child exception was:\n-- %s' % (
        self.message, self._child_message.replace('\n', '\n-- '))


class _MapperOutput(object):
  '''
  A _MapperOutput object is passed to the user mapper.

  When the map output is added, it is pushed onto a reducer queue based on
  the hash of the map key.
  '''
  def __init__(self, shuffle_queues):
    self.shuffle_queues = shuffle_queues

  def add(self, k, v):
    queue = self.shuffle_queues[hash(k) % len(self.shuffle_queues)]
    queue.put((k, cPickle.dumps(v, -1)))


class Collection(object):
  '''
  Represents an input to mapreduce: a set of files and an input reader
  that knows how to process them.
  '''
  def __init__(self, filenames_or_glob, mr_input):
    if not isinstance(filenames_or_glob, list):
      filenames = glob.glob(filenames_or_glob)
    else:
      filenames = filenames_or_glob

    assert isinstance(mr_input, MRInput), mr_input
    self.mr_input = mr_input
    self.splits = []
    for f in filenames:
      assert os.path.exists(f), 'Missing input: %s' % f
      self.splits.extend(
        mr_input.compute_splits(f, desired_splits=multiprocessing.cpu_count())
      )

  def __repr__(self):
    return 'Collection(%d items)' % len(self.splits)

  @staticmethod
  def from_list(list, mr_input=FilenameInput()):
    return Collection(list, mr_input)

  @staticmethod
  def from_glob(glob, mr_input=FilenameInput()):
    return Collection(glob, mr_input)

  @staticmethod
  def from_sharded(prefix, mr_input=LevelDBInput()):
    return Collection(prefix + '/*-of-*', mr_input)

  @staticmethod
  def from_sharded_list(shard_list, mr_input=LevelDBInput()):
    files = []
    for db in shard_list:
      files += glob.glob(db + '/*-of-*')
    return Collection(files, mr_input)

  def __iter__(self):
    for split in self.splits:
      yield split


class Counters(object):
  def __init__(self, master):
    self.master = master
    self._local_counts = collections.defaultdict(int)

  def increment(self, key):
    self._local_counts[key] += 1

  def add(self, key, value):
    self._local_counts[key] += value


def _run_mapper(mapper, shuffle_queues, split):
  with watchdog.Watchdog():
    try:
      setproctitle('python -- Mapper.%s' % split)
      map_output = _MapperOutput(shuffle_queues)
      map_input = split.mr_input.create_reader(split)
      return mapper.map_shard(map_input, map_output)
    except:
      print >> sys.stderr, 'Error running mapper (%s, %s)' % (mapper, split)
      return WrappedException()


def _run_reducer(reducer, queue, tmp_prefix, output_format, output_tmp, i, num_shards):
  with watchdog.Watchdog():
    try:
      setproctitle('python -- Reducer.%s-of-%s' % (i, num_shards))
      reducer.initialize(queue, tmp_prefix, output_format, output_tmp, i, num_shards)
      reducer.shuffle()
      return reducer.reduce_finished()
    except:
      print >> sys.stderr, 'Error running reducer (%s)' % reducer
      return WrappedException()


def mapreduce(
  inputs,
  mapper,
  reducer,
  output_prefix,
  output_format=LevelDBOutput(),
  num_shards=None,
  map_workers=None):

  if num_shards is None:
    num_shards = output_format.recommended_shards()

  if map_workers is None:
    map_workers = multiprocessing.cpu_count()

  # Managers "manage" the queues used by the mapper and reducers to communicate
  # A single manager ends up being a processing bottleneck; hence we shard the
  # queues across a number of managers.
  managers = [multiprocessing.Manager() for i in range(max(map_workers, num_shards))]
  shuffle_queues = [managers[i % len(managers)].Queue(1000) for i in range(num_shards)]

  mapper_pool = multiprocessing.Pool(processes=map_workers)
  reducer_pool = multiprocessing.Pool(processes=num_shards)

  assert isinstance(mapper, Mapper)
  assert isinstance(reducer, Reducer)

  if num_shards is None:
    num_shards = output_format.recommended_shards()

  if not isinstance(inputs, list):
    inputs = [inputs]

  logger.info('Starting MapReduce: %s -> %s@%s, M: %s, R: %s',
    inputs, output_prefix, num_shards, mapper.__class__.__name__, reducer.__class__.__name__)

  # output data is initially generated in a temporary output directory.
  # it is moved to it's final location when all reducers finish successfully.
  tmp_prefix = output_prefix + '-mapreduce-tmp-%s' % arrow.utcnow().format('YYYY-MM-DD-hh-mm')
  output_tmp = output_prefix + '-mapreduce-output-%s' % arrow.utcnow().format('YYYY-MM-DD-hh-mm')
  output_final = output_prefix

  try:
    os.system('mkdir -p %s' % os.path.dirname(tmp_prefix))

    mapper_tasks = []
    for input_collection in inputs:
      assert isinstance(input_collection, Collection)
      for split in input_collection:
        mapper_tasks.append(
          mapper_pool.apply_async(
            _run_mapper,
            args=(mapper, shuffle_queues, split)))

    reducer_tasks = []
    for i in range(num_shards):
      reducer_tasks.append(reducer_pool.apply_async(
        _run_reducer,
        args=(reducer, shuffle_queues[i], tmp_prefix, output_format, output_tmp, i, num_shards)))

    map_results = {}
    last_log_time = time.time()
    while len(map_results) < len(mapper_tasks):
      for i, task in enumerate(mapper_tasks):
        if i in map_results or not task.ready():
          continue

        result = task.get()
        map_results[i] = result
        if isinstance(result, WrappedException):
          logger.error('Caught exception during mapper execution.')
          logger.error('%s', result.exception_message)
          raise MRException('Mapper %d failed.' % i,
              child_message=result.exception_message)
        else:
          logger.info('Finished mapper: %d', i)
      time.sleep(0.1)

      if time.time() - last_log_time > 5:
        last_log_time = time.time()
        print 'Waiting for mappers: %d/%d' % (len(map_results), len(mapper_tasks)), '\r',

    logger.info('All mappers finished.')

    # flush shuffle queues
    for q in shuffle_queues:
      q.put(None)

    reduce_outputs = []
    for i, task in enumerate(reducer_tasks):
      print 'Waiting for reducers... %d/%d' % (i, len(reducer_tasks)), '\r',
      result = task.get()
      if isinstance(result, WrappedException):
        logger.info('Caught exception during reducer execution.')
        logger.info('%s', result.exception_message)
        raise MRException('Reducer %d failed.' % i,
            child_message=result.exception_message)

      reduce_outputs.append(result)

    logger.info('All reducers finished.')
    output_format.finalize(output_tmp, output_final)
    return reduce_outputs
  except:
    logger.error(
      'MapReduce run failed. mapper=%s reducer=%s input=%s output=%s',
      mapper, reducer, inputs, output_prefix, exc_info=1)
    os.system('rm -rf "%s"' % output_final)
    raise
  finally:
    # Ensure all the processes and queues we've allocated are cleaned up.
    #
    # This cleanup step shouldn't strictly be necessary: the finalizers for
    # pools and managers should destroy everything when the objects are
    # garbage collected; for some reason this isn't happening: it's
    # possible there is a reference loop somewhere that is preventing
    # collection.
    logger.info('Closing mapper pool.')
    mapper_pool.terminate()

    logger.info('Closing reducer pool.')
    reducer_pool.terminate()

    # Trying to close the queues fails, as they are actually proxy objects.
    # Quitting the managers should result in queues being destroyed as well.
    # [q.close() for q in shuffle_queues]

    logger.debug('Shutting down managers.')
    [m.shutdown() for m in managers]

    os.system('rm -rf "%s"' % output_tmp)
    os.system('rm -rf "%s"' % tmp_prefix)

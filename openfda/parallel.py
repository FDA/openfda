#!/usr/bin/python

'''
Parallel processing utilities.

The main function of interest is `map_reduce`, which provides a local
version of mapreduce.
'''

import leveldb
import logging
import multiprocessing
import os
import pprint
import sys
import traceback


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
  def __init__(self, filebase, num_shards):
    self.filebase = filebase
    self.num_shards = num_shards
    self._shards = []

  def _shard_for(self, key):
    return self._shards[hash(key) % self.num_shards]

  def put(self, key, value):
    self._shard_for(key).Put(key, value)

  def get(self, key):
    self._shard_for(key).Get(key)


class MapOutput(object):
  def __init__(self, shuffle_queues):
    self.shuffle_queues = shuffle_queues

  def add(self, k, v):
    self.shuffle_queues[hash(k) % len(self.shuffle_queues)].put((k, v))


def _map_helper(shuffle_queues, mapper_fn, shard):
  try:
    mapper_fn(shard, MapOutput(shuffle_queues))
  except:
    print >> sys.stderr, 'Error running mapper (%s)' % shard
    traceback.print_exc()


class Reducer(object):
  def __init__(self, input_queue, prefix, shard_idx, num_shards, reducer_fn):
    self.input_queue = input_queue
    self.prefix = prefix
    self.shard_idx = shard_idx
    self.num_shards = num_shards
    self.reducer_fn = reducer_fn

  def __call__(self):
    try:
      output_db = leveldb.LevelDB(
        self.prefix + '/shard-%05d-of-%05d.db' % (self.shard_idx,
          self.num_shards))
      self.reducer_fn(self.input_queue, output_db)
    except:
      print >> sys.stderr, 'Error running reducer (%s)' % self.shard_idx
      traceback.print_exc()
    finally:
      del output_db


def identity_reducer(input_queue, output_db):
  count = 0
  while 1:
    entry = input_queue.get()
    if entry is None:
      break
    count += 1
    k, v = entry
    output_db.Put(k, v)
    if count % 1000 == 0:
      logging.info('Reducer. Received %d entries.' % count)


def null_reducer(input_queue, output_db):
  while input_queue.get() is not None:
    continue


def mapreduce(input_collection,
              mapper_fn,
              reducer_fn,
              output_prefix,
              num_shards,
              map_workers=None):
  os.system('mkdir -p %s' % os.path.dirname(output_prefix))

  manager = multiprocessing.Manager()
  shuffle_queues = [manager.Queue(1000) for i in range(num_shards)]
  mapper_pool = multiprocessing.Pool(processes=map_workers)
  reducer_pool = multiprocessing.Pool(processes=num_shards)

  mapper_tasks = []
  for shard in input_collection:
    mapper_tasks.append(
      mapper_pool.apply_async(_map_helper,
                              args=(shuffle_queues, mapper_fn, shard)))

  reducer_tasks = []
  for i in range(num_shards):
    reducer = Reducer(shuffle_queues[i],
                      output_prefix,
                      i,
                      num_shards,
                      reducer_fn)
    reducer_tasks.append(reducer_pool.apply_async(reducer))

  for i, task in enumerate(mapper_tasks):
    logging.info('Waiting for mappers... %d/%d', i, len(mapper_tasks))
    task.wait()

  # flush shuffle queues
  for q in shuffle_queues:
    q.put(None)

  for i, task in enumerate(reducer_tasks):
    logging.info('Waiting for reducers... %d/%d', i, len(reducer_tasks))
    task.wait()

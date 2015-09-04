import cPickle
import glob
import logging
import os
import leveldb

class ShardedDB(object):
  '''
  Manages a number of leveldb "shards" (partitions).

  LevelDB does not support concurrent writers, so we create a separate output
  shard for each reducer.  `ShardedDB` provides a unified interface to
  multiple shards.
  '''
  def __init__(self, filebase, num_shards, create_if_missing):
    self.filebase = filebase
    self.num_shards = num_shards
    self._shards = []
    os.system('mkdir -p "%s"' % filebase)
    for i in range(num_shards):
      shard_file = '%s/shard-%05d-of-%05d.db' % (filebase, i, num_shards)
      self._shards.append(leveldb.LevelDB(shard_file, create_if_missing=create_if_missing))

    logging.info('Opened DB with %s files', num_shards)

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

  def as_dict(self):
    'Returns the content of this database as an in-memory Python dictionary'
    return dict(self.range_iter(None, None))

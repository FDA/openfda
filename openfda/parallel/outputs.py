import cPickle
import collections
import io
import multiprocessing
import os
import logging
import subprocess
import types

import leveldb
import simplejson as json

logger = logging.getLogger('mapreduce')

class MROutput(object):
  suffix = 'db'

  class Writer(object):
    def __init__(self, filename, **kw):
      self.filename = filename

    def put(self, key, value):
      assert False, "Don't use this class directly: use an output like LevelDBOutput"

    def flush(self):
      pass

  def __init__(self, **kw):
    self.writer_args = kw

  def create_writer(self, prefix, shard_idx, num_shards):
    assert prefix, 'No output prefix specified for output'
    assert shard_idx < num_shards, 'Invalid shard index (%d > %d)' % (shard_idx, num_shards)
    os.system('mkdir -p "%s"' % prefix)
    return self.__class__.Writer(
      prefix + '/shard-%05d-of-%05d.%s' % (shard_idx, num_shards, self.suffix),
      **self.writer_args)

  def recommended_shards(self):
    return multiprocessing.cpu_count()

  def finalize(self, tmp_dir, final_dir):
    logger.info('Moving results from %s -> %s', tmp_dir, final_dir)
    if os.path.exists(final_dir):
      logger.warn('Replace existing output directory.')
      subprocess.check_output('rm -rf "%s"' % final_dir, shell=True)
    subprocess.check_output('mv "%s" "%s"' % (tmp_dir, final_dir), shell=True)


class LevelDBOutput(MROutput):
  class Writer(MROutput.Writer):
    def __init__(self, filename):
      self.db = leveldb.LevelDB(filename)
      self._last_key = None

    def put(self, key, value):
      assert isinstance(key, str)
      assert key != self._last_key, (
          'Duplicate keys (%s) passed to LevelDBOutput.'
          'This output does not support multiple keys!' % key
      )
      self.db.Put(key, cPickle.dumps(value, -1))


class JSONOutput(MROutput):
  suffix = 'json'

  class Writer(MROutput.Writer):
    def __init__(self, filename, **kw):
      self.db = {}
      self.json_args = kw
      self.filename = filename

    def put(self, key, value):
      assert isinstance(key, str)
      self.db[key] = value

    def flush(self):
      with open(self.filename, 'w') as out_file:
        json.dump(self.db, out_file, **self.json_args)

  def create_writer(self, prefix, shard_idx, num_shards):
    assert num_shards == 1, 'JSONOutput only works with a single output shard!'
    return MROutput.create_writer(self, prefix, shard_idx, num_shards)

  def recommended_shards(self):
    return 1

  def finalize(self, tmp_dir, final_dir):
    '''
    Move the output JSON file to the final location.

    There should only be one file -- this will fail if the user specified multiple shards!
    '''
    import glob
    files = glob.glob('%s/*.json' % tmp_dir)
    assert len(files) == 1, 'JSONOutput expected one temporary file, got: %s' % files
    logger.info('Moving temporary file: %s to final destination: %s', files[0], final_dir)
    subprocess.check_output('mv "%s" "%s"' % (files[0], final_dir), shell=True)



class JSONLineOutput(MROutput):
  '''
  Writes values as JSON, with one value per line.

  The result is a single file.
  '''
  suffix = 'jsonline'

  def finalize(self, tmp_dir, final_dir):
    os.system('ls "%s"' % tmp_dir)
    subprocess.check_output('cat %s/*.jsonline > "%s"' % (tmp_dir, final_dir), shell=True)

  class Writer(MROutput.Writer):
    def __init__(self, filename):
      MROutput.Writer.__init__(self, filename)
      # convert to io.open() to support unicode writes
      self.output_file = io.open(filename, 'w')

    def put(self, key, value):
      # add recursive decoder to prevent unicode errors on writes.
      def _convert(data):
        if isinstance(data, basestring):
	  return data.decode('utf-8', 'replace')
	elif isinstance(data, collections.Mapping):
	  return dict(map(_convert, data.items()))
	elif isinstance(data, collections.Iterable):
	  return type(data)(map(_convert, data))
	else:
	  return data

      json_str = json.dumps(_convert(value), ensure_ascii=False, encoding='utf-8')
      self.output_file.write(unicode(json_str + '\n'))

    def flush(self):
      logger.info('Flushing: %s', self.filename)
      self.output_file.close()

class NullOutput(MROutput):
  '''
  Ignores all outputs and produces no output files.
  '''
  def finalize(self, tmp_dir, final_dir):
    os.system('rm -rf "%s"' % tmp_dir)

  class Writer(MROutput.Writer):
    def put(self, key, value):
      pass

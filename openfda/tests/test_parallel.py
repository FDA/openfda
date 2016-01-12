import glob
import json
import logging
import os
import unittest

from openfda import parallel, app

class KeyMapper(parallel.Mapper):
  def map(self, key, value, output):
    output.add(key, key)

class BadMapper(parallel.Mapper):
  def map(self, key, value, output):
    raise Exception('I crashed!')

class ParallelTest(unittest.TestCase):
  def make_files(self, input_prefix, data, input_format):
    os.system('mkdir -p "%s"' % input_prefix)
    source_files = []
    for i, val in enumerate(data):
      filename = os.path.join(input_prefix, '%d.txt' % i)
      source_files.append(filename)
      with open(filename, 'w') as f:
        f.write(val)

    return parallel.Collection(source_files, input_format)

  def run_mr(self,
             prefix, input_data,
             input_format=parallel.LineInput(),
             mapper=parallel.IdentityMapper(),
             reducer=parallel.IdentityReducer(),
             output_format=parallel.LevelDBOutput(),
             num_shards=5):
    os.system('rm -rf "%s"' % prefix)
    source = self.make_files(os.path.join(prefix, 'input'), input_data, input_format)
    output_prefix = os.path.join(prefix, 'output')

    parallel.mapreduce(source,
                       mapper=mapper,
                       reducer=reducer,
                       map_workers=1,
                       output_format=output_format,
                       output_prefix=output_prefix,
                       num_shards=num_shards)

    if isinstance(output_format, parallel.LevelDBOutput):
      return sorted(list(parallel.ShardedDB.open(output_prefix)))

    if isinstance(output_format, parallel.JSONOutput):
      return json.load(open(output_prefix))

    if isinstance(output_format, parallel.JSONLineOutput):
      result = []
      with open(output_prefix, 'r') as input_f:
        for line in input_f:
          result.append(json.loads(line))
      return result

  def test_identity(self):
    results = self.run_mr(
      '/tmp/test-identity/',
      ['hello' for i in range(10)],
      input_format=parallel.FilenameInput())

    for i in range(10):
      key, value = results[i]
      assert key == '/tmp/test-identity/input/%d.txt' % i, (i, results[i])
      assert value == ''

  def test_json_output(self):
    results = self.run_mr(
      '/tmp/test-json-output/',
      ['hello' for i in range(10)],
      input_format=parallel.FilenameInput(),
      output_format=parallel.JSONOutput(indent=2, sort_keys=True),
      num_shards=1)

    for i in range(10):
      key = '/tmp/test-json-output/input/%d.txt' % i
      assert key in results, key
      assert results[key] == '', { 'key' : key, 'value' : results[key] }

  def test_jsonline_output(self):
    results = self.run_mr(
      '/tmp/test-jsonline-output/',
      ['hello' for i in range(10)],
      mapper=KeyMapper(),
      input_format=parallel.FilenameInput(),
      output_format=parallel.JSONLineOutput())

    for i in range(10):
      key = '/tmp/test-jsonline-output/input/%d.txt' % i
      assert key in results

  def test_lines_input(self):
    input_data = []
    for i in range(10):
      input_data.append('\n'.join(['test-line'] * 10))

    results = self.run_mr('/tmp/test-lineinput', input_data)

    for i in range(10):
      key, value = results[i]
      assert value == 'test-line', (i, results[i])

  def test_sum(self):
    # 10 files with 100 lines each
    results = self.run_mr(
      '/tmp/test-sum',
      ['\n'.join([str(i) for i in range(100)]) for i in range(10)],
      reducer=parallel.SumReducer())

    results = set(dict(results).values())
    for i in range(100):
      assert i * 10 in results
      results.remove(i * 10)
    assert len(results) == 0, 'Unexpected output: %s' % results

  def test_exception(self):
    with self.assertRaises(parallel.MRException) as ctx:
      self.run_mr(
          '/tmp/test-bad-mapper',
          ['hello' for i in range(10)],
          mapper=BadMapper())


def main(argv):
  unittest.main(argv=argv)

if __name__ == '__main__':
  app.run()

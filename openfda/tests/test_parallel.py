import os
import unittest

from openfda import parallel, app


class ParallelTest(unittest.TestCase):
  def test_identity(self):
    os.system('rm -rf /tmp/test-identity*')
    source_files = ['/tmp/test-identity-%d' % i for i in range(10)]
    for f in source_files:
      os.system('touch "%s"' % f)
      
    source = parallel.Collection(source_files, parallel.FilenameInput)
    parallel.mapreduce(source, 
                       parallel.IdentityMapper(),
                       parallel.IdentityReducer(),
                       '/tmp/test-identity', 
                       2)
    
    results = sorted(list(parallel.ShardedDB.open('/tmp/test-identity/')))
    for i in range(10):
      key, value = results[i]
      assert key == '/tmp/test-identity-%d' % i, results[i]
      assert value == ''
      
  def test_sum(self):
    os.system('rm -rf /tmp/test-sum*')
    source_files = ['/tmp/test-sum-%d' % i for i in range(10)]
    for filename in source_files:
      with open(filename, 'w') as f:
        print >>f, '\n'.join([str(i) for i in range(100)])
     
    source = parallel.Collection(source_files, parallel.LineInput)
    parallel.mapreduce(source, 
                       parallel.IdentityMapper(),
                       parallel.SumReducer(),
                       '/tmp/test-sum', 5)
    
    results = dict(parallel.ShardedDB.open('/tmp/test-sum/'))
    for i in range(100):
      assert str(i) in results, str(i)
      value = results[str(i)]
      self.assertEqual(value, str(i * 10.0))
      
  

def main(argv):
  unittest.main()

if __name__ == '__main__':
  app.run() 

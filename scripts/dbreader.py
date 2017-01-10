#!/usr/bin/env python

'''
Tool for debugging leveldb output.

This takes a leveldb file and a start and stop key.  Records
between start_key and end_key are printed, one per line.
'''

import gflags
import leveldb
import os
import sys

import simplejson as json
from openfda import app, parallel


gflags.DEFINE_string('level_db', None, 'Database file to read.')
gflags.DEFINE_string('sharded_db', None, 'Database file to read.')
gflags.DEFINE_string('start_key', None, 'First key to read.')
gflags.DEFINE_string('end_key', None, 'Last key to read.')
gflags.DEFINE_string('key', None, 'Print just this key.')
gflags.DEFINE_boolean('key_only', False, 'Print only keys')
gflags.DEFINE_boolean('value_only', False, 'Print only values')
gflags.DEFINE_boolean('json_value_out', False, 'Prints values as JSON')

def main(argv):
  FLAGS = gflags.FLAGS
  if FLAGS.key is not None:
    FLAGS.start_key = FLAGS.end_key = FLAGS.key

  if FLAGS.level_db:
    assert os.path.exists(FLAGS.level_db)
    ldb = leveldb.LevelDB(FLAGS.level_db)
    if FLAGS.start_key and FLAGS.end_key:
      db_iter = ldb.RangeIter(FLAGS.start_key, FLAGS.end_key)
    else:
      db_iter = ldb.RangeIter()
  elif FLAGS.sharded_db:
    assert os.path.exists(FLAGS.sharded_db)
    db = parallel.ShardedDB.open(FLAGS.sharded_db)
    if FLAGS.start_key and FLAGS.end_key:
      db_iter = db.range_iter(FLAGS.start_key, FLAGS.end_key)
    else:
      db_iter = db.__iter__()
  else:
    print 'Must specify --level_db or --sharded_db'
    print FLAGS.GetHelp()
    sys.exit(1)

  for (k, v) in db_iter:
    if FLAGS.json_value_out: v = json.dumps(v, indent=2, sort_keys=True)

    if FLAGS.key_only: print k
    elif FLAGS.value_only: print v
    else: print k, v



if __name__ == '__main__':
  app.run()

#!/usr/bin/python

import collections
import os
import cStringIO
from os.path import basename, dirname, join
import subprocess
import logging
import re
import sys

from threading import Thread
# Default subnet to use for VPC instances
GATEWAY_SUBNET = 'subnet-75be9c5d'

INSTANCE_TAGS = {
  'fda-project': 'openfda',
}

# Because a.b.c is easier than a['b']['c']
# This extends dict to allow for easy json.dump action.
class ObjectDict(dict):
  def __init__(self, _dict):
    for k, v in _dict.iteritems():
      v = dict_to_obj(v)
      self[k] = v
      setattr(self, k, v)

  def get_nested(self, key_string, default=None):
    result = self
    nested = key_string.split(".")
    try:
      for i in nested:
        result = getattr(result, i)
    except AttributeError:
      if default:
        return default
      else:
        raise
    return result

def dict_to_obj(d):
  """Create a readonly view of a dictionary as ObjectDicts"""
  if isinstance(d, dict):
    return ObjectDict(d)
  if isinstance(d, list):
    return [dict_to_obj(v) for v in d]
  else:
    return d

class ProcessException(Exception):
  def __init__(self, code, stdout, stderr):
    self.stdout = stdout
    self.stderr = stderr
    self.code = code

  def __repr__(self):
    return 'ProcessException(\ncode=%s\nstderr=\n%s\nstdout=\n%s\n)' % (
      self.code, self.stderr, self.stdout)

class TeeStream(Thread):
  def __init__(self, input_stream, output_stream=sys.stdout, prefix=''):
    Thread.__init__(self)
    self._output = cStringIO.StringIO()
    self.output_stream = output_stream
    self.input_stream = input_stream
    self.prefix = prefix

  def run(self):
    while 1:
      line = self.input_stream.read()
      if not line:
        return
      self.output_stream.write(self.prefix + line)
      self._output.write(line)

  def output(self):
    return self._output.getvalue()


def _checked_subprocess(*args, **kw):
  kw['stderr'] = subprocess.PIPE
  kw['stdout'] = subprocess.PIPE

  proc = subprocess.Popen(*args, **kw)
  stdout = TeeStream(proc.stdout, prefix='OUT: ')
  stderr = TeeStream(proc.stderr, prefix='ERR: ')
  stdout.start()
  stderr.start()
  status_code = proc.wait()
  stdout.join()
  stderr.join()
  if status_code != 0:
    raise ProcessException(code, stdout.output(), stderr.output())

  return stdout.output()

def cmd(args):
  print 'Running: ', ' '.join(args)
  return _checked_subprocess(args)

def shell_cmd(fmt, *args):
  print 'Running: %s: %s' % (fmt, args)
  if len(args) > 0:
      cmd = fmt % args
  else:
      cmd = fmt

  return _checked_subprocess(cmd, shell=True)

def download(url, output_filename):
  shell_cmd('mkdir -p %s', dirname(output_filename))
  shell_cmd("curl -f '%s' > '%s'", url, output_filename)

def transform_dict(coll, transform_fn):
  '''Recursively call `tranform_fn` for each key and value in `coll`
    or any child dictionaries.  transform_fn should return (new_key, new_value).
   '''
  if isinstance(coll, list):
    return [transform_dict(v, transform_fn) for v in coll]
  if not isinstance(coll, dict):
    return coll
  res = {}
  for k, v in coll.iteritems():
    v = transform_dict(v, transform_fn)
    transformed = transform_fn(k, v)
    if transformed is None:
      continue
    if isinstance(transformed, tuple):
      res[transformed[0]] = transformed[1]
    else:
      for fk, fv in transformed: res[fk] = fv
  return res

def is_older(a, b):
  'Returns true if file `a` is older than `b`, or `a` does not exist.'
  if not os.path.exists(b):
    logging.warn('Trying to compare against non-existent test file %s', b)
    return True

  return not os.path.exists(a) or (os.path.getmtime(a) < os.path.getmtime(b))

def get_k_number(data):
  if re.match(r'^(K|BK|DEN)', data):
    return data

def get_p_number(data):
  if re.match(r'^(P|N|D[0-9]|BP|H)', data):
    return data

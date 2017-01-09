#!/usr/bin/python

import collections
import os
import cStringIO
from os.path import basename, dirname, join
import subprocess
import logging
import re
import sys
import time

from threading import Thread

DEFAULT_BATCH_SIZE = 100


class BatchHelper(object):
  '''
  Convenience class for batching operations.

  When more than `batch_size` operations have been added to the batch, `fn`
  will be invoked with `args` and `kw`.  The actual data must be expected
  via the `batch` argument.
  '''
  def __init__(self, fn, *args, **kw):
    self._fn = fn
    self._args = args
    self._kw = kw
    self._batch = []
    self._count = 0
    self._start_time = time.time()

  def flush(self):
    self._fn(*self._args, batch=self._batch, **self._kw)
    del self._batch[:]

  def add(self, obj):
    self._batch.append(obj)
    self._count += 1
    if len(self._batch) > DEFAULT_BATCH_SIZE:
      self.flush()

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.flush()
    elapsed = time.time() - self._start_time
    logging.info('BatchHelper: %d operations in %.2f seconds, %.2f docs/s',
        self._count, elapsed, self._count / elapsed)


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
    raise ProcessException(status_code, stdout.output(), stderr.output())

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
  if not os.path.exists(a):
    return True

  if not os.path.exists(b):
    logging.warn('Trying to compare against non-existent test file %s', b)
    return True

  return (os.path.getmtime(a) < os.path.getmtime(b))


def is_newer(a, b):
  'Returns true if file `a` is newer than `b`, or `b` does not exist.'
  if not os.path.exists(a):
    return False

  if not os.path.exists(b):
    logging.warn('Trying to compare against non-existent test file %s', b)
    return True

  return (os.path.getmtime(a) >= os.path.getmtime(b))


def get_k_number(data):
  if re.match(r'^(K|BK|DEN)', data):
    return data


def get_p_number(data):
  if re.match(r'^(P|N|D[0-9]|BP|H)', data):
    return data


def download(url, output_filename):
  shell_cmd('mkdir -p %s', dirname(output_filename))
  shell_cmd("curl -f '%s' > '%s.tmp'", url, output_filename)
  os.rename(output_filename + '.tmp', output_filename)


def download_to_file_with_retry(url, output_file):
  for i in range(25):
    try:
      download(url, output_file)
      return
    except:
      logging.info('Timeout trying %s, retrying...', url)
      time.sleep(5)
      continue

  raise Exception('Fetch of %s failed.' % url)


def strip_unicode(raw_str, remove_crlf = False):
  # http://stackoverflow.com/questions/10993612/python-removing-xa0-from-string
  a_str = raw_str
  if remove_crlf:
    a_str = a_str.replace(u'\x13', '')\
                 .replace(u'\x0A', '')

  return a_str.replace(u'\xa0', ' ')\
                .replace(u'\u2013', '-')\
                .replace(u'\xae', ' ')\
                .replace(u'\u201c', '')\
                .replace(u'\u201d', '')


def extract_date(date_str):
  '''
  Will transform a timestamp string to valid date string in YYYY-MM-DD format.
  :param date_str: A string having timestamp in YYYYMMDDhhmmss
  :return: a date string
  '''
  if not date_str:
    return '1900-01-01'

  year = date_str[0:4]
  month = date_str[4:6]
  day = date_str[6:8]


  if 1900 >= int(year) or 2050 < int(year):
    year = "1900"
  if 0 >= int(month) or 12 < int(month):
    month = "01"
  if 0 >= int(day) or 12 < int(day):
    day = "01"

  return year + '-' + month + '-' + day

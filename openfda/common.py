#!/usr/bin/python

import os
from os.path import basename, dirname, join
import subprocess

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


def dict_to_obj(d):
  """Create a readonly view of a dictionary as ObjectDicts"""
  if isinstance(d, dict):
    return ObjectDict(d)
  if isinstance(d, list):
    return [dict_to_obj(v) for v in d]
  else:
    return d

def _check(fn):
    try:
        return fn()
    except subprocess.CalledProcessError, e:
        print 'Command failed.'
        print 'Ouptut was: %s' % e.output
        raise

def cmd(args):
    print 'Running: ', ' '.join(args)
    return _check(lambda: subprocess.check_output(args))

def shell_cmd(fmt, *args):
    print 'Running: %s: %s' % (fmt, args)
    if len(args) > 0:
        cmd = fmt % args
    else:
        cmd = fmt

    return _check(lambda: subprocess.check_output(cmd, shell=True))

def download(url, output_filename):
  shell_cmd('mkdir -p %s', dirname(output_filename))
  shell_cmd("curl -f '%s' > '%s'", url, output_filename)

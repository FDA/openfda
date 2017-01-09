"""
There are a number of configuration options that are common across all of
the OpenFDA pipelines.

Previously, these were defined individually in each pipeline, which made it
annoying to specify defaults and easy to make errors.

These are now grouped under the FDAConfig 'psuedo-task' and shared across all
pipelines.  The settings can be adjusted using command-line flags or the luigi
config file.

"""

import luigi
from luigi import Parameter

import os.path

class FDAConfig(luigi.WrapperTask):
  data_dir = Parameter(default='./data')
  tmp_dir = Parameter(default='./data/openfda-tmp')
  es_host = Parameter(default='localhost:9200')
  aws_profile = luigi.Parameter(default='openfda')
  disable_downloads = luigi.Parameter(default=False)

  snapshot_path = luigi.Parameter(default='elasticsearch-snapshots/es-1.7')
  snapshot_bucket = luigi.Parameter(default='openfda-prod')


class Context(object):
  def __init__(self, path=''):
    self._path = path

  def path(self, path=None):
    return os.path.join(FDAConfig().data_dir, self._path, path)

def snapshot_path(): return FDAConfig().snapshot_path
def snapshot_bucket(): return FDAConfig().snapshot_bucket
def es_host(): return FDAConfig().es_host
def disable_downloads(): return FDAConfig().disable_downloads

def es_client(host=None):
  import elasticsearch
  if host is None:
    host = es_host()

  return elasticsearch.Elasticsearch(host, timeout=120)

def data_dir(*subdirs):
  return os.path.join(FDAConfig().data_dir, *subdirs)

def tmp_dir(*subdirs):
  return os.path.join(FDAConfig().tmp_dir, *subdirs)

def aws_profile():
  return FDAConfig().aws_profile

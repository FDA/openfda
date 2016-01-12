#!/usr/bin/python

''' Device Pre-market approval pipeline for downloading, converting to JSON and
    loading into elasticsearch.
'''

import collections
import glob
import logging
import os
from os.path import basename, dirname, join
import sys

import arrow
import elasticsearch
import luigi
import requests
import simplejson as json

from openfda import common, config, download_util, elasticsearch_requests, index_util, parallel

from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
  DeviceAnnotateMapper)
from openfda.device_pma import transform
from openfda.tasks import AlwaysRunTask

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
# A directory for holding files that track Task state
META_DIR = config.data_dir('device_pma/meta')
common.shell_cmd('mkdir -p %s', META_DIR)

DEVICE_PMA_ZIP = 'http://www.accessdata.fda.gov/premarket/ftparea/pma.zip'

class DownloadPMA(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(config.data_dir('device_pma/raw'))

  def run(self):
    output_filename = join(self.output().path, DEVICE_PMA_ZIP.split('/')[-1])
    common.download(DEVICE_PMA_ZIP, output_filename)

class ExtractAndCleanDownloadsPMA(luigi.Task):
  ''' Unzip each of the download files and remove all the non-UTF8 characters.
      Unzip -p streams the data directly to iconv which then writes to disk.
  '''
  def requires(self):
    return DownloadPMA()

  def output(self):
    return luigi.LocalTarget(config.data_dir('device_pma/extracted'))

  def run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    input_dir = self.input().path
    download_util.extract_and_clean(input_dir, 'ISO-8859-1', 'UTF-8', 'txt')

class PMAMapper(parallel.Mapper):
  def map(self, key, value, output):
    # TODO(hansnelsen): bring the `transform.py` logic into the mapper and
    #                   remove the file.
    new_value = transform.transform_device_pma(value)
    output.add(key, new_value)

class Pma2JSON(luigi.Task):
  def requires(self):
    return ExtractAndCleanDownloadsPMA()

  def output(self):
    return luigi.LocalTarget(config.data_dir('device_pma/json.db'))

  def run(self):
    common.shell_cmd('mkdir -p %s', dirname(self.output().path))
    input_files = glob.glob(self.input().path + '/*.txt')
    parallel.mapreduce(
      parallel.Collection.from_glob(
        input_files, parallel.CSVDictLineInput(delimiter='|', strip_str='\0')),
      mapper=PMAMapper(),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path)

class PMAAnnotateMapper(DeviceAnnotateMapper):
  def filter(self, data):
    product_code = data['product_code']
    harmonized = self.harmonized_db.get(product_code, None)
    if harmonized:
      # PMA should never have a 510k openfda key
      if '510k' in harmonized:
        del harmonized['510k']
      if self.table in harmonized:
        del harmonized[self.table]
      return harmonized
    return None

class AnnotateDevice(luigi.Task):
  def requires(self):
    return [Harmonized2OpenFDA(), Pma2JSON()]

  def output(self):
    return luigi.LocalTarget(config.data_dir('device_pma/annotate.db'))

  def run(self):
    harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      mapper=PMAAnnotateMapper(harmonized_db=harmonized_db),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=10)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'devicepma'
  type_name = 'pma'
  mapping_file = './schemas/pma_mapping.json'
  data_source = AnnotateDevice()
  use_checksum = False
  optimize_index = True


if __name__ == '__main__':
  luigi.run()

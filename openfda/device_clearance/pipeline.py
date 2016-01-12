#!/usr/bin/python

''' 510k pipeline for downloading, transforming to JSON and loading into
    Elasticsearch.
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

from openfda import common, config, elasticsearch_requests, index_util, parallel
from openfda import download_util
from openfda.device_clearance import transform
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
  DeviceAnnotateMapper)
from openfda.tasks import AlwaysRunTask

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
# A directory for holding files that track Task state
META_DIR = config.data_dir('510k/meta')
common.shell_cmd('mkdir -p %s', META_DIR)


CLEARED_DEVICE_URL = 'http://www.accessdata.fda.gov/premarket/ftparea/'
CLEARED_DEV_ZIPS = [CLEARED_DEVICE_URL + 'pmn96cur.zip',
  CLEARED_DEVICE_URL + 'pmn9195.zip',
  CLEARED_DEVICE_URL + 'pmn8690.zip',
  CLEARED_DEVICE_URL + 'pmn8185.zip',
  CLEARED_DEVICE_URL + 'pmn7680.zip']


class Download_510K(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(config.data_dir('510k/raw'))

  def run(self):
    output_dir = self.output().path

    for zip_url in CLEARED_DEV_ZIPS:
      output_filename = join(output_dir, zip_url.split('/')[-1])
      common.download(zip_url, output_filename)

class ExtractAndCleanDownloads510k(luigi.Task):
  ''' Unzip each of the download files and remove all the non-UTF8 characters.
      Unzip -p streams the data directly to iconv which then writes to disk.
  '''
  def requires(self):
    return Download_510K()

  def output(self):
    return luigi.LocalTarget(config.data_dir('510k/extracted'))

  def run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    input_dir = self.input().path
    download_util.extract_and_clean(input_dir, 'ISO-8859-1', 'UTF-8', 'txt')


class Clearance2JSON(parallel.MRTask):
  def map(self, key, value, output):
    # TODO(hansnelsen): bring the `transform.py` logic into the mapper and
    #                   remove the file.
    new_value = transform.transform_device_clearance(value)
    output.add(self.filename + ':' + key, new_value)

  def requires(self):
    return ExtractAndCleanDownloads510k()

  def output(self):
    return luigi.LocalTarget(config.data_dir('510k', 'json.db'))

  def mapreduce_inputs(self):
    input_files = glob.glob(self.input().path + '/*.txt')
    return parallel.Collection.from_glob(
      input_files, parallel.CSVDictLineInput(delimiter='|', strip_str='\0'))


class ClearanceAnnotateMapper(DeviceAnnotateMapper):
  def filter(self, data):
    product_code = data['product_code']
    harmonized = self.harmonized_db.get(product_code, None)
    if harmonized:
      # 510k should never have a PMA openfda key
      if 'device_pma' in harmonized:
        del harmonized['device_pma']
      if self.table in harmonized:
        del harmonized[self.table]
      return harmonized
    return None

class AnnotateDevice(luigi.Task):
  def requires(self):
    return [Harmonized2OpenFDA(), Clearance2JSON()]

  def output(self):
    return luigi.LocalTarget(config.data_dir('510k','annotate.db'))

  def run(self):
    harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      mapper=ClearanceAnnotateMapper(harmonized_db=harmonized_db),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=10)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'deviceclearance'
  type_name = 'device510k'
  mapping_file = './schemas/clearance_mapping.json'
  data_source = AnnotateDevice()
  use_checksum = False
  optimize_index = True

if __name__ == '__main__':
  luigi.run()

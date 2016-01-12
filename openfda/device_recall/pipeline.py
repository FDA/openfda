#!/usr/bin/python

''' Device pipeline for downloading, transforming to JSON and loading device
    recall data into Elasticsearch.
'''

import collections
import glob
import logging
import os
from os.path import dirname, join
import sys

import arrow
import elasticsearch
import luigi
import requests
import simplejson as json

from openfda import common, config, elasticsearch_requests, index_util, parallel
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
  DeviceAnnotateMapper)
from openfda.tasks import AlwaysRunTask

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
META_DIR = config.data_dir('device_recall/meta')
common.shell_cmd('mkdir -p %s', META_DIR)

DEVICE_RECALL_BUCKET = 's3://openfda-device-recalls/'
DEVICE_RECALL_LOCAL_DIR = config.data_dir('device_recall/s3_sync')

class SyncS3DeviceRecall(AlwaysRunTask):
  bucket = DEVICE_RECALL_BUCKET
  local_dir = DEVICE_RECALL_LOCAL_DIR

  def _run(self):
    common.cmd(['mkdir', '-p', self.local_dir])
    common.cmd(['aws',
                '--profile=' + config.aws_profile(),
                's3',
                'sync',
                self.bucket,
                self.local_dir])

class CSV2JSON(parallel.MRTask):
  input_dir = DEVICE_RECALL_LOCAL_DIR

  def map(self, key, value, output):


    RENAME_MAP = {
      'res event number': 'res_event_number',
      'rcl products res number': 'product_res_number',
      'rcl firm fei number': 'firm_fei_number',
      'rcl event info date terminated': 'event_date_terminated',
      'rcl event info root cause description': 'root_cause_description',
      'rcl products product code': 'product_code',
      'rcl products submission numbers': 'product_submission_numbers'
    }

    # Uses renamed fields
    DATE_KEYS = ['event_date_terminated']

    def _cleaner(k, v):
      ''' A helper function that is used for renaming dictionary keys and
          formatting dates.
      '''
      def is_other(data):
        if not common.get_p_number(data) and \
           not common.get_k_number(data) and \
           not data.startswith('G'):
          return True
        return False

      k = k.lower()
      if k in RENAME_MAP:
          k = RENAME_MAP[k]
      if k == 'product_submission_numbers':
        # We take this single key, split it on space and then return three new
        # keys and not the original.
        submissions = [s for s in v.split(' ') if s]
        k_numbers = [d for d in submissions if common.get_k_number(d)]
        pma_numbers = [d for d in submissions if common.get_p_number(d)]
        other = ' '.join([d.strip() for d in submissions if is_other(d)])
        return [('k_numbers', k_numbers),
                ('pma_numbers', pma_numbers),
                ('other_submission_description', other)]

      if v != None:
        if k in DATE_KEYS:
          # Elasticsearch cannot handle null dates, emit None so the wrapper
          # function `transform_dict` will omit it from its transformation
          if not v: return None
          v = arrow.get(v, 'YYYY/MM/DD HH:mm:ss').format('YYYY-MM-DD')
        else: return (k, v.strip())
      return (k, v)

    new_value = common.transform_dict(value, _cleaner)
    output.add(key, new_value)

  def requires(self):
    return SyncS3DeviceRecall()

  def output(self):
    return luigi.LocalTarget(config.data_dir('device_recall/json.db'))

  def mapreduce_inputs(self):
    input_files = glob.glob(self.input_dir + '/*.csv')
    return parallel.Collection.from_glob(input_files,
      parallel.CSVDictLineInput())


class DeviceRecallAnnotateMapper(DeviceAnnotateMapper):
  ''' Device recalls have a unique filter requirement.
      There are two filters that get applied, as such, we over-ride the
      `filter()` method of the mapper.

      First, `firm_fei_number` is part of the recall data, so we only want openfda
      records for that matching `fei_number`.

      Second, there is the field `product_submission_numbers` which is a list
      of submission numbers that are either device_pma.pma_number or
      510k.k_number (from the openfda.db) but not both (a device has to be one
      or the other). We filter out anything from both sources that does not
      explicitly match a key in `product_submission_numbers`.
  '''
  def filter(self, data):
    product_code = data['product_code']
    fei_number = data['firm_fei_number']
    k_numbers = data.get('k_numbers', [])
    pma_numbers = data.get('pma_numbers', [])
    pma_numbers = [d.split('/')[0] for d in pma_numbers]

    harmonized = self.harmonized_db.get(product_code, None)

    if harmonized:
      pma_index = set()
      registration = list(harmonized['registration'])
      pma_device = []

      for row in harmonized['device_pma']:
        if row['pma_number'] not in pma_index:
          pma_device.append(row)
          pma_index.add(row['pma_number'])

      clearance = harmonized.get('510k',[])

      harmonized['registration'] = \
        [d for d in registration if d['fei_number'] == fei_number]

      harmonized['device_pma'] = \
        [d for d in pma_device if d['pma_number'] in pma_numbers]

      harmonized['510k'] = \
        [d for d in clearance if d['k_number'] in k_numbers]

      return harmonized
    return None

class AnnotateDevice(luigi.Task):
  def requires(self):
    return [Harmonized2OpenFDA(), CSV2JSON()]

  def output(self):
    return luigi.LocalTarget(config.data_dir('device_recall/annotate.db'))

  def run(self):
    harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      mapper=DeviceRecallAnnotateMapper(harmonized_db=harmonized_db),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=10)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'devicerecall'
  type_name = 'recall'
  mapping_file = './schemas/device_recall_mapping.json'
  data_source = AnnotateDevice()
  use_checksum = False
  optimize_index = True


if __name__ == '__main__':
  luigi.run()

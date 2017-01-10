#!/usr/bin/python

''' A pipeline for loading SPL data into ElasticSearch
'''

import collections
import csv
import glob
import logging
import os
from os.path import basename, join, dirname
import simplejson as json
import sys
import time

import arrow
import elasticsearch
import luigi

from openfda import config, common, elasticsearch_requests, index_util, parallel
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.tasks import AlwaysRunTask
from openfda.spl import annotate
from openfda.parallel import IdentityReducer


RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
META_DIR = config.data_dir('spl/meta')
# Ensure meta directory is available for task tracking
common.shell_cmd('mkdir -p %s', META_DIR)

SPL_JS = join(RUN_DIR, 'spl/spl_to_json.js')
LOINC = join(RUN_DIR, 'spl/data/sections.csv')

SPL_S3_BUCKET = 's3://openfda-data-spl/data/'
SPL_S3_LOCAL_DIR = config.data_dir('spl/s3_sync')
SPL_S3_CHANGE_LOG = join(SPL_S3_LOCAL_DIR, 'change_log/SPLDocuments.csv')
SPL_BATCH_DIR = join(META_DIR, 'batch')
SPL_PROCESS_DIR = config.data_dir('spl/batches')

common.shell_cmd('mkdir -p %s', SPL_S3_LOCAL_DIR)
common.shell_cmd('mkdir -p %s', SPL_PROCESS_DIR)

class SyncS3SPL(luigi.Task):
  bucket = SPL_S3_BUCKET
  local_dir = SPL_S3_LOCAL_DIR

  def flag_file(self):
    return os.path.join(self.local_dir, '.last_sync_time')

  def complete(self):
    # Only run S3 sync once per day.
    if config.disable_downloads():
      return True

    return os.path.exists(self.flag_file()) and (
        arrow.get(os.path.getmtime(self.flag_file())) > arrow.now().floor('day'))

  def run(self):
    common.cmd(['aws',
                '--cli-read-timeout=3600',
		'--profile=' + config.aws_profile(),
                's3', 'sync',
                self.bucket,
                self.local_dir])

    with open(self.flag_file(), 'w') as out_f:
      out_f.write('')


class CreateBatchFiles(AlwaysRunTask):
  batch_dir = SPL_BATCH_DIR
  change_log_file = SPL_S3_CHANGE_LOG

  def requires(self):
    return SyncS3SPL()

  def output(self):
    return luigi.LocalTarget(self.batch_dir)

  def _run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    change_log = csv.reader(open(self.change_log_file, 'r'))
    batches = collections.defaultdict(list)

    for row in change_log:
      spl_id, spl_type, spl_date = row
      # Only grab the human labels for this index
      if spl_type.lower().find('human') != -1:
        # All blank dates to be treated as the week of June 1, 2009
        if not spl_date:
          spl_date = '20090601120000'
        date = arrow.get(spl_date, 'YYYYMMDDHHmmss')
        batches[date.ceil('week')].append(spl_id)

    for batch_date, ids in batches.items():
      batch_file = '%s.ids' % batch_date.format('YYYYMMDD')
      batch_out = open(join(output_dir, batch_file), 'w')
      unique_ids = list(set(ids))
      batch_out.write('\n'.join(unique_ids))

class SPL2JSON(parallel.MRTask):
  spl_path = SPL_S3_LOCAL_DIR
  batch = luigi.Parameter()

  def map(self, _, value, output):
    value = value.strip()
    xml_file = join(self.spl_path, value, value + '.xml')
    if not os.path.exists(xml_file):
      logging.info('File does not exist, skipping %s', xml_file)
      return
    spl_js = SPL_JS
    loinc = LOINC
    cmd = 'node %(spl_js)s %(xml_file)s %(loinc)s' % locals()
    json_str = ''
    try:
      json_str = os.popen(cmd).read()
      json_obj = json.loads(json_str)
      output.add(xml_file, json_obj)
    except:
      logging.error('Unable to convert SPL XML to JSON: %s', xml_file)
      logging.error('cmd: %s', cmd)
      logging.error('json: %s', json_str)
      logging.error(sys.exc_info()[0])
      raise

  def requires(self):
    return CreateBatchFiles()

  def output(self):
    weekly = self.batch.split('/')[-1].split('.')[0]
    dir_name = join(SPL_PROCESS_DIR, weekly)
    return luigi.LocalTarget(join(dir_name, 'json.db'))

  def num_shards(self):
    return 8

  def mapreduce_inputs(self):
    return parallel.Collection.from_glob(self.batch, parallel.LineInput())


class AnnotateJSON(luigi.Task):
  batch = luigi.Parameter()

  def requires(self):
    return [SPL2JSON(self.batch), CombineHarmonization()]

  def output(self):
    output_dir = self.input()[0].path.replace('json.db', 'annotated.db')
    return  luigi.LocalTarget(output_dir)

  def run(self):
    input_db = self.input()[0].path
    harmonized_file = self.input()[1].path

    parallel.mapreduce(
      parallel.Collection.from_sharded(input_db),
      mapper=annotate.AnnotateMapper(harmonized_file),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=1)



class PrepareBatch(luigi.Task):
  ''' Prepares all of the pre-annoation steps. This task is called by the
      weekly harmonization process.
  '''
  def requires(self):
    start = arrow.get('20090601', 'YYYYMMDD').ceil('week')
    end = arrow.utcnow().ceil('week')
    for batch in arrow.Arrow.range('week', start, end):
      batch_file = join(SPL_BATCH_DIR, batch.format('YYYYMMDD') + '.ids')
      yield SPL2JSON(batch_file)

  def output(self):
    return self.input()

  def run(self):
    pass

class LoadJSON(index_util.LoadJSONBase):
  batch = luigi.Parameter()
  index_name = 'druglabel'
  type_name = 'spl'
  mapping_file = './schemas/spl_mapping.json'
  use_checksum = True
  docid_key = 'set_id'
  optimize_index = False

  def _data(self):
    return AnnotateJSON(self.batch)


class ProcessBatch(AlwaysRunTask):
  def requires(self):
    start = arrow.get('20090601', 'YYYYMMDD').ceil('week')
    end = arrow.utcnow().ceil('week')
    previous_task = None
    for batch in arrow.Arrow.range('week', start, end):
      batch_file = join(SPL_BATCH_DIR, batch.format('YYYYMMDD') + '.ids')
      task = LoadJSON(batch=batch_file, previous_task=previous_task)
      previous_task = task
      yield task

  def _run(self):
    index_util.optimize_index('druglabel', wait_for_merge=0)


if __name__ == '__main__':
  luigi.run()

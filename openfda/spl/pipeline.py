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

from openfda import common, elasticsearch_requests, index_util, parallel
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.index_util import AlwaysRunTask
from openfda.spl import annotate
from openfda.parallel import IdentityReducer


RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data'
META_DIR = join(BASE_DIR, 'spl/meta')
# Ensure meta directory is available for task tracking
common.shell_cmd('mkdir -p %s', META_DIR)

SPL_JS = join(RUN_DIR, 'spl/spl_to_json.js')
LOINC = join(RUN_DIR, 'spl/data/sections.csv')

SPL_S3_BUCKET = 's3://openfda.spl.data/data/'
SPL_S3_LOCAL_DIR = join(BASE_DIR, 'spl/s3_sync')
SPL_S3_CHANGE_LOG = join(SPL_S3_LOCAL_DIR, 'change_log/SPLDocuments.csv')
SPL_BATCH_DIR = join(META_DIR, 'batch')
SPL_PROCESS_DIR = join(BASE_DIR, 'spl/batches')

common.shell_cmd('mkdir -p %s', SPL_S3_LOCAL_DIR)
common.shell_cmd('mkdir -p %s', SPL_PROCESS_DIR)

ES_HOST = luigi.Parameter('localhost:9200', is_global=True)
SPL_S3_PROFILE = luigi.Parameter(default='openfda', is_global=True)

class SyncS3SPL(AlwaysRunTask):
  profile = SPL_S3_PROFILE
  bucket = SPL_S3_BUCKET
  local_dir = SPL_S3_LOCAL_DIR

  def _run(self):
    common.cmd(['aws',
                '--profile=' + self.profile,
                's3',
                'sync',
                self.bucket,
                self.local_dir])

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

class SPL2JSONMapper(parallel.Mapper):
  spl_path = SPL_S3_LOCAL_DIR

  def map(self, _, value, output):
    value = value.strip()
    xml_file = join(self.spl_path, value, value + '.xml')
    if not os.path.exists(xml_file):
      logging.info('File does not exist, skipping %s', xml_file)
      return
    spl_js = SPL_JS
    loinc = LOINC
    cmd = 'node %(spl_js)s %(xml_file)s %(loinc)s' % locals()
    json_str = os.popen(cmd).read()
    json_obj = json.loads(json_str)
    output.add(xml_file, json_obj)

class SPL2JSON(luigi.Task):
  batch = luigi.Parameter()

  def requires(self):
    return CreateBatchFiles()

  def output(self):
    weekly = self.batch.split('/')[-1].split('.')[0]
    dir_name = join(SPL_PROCESS_DIR, weekly)
    return luigi.LocalTarget(join(dir_name, 'json.db'))

  def run(self):
    parallel.mapreduce(
      parallel.Collection.from_glob(self.batch, parallel.LineInput),
      mapper=SPL2JSONMapper(),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path)

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
      input_collection=parallel.Collection.from_sharded(input_db),
      mapper=annotate.AnnotateMapper(harmonized_file),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=1,
      map_workers=1)

class ResetElasticSearch(AlwaysRunTask):
  es_host = ES_HOST

  def _run(self):
    es = elasticsearch.Elasticsearch(self.es_host)
    elasticsearch_requests.load_mapping(
      es, 'druglabel.base', 'spl', './schemas/spl_mapping.json')

class LoadJSON(luigi.Task):
  batch = luigi.Parameter()
  es_host = ES_HOST
  epoch = time.time()

  def requires(self):
    return [ResetElasticSearch(), AnnotateJSON(self.batch)]

  def output(self):
    return luigi.LocalTarget(self.batch.replace('.ids', '.done')\
                                       .replace('batch', 'complete'))

  def run(self):
    # Since we only iterate over dates in the umbrella process, we need to
    # skip batch files that do not exist
    output_file = self.output().path
    if not os.path.exists(self.batch):
      common.shell_cmd('touch %s', output_file)
      return

    input_file = self.input()[1].path
    es = elasticsearch.Elasticsearch(self.es_host)
    index_util.start_index_transaction(es, 'druglabel', self.epoch)
    parallel.mapreduce(
      input_collection=parallel.Collection.from_sharded(input_file),
      mapper=index_util.LoadJSONMapper(self.es_host,
                                       'druglabel',
                                       'spl',
                                       self.epoch,
                                       docid_key='set_id',
                                       version_key='version'),
      reducer=parallel.NullReducer(),
      output_prefix='/tmp/loadjson.druglabel',
      num_shards=1,
      map_workers=1)
    index_util.commit_index_transaction(es, 'druglabel')
    common.shell_cmd('touch %s', output_file)

class ProcessBatch(luigi.Task):
  def requires(self):
    start = arrow.get('20090601', 'YYYYMMDD').ceil('week')
    end = arrow.utcnow().ceil('week')
    for batch in arrow.Arrow.range('week', start, end):
      batch_file = join(SPL_BATCH_DIR, batch.format('YYYYMMDD') + '.ids')
      yield LoadJSON(batch_file)

if __name__ == '__main__':
  fmt_string = '%(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s'
  logging.basicConfig(stream=sys.stderr,
                      format=fmt_string,
                      level=logging.INFO)
  # elasticsearch is too verbose by default (logs every successful request)
  logging.getLogger('elasticsearch').setLevel(logging.WARN)

  luigi.run(main_task_cls=ProcessBatch)

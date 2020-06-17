#!/usr/bin/python

''' A pipeline for loading SPL data into ElasticSearch
'''

import collections
import csv
import glob
import logging
import os
import re
import sys
from os.path import join, dirname

import arrow
import luigi
import simplejson as json

from openfda import config, common, index_util, parallel
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import ProcessException
from openfda.parallel import NullOutput
from openfda.spl import annotate
from openfda.tasks import AlwaysRunTask
from bs4 import BeautifulSoup
import urllib2

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

DAILY_MED_DIR = config.data_dir('spl/dailymed')
DAILY_MED_DOWNLOADS_DIR = config.data_dir('spl/dailymed/raw')
DAILY_MED_EXTRACT_DIR = config.data_dir('spl/dailymed/extract')
DAILY_MED_FLATTEN_DIR = config.data_dir('spl/dailymed/flatten')
DAILY_MED_DOWNLOADS_PAGE = 'https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm'


common.shell_cmd('mkdir -p %s', SPL_S3_LOCAL_DIR)
common.shell_cmd('mkdir -p %s', SPL_PROCESS_DIR)
common.shell_cmd('mkdir -p %s', DAILY_MED_DIR)

class DownloadDailyMedSPL(luigi.Task):
  local_dir = DAILY_MED_DOWNLOADS_DIR

  def output(self):
    return luigi.LocalTarget(self.local_dir)

  def run(self):
    common.shell_cmd('mkdir -p %s', self.local_dir)
    soup = BeautifulSoup(urllib2.urlopen(DAILY_MED_DOWNLOADS_PAGE).read(), 'lxml')
    for a in soup.find_all(href=re.compile('.*.zip')):
      if '_human_' in a.text:
        try:
          common.download(a['href'], join(self.local_dir, a['href'].split('/')[-1]))
        except ProcessException as e:
          logging.error("Could not download a DailyMed SPL archive: {0}: {1}".format(a['href'], e))


class ExtractDailyMedSPL(luigi.Task):
  local_dir = DAILY_MED_EXTRACT_DIR

  def requires(self):
    return DownloadDailyMedSPL()

  def output(self):
    return luigi.LocalTarget(self.local_dir)

  def run(self):
    src_dir = self.input().path
    os.system('mkdir -p "%s"' % self.output().path)
    pattern = join(src_dir, '*.zip')
    zip_files = glob.glob(pattern)

    if len(zip_files) == 0:
      logging.warn(
        'Expected to find one or more daily med SPL files')

    extract_dir = self.output().path
    for zip_file in zip_files:
      os.system('unzip -oq -d %(extract_dir)s %(zip_file)s' % locals())

class FlattenDailyMedSPL(parallel.MRTask):
  local_dir = DAILY_MED_FLATTEN_DIR

  def map(self, zip_file, value, output):
    cmd = 'zipinfo -1 %(zip_file)s' % locals()
    xml_file_name = None
    zip_contents = common.shell_cmd_quiet(cmd)
    xml_match = re.search('^([0-9a-f-]{36})\.xml$', zip_contents, re.I | re.M)
    if (xml_match):
      xml_file_name = xml_match.group()
      spl_dir_name = os.path.join(self.output().path, xml_match.group(1))
      os.system('mkdir -p "%s"' % spl_dir_name)
      common.shell_cmd_quiet('unzip -oq %(zip_file)s -d %(spl_dir_name)s' % locals())
      output.add(xml_file_name, zip_file)


  def requires(self):
    return ExtractDailyMedSPL()

  def output(self):
    return luigi.LocalTarget(self.local_dir)

  def mapreduce_inputs(self):
    return parallel.Collection.from_glob(os.path.join(self.input().path, '*/*.zip'))

  def output_format(self):
    return NullOutput()


class AddInDailyMedSPL(luigi.Task):
  local_dir = DAILY_MED_DIR

  def requires(self):
    return FlattenDailyMedSPL()

  def flag_file(self):
    return os.path.join(self.local_dir, '.last_sync_time')

  def complete(self):
    # Only run daily med once per day.
    if config.disable_downloads():
      return True

    return os.path.exists(self.flag_file()) and (
        arrow.get(os.path.getmtime(self.flag_file())) > arrow.now().floor('day'))

  def run(self):
    src = self.input().path
    dest = SPL_S3_LOCAL_DIR
    os.system('cp -nr %(src)s/. %(dest)s/' % locals())

    with open(self.flag_file(), 'w') as out_f:
      out_f.write('')


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
    return [SyncS3SPL(), AddInDailyMedSPL()]

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
      json_str = common.shell_cmd_quiet(cmd)
      json_obj = json.loads(json_str)
      if not json_obj.get('set_id'):
        logging.error('SPL file has no set_id: %s', xml_file)
      else:
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

#!/usr/bin/python

"""
Pipeline for converting AERS and FAERS data in JSON and
importing into Elasticsearch.
"""

from bs4 import BeautifulSoup
import collections
import glob
import logging
import os
from os.path import join, dirname
import re
import subprocess
import sys
import time
import urllib2

import arrow
import elasticsearch
import luigi

from openfda import parallel, index_util, elasticsearch_requests
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.faers import annotate
from openfda.faers import xml_to_json
from openfda.index_util import AlwaysRunTask


# this should be a symlink to wherever the real data directory is
RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data/'
FAERS_HISTORIC = ('http://www.fda.gov/Drugs/GuidanceCompliance'
  'RegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083765.htm')
FAERS_CURRENT = ('http://www.fda.gov/Drugs/GuidanceCompliance'
  'RegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm')

MAX_RECORDS_PER_FILE = luigi.IntParameter(-1, is_global=True)
ES_HOST = luigi.Parameter('localhost:9200', is_global=1)

class DownloadDataset(AlwaysRunTask):
  '''
  This task downloads all datasets that have not yet been fetched.
  '''
  def _fetch(self):
    for page in [self._faers_current.find_all(href=re.compile('.*.zip')),
                 self._faers_historic.find_all(href=re.compile('.*.zip'))]:
      for a in page:
        filename = a.text.split(u'\xa0')[0]
        url = 'http://www.fda.gov' + a['href']
        yield filename, url

  def _download_with_retry(self, url, target_name):
    if os.path.exists(target_name):
      return

    for i in range(10):
      logging.info('Downloading: ' + url)
      cmd = "curl '%s' > '%s'" % (url, target_name)
      subprocess.check_call(cmd, shell=True)
      try:
        subprocess.check_call('unzip -t %s' % target_name, shell=True)
        return
      except:
        logging.info('Problem with zip %s, retrying...' % target_name)
    logging.fatal('Zip File: %s is not valid, stop all processing')

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'faers/raw'))

  def _run(self):
    os.system('mkdir -p "%s"' % self.output().path)

    self._faers_current = BeautifulSoup(urllib2.urlopen(FAERS_CURRENT).read())
    self._faers_historic = BeautifulSoup(urllib2.urlopen(FAERS_HISTORIC).read())

    for filename, url in list(self._fetch()):
      target_name = join(BASE_DIR, 'faers/raw', filename)
      self._download_with_retry(url, target_name)


class ExtractZip(luigi.Task):
  quarter = luigi.Parameter()

  def requires(self):
    return DownloadDataset()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'faers/extracted/', self.quarter))

  def run(self):
    logging.info('Extracting: %s', self.quarter)

    src_dir = self.input().path
    os.system('mkdir -p "%s"' % self.output().path)
    pattern = join(src_dir, '*%s*.[Zz][Ii][Pp]' % self.quarter)
    zip_files = glob.glob(pattern)

    if len(zip_files) == 0:
      logging.warn(
        'Expected to find one or more files for quarter %s (searched for: %s)\n'
        'This may be a result of a bad download, or simply missing quarter data.',
        self.quarter, pattern)

    extract_dir = self.output().path
    for zip_file in zip_files:
      os.system('unzip -o -d %(extract_dir)s %(zip_file)s' % locals())

    # AERS SGM records don't always properly escape &
    for sgm in glob.glob(self.output().path + '/*/sgml/*.SGM'):
      logging.info('Escaping SGML file: %s', sgm)
      os.system('LANG=C sed -i -e "s/&/&amp;/" %s' % sgm)

      # Apply patches if they exist for this SGM
      sgm_filename = sgm.split('/')[-1]
      sgm_diff_glob = join(RUN_DIR, 'faers/diffs/*/*/%(sgm_filename)s.diff' % locals())
      for sgm_diff in glob.glob(sgm_diff_glob):
        logging.info('Patching: %s', sgm)
        cmd = 'patch -N %(sgm)s -i %(sgm_diff)s' % locals()
        logging.info(cmd)
        os.system(cmd)

class XML2JSON(luigi.Task):
  quarter = luigi.Parameter()
  max_records_per_file = MAX_RECORDS_PER_FILE

  def requires(self):
    if self.quarter == 'all':
      quarters = []
      now = arrow.now()
      for year in range(2004, now.year):
        for quarter in range(1, 5):
          quarters.append(ExtractZip('%4dq%d' % (year, quarter)))
      return quarters
    else:
      return [ExtractZip(self.quarter)]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'faers/json/', self.quarter))

  def run(self):
    # AERS_SGML_2007q4.ZIP has files in sqml

    filenames = []
    for input in self.input():
      sgml_path = '/s[gq]ml/*.SGM'
      xml_path = '/[Xx][Mm][Ll]/*.xml'
      logging.info('Checking for inputs in: %s', input.path)
      filenames.extend(glob.glob(input.path + sgml_path))
      filenames.extend(glob.glob(input.path + xml_path))

    assert len(filenames) > 0, 'No files to process for quarter? %s' % self.quarter

    input_shards = []
    for filename in filenames:
      if 'test' in filename.lower():
        continue
      logging.info('Adding input file to pool: %s', filename)
      input_shards.append(filename)

    report_counts = parallel.mapreduce(
      parallel.Collection.from_list(input_shards),
      xml_to_json.ExtractSafetyReportsMapper(),
      xml_to_json.MergeSafetyReportsReducer(),
      self.output().path)

    combined_counts = collections.defaultdict(int)
    for rc in report_counts:
      for timestamp, count in rc.iteritems():
        combined_counts[timestamp] += count

    print '----REPORT COUNTS----'
    for timestamp, count in sorted(combined_counts.items()):
      print '>> ', timestamp, count


class AnnotateJSON(luigi.Task):
  quarter = luigi.Parameter()
  def requires(self):
    return [CombineHarmonization(), XML2JSON(self.quarter)]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'faers/annotated/', self.quarter))

  def run(self):
    harmonized_file = self.input()[0].path
    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      annotate.AnnotateMapper(harmonized_file),
      parallel.IdentityReducer(),
      self.output().path,
      map_workers=16)


class ResetElasticSearch(AlwaysRunTask):
  es_host = ES_HOST
  def _run(self):
    es = elasticsearch.Elasticsearch(self.es_host)
    elasticsearch_requests.load_mapping(
      es, 'drugevent.base', 'safetyreport', './schemas/faers_mapping.json')


class LoadJSON(AlwaysRunTask):
  quarter = luigi.Parameter()
  es_host = ES_HOST

  def __init__(self, *args, **kw):
    AlwaysRunTask.__init__(self, *args, **kw)
    self.epoch = time.time()

  def requires(self):
    return [ResetElasticSearch(), AnnotateJSON(self.quarter)]

  def run(self):
    es = elasticsearch.Elasticsearch(self.es_host)
    index_util.start_index_transaction(es, 'drugevent', self.epoch)

    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      index_util.LoadJSONMapper(self.es_host,
                                'drugevent',
                                'safetyreport',
                                self.epoch,
                                docid_key='@case_number',
                                version_key='@version'),
      parallel.NullReducer(),
      output_prefix='/tmp/loadjson.drugevent',
      num_shards=1,
      map_workers=1)

    index_util.commit_index_transaction(es, 'drugevent')


if __name__ == '__main__':
  logging.basicConfig(
    stream=sys.stderr,
    format='%(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s',
    level=logging.INFO)

  # elasticsearch is too verbose by default (logs every successful request)
  logging.getLogger('elasticsearch').setLevel(logging.WARN)
  luigi.run()

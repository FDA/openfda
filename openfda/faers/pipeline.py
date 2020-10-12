#!/usr/bin/python

"""
Pipeline for converting AERS and FAERS data in JSON and
importing into Elasticsearch.
"""

import collections
import glob
import logging
import os
import re
import subprocess
from os.path import join, dirname
from urllib.request import urlopen

import arrow
import luigi
from bs4 import BeautifulSoup

from openfda import parallel, config, index_util
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.faers import annotate
from openfda.faers import xml_to_json
from openfda.tasks import AlwaysRunTask, DependencyTriggeredTask

# this should be a symlink to wherever the real data directory is
RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = config.data_dir()
FAERS_HISTORIC = 's3://openfda-data-faers-historical'
FAERS_CURRENT = 'https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html'

MAX_RECORDS_PER_FILE = luigi.IntParameter(-1, is_global=True)

class DownloadDataset(luigi.Task):
  '''
  This task downloads all datasets that have not yet been fetched.
  '''
  def _fetch(self):
    for page in [self._faers_current.find_all(href=re.compile('.*.zip'))]:
      for a in page:
        filename = a['href'].split('/')[-1]
        yield filename, a['href']

  def _download_with_retry(self, url, target_name):
    if os.path.exists(target_name):
      return

    for i in range(10):
      try:
        logging.info('Downloading: ' + url)
        cmd = "curl '%s' > '%s'" % (url, target_name)
        subprocess.check_call(cmd, shell=True)
        subprocess.check_call('unzip -t %s' % target_name, shell=True)
        return
      except:
        logging.info('Problem while unzipping[download URL:%s, zip file:%s], retrying...' , url, target_name)
    logging.fatal('Zip File: %s from URL :%s is not valid, stop all processing', target_name, url)

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'faers/raw'))

  def run(self):
    os.system('mkdir -p "%s"' % self.output().path)
    self._faers_current = BeautifulSoup(urlopen(FAERS_CURRENT).read())
    for filename, url in list(self._fetch()):
      target_name = join(BASE_DIR, 'faers/raw', filename.lower())
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
      logging.warning(
        'Expected to find one or more files for quarter %s (searched for: %s)\n'
        'This may be a result of a bad download, or simply missing quarter data.',
        self.quarter, pattern)

    extract_dir = self.output().path
    for zip_file in zip_files:
      os.system('unzip -o -d %(extract_dir)s %(zip_file)s' % locals())

    # FAERS XML files aren't always well-formed due to invalid characters. E.g. ADR12Q4.xml
    for xml in glob.glob(self.output().path + '/**/*.xml'):
      logging.info('Cleaning XML file: %s', xml)
      cleaned = os.path.splitext(xml)[0]+'_cleaned.xml'
      os.system('iconv -f UTF-8 -t UTF-8 -c %(xml)s > %(cleaned)s' % locals())
      os.remove(xml)


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
    return [ExtractZip(self.quarter)]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'faers/json/', self.quarter))

  def run(self):
    # AERS_SGML_2007q4.ZIP has files in sqml

    os.system('mkdir -p "%s"' % self.output().path)

    filenames = []
    for input in self.input():
      sgml_path = '/s[gq]ml/*.SGM'
      xml_path = '/[Xx][Mm][Ll]/*.xml'
      logging.info('Checking for inputs in: %s', input.path)
      filenames.extend(glob.glob(input.path + sgml_path))
      filenames.extend(glob.glob(input.path + xml_path))

    if len(filenames) == 0:
      logging.warning('No files to process for quarter? %s', self.quarter)
      return

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
      self.output().path,
      num_shards=16)

    combined_counts = collections.defaultdict(int)
    for rc in report_counts:
      for timestamp, count in iter(rc.items()):
        combined_counts[timestamp] += count

    print('----REPORT COUNTS----')
    for timestamp, count in sorted(combined_counts.items()):
      print('>> ', timestamp, count)


class AnnotateJSON(DependencyTriggeredTask):
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
      self.output().path)


class LoadJSONQuarter(index_util.LoadJSONBase):
  quarter = luigi.Parameter()
  index_name = 'drugevent'
  type_name = 'safetyreport'
  mapping_file = './schemas/faers_mapping.json'
  docid_key='@case_number'
  use_checksum = True

  # Optimize after all quarters are finished.
  optimize_index = False

  def _data(self):
    return AnnotateJSON(self.quarter)


class LoadJSON(AlwaysRunTask):
  quarter = luigi.Parameter()
  def requires(self):
    if self.quarter == 'all':
      now = arrow.now()
      previous_task = None
      for year in range(2004, now.year + 1):
        for quarter in range(1, 5):
          task = LoadJSONQuarter(quarter='%4dq%d' % (year, quarter), previous_task=previous_task)
          previous_task = task
          yield task
    else:
      yield LoadJSONQuarter(quarter=self.quarter)

  def _run(self):
    index_util.optimize_index('drugevent', wait_for_merge=0)


if __name__ == '__main__':
  luigi.run()

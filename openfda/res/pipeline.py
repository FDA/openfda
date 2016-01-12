#!/usr/local/bin/python

'''
Pipeline for converting XML RES data in JSON and importing into Elasticsearch.
'''

from bs4 import BeautifulSoup
import glob
import hashlib
import logging
import os
from os.path import join, dirname
import random
import re
import requests
import simplejson as json
import socket
import sys
import time
import urllib2
import xmltodict

import arrow
import elasticsearch
import luigi

from openfda import common, config, parallel, index_util, elasticsearch_requests
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import download_to_file_with_retry
from openfda.tasks import AlwaysRunTask
from openfda.res import annotate, extract

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))

CURRENT_XML_BASE_URL = ('http://www.accessdata.fda.gov/scripts/'
                        'enforcement/enforce_rpt-Product-Tabs.cfm?'
                        'action=Expand+Index&w=WEEK&lang=eng&xml')
RES_BASE_URL = ('http://www.fda.gov/Safety/Recalls/'
                'EnforcementReports/default.htm')

def get_latest_date(input_url, download_url):
  ''' A function that grabs all of the hrefs pointing to the download page
      and then grabs the max date from the url to set the upper bound date of
      the download loop.
  '''
  url_open = urllib2.urlopen(input_url)
  soup = BeautifulSoup(url_open)
  re_string = download_url.split('WEEK')[0]
  re_string = re_string.replace('?','\?')\
                       .replace('http:', '^http:')\
                       .replace('+','\+')
  urls = soup.find_all('a', href=re.compile(re_string))

  def date_from_url(url):
    dt = str(url).split('w=')[1].split('&')[0]
    (month, day, year) = (dt[:2], dt[2:4], dt[4:])
    return arrow.get(int(year), int(month), int(day))

  dates = [date_from_url(url) for url in urls]
  return max(dates)

XML_START_DATE = arrow.get(2012, 6, 20)
XML_END_DATE = get_latest_date(RES_BASE_URL, CURRENT_XML_BASE_URL)

# The FDA transitioned from HTML recall events to XML during the summer of 2012.
# During that transition, there are two dates where the enforcement reports are
# availabe as XML, but using a slightly different URL. Also, what can be
# imagined as a manual transition between formats, the last date is 07-05-2012,
# messes up the date offset logic of 'every 7 days' and makes the transition
# to XML date logic a little quirky. These are collectively referred to as
# CROSSOVER_XML dates.

CROSSOVER_XML_START_DATE = XML_START_DATE
CROSSOVER_XML_END_DATE = XML_START_DATE.replace(days=+7)
CROSSOVER_XML_WEIRD_DATE = XML_END_DATE.replace(days=+8)

CROSSOVER_XML_URL = ('http://www.accessdata.fda.gov/scripts/'
                     'enforcement/enforce_rpt-Event-Tabs.cfm?'
                     'action=Expand+Index&w=WEEK&lang=eng&xml')

class DownloadXMLReports(luigi.Task):
  batch = luigi.Parameter()

  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(config.data_dir('res/batches/%s' % self.batch.strftime('%Y%m%d')))

  def run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    date = self.batch
    if date >= CROSSOVER_XML_START_DATE and date <= CROSSOVER_XML_END_DATE:
      url = CROSSOVER_XML_URL
    else:
      url = CURRENT_XML_BASE_URL
    url = url.replace('WEEK', date.strftime('%m%d%Y'))
    file_name = 'enforcementreport.xml'
    xml_file = '%(output_dir)s/%(file_name)s' % locals()
    download_to_file_with_retry(url, xml_file)


class XML2JSONMapper(parallel.Mapper):
  ''' Mapper for the XML2JSON map-reduction. There is some special logic in here
      to generate a hash() from the top level key/value pairs for the id in
      Elasticsearch.
      Also, upc and ndc are extracted and added to reports that are of drug
      type.
  '''
  def _hash(self, doc_json):
    ''' Hash function used to create unique IDs for the reports
    '''
    hasher = hashlib.sha256()
    hasher.update(doc_json)
    return hasher.hexdigest()

  def map(self, key, value, output):
    # These keys must exist in the JSON records for the annotation logic to work
    logic_keys = [
      'code-info',
      'report-date',
      'product-description'
    ]

    for val in value['recall-number']:
      if val['product-type'] == 'Drugs':
        val['upc'] = extract.extract_upc_from_recall(val)
        val['ndc'] = extract.extract_ndc_from_recall(val)

      # Copy the recall-number attribute value to an actual field
      # The recall-number is not a reliable id, since it repeats
      val['recall-number'] = val['@id']

      # There is no good ID for the report, so we need to make one
      doc_id = self._hash(json.dumps(val, sort_keys=True))
      val['@id'] = doc_id
      val['@version'] = 1

      # Only write out vals that have required keys and a meaningful date
      if set(logic_keys).issubset(val) and val['report-date'] != None:
        output.add(doc_id, val)
      else:
        logging.warn('Docuemnt is missing required fields. %s',
                     json.dumps(val, indent=2, sort_keys=True))

class XML2JSON(luigi.Task):
  batch = luigi.Parameter()

  def requires(self):
    return DownloadXMLReports(self.batch)

  def output(self):
    return luigi.LocalTarget(join(self.input().path, 'json.db'))

  def run(self):
    input_dir = self.input().path
    for xml_filename in glob.glob('%(input_dir)s/*.xml' % locals()):
      parallel.mapreduce(
        parallel.Collection.from_glob(
          xml_filename, parallel.XMLDictInput(depth=1)),
        mapper=XML2JSONMapper(),
        reducer=parallel.IdentityReducer(),
        output_prefix=self.output().path,
        num_shards=1,
        map_workers=8)

class AnnotateJSON(luigi.Task):
  batch = luigi.Parameter()

  def requires(self):
    return [XML2JSON(self.batch), CombineHarmonization()]

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
      num_shards=1,
      map_workers=1)


class LoadJSON(index_util.LoadJSONBase):
  batch = luigi.Parameter()
  index_name = 'recall'
  type_name = 'enforcementreport'
  mapping_file = './schemas/res_mapping.json'
  use_checksum = True
  optimize_index = False
  docid_key = '@id'

  def _data(self):
    return AnnotateJSON(self.batch)


class RunWeeklyProcess(AlwaysRunTask):
  ''' Generates a date object that is passed through the pipeline tasks in order
      to generate and load JSON documents for the weekly enforcement reports.

      There is some special date logic due to some gaps in the every 7 day
      pattern.
  '''
  # TODO(hansnelsen): find a way to detect and auto adjust a gap in the every 7
  #                   days logic.
  def requires(self):
    start = XML_START_DATE
    end = get_latest_date(RES_BASE_URL, CURRENT_XML_BASE_URL)
    previous_task = None

    for batch_date in arrow.Arrow.range('week', start, end):
      # Handle annoying cases like July 5th, 2012 (it is +8)
      if batch_date == arrow.get(2012, 7, 4):
        batch = batch_date.replace(days=+1)
      elif batch_date == arrow.get(2013, 12, 25):
        batch = batch_date.replace(days=+1)
      elif batch_date == arrow.get(2014, 10, 8):
        batch = batch_date.replace(days=-1)
      elif batch_date == arrow.get(2014, 12, 3):
        batch = batch_date.replace(days=-1)
      else:
        batch = batch_date

      task = LoadJSON(batch=batch, previous_task=previous_task)
      previous_task = task
      yield task

  def _run(self):
    index_util.optimize_index('recall', wait_for_merge=0)

if __name__ == '__main__':
  luigi.run()

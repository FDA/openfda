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

from openfda import common, parallel, index_util, elasticsearch_requests
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.index_util import AlwaysRunTask
from openfda.res import annotate
from openfda.res import extract

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data'

ES_HOST = luigi.Parameter('localhost:9200', is_global=True)

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

def random_sleep():
  # Give the FDA webservers a break between requests
  sleep_seconds = random.randint(1, 2)
  logging.info('Sleeping %d seconds' % sleep_seconds)
  time.sleep(sleep_seconds)

def download_to_file_with_retry(url, output_file):
  logging.info('Downloading: ' + url)
  url_open = None
  # Retry up to 25 times before failing
  for i in range(25):
    try:
      random_sleep()
      url_open = urllib2.urlopen(url, timeout=5)
      break
    except socket.timeout:
      logging.info('Timeout trying %s, retrying...', url)
      continue
  try:
    content = url_open.read()
    output_file.write(content)
    return
  except:
    logging.fatal('Count not fetch in twenty five tries: ' + url)

class DownloadXMLReports(luigi.Task):
  batch = luigi.Parameter()

  def requires(self):
    return []

  def output(self):
    batch_str = self.batch.strftime('%Y%m%d')
    return luigi.LocalTarget(join(BASE_DIR, 'res/batches', batch_str))

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
    xml_filepath = '%(output_dir)s/%(file_name)s' % locals()
    xml_file = open(xml_filepath, 'w')
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
        input_collection=parallel.Collection.from_glob(xml_filename,
                                                       parallel.XMLDictInput),
        mapper=XML2JSONMapper(),
        reducer=parallel.IdentityReducer(),
        output_prefix=self.output().path,
        num_shards=1,
        map_workers=1)

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
      es, 'recall.base', 'enforcementreport', './schemas/res_mapping.json')


class LoadJSON(luigi.Task):
  ''' Load the annotated JSON documents into Elasticsearch
  '''
  batch = luigi.Parameter()
  es_host = ES_HOST
  epoch = time.time()

  def requires(self):
    return [ResetElasticSearch(), AnnotateJSON(self.batch)]

  def output(self):
    output = self.input()[1].path.replace('annotated.db', 'load.done')
    return luigi.LocalTarget(output)

  def run(self):
    output_file = self.output().path
    input_file = self.input()[1].path
    es = elasticsearch.Elasticsearch(self.es_host)
    index_util.start_index_transaction(es, 'recall', self.epoch)
    parallel.mapreduce(
      input_collection=parallel.Collection.from_sharded(input_file),
      mapper=index_util.LoadJSONMapper(self.es_host,
                                       'recall',
                                       'enforcementreport',
                                       self.epoch,
                                       docid_key='@id',
                                       version_key='@version'),
      reducer=parallel.NullReducer(),
      output_prefix='/tmp/loadjson.recall',
      num_shards=1,
      map_workers=1)
    index_util.commit_index_transaction(es, 'recall')
    common.shell_cmd('touch %s', output_file)

class RunWeeklyProcess(luigi.Task):
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
      yield LoadJSON(batch)

if __name__ == '__main__':
  fmt_string = '%(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s'
  logging.basicConfig(stream=sys.stderr,
                      format=fmt_string,
                      level=logging.INFO)
  # elasticsearch is too verbose by default (logs every successful request)
  logging.getLogger('elasticsearch').setLevel(logging.WARN)

  luigi.run(main_task_cls=RunWeeklyProcess)

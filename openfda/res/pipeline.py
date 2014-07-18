#!/usr/local/bin/python
"""
Pipeline for converting historic HTML and current XML RES data in JSON and
importing into Elasticsearch.

Note: RES data prior to July 2012 are only publically available in HTML format.
The HTML2JSON step of this pipeline converts this HTML format into the canonical
format used by Elasticsearch.
"""

from bs4 import BeautifulSoup
import datetime
import glob
import logging
import luigi
import os
from os.path import join, dirname
import random
import requests
import simplejson as json
import StringIO
import sys
import time
import urllib2
import xmltodict

from openfda import parallel
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.res import annotate
from openfda.res import extract
from openfda.res import scrape_historic

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data'

CURRENT_XML_BASE_URL = ('http://www.accessdata.fda.gov/scripts/'
                        'enforcement/enforce_rpt-Product-Tabs.cfm?'
                        'action=Expand+Index&w=WEEK&lang=eng&xml')

XML_START_DATE = datetime.date(2012, 06, 20)
XML_END_DATE = datetime.date(2014, 06, 18)


# The FDA transitioned from HTML recall events to XML during the summer of 2012.
# During that transition, there are two dates where the enforcement reports are
# availabe as XML, but using a slightly different URL. Also, what can be
# imagined as a manual transition between formats, the last date is 07-05-2012,
# messes up the date offset logic of 'every 7 days' and makes the transition
# to XML date logic a little quirky. These are collectively referred to as
# CROSSOVER_XML dates.

CROSSOVER_XML_START_DATE = datetime.date(2012, 06, 20)
CROSSOVER_XML_END_DATE = datetime.date(2012, 06, 27)
CROSSOVER_XML_WEIRD_DATE = datetime.date(2012, 07, 05)

CROSSOVER_XML_URL = ('http://www.accessdata.fda.gov/scripts/'
                     'enforcement/enforce_rpt-Event-Tabs.cfm?'
                     'action=Expand+Index&w=WEEK&lang=eng&xml')

HISTORIC_HTML_BASE_URL = ('http://www.fda.gov/Safety/Recalls/'
                          'EnforcementReports/YEAR/default.htm')

HISTORIC_HTML_YEARS = [
  '2004',
  '2005',
  '2006',
  '2007',
  '2008',
  '2009',
  '2010',
  '2011',
  '2012'  # only through June 13, 2012
]

def random_sleep():
  # Give the FDA webservers a break between requests
  sleep_seconds = random.randint(1, 2)
  logging.info('Sleeping %d seconds' % sleep_seconds)
  time.sleep(sleep_seconds)

def download_to_file_with_retry(url, output_file):
  logging.info('Downloading: ' + url)

  # Retry up to 10 times before failing
  for i in range(10):
    try:
      random_sleep()
      content = urllib2.urlopen(url).read()
      if 'Gateway Timeout' in content:
        raise HTTPError
      output_file.write(content)
      return

    except HTTPError:
      logging.info('Error fetching: %s. Retrying...' % url)

  logging.fatal('Count not fetch in ten tries: ' + url)

class DownloadHistoricHTMLReports(luigi.Task):
  def _find_urls(self, year):
    url = HISTORIC_HTML_BASE_URL.replace('YEAR', year)
    page_file = StringIO.StringIO()
    download_to_file_with_retry(url, page_file)
    page_file.seek(0)

    urls = []
    soup = BeautifulSoup(page_file.read())
    urls_raw = soup.find('div',
                         attrs={'class': 'middle-column_2'}).find_all('a')
    for url_raw in urls_raw:
      # ucm in the url indicates the old, static HTML links as opposed new,
      # dynamic coldfusion links
      url = 'http://www.fda.gov%s' % url_raw['href']
      if 'ucm' in url:
        urls.append(url)
    return urls

  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'res/historic/html'))

  def run(self):
    output_dir = self.output().path
    os.system('mkdir -p "%(output_dir)s"' % locals())

    for year in HISTORIC_HTML_YEARS:
      year_dir = '%(output_dir)s/%(year)s' % locals()
      os.system('mkdir -p "%(year_dir)s"' % locals())

      urls = self._find_urls(year)
      for url in urls:
        html_filename = url.split('/')[-1]
        html_filepath = join(year_dir, html_filename)
        html_file = open(html_filepath, 'w')
        download_to_file_with_retry(url, html_file)

class HistoricHTML2JSON(luigi.Task):
  def requires(self):
    return DownloadHistoricHTMLReports()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'res/historic/json'))

  def run(self):
    input_dir = self.input().path
    output_dir = self.output().path
    os.system('mkdir -p "%s"' % output_dir)
    target_filename = join(output_dir, 'res_historic.json')
    json_filename = open(target_filename, 'w')
    for html_filename in glob.glob(input_dir + '/*/*.htm'):
      html_filename = open(html_filename, 'r')
      scraped_file = scrape_historic.scrape_report(html_filename)
      for report in scraped_file:
        json_filename.write(json.dumps(report) + '\n')

class DownloadCurrentXMLReports(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'res/current/xml'))

  def run(self):
    output_dir = self.output().path
    os.system('mkdir -p "%(output_dir)s"' % locals())
    # Required weirdness to work around both the URL mismatch and the 3 special
    # date offsets that apply during the transition from HTML to XML
    date = XML_START_DATE
    while date <= XML_END_DATE:
      if date >= CROSSOVER_XML_START_DATE and date <= CROSSOVER_XML_END_DATE:
        url = CROSSOVER_XML_URL
      else:
        url = CURRENT_XML_BASE_URL

      url = url.replace('WEEK', date.strftime('%m%d%Y'))
      week = date.strftime('%Y-%m-%d')

      xml_filepath = '%(output_dir)s/%(week)s.xml' % locals()
      xml_file = open(xml_filepath, 'w')
      download_to_file_with_retry(url, xml_file)

      if date == CROSSOVER_XML_END_DATE:
        date += datetime.timedelta(days=8)
      if date == CROSSOVER_XML_WEIRD_DATE:
        date += datetime.timedelta(days=6)
      else:
        date += datetime.timedelta(days=7)

class CurrentXML2JSON(luigi.Task):
  def requires(self):
    return DownloadCurrentXMLReports()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'res/current/json'))

  def run(self):
    input_dir = self.input().path
    output_dir = self.output().path
    os.system('mkdir -p "%s"' % output_dir)
    output_file = self.output().path + '/current_res.json'
    out = open(output_file, 'w')

    # Callback function for xmltodict.parse()
    def handle_recall(_, recall):
      out.write(json.dumps(recall) + '\n')
      return True

    for xml_filename in glob.glob('%(input_dir)s/*.xml' % locals()):
      xmltodict.parse(open(xml_filename),
                      item_depth=2,
                      item_callback=handle_recall)

class ExtractUpcNdcFromJSON(luigi.Task):
  def requires(self):
    return [CurrentXML2JSON(), HistoricHTML2JSON()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'res/json'))

  def run(self):
    output_dir = self.output().path
    os.system('mkdir -p "%s"' % output_dir)
    current_event_files = glob.glob(self.input()[0].path + '/*.json')
    historic_event_files = glob.glob(self.input()[1].path + '/*.json')
    all_files = current_event_files + historic_event_files
    output_file = self.output().path + '/all_res.json'
    out = open(output_file, 'w')
    # These keys must exist in the JSON records for the annotation logic to work
    logic_keys = [
      'code-info',
      'report-date',
      'product-description'
    ]

    for filename in all_files:
      json_file = open(filename, 'r')
      for row in json_file:
        record = json.loads(row)
        if record['product-type'] == 'Drugs':
          record['upc'] = extract.extract_upc_from_recall(record)
          record['ndc'] = extract.extract_ndc_from_recall(record)
        # Only write out records that have required keys and a meaningful date
        if set(logic_keys).issubset(record) and record['report-date'] != None:
          out.write(json.dumps(record) + '\n')

class AnnotateRes(luigi.Task):
  def requires(self):
    return [CombineHarmonization(), ExtractUpcNdcFromJSON()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'res/annotated'))

  def run(self):
    output_dir = self.output().path
    os.system('mkdir -p "%s"' % output_dir)
    harmonized_file = self.input()[0].path
    event_file = glob.glob(self.input()[1].path + '/*.json')
    output_file = self.output().path + '/annotated_res.json'
    recall_event = annotate.AnnotateMapper(harmonized_file)
    for file_name in event_file:
      recall_event(file_name, output_file)

class ResetElasticSearch(luigi.Task):
  def requires(self):
    return AnnotateRes()

  def output(self):
    return luigi.LocalTarget('/tmp/elastic.res.initialized')

  def run(self):
    cmd = join(RUN_DIR, 'res/clear_and_load_mapping.sh')
    logging.info('Running Shell Script:' + cmd)
    os.system(cmd)
    os.system('touch "%s"' % self.output().path)

# TODO(hansnelsen): Look to consolidate this code into a shared library. It is
# used by both res and faers pipeline.py. Res is slightly different, in that it
# writes to JSON instead of LevelDB
def load_json(input_file):
  event_json = open(input_file, 'r')
  json_batch = []
  def _post_batch():
    response = \
    requests.post('http://localhost:9200/recall/enforcementreport/_bulk',
                  data='\n'.join(json_batch))
    del json_batch[:]
    if response.status_code != 200:
      logging.info('Bad response: %s', response)

  for row in event_json:
    event_raw = json.loads(row)
    json_batch.append('{ "index" : {} }')
    json_batch.append(json.dumps(event_raw))
    if len(json_batch) > 1000:
      _post_batch()

  _post_batch()

class LoadJSON(luigi.Task):
  def requires(self):
    return [ResetElasticSearch(), AnnotateRes()]

  def output(self):
    return luigi.LocalTarget('/tmp/elastic.res.done')

  def run(self):
    input_json = glob.glob(self.input()[1].path + '/*.json')
    for filename in input_json:
      load_json(filename)
    os.system('touch "%s"' % self.output().path)

if __name__ == '__main__':
  logging.basicConfig(stream=sys.stderr,
                      format=\
                      '%(created)f %(filename)s:%(lineno)s \
                      [%(funcName)s] %(message)s',
                      level=logging.DEBUG)

  luigi.run()

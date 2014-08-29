#!/usr/bin/python

''' MAUDE pipeline for downloading, joining and loading into elasticsearch
'''
from bs4 import BeautifulSoup
import csv
import glob
import logging
import luigi
import multiprocessing
import os
from os.path import dirname, join
import re
import requests
import simplejson as json
import sys
import urllib2

from openfda.maude import join_maude

# Exceed default field_size limit, need to set to sys.maxsize
csv.field_size_limit(sys.maxsize)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data/'
# A directory for holding files that track Task state
META_DIR = join(BASE_DIR, 'maude/meta')
os.system('mkdir -p %s' % META_DIR)


PARTITIONS = 32
CATEGORIES = ['mdrfoi', 'patient', 'foidev', 'foitext']
IGNORE_FILES = ['problem', 'add', 'change']


DEVICE_DOWNLOAD_PAGE = ('http://www.fda.gov/MedicalDevices/'
                       'DeviceRegulationandGuidance/PostmarketRequirements/'
                       'ReportingAdverseEvents/ucm127891.htm')

DEVICE_CLASS_DOWNLOAD = ('http://www.fda.gov/MedicalDevices/'
                         'DeviceRegulationandGuidance/Overview/'
                         'ClassifyYourDevice/ucm051668.htm')

DEVICE_CLASS_ZIP = ('http://www.accessdata.fda.gov/premarket/'
                    'ftparea/foiclass.zip')

CLEARED_DEVICE_URL = 'http://www.accessdata.fda.gov/premarket/ftparea/'
CLEARED_DEV_ZIPS = [CLEARED_DEVICE_URL + 'pmn96cur.zip',
  CLEARED_DEVICE_URL + 'pmn9195.zip',
  CLEARED_DEVICE_URL + 'pmn8690.zip',
  CLEARED_DEVICE_URL + 'pmn8185.zip',
  CLEARED_DEVICE_URL + 'pmn7680.zip']

# patient and text records are missing header rows
PATIENT_KEYS = ['mdr_report_key',
  'patient_sequence_number',
  'date_received',
  'sequence_number_treatment',
  'sequence_number_outcome']

TEXT_KEYS = ['mdr_report_key',
  'mdr_text_key',
  'text_type_code',
  'patient_sequence_number',
  'date_report',
  'text']

# Replacement text for field names. Old value on left, new value on right.
REPLACEMENTS = [('__', '_'),
  ('_no', '_number'),
  ('evaluated_by_manufactur', 'evaluated_by_manufacturer'),
  ('_state_code', '_state'),
  ('_country_code', '_country'),
  ('_preamendment', '_preamendment_flag'),
  ('_transitional', '_transitional_flag'),
  ('_link_flag_', '_link_flag'),
  ('_contact_postal', '_contact_postal_code'),
  ('contact_street_', 'contact_address_')]

def download(url, output_filename):
  os.system('mkdir -p %s' % dirname(output_filename))
  os.system("curl '%s' > '%s'" % (url, output_filename))

class DownloadDeviceEvents(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/raw/events'))

  def run(self):
    # TODO(hansnelsen): copied from the FAERS pipeline, consider refactoring
    #                   into a generalized approach
    zip_urls = []
    soup = BeautifulSoup(urllib2.urlopen(DEVICE_DOWNLOAD_PAGE).read())
    for a in soup.find_all(href=re.compile('.*.zip')):
      zip_urls.append(a['href'])
    if not zip_urls:
      logging.fatal('No MAUDE Zip Files Found At %s' % DEVICE_CLASS_DOWNLOAD)
    for zip_url in zip_urls:
      filename = zip_url.split('/')[-1]
      download(zip_url, join(self.output().path, filename))

class DownloadFoiClass(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/raw/class'))

  def run(self):
    output_filename = join(self.output().path, DEVICE_CLASS_ZIP.split('/')[-1])
    download(DEVICE_CLASS_ZIP, output_filename)

class Download_510K(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/raw/510k'))

  def run(self):
    output_dir = self.output().path

    for zip_url in CLEARED_DEV_ZIPS:
      output_filename = join(output_dir, zip_url.split('/')[-1])
      download(zip_url, output_filename)

class ExtractAndCleanDownloads(luigi.Task):
  ''' Unzip each of the download files and remove all the non-UTF8 characters.
      Unzip -p streams the data directly to iconv which then writes to disk.
  '''
  def requires(self):
    return [DownloadDeviceEvents(), DownloadFoiClass(), Download_510K()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/extracted'))

  def run(self):
    output_dir = self.output().path
    os.system('mkdir -p %(output_dir)s' % locals())
    for i in range(len(self.input())):
      input_dir = self.input()[i].path
      for zip_filename in glob.glob(input_dir + '/*.zip'):
        txt_name = zip_filename.replace('zip', 'txt')
        txt_name = txt_name.replace('raw', 'extracted')
        os.system('mkdir -p %s' % dirname(txt_name))
        cmd = 'unzip -p %(zip_filename)s' % locals()
        cmd += '| iconv -f "ISO-8859-1//TRANSLIT" '
        cmd += '-t UTF8 -c > %(txt_name)s' % locals()
        logging.info('Unzipping and converting %s', zip_filename)
        os.system(cmd)

class PartionEventData(luigi.Task):
  ''' The historic files are not balanced (patient and text are split by year)
      and master and device are huge. This process partitions all of the data
      by MDR_REPORT_KEY (which is the first column in all files) and puts the
      data in an appropriate category file. This also gives us the opportunity
      to clean out records that do not have a join key as well as lower casing
      the header names.

      For example: all of the data for a particular MDR_REPORT_KEY will be in
                   four files, let's say partition 5:
                   5.mdrfoi.txt, 5.patient.txt, 5.foidev.txt, 5.foitext.txt
                   All records in these files will have a MDR_REPORT_KEY modulo
                   PARTITIONS = 5
  '''
  def requires(self):
    return ExtractAndCleanDownloads()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/partitioned/events'))

  def run(self):
    input_dir = join(self.input().path, 'events')
    output_dir = self.output().path
    fh_dict = {}
    os.system('mkdir -p %s' % output_dir)

    # Headers need to be written to the start of each partition. The patient
    # and foitext headers are manually created. The headers for mdrfoi and
    # foidev are detected from the source and placed into the header dictionary.
    header = {}
    header['patient'] = PATIENT_KEYS
    header['foitext'] = TEXT_KEYS

    for i in range(PARTITIONS):
      for category in CATEGORIES:
        filename = str(i) + '.' + category + '.txt'
        logging.info('Creating file handles for writing %s', filename)
        # Append mode for the files
        output_handle = open(join(output_dir, filename), 'a')
        fh_dict[category + str(i)] = output_handle

    # Because we download all zips from the site, we need to ignore some of the
    # files for the partitioning process. Remove if files are excluded from
    # download.
    for filename in glob.glob(input_dir + '/*.txt'):
      skip = False
      for ignore in IGNORE_FILES:
        if ignore in filename:
          skip = True
      if skip:
        continue

      for category in CATEGORIES:
        if category in filename:
          file_category = category

      file_handle = csv.reader(open(filename, 'r'), delimiter='|')

      if file_category not in header:
        head = file_handle.next()
        # Clean up keys and lower() them. Saves lots of time downstream
        for repl_text in REPLACEMENTS:
          for i, head_val in enumerate(head):
            head[i] = head_val.lower().replace(repl_text[0], repl_text[1])
        header[file_category] = head

      partioned = []
      for row in file_handle:
        # Only work with rows that have data and the first column is a number
        if row and row[0].isdigit():
          partioned.append((int(row[0]) % PARTITIONS, row))

      partioned.sort()

      for i in range(PARTITIONS):
        partition_file_handle = fh_dict[file_category + str(i)]
        csv_writer = csv.writer(partition_file_handle, delimiter='|')
        # Write a header if file is empty
        if partition_file_handle.tell() == 0:
          csv_writer.writerow(header[file_category])
        # Write out only the current partition
        csv_writer.writerows([part[1] for part in partioned if part[0] == i])

class JoinPartitions(luigi.Task):
  def requires(self):
    return PartionEventData()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/json'))

  def run(self):
    input_dir = self.input().path
    output_dir = self.output().path

    os.system('mkdir -p %s' % output_dir)
    # TODO(hansnelsen): change to the openfda.parallel version of multiprocess
    pool = multiprocessing.Pool(processes=6)
    for i in range(PARTITIONS):
      partition_dict = {}
      output_filename = join(output_dir, str(i) + '.maude.json')
      # Get all of the files for the current partition
      for filename in glob.glob(input_dir + '/' + str(i) + '.*.txt'):
        for file_type in CATEGORIES:
          if file_type in filename:
            logging.info('Using file %s for joining', filename)
            partition_dict[file_type] = filename

      logging.info('Starting Partition %d', i)
      master_file = partition_dict['mdrfoi']
      patient_file = partition_dict['patient']
      device_file = partition_dict['foidev']
      text_file = partition_dict['foitext']
      pool.apply_async(join_maude.join_maude, (master_file,
                                               patient_file,
                                               device_file,
                                               text_file,
                                               output_filename))

    pool.close()
    pool.join()


class ResetElasticSearch(luigi.Task):
  def requires(self):
    return JoinPartitions()

  def output(self):
    return luigi.LocalTarget(join(META_DIR, 'ResetElasticSearch.done'))

  def run(self):
    cmd = join(RUN_DIR, 'maude/clear_and_load_mapping.sh')
    logging.info('Running Shell Script:' + cmd)
    os.system(cmd)
    os.system('touch "%s"' % self.output().path)

def load_json(input_file):
  event_json = open(input_file, 'r')
  json_batch = []
  def _post_batch():
    response = \
    requests.post('http://localhost:9200/deviceevent/maude/_bulk',
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
    return [ResetElasticSearch(), JoinPartitions()]

  def output(self):
    return luigi.LocalTarget(join(META_DIR, 'LoadJSON.done'))

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

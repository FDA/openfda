#!/usr/bin/python

''' MAUDE pipeline for downloading, joining and loading into elasticsearch
'''
from bs4 import BeautifulSoup
import collections
import csv
import glob
import logging
import multiprocessing
import os
from os.path import dirname, join
import re
import sys
import urllib2

import luigi

from openfda import common, config, parallel, index_util
from openfda import download_util
from openfda.maude import join_maude
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
                                                   DeviceAnnotateMapper)

# Exceed default field_size limit, need to set to sys.maxsize
csv.field_size_limit(sys.maxsize)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data/'
# A directory for holding files that track Task state
META_DIR = join(BASE_DIR, 'maude/meta')
common.shell_cmd('mkdir -p %s', META_DIR)

PARTITIONS = 32
CATEGORIES = ['mdrfoi', 'patient', 'foidev', 'foitext']
IGNORE_FILES = ['problem', 'add', 'change']


DEVICE_DOWNLOAD_PAGE = ('http://www.fda.gov/MedicalDevices/'
                        'DeviceRegulationandGuidance/PostmarketRequirements/'
                        'ReportingAdverseEvents/ucm127891.htm')

DEVICE_CLASS_DOWNLOAD = ('http://www.fda.gov/MedicalDevices/'
                         'DeviceRegulationandGuidance/Overview/'
                         'ClassifyYourDevice/ucm051668.htm')

# patient and text records are missing header rows
PATIENT_KEYS = [
  'mdr_report_key',
  'patient_sequence_number',
  'date_received',
  'sequence_number_treatment',
  'sequence_number_outcome']

TEXT_KEYS = [
  'mdr_report_key',
  'mdr_text_key',
  'text_type_code',
  'patient_sequence_number',
  'date_report',
  'text']

DEVICE_KEYS = [
 'mdr_report_key',
 'device_event_key',
 'implant_flag',
 'date_removed_flag',
 'device_sequence_number',
 'date_received',
 'brand_name',
 'generic_name',
 'manufacturer_d_name',
 'manufacturer_d_address_1',
 'manufacturer_d_address_2',
 'manufacturer_d_city',
 'manufacturer_d_state',
 'manufacturer_d_zip_code',
 'manufacturer_d_zip_code_ext',
 'manufacturer_d_country',
 'manufacturer_d_postal_code',
 'expiration_date_of_device',
 'model_number',
 'catalog_number',
 'lot_number',
 'other_id_number',
 'device_operator',
 'device_availability',
 'date_returned_to_manufacturer',
 'device_report_product_code',
 'device_age_text',
 'device_evaluated_by_manufacturer',
 'baseline_brand_name',
 'baseline_generic_name',
 'baseline_model_number',
 'baseline_catalog_number',
 'baseline_other_id_number',
 'baseline_device_family',
 'baseline_shelf_life_contained',
 'baseline_shelf_life_in_months',
 'baseline_pma_flag',
 'baseline_pma_number',
 'baseline_510_k__flag',
 'baseline_510_k__number',
 'baseline_preamendment_flag',
 'baseline_transitional_flag',
 'baseline_510_k__exempt_flag',
 'baseline_date_first_marketed',
 'baseline_date_ceased_marketing'
]

MDR_KEYS = [
  'mdr_report_key',
  'event_key',
  'report_number',
  'report_source_code',
  'manufacturer_link_flag',
  'number_devices_in_event',
  'number_patients_in_event',
  'date_received',
  'adverse_event_flag',
  'product_problem_flag',
  'date_report',
  'date_of_event',
  'reprocessed_and_reused_flag',
  'reporter_occupation_code',
  'health_professional',
  'initial_report_to_fda',
  'distributor_name',
  'distributor_address_1',
  'distributor_address_2',
  'distributor_city',
  'distributor_state',
  'distributor_zip_code',
  'distributor_zip_code_ext',
  'date_facility_aware',
  'type_of_report',
  'report_date',
  'report_to_fda',
  'date_report_to_fda',
  'event_location',
  'report_to_manufacturer',
  'date_report_to_manufacturer',
  'date_manufacturer_received',
  'manufacturer_name',
  'manufacturer_address_1',
  'manufacturer_address_2',
  'manufacturer_city',
  'manufacturer_state',
  'manufacturer_zip_code',
  'manufacturer_zip_code_ext',
  'manufacturer_country',
  'manufacturer_postal_code',
  'manufacturer_contact_t_name',
  'manufacturer_contact_f_name',
  'manufacturer_contact_l_name',
  'manufacturer_contact_address_1',
  'manufacturer_contact_address_2',
  'manufacturer_contact_city',
  'manufacturer_contact_state',
  'manufacturer_contact_zip_code',
  'manufacturer_contact_zip_ext',
  'manufacturer_contact_country',
  'manufacturer_contact_postal_code',
  'manufacturer_contact_area_code',
  'manufacturer_contact_exchange',
  'manufacturer_contact_phone_number',
  'manufacturer_contact_extension',
  'manufacturer_contact_pcountry',
  'manufacturer_contact_pcity',
  'manufacturer_contact_plocal',
  'manufacturer_g1_name',
  'manufacturer_g1_address_1',
  'manufacturer_g1_address_2',
  'manufacturer_g1_city',
  'manufacturer_g1_state',
  'manufacturer_g1_zip_code',
  'manufacturer_g1_zip_code_ext',
  'manufacturer_g1_country',
  'manufacturer_g1_postal_code',
  'source_type',
  'device_date_of_manufacturer',
  'single_use_flag',
  'remedial_action',
  'previous_use_code',
  'removal_correction_number',
  'event_type'
]


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
      common.download(zip_url, join(self.output().path, filename))


class ExtractAndCleanDownloadsMaude(luigi.Task):
  ''' Unzip each of the download files and remove all the non-UTF8 characters.
      Unzip -p streams the data directly to iconv which then writes to disk.
  '''
  def requires(self):
    return [DownloadDeviceEvents()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/extracted'))

  def run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    for i in range(len(self.input())):
      input_dir = self.input()[i].path
      download_util.extract_and_clean(input_dir,
                                      'ISO-8859-1//TRANSLIT',
                                      'UTF-8',
                                      'txt')


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
    return ExtractAndCleanDownloadsMaude()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/partitioned/events'))

  def run(self):
    input_dir = join(self.input().path, 'events')
    output_dir = self.output().path
    fh_dict = {}
    common.shell_cmd('mkdir -p %s', output_dir)

    # Headers need to be written to the start of each partition. The patient
    # and foitext headers are manually created. The headers for mdrfoi and
    # foidev are detected from the source and placed into the header dictionary.
    header = {}
    header['patient'] = PATIENT_KEYS
    header['foitext'] = TEXT_KEYS
    header['mdrfoi'] = MDR_KEYS
    header['foidev'] = DEVICE_KEYS

    for i in range(PARTITIONS):
      for category in CATEGORIES:
        filename = str(i) + '.' + category + '.txt'
        filename = join(output_dir, filename)
        logging.info('Creating file handles for writing %s', filename)
        output_handle = open(filename, 'w')
        csv_writer = csv.writer(output_handle, delimiter='|')
        csv_writer.writerow(header[category])
        fh_dict[category + str(i)] = output_handle

    # Because we download all zips from the site, we need to ignore some of the
    # files for the partitioning process. Remove if files are excluded from
    # download.
    for filename in glob.glob(input_dir + '/*.txt'):
      logging.info('Processing: %s', filename)
      skip = False
      for ignore in IGNORE_FILES:
        if ignore in filename:
          skip = True

      if skip:
        logging.info('Skipping: %s', filename)
        continue

      for category in CATEGORIES:
        if category in filename:
          file_category = category

      # MAUDE files do not escape quote characters, we just hope that no
      # pipe characters occur in records...
      file_handle = csv.reader(open(filename, 'r'),
                               quoting=csv.QUOTE_NONE,
                               delimiter='|')
      partioned = collections.defaultdict(list)
      for i, row in enumerate(file_handle):
        # skip header rows
        if (i == 0) and ('MDR_REPORT_KEY' in row): continue

        # Only work with rows that have data and the first column is a number
        if row and row[0].isdigit():
          partioned[int(row[0]) % PARTITIONS].append(row)
        else:
          logging.warn('Skipping row: %s', row)

      for partnum, rows in partioned.iteritems():
        output_handle = fh_dict[file_category + str(partnum)]
        csv_writer = csv.writer(output_handle, delimiter='|')
        logging.info('Writing: %s %s %s %s',
                     partnum,
                     file_category + str(partnum),
                     output_handle,
                     len(rows))
        csv_writer.writerows(rows)


class JoinPartitions(luigi.Task):
  def requires(self):
    return PartionEventData()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/json'))

  def run(self):
    input_dir = self.input().path
    output_dir = self.output().path

    common.shell_cmd('mkdir -p %s', output_dir)
    # TODO(hansnelsen): change to the openfda.parallel version of multiprocess
    pool = multiprocessing.Pool(processes=3)
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


class MaudeAnnotationMapper(DeviceAnnotateMapper):
  def filter(self, data, lookup=None):
    product_code = data['device_report_product_code']
    harmonized = self.harmonized_db.get(product_code, None)
    if harmonized:
      # Taking a very conservative approach to annotation to start. Only
      # including the classification data and a matching registration.
      if '510k' in harmonized:
        del harmonized['510k']
      if 'device_pma' in harmonized:
        del harmonized['device_pma']
      registration = list(harmonized['registration'])
      new_reg = [d for d in registration if d['registration_number'] == lookup]
      harmonized['registration'] = new_reg
      return harmonized
    return None

  def harmonize(self, data):
    result = dict(data)
    report_number = data['report_number']

    if not report_number:
      return result

    registration_number = report_number.split('-')[0]
    if not registration_number:
      return result

    devices = []
    for row in result.get('device', []):
      d = dict(row)
      harmonized = self.filter(row, lookup=registration_number)
      if harmonized:
        d['openfda'] = self.flatten(harmonized)
      else:
        d['openfda'] = {}
      devices.append(d)
    result['device'] = devices

    return result


class AnnotateReport(luigi.Task):
  def requires(self):
    return [Harmonized2OpenFDA(), JoinPartitions()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude', 'annotate.db'))

  def run(self):
    harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()
    json_glob = glob.glob(self.input()[1].path + '/*.json')
    parallel.mapreduce(
      parallel.Collection.from_glob(json_glob, parallel.JSONLineInput()),
      mapper=MaudeAnnotationMapper(harmonized_db=harmonized_db),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=10,
      map_workers=5)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'deviceevent'
  type_name = 'maude'
  mapping_file = 'schemas/maude_mapping.json'
  data_source = AnnotateReport()
  optimize_index = True

if __name__ == '__main__':
  luigi.run()

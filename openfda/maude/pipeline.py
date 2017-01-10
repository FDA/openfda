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
from os.path import basename, dirname, join
import re
import sys
import urllib2

import arrow
import luigi

from openfda import common, config, parallel, index_util
from openfda import download_util
from openfda.maude import join_maude
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
                                                   DeviceAnnotateMapper)
from openfda.tasks import AlwaysRunTask, DependencyTriggeredTask

# Exceed default field_size limit, need to set to sys.maxsize
csv.field_size_limit(sys.maxsize)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data/'

# Use to ensure a standard naming of level db outputs is achieved across tasks.
DATE_FMT = 'YYYY-MM-DD'
CATEGORIES = ['mdrfoi', 'patient', 'foidev', 'foitext']
IGNORE_FILES = ['problem', 'add', 'change']

DEVICE_DOWNLOAD_PAGE = ('http://www.fda.gov/MedicalDevices/'
                        'DeviceRegulationandGuidance/PostmarketRequirements/'
                        'ReportingAdverseEvents/ucm127891.htm')

DEVICE_CLASS_DOWNLOAD = ('http://www.fda.gov/MedicalDevices/'
                         'DeviceRegulationandGuidance/Overview/'
                         'ClassifyYourDevice/ucm051668.htm')

enum_file = join(RUN_DIR, 'maude/data/enums.csv')
enum_csv = csv.DictReader(open(enum_file))

ENUM = collections.defaultdict(lambda: collections.defaultdict(dict))

for row in enum_csv:
  key = row['key_name']
  code = row['code']
  desc = row['code_desc']

  ENUM[key][code] = desc

# patient and text records are missing header rows
FILE_HEADERS = {
  'patient': [
    'mdr_report_key',
    'patient_sequence_number',
    'date_received',
    'sequence_number_treatment',
    'sequence_number_outcome'],
  'foitext': [
    'mdr_report_key',
    'mdr_text_key',
    'text_type_code',
    'patient_sequence_number',
    'date_report',
    'text'],
  'foidev': [
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
  ],
  'mdrfoi': [
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
}

DATE_KEYS = [
  'date_received',
  'baseline_date_first_marketed',
  'date_returned_to_manufacturer',
  'date_report_to_fda',
  'baseline_date_ceased_marketing',
  'date_report_to_manufacturer',
  'expiration_date_of_device',
  'device_date_of_manufacturer',
  'date_facility_aware',
  'report_date',
  'date_report',
  'date_manufacturer_received',
  'date_of_event'
]

# split these keys in an array on ';'
SPLIT_KEYS = ['sequence_number_treatment', 'sequence_number_outcome']
# multiple submits are separated by ',' need to split these keys on ','
MULTI_SUBMIT = ['source_type', 'remedial_action', 'type_of_report']

# Swap map for current mdrfoi array (from csv file) to the origianl.
# It is not clear that the file changes is intentional, so, as a temporary
# work around, we will shuffle each row to resemble the original order.
# The left represents the list address of the new format and the right
# represents the address of the original that we want to keep until further
# notice.
# TODO(hansnelsen): once the format stabalizes, we should remove this shim
#                   approach and change everything: pipeline, mapping, schema
#                   and documentation.
MDRFOI_SWAP = {
 0: 0,    # MDR_REPORT_KEY,
 1: 1,    # EVENT_KEY,
 2: 2,    # REPORT_NUMBER,
 3: 3,    # REPORT_SOURCE_CODE,
 4: 4,    # MANUFACTURER_LINK_FLAG_,
 5: 5,    # NUMBER_DEVICES_IN_EVENT,
 6: 6,    # NUMBER_PATIENTS_IN_EVENT,
 7: 7,    # DATE_RECEIVED,
 8: 8,    # ADVERSE_EVENT_FLAG,
 9: 9,    # PRODUCT_PROBLEM_FLAG,
 10: 10,  # DATE_REPORT,
 11: 11,  # DATE_OF_EVENT,
 12: 12,  # REPROCESSED_AND_REUSED_FLAG,
 13: 13,  # REPORTER_OCCUPATION_CODE,
 14: 14,  # HEALTH_PROFESSIONAL,
 15: 15,  # INITIAL_REPORT_TO_FDA,
 16: 23,  # DATE_FACILITY_AWARE,
 17: 25,  # REPORT_DATE,
 18: 26,  # REPORT_TO_FDA,
 19: 27,  # DATE_REPORT_TO_FDA,
 20: 28,  # EVENT_LOCATION,
 21: 30,  # DATE_REPORT_TO_MANUFACTURER,
 22: 41,  # MANUFACTURER_CONTACT_T_NAME,
 23: 42,  # MANUFACTURER_CONTACT_F_NAME,
 24: 43,  # MANUFACTURER_CONTACT_L_NAME,
 25: 44,  # MANUFACTURER_CONTACT_STREET_1,
 26: 45,  # MANUFACTURER_CONTACT_STREET_2,
 27: 46,  # MANUFACTURER_CONTACT_CITY,
 28: 47,  # MANUFACTURER_CONTACT_STATE,
 29: 48,  # MANUFACTURER_CONTACT_ZIP_CODE,
 30: 49,  # MANUFACTURER_CONTACT_ZIP_EXT,
 31: 50,  # MANUFACTURER_CONTACT_COUNTRY,
 32: 51,  # MANUFACTURER_CONTACT_POSTAL,
 33: 52,  # MANUFACTURER_CONTACT_AREA_CODE,
 34: 53,  # MANUFACTURER_CONTACT_EXCHANGE,
 35: 54,  # MANUFACTURER_CONTACT_PHONE_NO,
 36: 55,  # MANUFACTURER_CONTACT_EXTENSION,
 37: 56,  # MANUFACTURER_CONTACT_PCOUNTRY,
 38: 57,  # MANUFACTURER_CONTACT_PCITY,
 39: 58,  # MANUFACTURER_CONTACT_PLOCAL,
 40: 59,  # MANUFACTURER_G1_NAME,
 41: 60,  # MANUFACTURER_G1_STREET_1,
 42: 61,  # MANUFACTURER_G1_STREET_2,
 43: 62,  # MANUFACTURER_G1_CITY,
 44: 63,  # MANUFACTURER_G1_STATE_CODE,
 45: 64,  # MANUFACTURER_G1_ZIP_CODE,
 46: 65,  # MANUFACTURER_G1_ZIP_CODE_EXT,
 47: 66,  # MANUFACTURER_G1_COUNTRY_CODE,
 48: 67,  # MANUFACTURER_G1_POSTAL_CODE,
 49: 31,  # DATE_MANUFACTURER_RECEIVED,
 50: 69,  # DEVICE_DATE_OF_MANUFACTURE,
 51: 70,  # SINGLE_USE_FLAG,
 52: 71,  # REMEDIAL_ACTION,
 53: 72,  # PREVIOUS_USE_CODE,
 54: 73,  # REMOVAL_CORRECTION_NUMBER,
 55: 74,  # EVENT_TYPE,
 56: 16,  # DISTRIBUTOR_NAME,
 57: 17,  # DISTRIBUTOR_ADDRESS_1,
 58: 18,  # DISTRIBUTOR_ADDRESS_2,
 59: 19,  # DISTRIBUTOR_CITY,
 60: 20,  # DISTRIBUTOR_STATE_CODE,
 61: 21,  # DISTRIBUTOR_ZIP_CODE,
 62: 22,  # DISTRIBUTOR_ZIP_CODE_EXT,
 63: 29,  # REPORT_TO_MANUFACTURER,
 64: 32,  # MANUFACTURER_NAME,
 65: 33,  # MANUFACTURER_ADDRESS_1,
 66: 34,  # MANUFACTURER_ADDRESS_2,
 67: 35,  # MANUFACTURER_CITY,
 68: 36,  # MANUFACTURER_STATE_CODE,
 69: 37,  # MANUFACTURER_ZIP_CODE,
 70: 38,  # MANUFACTURER_ZIP_CODE_EXT,
 71: 39,  # MANUFACTURER_COUNTRY_CODE,
 72: 40,  # MANUFACTURER_POSTAL_CODE,
 73: 24,  # TYPE_OF_REPORT,
 74: 68   # SOURCE_TYPE
          # DATE_ADDED (not used)
          # DATE_CHANGED (not used)
}


def _fix_date(input_date):
  ''' Converts input dates for known formats to a standard format that is
      Elasticsearch friendly.
      Returns the input_date if it is not a known format
  '''
  supported_formats = [
    'DD-MMM-YY',
    'YYYY/MM/DD HH:mm:ss.SSS',
    'MM/DD/YYYY',
    'YYYYMMDD'
  ]

  # arrow needs 3 char months to be sentence case: e.g. Dec not DEC
  formated_date = input_date.title()
  try:
    date = arrow.get(formated_date, supported_formats).format('YYYYMMDD')
    return date.format('YYYYMMDD')
  except:
    if input_date:
      logging.info('%s with input %s', input_date, formated_date)

  return None


def _exact_split(key, value, sep):
  ''' Helper function that splits a string into an array, swaps the encoded
      values for more descriptive ones and then generates an _exact field
  '''
  value = value.split(sep)

  if key in ENUM:
    value = [ENUM[key].get(val, val) for val in value]

  new_key, new_value = key + '_exact', value

  return [(key, value), (new_key, new_value)]


class DownloadDeviceEvents(AlwaysRunTask):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/raw/events'))

  def _run(self):
    zip_urls = []
    soup = BeautifulSoup(urllib2.urlopen(DEVICE_DOWNLOAD_PAGE).read())
    for a in soup.find_all(href=re.compile('.*.zip')):
      zip_urls.append(a['href'])
    if not zip_urls:
      logging.fatal('No MAUDE Zip Files Found At %s' % DEVICE_CLASS_DOWNLOAD)
    for zip_url in zip_urls:
      filename = zip_url.split('/')[-1]
      common.download(zip_url, join(self.output().path, filename))


class ExtractAndCleanDownloadsMaude(AlwaysRunTask):
  ''' Unzip each of the download files and remove all the non-UTF8 characters.
      Unzip -p streams the data directly to iconv which then writes to disk.
  '''
  def requires(self):
    return [DownloadDeviceEvents()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/extracted'))

  def _run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    for i in range(len(self.input())):
      input_dir = self.input()[i].path
      download_util.extract_and_clean(input_dir,
                                      'ISO-8859-1//TRANSLIT',
                                      'UTF-8',
                                      'txt')


class CSV2JSONMapper(parallel.Mapper):
  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename
    return parallel.Mapper.map_shard(self, map_input, map_output)

  @staticmethod
  def cleaner(k, v):
    if k is None:
      return None

    if k in DATE_KEYS:
      new_date = _fix_date(v)
      return (k, new_date) if new_date else None

    if k in SPLIT_KEYS:
      return _exact_split(k, v, ';')

    if k in MULTI_SUBMIT:
      return _exact_split(k, v, ',')

    if k in ENUM:
      if v in ENUM[k]:
        if isinstance(v, list):
          v = [ENUM[k].get(val, val) for val in v]
        else:
          v = ENUM[k][v]

    return (k, v)

  def shuffle_mdrfoi_value(self, value):
    ''' Function for re-shuffling mdrfoi data, since it is coming in out of
        order. We only need the first 75 values from the list, the remaining
        are not needed.

        TODO(hansnelsen): Remove once the schema for mdrfoi records is stable.
    '''
    projected_size = len(MDRFOI_SWAP.keys())
    # If value array not long enough, let downstream validators deal with it.
    if len(value) < projected_size:
      return value

    result = [''] * projected_size
    for source, target in MDRFOI_SWAP.items():
      result[target] = value[source]

    return result

  def map(self, key, value, output):
    if len(value) < 1:
      return

    mdr_key = value[0]

    # Some of the files have headers, we will apply our own, so row that starts
    # with this value is safe to skip
    if 'MDR_REPORT_KEY' in mdr_key:
      return

    file_type = [s for s in CATEGORIES if s in self.filename][0]
    # logging.info('file type: %s, file: %s', file_type, self.filename)

    # TODO(hansnelsen): remove once file format is stable
    if file_type == 'mdrfoi':
      value = self.shuffle_mdrfoi_value(value)

    # We send all data anomalies to a reducer for each file type.
    # These non-conforming data are written to a reject file for review.
    # This file type as variable lengths over time, so it needs its owne check
    if file_type == 'foidev':
      if len(value) not in [28, 45]:
        logging.info('Does not conform to foidev structure. Skipping: %s, %s',
          mdr_key, '#' * 200)
        output.add(file_type, '%s: missing fields' % mdr_key + ':' +  '|'.join(value))
        return

    elif len(value) != len(FILE_HEADERS[file_type]):
      logging.info('Does not conform to %s structure. Skipping: %s, %s',
        file_type, mdr_key, '#' * 200)
      output.add(file_type, '%s: missing fields' % mdr_key + ':' +  '|'.join(value))
      return

    if not isinstance(int(mdr_key), int):
      logging.info('%s is not a number', mdr_key)
      output.add(file_type, '%s: NaN' % mdr_key + ':' +  '|'.join(value))
      return

    # If it makes it this far, it is a good record
    new_value = dict(zip(FILE_HEADERS[file_type], value))
    new_value = common.transform_dict(new_value, self.cleaner)

    output.add(mdr_key, (file_type, new_value))


class CSV2JSONJoinReducer(parallel.Reducer):
  # File type to nested key name mapping
  join_map = {
    'foidev': 'device',
    'foitext': 'mdr_text',
    'patient': 'patient'
  }

  def _join(self, key, values):
    val = parallel.pivot_values(values)

    final = {
      'device': [],
      'mdr_text': [],
      'patient': []
    }

    if not val.get('mdrfoi', []):
      logging.info('MDR REPORT %s: Missing mdrfoi record, Skipping join', key)
      return

    for i, main_report in enumerate(val.get('mdrfoi', [])):
      final.update(main_report)

    try:
      int(final.get('mdr_report_key', None))
    except TypeError:
      logging.info('%s', '*' * 2400)
      return

    for source_file, target_key in self.join_map.items():
      for row in val.get(source_file, []):
        row.pop('mdr_report_key', 0) # No need to keep join key on nested data
        final[target_key].append(row)

    return final

  def reduce(self, key, values, output):
    # Write out the rejected records
    if key in CATEGORIES:
      with open(join(BASE_DIR, 'maude', key + '-rejects.txt'), 'a') as rejects:
        for row in values:
          rejects.write(row + '\n')
    else:
      output.put(key, self._join(key, values))


class CSV2JSON(luigi.Task):
  ''' Task that loads different CSV files, depending upon what the value of
      `loader_task`.
      `init`: process all files except those listed in IGNORE_FILES.
       `add`: process all files with the word `add` in the filename.
   `changes`: process all files with the word `change` in the filename.
  '''
  run_date = luigi.Parameter()
  loader_task = luigi.Parameter()

  def requires(self):
    return ExtractAndCleanDownloadsMaude()

  def output(self):
    file_name = '-'.join([self.loader_task, self.run_date, 'json.db'])

    return luigi.LocalTarget(join(BASE_DIR, 'maude', file_name))

  def run(self):
    files = glob.glob(self.input().path + '/*/*.txt')

    if self.loader_task == 'init':
      input_files = [f for f in files if not any(i for i in IGNORE_FILES if i in f)]
    else:
      input_files = [f for f in files if self.loader_task in f]

    parallel.mapreduce(
      parallel.Collection.from_glob(
        input_files, parallel.CSVLineInput(quoting=csv.QUOTE_NONE, delimiter='|')),
      mapper=CSV2JSONMapper(),
      reducer=CSV2JSONJoinReducer(),
      output_prefix=self.output().path)


class MergeUpdatesMapper(parallel.Mapper):
  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename
    self.table =  basename(dirname(dirname(self.filename)))
    return parallel.Mapper.map_shard(self, map_input, map_output)

  def map(self, key, value, output):
    source_type = None
    if 'init' in self.filename:
      source_type = 'init'
    if 'add' in self.filename:
      source_type = 'add'
    if 'change' in self.filename:
      source_type = 'change'

    assert source_type, 'Unable to continue for source type %s' % self.filename

    output.add(key, (source_type, value))


class MergeUpdatesReducer(parallel.Reducer):
  ''' This step resolves conflicting data for the same key, which is the result
      of merging the init, add and change pipeline outputs.

      Reducer that takes in an array of tuples:
        [(source, value), (source, value), ...]

      One and only one is selected by the reducer.

  '''
  def reduce(self, key, values, output):
    def _safe_get(value):
      if isinstance(value, list):
        if len(value) > 0:
          value = value[0]
        else:
          return None

      return value

    # If there is only one value, then we use it. If there are many then
    # then choose the right one in the order: changed, added, or existing.
    # Remember, we are merging the additions and updates with last weeks run,
    # which is where the existing come from. All of this is due to the fact
    # that a record can exist in all three places, which is not ideal but is
    # reality.
    if len(values) == 1:
      value = _safe_get(values[0][1])
      if value:
        output.put(key, value)
    elif len(values) > 1:
      pivoted = parallel.pivot_values(values)

      change = _safe_get(pivoted.get('change', []))
      add = _safe_get(pivoted.get('add', []))
      init = _safe_get(pivoted.get('init', []))

      if change:
        output.put(key, change)
      elif add:
        output.put(key, add)
      else:
        output.put(key, init)


class MergeUpdates(luigi.Task):
  ''' Task that takes all three loader_task (init, add, and change), streams
      them into a reducer and picks one to write to the weekly output.

      Please note that the `init` process attempts to use last weeks init file
      as input. If it does not exist, it will make it first.
  '''
  run_date = luigi.Parameter()

  def requires(self):
    previous_run_date = arrow.get(self.run_date).replace(weeks=-1).format(DATE_FMT)

    return [
      CSV2JSON(loader_task='init', run_date=previous_run_date),
      CSV2JSON(loader_task='add', run_date=self.run_date),
      CSV2JSON(loader_task='change', run_date=self.run_date)
    ]

  def output(self):
    file_name = '-'.join(['init', self.run_date, 'json.db'])

    return luigi.LocalTarget(join(BASE_DIR, 'maude', file_name))

  def run(self):
    db_list = [s.path for s in self.input()]
    parallel.mapreduce(
      parallel.Collection.from_sharded_list(db_list),
      mapper=MergeUpdatesMapper(),
      reducer=MergeUpdatesReducer(),
      output_prefix=self.output().path)


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


class AnnotateReport(DependencyTriggeredTask):
  run_date = luigi.Parameter()

  def requires(self):
    return [Harmonized2OpenFDA(), MergeUpdates(run_date=self.run_date)]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude', 'annotate.db'))

  def run(self):
    harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()
    db_list = [s.path for s in self.input()[1:]]
    parallel.mapreduce(
      parallel.Collection.from_sharded_list(db_list),
      mapper=MaudeAnnotationMapper(harmonized_db=harmonized_db),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path)


class LoadJSONByRunDate(index_util.LoadJSONBase):
  run_date = luigi.Parameter()
  index_name = 'deviceevent'
  type_name = 'maude'
  mapping_file = 'schemas/maude_mapping.json'
  optimize_index = False
  docid_key='mdr_report_key'
  use_checksum = True

  def _data(self):
    return AnnotateReport(run_date=self.run_date)


class LoadJSON(luigi.WrapperTask):
  run_date = arrow.utcnow().ceil('weeks').format(DATE_FMT)

  def requires(self):
    return LoadJSONByRunDate(run_date=self.run_date)

if __name__ == '__main__':
  luigi.run()

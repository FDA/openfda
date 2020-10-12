#!/usr/bin/python

''' MAUDE pipeline for downloading, joining and loading into elasticsearch
'''
import collections
import csv
import glob
import logging
import multiprocessing
import os
import re
import sys
from os.path import basename, dirname, join
from urllib.request import urlopen

import arrow
import luigi
from bs4 import BeautifulSoup

from openfda import common, parallel, index_util
from openfda import download_util
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
                                                   DeviceAnnotateMapper)
from openfda.tasks import AlwaysRunTask, DependencyTriggeredTask

# Exceed default field_size limit, need to set to sys.maxsize
csv.field_size_limit(sys.maxsize)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data/'

# See https://github.com/FDA/openfda/issues/27
# Files for resolving device problem codes
DEVICE_PROBLEM_CODES_FILE = join(BASE_DIR, 'maude/extracted/events/deviceproblemcodes.txt')
DEVICE_PROBLEMS_FILE = join(BASE_DIR, 'maude/extracted/events/foidevproblem.txt')

# Use to ensure a standard naming of level db outputs is achieved across tasks.
DATE_FMT = 'YYYY-MM-DD'
CATEGORIES = ['mdrfoi', 'patient', 'foidev', 'foitext', 'device']
IGNORE_FILES = ['problem', 'add', 'change']

DEVICE_DOWNLOAD_PAGE = ('https://www.fda.gov/medical-devices/'
                        'mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/'
                        'manufacturer-and-user-facility-device-experience-database-maude')

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
  'device': [
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
    'device_operator',
    'expiration_date_of_device',
    'model_number',
    'catalog_number',
    'lot_number',
    'other_id_number',
    'device_availability',
    'date_returned_to_manufacturer',
    'device_report_product_code',
    'device_age_text',
    'device_evaluated_by_manufacturer',
    'combination_product_flag'
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
    'date_facility_aware',
    'report_date',
    'report_to_fda',
    'date_report_to_fda',
    'event_location',
    'date_report_to_manufacturer',
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
    'date_manufacturer_received',
    'device_date_of_manufacturer',
    'single_use_flag',
    'remedial_action',
    'previous_use_code',
    'removal_correction_number',
    'event_type',
    'distributor_name',
    'distributor_address_1',
    'distributor_address_2',
    'distributor_city',
    'distributor_state',
    'distributor_zip_code',
    'distributor_zip_code_ext',
    'report_to_manufacturer',
    'manufacturer_name',
    'manufacturer_address_1',
    'manufacturer_address_2',
    'manufacturer_city',
    'manufacturer_state',
    'manufacturer_zip_code',
    'manufacturer_zip_code_ext',
    'manufacturer_country',
    'manufacturer_postal_code',
    'type_of_report',
    'source_type',
    'date_added',
    'date_changed',
    'reporter_country_code',
    'pma_pmn_number',
    'exemption_number',
    'summary_report_flag'
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
  'date_of_event',
  'date_added',
  'date_changed'
]

# split these keys in an array on ';'
SPLIT_KEYS = ['sequence_number_treatment', 'sequence_number_outcome']
# multiple submits are separated by ',' need to split these keys on ','
MULTI_SUBMIT = ['source_type', 'remedial_action', 'type_of_report']
# These keys have malformed integers in them: left-padded with a space and decimal point added.
MALFORMED_KEYS = ['mdr_report_key', 'device_event_key', 'device_sequence_number']

def _fix_date(input_date):
  ''' Converts input dates for known formats to a standard format that is
      Elasticsearch friendly.
      Returns the input_date if it is not a known format
  '''
  supported_formats = [
    'DD-MMM-YY',
    'YYYY/MM/DD HH:mm:ss.SSS',
    'MM/DD/YYYY',
    'YYYYMMDD',
    'YYYY/MM/DD'
  ]

  # arrow needs 3 char months to be sentence case: e.g. Dec not DEC
  formated_date = input_date.title()
  try:
    date = arrow.get(formated_date, supported_formats).format('YYYYMMDD')
    return date.format('YYYYMMDD')
  except:
    if input_date:
      logging.info('unparseable date: %s with input %s', input_date, formated_date)

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
    soup = BeautifulSoup(urlopen(DEVICE_DOWNLOAD_PAGE).read())
    for a in soup.find_all(href=re.compile('.*.zip')):
      zip_urls.append(a['href'])
    if not zip_urls:
      logging.fatal('No MAUDE Zip Files Found At %s' % DEVICE_DOWNLOAD_PAGE)
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

# This task no longer works properly. Needs refactoring
class PreprocessFilesToFixIssues(AlwaysRunTask):
  ''' The pipe-separated MAUDE files come with issues: no escaping of special characters.
      Many foitext files contain pipe characters that are part of field values and thus are
      breaking the layout due to not being escaped properly. In other cases new line characters appear
      unescaped and break single lines into multi-line chunks. This task attempts to deal with these
      issues via regular expression search & replace.
  '''
  def requires(self):
    return ExtractAndCleanDownloadsMaude()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'maude/extracted'))

  def _run(self):
    for filename in glob.glob(self.input().path + '/*/*foi*.txt') + glob.glob(self.input().path + '/*/device*.txt'):
      logging.info('Pre-processing %s', filename)

      filtered = filename + '.filtered'
      out = open(filtered, 'w')
      line_num = 0
      bad_lines = 0
      with open(filename, 'rU') as fp:
        for line in fp:
          line = line.strip()
          if line_num < 1:
            # First line is usually the header
            out.write(line)
          else:
            if len(line.strip()) > 0:
              if re.search(r'^\d{2,}(\.\d)?\|', line):
                # Properly formatted line. Append it and move on.
                out.write('\n'+line)
              else:
                # Bad line, most likely due to an unescaped carriage return. Tuck it onto the previous line
                out.write(' ' + line)
                bad_lines += 1

          line_num += 1

      logging.info('Issues found & fixed: %s', bad_lines)
      out.close()
      os.remove(filename)
      os.rename(filtered, filename)

class CSV2JSONMapper(parallel.Mapper):
  def __init__(self, problem_codes_reference, device_problem_codes):
    parallel.Mapper.__init__(self)
    self.problem_codes_reference = problem_codes_reference
    self.device_problem_codes = device_problem_codes

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

    # The DEVICE files have mdr_report_key padded with a space and in decimal format with a ".0" at the end.
    if k in MALFORMED_KEYS:
      v = v.strip().replace('.0', '')

    if k in ENUM:
      if v in ENUM[k]:
        if isinstance(v, list):
          v = [ENUM[k].get(val, val) for val in v]
        else:
          v = ENUM[k][v]

    return (k, v)

  # We are seeing a large number of foitext rows not following the column definition and thus
  # getting rejected by the reducer. The root cause is the fact that the last column (FOI_TEXT) contains
  # text that includes one or more "pipe" | characters that have not been properly escaped
  # in the file and thus are throwing column count off.
  # We are dealing with that by merely concatenating the extra text columns into a single
  # string and stripping out the bogus columns at the end.
  def handle_oversized_foitext(self, value):
    no_columns = len(FILE_HEADERS['foitext'])
    combined_text = '|'.join([t for t in value[no_columns - 1:]])
    value[no_columns - 1] = combined_text[:-1] if combined_text.endswith("|") else combined_text
    return value[0:no_columns]

  def map(self, key, value, output):
    if len(value) < 1:
      return

    mdr_key = self.cleaner('mdr_report_key', value[0])[1]

    # Some of the files have headers, we will apply our own, so row that starts
    # with this value is safe to skip
    if 'MDR_REPORT_KEY' in mdr_key:
      return

    file_type = [s for s in CATEGORIES if s in self.filename][0]

    if file_type == 'foitext' and len(value) > len(FILE_HEADERS[file_type]):
      value = self.handle_oversized_foitext(value)

    # We send all data anomalies to a reducer for each file type.
    # These non-conforming data are written to a reject file for review.
    # This file type as variable lengths over time, so it needs its own check
    if file_type == 'foidev':
      if len(value) not in [28, 45]:
        logging.info('Does not conform to foidev structure. Skipping: %s, %s',
          mdr_key, '#' * 5)
        output.add(file_type, '%s: missing fields' % mdr_key + ':' +  '|'.join(value))
        return
    elif file_type == 'mdrfoi':
      if len(value) not in [77, 81]:
        logging.info('Does not conform to mdrfoi structure. Skipping: %s, %s',
          mdr_key, '#' * 5)
        output.add(file_type, '%s: missing fields' % mdr_key + ':' +  '|'.join(value))
        return
    elif len(value) != len(FILE_HEADERS[file_type]):
      logging.info('Does not conform to %s structure. Skipping: %s, %s',
        file_type, mdr_key, '#' * 5)
      output.add(file_type, '%s: missing fields' % mdr_key + ':' +  '|'.join(value))
      return

    if not isinstance(int(mdr_key), int):
      logging.info('%s is not a number', mdr_key)
      output.add(file_type, '%s: NaN' % mdr_key + ':' +  '|'.join(value))
      return

    # If it makes it this far, it is a good record
    new_value = dict(list(zip(FILE_HEADERS[file_type], value)))
    new_value = common.transform_dict(new_value, self.cleaner)

    # https://github.com/FDA/openfda/issues/27
    # We need to see if device problem code is available for this report in the
    # foidevproblem.txt file, resolve it to a problem description, and add it to the
    # master record.
    if file_type == 'mdrfoi':
      problem_codes = self.device_problem_codes.get(mdr_key)
      if problem_codes is not None:
        product_problems = [self.problem_codes_reference.get(code) for code in problem_codes]
        new_value['product_problems'] = product_problems

    output.add(mdr_key, (file_type, new_value))


class CSV2JSONJoinReducer(parallel.Reducer):
  # File type to nested key name mapping
  join_map = {
    'device': 'device',
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
    return PreprocessFilesToFixIssues()

  def output(self):
    file_name = '-'.join([self.loader_task, self.run_date, 'json.db'])

    return luigi.LocalTarget(join(BASE_DIR, 'maude', file_name))

  def run(self):
    files = glob.glob(self.input().path + '/*/*.txt')

    if self.loader_task == 'init':
      input_files = [f for f in files if not any(i for i in IGNORE_FILES if i in f)]
    else:
      input_files = [f for f in files if self.loader_task in f]

    # Load and cache device problem codes.
    problem_codes_reference = {}
    device_problem_codes = {}

    reader = csv.reader(open(DEVICE_PROBLEM_CODES_FILE), quoting=csv.QUOTE_NONE, delimiter='|')
    for idx, line in enumerate(reader):
      if len(line) > 1:
        problem_codes_reference[line[0]] = line[1].strip()

    reader = csv.reader(open(DEVICE_PROBLEMS_FILE), quoting=csv.QUOTE_NONE, delimiter='|')
    for idx, line in enumerate(reader):
      if len(line) > 1:
        device_problem_codes[line[0]] = [line[1]] if device_problem_codes.get(line[0]) is None else \
        device_problem_codes[line[0]] + [line[1]]

    parallel.mapreduce(
      parallel.Collection.from_glob(
        input_files, parallel.CSVLineInput(quoting=csv.QUOTE_NONE, delimiter='|')),
      mapper=CSV2JSONMapper(problem_codes_reference=problem_codes_reference, device_problem_codes=device_problem_codes),
      reducer=CSV2JSONJoinReducer(),
      output_prefix=self.output().path,
      map_workers=int(multiprocessing.cpu_count() / 5),
      num_shards=int(multiprocessing.cpu_count() / 5))


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
    previous_run_date = arrow.get(self.run_date).shift(weeks=-1).format(DATE_FMT)

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
      new_reg = [d for d in registration if d.get('registration_number') == lookup]
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

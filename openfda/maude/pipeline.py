#!/usr/bin/python

''' MAUDE pipeline for downloading, joining and loading into elasticsearch
'''
import collections
import csv
import glob
import logging
import os
import re
import sys
import time
from os.path import basename, dirname, join
from urllib.request import Request, urlopen

import arrow
import luigi
from bs4 import BeautifulSoup

from openfda import common, parallel, index_util
from openfda import download_util
from openfda.common import newest_file_timestamp
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
                                                   DeviceAnnotateMapper)
from openfda.tasks import AlwaysRunTask, DependencyTriggeredTask

# Exceed default field_size limit, need to set to sys.maxsize
csv.field_size_limit(sys.maxsize)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data/'
RAW_DIR = join(BASE_DIR, 'maude/raw/events')

# See https://github.com/FDA/openfda/issues/27
# Files for resolving device problem codes
DEVICE_PROBLEM_CODES_FILE = join(BASE_DIR, 'maude/extracted/events/deviceproblemcodes.txt')
PATIENT_PROBLEM_CODES_FILE = join(BASE_DIR, 'maude/extracted/events/patientproblemdata.txt')

# Use to ensure a standard naming of level db outputs is achieved across tasks.
DATE_FMT = 'YYYY-MM-DD'
CATEGORIES = ['foidevproblem', 'patientproblemcode', 'mdrfoi', 'patient', 'foidev', 'foitext', 'device']
IGNORE_FILES = ['deviceproblemcodes', 'patientproblemdata', 'add', 'change']

DEVICE_DOWNLOAD_PAGE = 'https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files'

enum_file = join(RUN_DIR, 'maude/data/enums.csv')
enum_csv = csv.DictReader(open(enum_file))

ENUM = collections.defaultdict(lambda: collections.defaultdict(dict))

for row in enum_csv:
  key = row['key_name']
  code = row['code']
  desc = row['code_desc']

  ENUM[key][code] = desc

FIELD_MAPPING = {
  "ADVERSE_EVENT_FLAG": "adverse_event_flag",
  "BASELINE_510_K__EXEMPT_FLAG": "baseline_510_k__exempt_flag",
  "BASELINE_510_K__FLAG": "baseline_510_k__flag",
  "BASELINE_510_K__NO": "baseline_510_k__number",
  "BASELINE_BRAND_NAME": "baseline_brand_name",
  "BASELINE_CATALOG_NO": "baseline_catalog_number",
  "BASELINE_DATE_CEASED_MARKETING": "baseline_date_ceased_marketing",
  "BASELINE_DATE_FIRST_MARKETED": "baseline_date_first_marketed",
  "BASELINE_DEVICE_FAMILY": "baseline_device_family",
  "BASELINE_GENERIC_NAME": "baseline_generic_name",
  "BASELINE_MODEL_NO": "baseline_model_number",
  "BASELINE_OTHER_ID_NO": "baseline_other_id_number",
  "BASELINE_PMA_FLAG": "baseline_pma_flag",
  "BASELINE_PMA_NO": "baseline_pma_number",
  "BASELINE_PREAMENDMENT": "baseline_preamendment_flag",
  "BASELINE_SHELF_LIFE_CONTAINED": "baseline_shelf_life_contained",
  "BASELINE_SHELF_LIFE_IN_MONTHS": "baseline_shelf_life_in_months",
  "BASELINE_TRANSITIONAL": "baseline_transitional_flag",
  "BRAND_NAME": "brand_name",
  "CATALOG_NUMBER": "catalog_number",
  "COMBINATION_PRODUCT_FLAG": "combination_product_flag",
  "DATE_ADDED": "date_added",
  "DATE_CHANGED": "date_changed",
  "DATE_FACILITY_AWARE": "date_facility_aware",
  "DATE_MANUFACTURER_RECEIVED": "date_manufacturer_received",
  "DATE_OF_EVENT": "date_of_event",
  "DATE_RECEIVED": "date_received",
  "DATE_REMOVED_FLAG": "date_removed_flag",
  "DATE_REMOVED_YEAR": "date_removed_year",
  "DATE_REPORT": "date_report",
  "DATE_REPORT_TO_FDA": "date_report_to_fda",
  "DATE_REPORT_TO_MANUFACTURER": "date_report_to_manufacturer",
  "DATE_RETURNED_TO_MANUFACTURER": "date_returned_to_manufacturer",
  "DEVICE_AGE_TEXT": "device_age_text",
  "DEVICE_AVAILABILITY": "device_availability",
  "DEVICE_DATE_OF_MANUFACTURE": "device_date_of_manufacturer",
  "DEVICE_EVALUATED_BY_MANUFACTUR": "device_evaluated_by_manufacturer",
  "DEVICE_EVENT_KEY": "device_event_key",
  "DEVICE_OPERATOR": "device_operator",
  "DEVICE_REPORT_PRODUCT_CODE": "device_report_product_code",
  "DEVICE_SEQUENCE_NO": "device_sequence_number",
  "DISTRIBUTOR_ADDRESS_1": "distributor_address_1",
  "DISTRIBUTOR_ADDRESS_2": "distributor_address_2",
  "DISTRIBUTOR_CITY": "distributor_city",
  "DISTRIBUTOR_NAME": "distributor_name",
  "DISTRIBUTOR_STATE_CODE": "distributor_state",
  "DISTRIBUTOR_ZIP_CODE": "distributor_zip_code",
  "DISTRIBUTOR_ZIP_CODE_EXT": "distributor_zip_code_ext",
  "EVENT_KEY": "event_key",
  "EVENT_LOCATION": "event_location",
  "EVENT_TYPE": "event_type",
  "EXEMPTION_NUMBER": "exemption_number",
  "EXPIRATION_DATE_OF_DEVICE": "expiration_date_of_device",
  "FOI_TEXT": "text",
  "GENERIC_NAME": "generic_name",
  "HEALTH_PROFESSIONAL": "health_professional",
  "IMPLANT_DATE_YEAR": "implant_date_year",
  "IMPLANT_FLAG": "implant_flag",
  "INITIAL_REPORT_TO_FDA": "initial_report_to_fda",
  "LOT_NUMBER": "lot_number",
  "MANUFACTURER_ADDRESS_1": "manufacturer_address_1",
  "MANUFACTURER_ADDRESS_2": "manufacturer_address_2",
  "MANUFACTURER_CITY": "manufacturer_city",
  "MANUFACTURER_CONTACT_AREA_CODE": "manufacturer_contact_area_code",
  "MANUFACTURER_CONTACT_CITY": "manufacturer_contact_city",
  "MANUFACTURER_CONTACT_COUNTRY": "manufacturer_contact_country",
  "MANUFACTURER_CONTACT_EXCHANGE": "manufacturer_contact_exchange",
  "MANUFACTURER_CONTACT_EXTENSION": "manufacturer_contact_extension",
  "MANUFACTURER_CONTACT_F_NAME": "manufacturer_contact_f_name",
  "MANUFACTURER_CONTACT_L_NAME": "manufacturer_contact_l_name",
  "MANUFACTURER_CONTACT_PCITY": "manufacturer_contact_pcity",
  "MANUFACTURER_CONTACT_PCOUNTRY": "manufacturer_contact_pcountry",
  "MANUFACTURER_CONTACT_PHONE_NO": "manufacturer_contact_phone_number",
  "MANUFACTURER_CONTACT_PLOCAL": "manufacturer_contact_plocal",
  "MANUFACTURER_CONTACT_POSTAL": "manufacturer_contact_postal_code",
  "MANUFACTURER_CONTACT_STATE": "manufacturer_contact_state",
  "MANUFACTURER_CONTACT_STREET_1": "manufacturer_contact_address_1",
  "MANUFACTURER_CONTACT_STREET_2": "manufacturer_contact_address_2",
  "MANUFACTURER_CONTACT_T_NAME": "manufacturer_contact_t_name",
  "MANUFACTURER_CONTACT_ZIP_CODE": "manufacturer_contact_zip_code",
  "MANUFACTURER_CONTACT_ZIP_EXT": "manufacturer_contact_zip_ext",
  "MANUFACTURER_COUNTRY_CODE": "manufacturer_country",
  "MANUFACTURER_D_ADDRESS_1": "manufacturer_d_address_1",
  "MANUFACTURER_D_ADDRESS_2": "manufacturer_d_address_2",
  "MANUFACTURER_D_CITY": "manufacturer_d_city",
  "MANUFACTURER_D_COUNTRY_CODE": "manufacturer_d_country",
  "MANUFACTURER_D_NAME": "manufacturer_d_name",
  "MANUFACTURER_D_POSTAL_CODE": "manufacturer_d_postal_code",
  "MANUFACTURER_D_STATE_CODE": "manufacturer_d_state",
  "MANUFACTURER_D_ZIP_CODE": "manufacturer_d_zip_code",
  "MANUFACTURER_D_ZIP_CODE_EXT": "manufacturer_d_zip_code_ext",
  "MANUFACTURER_G1_CITY": "manufacturer_g1_city",
  "MANUFACTURER_G1_COUNTRY_CODE": "manufacturer_g1_country",
  "MANUFACTURER_G1_NAME": "manufacturer_g1_name",
  "MANUFACTURER_G1_POSTAL_CODE": "manufacturer_g1_postal_code",
  "MANUFACTURER_G1_STATE_CODE": "manufacturer_g1_state",
  "MANUFACTURER_G1_STREET_1": "manufacturer_g1_address_1",
  "MANUFACTURER_G1_STREET_2": "manufacturer_g1_address_2",
  "MANUFACTURER_G1_ZIP_CODE": "manufacturer_g1_zip_code",
  "MANUFACTURER_G1_ZIP_CODE_EXT": "manufacturer_g1_zip_code_ext",
  "MANUFACTURER_LINK_FLAG_": "manufacturer_link_flag",
  "MANUFACTURER_NAME": "manufacturer_name",
  "MANUFACTURER_POSTAL_CODE": "manufacturer_postal_code",
  "MANUFACTURER_STATE_CODE": "manufacturer_state",
  "MANUFACTURER_ZIP_CODE": "manufacturer_zip_code",
  "MANUFACTURER_ZIP_CODE_EXT": "manufacturer_zip_code_ext",
  "MDR_REPORT_KEY": "mdr_report_key",
  "MDR_TEXT_KEY": "mdr_text_key",
  "MFR_REPORT_TYPE": "mfr_report_type",
  "MODEL_NUMBER": "model_number",
  "NOE_SUMMARIZED": "noe_summarized",
  "NUMBER_DEVICES_IN_EVENT": "number_devices_in_event",
  "NUMBER_PATIENTS_IN_EVENT": "number_patients_in_event",
  "OTHER_ID_NUMBER": "other_id_number",
  "PATIENT_AGE": "patient_age",
  "PATIENT_ETHNICITY": "patient_ethnicity",
  "PATIENT_RACE": "patient_race",
  "PATIENT_SEQUENCE_NO": "patient_sequence_number",
  "PATIENT_SEQUENCE_NUMBER": "patient_sequence_number",
  "PATIENT_SEX": "patient_sex",
  "PATIENT_WEIGHT": "patient_weight",
  "PMA_PMN_NUM": "pma_pmn_number",
  "PREVIOUS_USE_CODE": "previous_use_code",
  "PROBLEM_CODE": "problem_code",
  "PRODUCT_PROBLEM_FLAG": "product_problem_flag",
  "REMEDIAL_ACTION": "remedial_action",
  "REMOVAL_CORRECTION_NUMBER": "removal_correction_number",
  "REPORTER_COUNTRY_CODE": "reporter_country_code",
  "REPORTER_OCCUPATION_CODE": "reporter_occupation_code",
  "REPORTER_STATE_CODE": "reporter_state_code",
  "REPORT_DATE": "report_date",
  "REPORT_NUMBER": "report_number",
  "REPORT_SOURCE_CODE": "report_source_code",
  "REPORT_TO_FDA": "report_to_fda",
  "REPORT_TO_MANUFACTURER": "report_to_manufacturer",
  "REPROCESSED_AND_REUSED_FLAG": "reprocessed_and_reused_flag",
  "SEQUENCE_NUMBER_OUTCOME": "sequence_number_outcome",
  "SEQUENCE_NUMBER_TREATMENT": "sequence_number_treatment",
  "SERVICED_BY_3RD_PARTY_FLAG": "serviced_by_3rd_party_flag",
  "SINGLE_USE_FLAG": "single_use_flag",
  "SOURCE_TYPE": "source_type",
  "SUMMARY_REPORT": "summary_report_flag",
  "SUPPL_DATES_FDA_RECEIVED": "suppl_dates_fda_received",
  "SUPPL_DATES_MFR_RECEIVED": "suppl_dates_mfr_received",
  "TEXT_TYPE_CODE": "text_type_code",
  "TYPE_OF_REPORT": "type_of_report",
  "UDI-DI": "udi_di",
  "UDI-PUBLIC": "udi_public"
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
MALFORMED_KEYS = ['mdr_report_key', 'device_event_key', 'device_sequence_number', 'patient_sequence_number']
# These keys use dashes instead of spaces in the key name. Fix in mapper.
DASH_KEYS = ['udi-di', 'public-udi']

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

  # This shows up in suppl_dates_mfr_received in the FOI records.
  if formated_date == '*':
    return None

  try:
    date = arrow.get(formated_date, supported_formats).format('YYYYMMDD')
    return date.format('YYYYMMDD')
  except:
    if input_date:
      logging.info('unparseable date: %s with input %s', input_date, formated_date)

  return None


def _split(key, value, sep):
  ''' Helper function that splits a string into an array, swaps the encoded
      values for more descriptive ones and then generates an _exact field
  '''
  value = value.split(sep)

  if key in ENUM:
    value = [ENUM[key].get(val, val) for val in value]

  return key, value


class DownloadDeviceEvents(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(RAW_DIR)

  def run(self):
    zip_urls = []
    req = Request(DEVICE_DOWNLOAD_PAGE)
    req.add_header('From', 'Open@fda.hhs.gov')
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
    soup = BeautifulSoup(urlopen(req).read(), 'lxml')
    for a in soup.find_all(href=re.compile('.*.zip')):
      if (not 'ASR' in a['href']) and (not 'mdr8' in a['href']) and (not 'mdr9' in a['href']) and (
      not 'disclaim' in a['href']):
        zip_urls.append(a['href'])

    if not zip_urls:
      logging.fatal('No MAUDE Zip Files Found At %s' % DEVICE_DOWNLOAD_PAGE)
    for zip_url in zip_urls:
      filename = zip_url.split('/')[-1]
      common.download(zip_url, join(self.output().path, filename))
      time.sleep(20)  # Be nice to the server


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
    for filename in glob.glob(self.input().path + '/*/*foi*.txt') + glob.glob(self.input().path + '/*/device.txt') \
                    + glob.glob(self.input().path + '/*/device2*.txt') + glob.glob(
      self.input().path + '/*/deviceadd*.txt') + glob.glob(self.input().path + '/*/devicechange*.txt'):
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

  def __init__(self, device_problem_codes_ref,  patient_problem_codes_ref):
    parallel.Mapper.__init__(self)
    self.filename = None
    self.fields = None
    self.device_problem_codes_ref = device_problem_codes_ref
    self.patient_problem_codes_ref = patient_problem_codes_ref

  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename

    # foidevproblem is the only data file without a header row
    if 'foidevproblem' in self.filename:
      self.fields = [
        'MDR_REPORT_KEY',
        'PROBLEM_CODE']
    else:
      with open(self.filename, 'r', encoding='utf-8') as f:
        line = f.readline()
      # Split by pipe, strip whitespace
      self.fields = [p.strip() for p in line.split('|')]

    return parallel.Mapper.map_shard(self, map_input, map_output)

  @staticmethod
  def cleaner(k, v):
    if k is None:
      return None

    if k in DASH_KEYS:
      k = k.replace('-', '_')

    if k in DATE_KEYS:
      new_date = _fix_date(v)
      return (k, new_date) if new_date else None

    if k in SPLIT_KEYS:
      return _split(k, v, ';')

    if k in MULTI_SUBMIT:
      return _split(k, v, ',')

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
    no_columns = len(self.fields)
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

    if file_type == 'foitext' and len(value) > len(self.fields):
      value = self.handle_oversized_foitext(value)

    # We send all data anomalies to a reducer for each file type.
    # These non-conforming data are written to a reject file for review.
    # This file type as variable lengths over time, so it needs its own check
    if len(value) != len(self.fields):
      logging.info('Does not conform to %s structure. Skipping: %s, %s',
        file_type, mdr_key, '#' * 5)
      output.add(file_type, '%s: missing fields' % mdr_key + ':' +  '|'.join(value))
      return

    if not isinstance(int(mdr_key), int):
      logging.info('%s is not a number', mdr_key)
      output.add(file_type, '%s: NaN' % mdr_key + ':' +  '|'.join(value))
      return

    # If it makes it this far, it is a good record
    mapped_fields = []

    for f in self.fields:
      if f not in FIELD_MAPPING:
        raise KeyError(f"Mapping not found for field: {f}. A potentially new field has been added?")
      mapped_fields.append(FIELD_MAPPING[f])

    new_value = dict(list(zip(mapped_fields, value)))
    new_value = common.transform_dict(new_value, self.cleaner)

    # https://github.com/FDA/openfda/issues/27
    # We need to see if device problem code is available for this report in the
    # foidevproblem.txt file, resolve it to a problem description, and add it to the
    # master record.
    if file_type == 'foidevproblem':
      product_problem = self.device_problem_codes_ref.get(new_value['problem_code'])
      new_value['product_problem'] = product_problem

    # Same applies to patient problem codes.
    if file_type == 'patientproblemcode':
      patient_problem = self.patient_problem_codes_ref.get(new_value['problem_code'])
      new_value['patient_problem'] = patient_problem

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
      # logging.info('MDR REPORT %s: Missing mdrfoi record, Skipping join', key)
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
        row.pop('mdr_report_key', 0)  # No need to keep join key on nested data
        if target_key != 'mdr_text' or len(
          [txt for txt in final[target_key] if txt['mdr_text_key'] == row['mdr_text_key']]) == 0:
          final[target_key].append(row)

    # Now tuck the device and patient problem codes onto the final record
    if val.get('foidevproblem', []):
      final['product_problems'] = list(map(lambda x: x['product_problem'], val['foidevproblem']))

    # https://github.com/FDA/openfda/issues/179
    # In some cases we have patient problem codes without the actual patient.
    # We create a 'placeholder' empty patient record in this case just to hold the problem codes.
    for patient_problem in val.get('patientproblemcode', []):
      if len([patient for patient in final['patient'] if
              patient['patient_sequence_number'] == patient_problem['patient_sequence_number']]) == 0:
        patient = {'patient_sequence_number': patient_problem['patient_sequence_number']}
        final['patient'].append(patient)

    for patient in final['patient']:
      for patient_problem in val.get('patientproblemcode', []):
        if patient['patient_sequence_number'] == patient_problem['patient_sequence_number']:
          patient['patient_problems'] = [patient_problem['patient_problem']] if patient.get(
            'patient_problems') is None else patient['patient_problems'] + [patient_problem['patient_problem']]

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
    device_problems = glob.glob(self.input().path + '/*/foidevproblem*.txt')
    patient_problems = glob.glob(self.input().path + '/*/patientproblemcode*.txt')

    if self.loader_task == 'init':
      input_files = [f for f in files if not any(i for i in IGNORE_FILES if i in f)]
    else:
      input_files = [f for f in files if self.loader_task in f] + device_problems + patient_problems

    # Load and cache device problem codes.
    device_problem_codes_ref = {}
    reader = csv.reader(open(DEVICE_PROBLEM_CODES_FILE), quoting=csv.QUOTE_MINIMAL, quotechar='"', delimiter=',')
    for idx, line in enumerate(reader):
      if len(line) > 1:
        device_problem_codes_ref[line[0]] = line[1].strip()

    # Load and cache patient problem codes.
    patient_problem_codes_ref = {}
    reader = csv.reader(open(PATIENT_PROBLEM_CODES_FILE), quoting=csv.QUOTE_MINIMAL, quotechar='"', delimiter=',')
    for idx, line in enumerate(reader):
      if len(line) > 1:
        patient_problem_codes_ref[line[0]] = line[1].strip()

    parallel.mapreduce(
      parallel.Collection.from_glob(
        input_files, parallel.CSVSplitLineInput(quoting=csv.QUOTE_NONE, delimiter='|')),
      mapper=CSV2JSONMapper(device_problem_codes_ref=device_problem_codes_ref,
                            patient_problem_codes_ref=patient_problem_codes_ref
                            ),
      reducer=CSV2JSONJoinReducer(),
      output_prefix=self.output().path
    )


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

      # FDA-467 MDR Text Missing in Device Events: a "change" record isn't always complete meaning it may not
      # include 'mdr_text' collection if adverse event text did not change. This may lead to the fact that mdr text
      # is gone altogether from the final record, e.g. Report Number	2647580-2020-01444.
      # So here we have to merge mdr_text from the "init" record, if so.
      if change and change.get('mdr_text') == [] and init and len(init.get('mdr_text')) > 0:
        change['mdr_text'] = init.get('mdr_text')

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
  mapping_file = 'schemas/maude_mapping.json'
  optimize_index = False
  docid_key='mdr_report_key'
  use_checksum = True
  last_update_date = lambda _: newest_file_timestamp(RAW_DIR)


  def _data(self):
    return AnnotateReport(run_date=self.run_date)


class LoadJSON(luigi.WrapperTask):
  run_date = arrow.utcnow().ceil('weeks').format(DATE_FMT)

  def requires(self):
    return LoadJSONByRunDate(run_date=self.run_date)

if __name__ == '__main__':
  luigi.run()

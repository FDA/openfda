#!/usr/local/bin/python

''' Joins together the main files of the MAUDE dataset and produces a single
    JSON file.
'''
import csv
import logging
import os
from os.path import dirname, join
import simplejson as json

import arrow

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
ENUM_FILE = join(RUN_DIR, 'maude/data/enums.csv')

# Need a list of date fields for fixing dates and removing date keys
# that are not populated
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
NESTED = ['mdr_text', 'patient', 'device']


def _add_exact(row_dict, key, val):
  if type(val) == type([]):
    if val[0] and type(val[0]) != type({}):
      if key != None:
        if '_exact' not in key:
          row_dict[key + '_exact'] = val

def _bad_record(row):
  ''' A recursive function that looks for a key or value in a dictionary
      that is equal to None. Returns True when it finds one.
  '''
  found = False
  for key, value in row.items():
    if key == None:
      found = True
      return found
    else:
      if type(value) == type([]):
        for item in value:
          if type(item) == type({}):
            found = _bad_record(item)
  return found

# TODO(hansnelsen): generalize this function, it was cut/pasted from
#                   openfda/annotation_table/combine_harmonization.py
def _combine_dicts(record_dict, new_data_dict, new_data_key):
  record_list = []
  for join_key, record_value in record_dict.iteritems():
    for pivot in record_value:
      joined_dict = {}
      if join_key in new_data_dict:
        for new_data_value in new_data_dict[join_key]:
          if new_data_key not in joined_dict:
            joined_dict = dict(pivot.items() + {new_data_key: []}.items())
          joined_dict[new_data_key].append(new_data_value)
        record_list.append(joined_dict)
      else:
        if new_data_key == None:
          continue
        joined_dict = dict(pivot.items() + {new_data_key: []}.items())
        record_list.append(joined_dict)
  return record_list

def _file_to_dict(filename, separator='|'):
  ''' Reads a delimited file into a dictionary
  '''
  file_list = []
  file_handle = open(filename, 'r')
  iter_file = csv.DictReader(file_handle, delimiter=separator)
  for row in iter_file:
    file_list.append(row)
  return file_list

def _fix_date(input_date):
  ''' Converts input dates for known formats to a standard format that is
      Elasticsearch friendly.
      Returns the input_date if it is not a known format
  '''
  supported_formats = ['DD-MMM-YY',
    'YYYY/MM/DD HH:mm:ss.SSS',
    'MM/DD/YYYY',
    'YYYYMMDD']
  # arrow needs 3 char months to be sentence case: e.g. Dec not DEC
  formated_date = input_date.title()
  try:
    date = arrow.get(formated_date, supported_formats)
    return date.format('YYYYMMDD')
  except:
      logging.info('%s is a malformed date string', input_date)
  return input_date

# TODO(hansnelsen): generalize this function, it was cut/pasted from
#                   openfda/annotation_table/combine_harmonization.py
#                   There is a slight difference: if s in record check
def _joinable_dict(record_list, join_key_list):
  ''' Pivots a list of dictionaries into dictionary of lists
      So that it can be joined against with ease
      Also contains some date cleanup logic for Elasticsearch (pop missing)
  '''
  joinable_dict = {}
  sep = ''
  if len(join_key_list) > 1:
    sep = ':'
  for record in record_list:
    _join_key = sep.join([record[s] for s in join_key_list if s in record])
    # TODO(hansnelsen): Consider a better place for the date removal logic.
    for date in DATE_KEYS:
      if date in record:
        if record[date]:
          record[date] = _fix_date(record[date])
        else:
          record.pop(date, None)
    if _join_key in joinable_dict:
      joinable_dict[_join_key].append(record)
    else:
      joinable_dict[_join_key] = [record]
  return joinable_dict

def _split_keys(row, key_name, joinable_enum):
  for pos, patient in enumerate(row['patient']):
    value = _split_string(patient[key_name], ';')
    transform = []
    for split_value in value:
      if split_value:
        key = ':'.join([key_name, split_value])
        if key in joinable_enum:
          transform.append(joinable_enum[key][0]['code_desc'])
        else:
          transform.append(split_value)
    sub_transform = []
    for i, sub_value in enumerate(transform):
      value = _split_string(sub_value, ',')

      for split_sub_value in value:
        if split_sub_value:
          key = ':'.join([key_name, split_sub_value])
          if key in joinable_enum:
            sub_transform.append(joinable_enum[key][0]['code_desc'])
          else:
            sub_transform.append(split_sub_value)
        transform[i] = ', '.join(sub_transform)
    row['patient'][pos][key_name] = transform

def _split_multi_submit(row, key_name, joinable_enum):
  value = _split_string(row[key_name], ',')
  transform = []
  for s in value:
    key = ':'.join([key_name, s])
    if key in joinable_enum:
      transform.append(joinable_enum[key][0]['code_desc'])
    else:
      transform.append(s)
    row[key_name] = transform

def _split_string(input_string, delimiter):
  if not input_string:
    return []
  else:
    return input_string.split(delimiter)


def join_maude(master_file,
               patient_file,
               device_file,
               text_file,
               out_file):
  ''' A function that takes in the MAUDE records ('|' delimited), joins them
      appropriately and outputs a single json file.
      There is some general cleaning that takes place along the way (key names,
      extra characters, etc.)
      NOTE: The MAUDE files must converted to UTF-8 for this function to work,
            this is taken care of in the pipeline itself. If this function is
            required as a standalone piece, use the UNIX command:
              iconv -f "ISO-8859-1//TRANSLIT" -t UTF8 -c | \
              iconv -f "UTF8//TRANSLIT" -t UTF8 -c \
              file_name_here > file_name_here
            which will convert the file to UTF-8 and ignore all non-UTF-8
            characters.
  '''
  enums = _file_to_dict(ENUM_FILE, separator=',')

  out = open(out_file, 'w')
  reject_file = out_file.replace('.json', '.rejects')
  rejected = open(reject_file, 'w')
  master = _file_to_dict(master_file)
  patient = _file_to_dict(patient_file)
  device = _file_to_dict(device_file)
  text = _file_to_dict(text_file)

  enum_list = []
  for enum in enums:
    enum_list.append(enum)

  distinct_keys = {}
  for enum in enum_list:
    distinct_keys[enum['key_name']] = True
  swap_keys = [s for s in distinct_keys.keys()]

  joinable_enum = _joinable_dict(enum_list, ['key_name', 'code'])

  # Join master with the patient data
  joinable_master = _joinable_dict(master, ['mdr_report_key'])
  joinable_patient = _joinable_dict(patient, ['mdr_report_key'])
  record_list = _combine_dicts(joinable_master, joinable_patient, 'patient')

  # Join the ouput of the first join with the device data
  joinable_record_dict = _joinable_dict(record_list, ['mdr_report_key'])
  joinable_device = _joinable_dict(device, ['mdr_report_key'])
  record_list = _combine_dicts(joinable_record_dict, joinable_device, 'device')

  # Join the output of the second join with the text data
  joinable_record_dict = _joinable_dict(record_list, ['mdr_report_key'])
  joinable_text = _joinable_dict(text, ['mdr_report_key'])
  record_list = _combine_dicts(joinable_record_dict, joinable_text, 'mdr_text')

  logging.info('Joined record total: %d', len(record_list))
  logging.info('Beginning to write to file %s', out_file)
  logging.info('Rejected records will be in file %s', reject_file)


  # TODO(hansnelsen): Find a way to simplify this process. There is too much
  #                   going on in this loop.
  for i, row in enumerate(record_list):
    ''' There are several things happing here, so an explanation is needed.
        1) Checking for bad records and writing them to a reject file.
        2) Iterating over distinct list of keys from enum file, which breaks
           into three categories: SPLIT_KEYS, MULTI_SUBMIT and all other. This
           is where code values are swapped for the description from the enum
           file.
        3) Creating lists from delimited fields (_split_keys and
           _split_multi_submit).
        4) Adding the _exact fields for the lists created by the splitting.
        5) Writing the row to the output_file.
    '''
    bad_row = _bad_record(row)
    if bad_row:
      rejected.write(json.dumps(row) + '\n')
      continue
    # swap_keys is a distinct list of keys to transform (taken from enum_list)
    for key_name in swap_keys:
      if key_name in SPLIT_KEYS:
        _split_keys(row, key_name, joinable_enum)
      elif key_name in MULTI_SUBMIT:
        _split_multi_submit(row, key_name, joinable_enum)
      else:
        # Look for transforms in all NESTED records (explicit list)
        for nested in NESTED:
          if nested in row:
            for i, nested_dict in enumerate(row[nested]):
              if key_name in nested_dict:
                key = ':'.join([key_name, nested_dict[key_name]])
                if key in joinable_enum:
                  row[nested][i][key_name] = joinable_enum[key][0]['code_desc']
        # Look for transforms in all top level records
        if key_name in row:
          key = ':'.join([key_name, row[key_name]])
          if key in joinable_enum:
            row[key_name] = joinable_enum[key][0]['code_desc']

    # Add _exact fields for any list types
    for key, value in row.items():
      # Handle Nested List Types
      if key in NESTED:
        if row[key]:
          for i, dict_val in enumerate(value):
            for nested_key, nested_value in dict_val.items():
              # Remove the join key from the nested records
              if 'mdr_report_key' == nested_key:
                dict_val.pop(nested_key, None)
              if nested_value:
                _add_exact(row[key][i], nested_key, nested_value)

      # Handle Top Level List Types
      else:
        _add_exact(row, key, value)
    try:
      out.write(json.dumps(row) + '\n')
    except:
      logging.info('Having problems with %s', row)


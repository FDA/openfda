#!/usr/bin/python

import collections
import re

import leveldb
import simplejson as json
from openfda import parallel

STRIP_PUNCT_RE = re.compile('[^a-zA-Z -]+')
def normalize_product_name(product_name):
  'Simple drugname normalization: strip punctuation and whitespace and lowercase.'
  product_name = product_name.strip().lower()
  return STRIP_PUNCT_RE.sub('', product_name)

def read_json_file(json_file):
  for line in json_file:
    yield json.loads(line)


def read_harmonized_file(harmonized_file):
  '''
  Create a dictionary that is keyed by drug names. First brand_name,
  then brand_name + brand_name_suffix, finally generic_name. Used to join
  against the event medicinalproduct value.
  '''
  return_dict = collections.defaultdict(list)
  for row in read_json_file(harmonized_file):
    drug = row['brand_name'].strip().lower()
    generic_name = row['generic_name'].strip().lower()
    suffix = row['brand_name_suffix'].strip().lower()

    return_dict[normalize_product_name(drug)].append(row)

    if suffix:
      return_dict[normalize_product_name(drug + ' ' + suffix)].append(row)

    if generic_name:
      return_dict[normalize_product_name(generic_name)].append(row)

  return return_dict



def _add_field(openfda, field, value):
  if not isinstance(value, list):
    value = [value]

  for v in value:
    if not v:
      continue
    if field not in openfda:
      openfda[field] = {}
    openfda[field][v] = True
    exact_field = field + '_exact'
    if exact_field not in openfda:
      openfda[exact_field] = {}
    openfda[exact_field][v] = True
  return


def AddHarmonizedRowToOpenfda(openfda, row):
  if not row['is_original_packager']:
    return

  if row['product_ndc'] in row['spl_product_ndc']:
    _add_field(openfda, 'application_number', row['application_number'])
    # Using the most precise possible name for brand_name
    if row['brand_name_suffix']:
      brand_name = row['brand_name'] + ' ' + row['brand_name_suffix']
    else:
      brand_name = row['brand_name']

    _add_field(openfda, 'brand_name', brand_name.rstrip().upper())
    _add_field(openfda, 'generic_name', row['generic_name'].upper())
    _add_field(openfda, 'manufacturer_name', row['manufacturer_name'])
    _add_field(openfda, 'product_ndc', row['product_ndc'])
    _add_field(openfda, 'product_ndc', row['spl_product_ndc'])
    _add_field(openfda, 'product_type', row['product_type'])

    for route in row['route'].split(';'):
      route = route.strip()
      _add_field(openfda, 'route', route)

    for substance in row['substance_name'].split(';'):
      substance = substance.strip()
      _add_field(openfda, 'substance_name', substance)

    for rxnorm in row['rxnorm']:
      _add_field(openfda, 'rxcui', rxnorm['rxcui'])

    _add_field(openfda, 'spl_id', row['id'])
    _add_field(openfda, 'spl_set_id', row['spl_set_id'])
    _add_field(openfda, 'package_ndc', row['package_ndc'])

    if row['unii_indexing'] != []:
      for key, value in row['unii_indexing'].items():
        if key == 'unii':
          _add_field(openfda, 'unii', value)
        if key == 'va':
          for this_item in value:
            for va_key, va_value in this_item.items():
              if va_key == 'name':
                if va_value.find('[MoA]') != -1:
                  _add_field(openfda, 'pharm_class_moa', va_value)
                if va_value.find('[Chemical/Ingredient]') != -1:
                  _add_field(openfda, 'pharm_class_cs', va_value)
                if va_value.find('[PE]') != -1:
                  _add_field(openfda, 'pharm_class_pe', va_value)
                if va_value.find('[EPC]') != -1:
                  _add_field(openfda, 'pharm_class_epc', va_value)
              if va_key == 'number':
                _add_field(openfda, 'nui', va_value)

def AnnotateDrug(drug, harmonized_dict):
  if 'medicinalproduct' not in drug or not drug['medicinalproduct']:
    return None

  medicinal_product = normalize_product_name(drug['medicinalproduct'])
  if medicinal_product in harmonized_dict:
    openfda = {}

    for row in harmonized_dict[medicinal_product]:
      AddHarmonizedRowToOpenfda(openfda, row)

    openfda_lists = {}
    for field, value in openfda.items():
      openfda_lists[field] = [s for s in value.keys()]

    drug['openfda'] = openfda_lists


def AnnotateEvent(event, version, harmonized_dict):
  if 'patient' not in event:
    return

  for drug in event['patient']['drug']:
    AnnotateDrug(drug, harmonized_dict)

  date = event['receiptdate']
  event['@timestamp'] = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
  event['@version'] = version


class AnnotateMapper(parallel.Mapper):
  def __init__(self, harmonized_file):
    self.harmonized_file = harmonized_file

  def map_shard(self, map_input, map_output):
    harmonized_dict = read_harmonized_file(open(self.harmonized_file))

    for case_number, (timestamp, event) in map_input:
      # TODO(hansnelsen): Change all of the code to refer to the timestamp
      #                   as 'version'
      AnnotateEvent(event, timestamp, harmonized_dict)
      map_output.add(case_number, event)

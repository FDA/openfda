#!/usr/bin/python

import re
import simplejson as json
import arrow
from openfda import parallel

def read_json_file(json_file):
  for line in json_file:
    yield json.loads(line)

#TODO(hansnelsen): add to common library, is used in all annotation processing
def read_harmonized_file(harmonized_file):
  '''
  Create a dictionary that is keyed by UPC and NDC.
  '''
  upc_or_ndc_to_harmonized = {}
  for row in read_json_file(harmonized_file):
    if row['upc'] != []:
      row['upc'] = []

    for upc in row['upc']:
      if upc in upc_or_ndc_to_harmonized:
        upc_or_ndc_to_harmonized[upc].append(row)
      else:
        upc_or_ndc_to_harmonized[upc] = [row]

  harmonized_file.seek(0)
  for row in read_json_file(harmonized_file):
    for product_ndc in row['spl_product_ndc']:
      if product_ndc in upc_or_ndc_to_harmonized:
        upc_or_ndc_to_harmonized[product_ndc].append(row)
      else:
        upc_or_ndc_to_harmonized[product_ndc] = [row]

  harmonized_file.seek(0)
  for row in read_json_file(harmonized_file):
    for package_ndc in row['package_ndc']:
      if package_ndc in upc_or_ndc_to_harmonized:
        upc_or_ndc_to_harmonized[package_ndc].append(row)
      else:
        upc_or_ndc_to_harmonized[package_ndc] = [row]
  return upc_or_ndc_to_harmonized

# TODO(hansnelsen): Add to a common library, since it is used by faers and res
# annotate.py. _add_field() is identical in both files.
def _add_field(openfda, field, value):
  if type(value) != type([]):
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

def _get_ndc_type(value):
  ''' Identifying the NDC type is based on string length WITHOUT '-', we need
      to strip all non-numeric characters before we look at the length
  '''
  ndc_type = ''
  numbers_only = re.sub(r'[^\d+]', '', value)
  if len(numbers_only) == 8:
    ndc_type = 'product_ndc'
  if len(numbers_only) in [10, 11]:
    ndc_type = 'package_ndc'
  else:
    ndc_type = None
  return ndc_type

def _insert_or_update(recall, code_type, code_value):
  if code_type == 'ndc':
    if 'product_ndc' in recall['openfda']:
      combined_ndcs = recall['openfda']['product_ndc'] + \
                      recall['openfda']['package_ndc']
      if code_value not in combined_ndcs:
        ndc_type = _get_ndc_type(code_value)
        if ndc_type in ['product_ndc', 'package_ndc']:
          recall['openfda'][ndc_type].append(code_value)
          recall['openfda'][ndc_type + '_exact'].append(code_value)

  if code_type == 'upc':
    if len(recall['openfda']) > 0:
      if 'upc' in recall['openfda']:
        if code_value not in recall['openfda']['upc']:
          recall['openfda']['upc'].append(code_value)
          recall['openfda']['upc_exact'].append(code_value)
      else:
        recall['openfda']['upc'] = [code_value]
        recall['openfda']['upc_exact'] = [code_value]

# TODO(hansnelsen): Looks very similiar to the code in faers/annotate.py, we
# should consider refactoring this into a general piece of code for generating
# a de-duped list for each key in the openfda dict. Note: this openfda record
# has a few more keys than its faers counterpart
def AddHarmonizedRowToOpenfda(openfda, row):
  _add_field(openfda, 'application_number', row['application_number'])
  _add_field(openfda, 'brand_name', row['brand_name'].rstrip().upper())
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
  _add_field(openfda,
             'original_packager_product_ndc',
             row['original_packager_product_ndc'])
  _add_field(openfda,
             'is_original_packager',
             row['is_original_packager'])

  _add_field(openfda, 'upc', row['upc'])

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

def AnnotateRecall(recall, harmonized_dict):
  openfda = {}
  if recall['product-type'] == 'Drugs':
    for ndc in recall['ndc']:
      if ndc in harmonized_dict:
        AddHarmonizedRowToOpenfda(openfda, harmonized_dict[ndc][0])
    for upc in recall['upc']:
      if upc in harmonized_dict:
        AddHarmonizedRowToOpenfda(openfda, harmonized_dict[upc][0])

  openfda_lists = {}
  for field, value in openfda.items():
    openfda_lists[field] = [s for s in value.keys()]

  recall['openfda'] = openfda_lists


def AnnotateEvent(recall, harmonized_dict):
  '''Doing cleanup work here so that the json to be loaded in to ES is date
  friendly and the naming conventions line up with the API standard
  '''
  AnnotateRecall(recall, harmonized_dict)
  recall['@timestamp'] = arrow.get(recall['report-date']).format('YYYY-MM-DD')

  for key, value in recall.items():
    if '-' in key:
      new_name = key.replace('-', '_')
      recall[new_name] = value
      recall.pop(key, None)
    if key in ['upc', 'ndc']:
      if value:
        for this_value in value:
          _insert_or_update(recall, key, this_value)
      recall.pop(key, None)

class AnnotateMapper(parallel.Mapper):
  def __init__(self, harmonized_file):
    self.harmonized_file = harmonized_file

  def map_shard(self, map_input, map_output):
    self.harmonized_dict = read_harmonized_file(open(self.harmonized_file))
    parallel.Mapper.map_shard(self, map_input, map_output)

  def map(self, key, recall, out):
    AnnotateEvent(recall, self.harmonized_dict)
    out.add(key, recall)
#!/usr/bin/python

import leveldb
import simplejson as json
from openfda import parallel


def read_json_file(json_file):
  for line in json_file:
    yield json.loads(line)


def read_harmonized_file(harmonized_file):
  '''
  Create a dictionary that is keyed by drug names. First brand_name,
  then brand_name + brand_name_suffix, finally generic_name. Used to join
  against the event medicinalproduct value.
  '''
  return_dict = {}
  for row in read_json_file(harmonized_file):
    drug = row['brand_name'].rstrip().lower()
    if drug in return_dict:
      return_dict[drug].append(row)
    else:
      return_dict[drug] = [row]

  harmonized_file.seek(0)
  for row in read_json_file(harmonized_file):
    drug = row['brand_name'].rstrip() + ' ' + row['brand_name_suffix'].rstrip()
    drug = drug.lower()
    if drug in return_dict:
      return_dict[drug].append(row)
    else:
      return_dict[drug] = [row]

  harmonized_file.seek(0)
  for row in read_json_file(harmonized_file):
    generic_drug = row['generic_name'].lower()
    if generic_drug in return_dict:
      return_dict[generic_drug].append(row)
    else:
      return_dict[generic_drug] = [row]
  return return_dict



def _add_field(openfda, field, value):
  if type(value) != type([]):
    value = [ value ]
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
  if row['is_original_packager'] is True:
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

  medicinal_product = drug['medicinalproduct'].lower()
  if medicinal_product in harmonized_dict:
    openfda = {}

    for row in harmonized_dict[medicinal_product]:
      AddHarmonizedRowToOpenfda(openfda, row)

    openfda_lists = {}
    for field, value in openfda.items():
      openfda_lists[field] = [s for s in value.keys()]

    drug['openfda'] = openfda_lists


def AnnotateEvent(event, harmonized_dict):
  if 'patient' not in event:
    return

  for drug in event['patient']['drug']:
    AnnotateDrug(drug, harmonized_dict)

  date = event['receiptdate']
  event['@timestamp'] = date[0:4] + '-' + date[4:6] + '-' + date[6:8]


class AnnotateMapper(parallel.Mapper):
  def __init__(self, harmonized_file):
    self.harmonized_file = harmonized_file

  def map_shard(self, map_input, map_output):
    harmonized_dict = read_harmonized_file(open(self.harmonized_file))

    for case_number, event_raw in map_input:
      event = json.loads(event_raw)
      AnnotateEvent(event, harmonized_dict)
      map_output.add(case_number, json.dumps(event))

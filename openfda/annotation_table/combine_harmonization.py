#!/usr/bin/python

"""
Combines the extracted information into the annotation table for harmonization.

Inputs:
        1) NDC product.txt
        2) SPL extract
        3) RX Norm extract
        4) UNII extract
        5) UPC extract

Process: Serially merge each of the input files

        1) Combine NDC product.txt and SPL Extract
           Join Key: id
        2) Combine Output of step 1 with RX Norm Extract
           Join Key: spl_set_id and spl_version
        3) Combine Output of step 2 with UNII Extract
           Join Key: spl_set_id
        4) Combine Output of step 3 with UPC Extract
           Join Key: spl_set_id
"""

import csv
import simplejson as json

def read_json_file(json_file):
  for line in json_file:
    yield json.loads(line)

def _joinable_dict(record_list, join_key_list):
  joinable_dict = {}
  key_seperator = ''

  if len(join_key_list) > 1:
    key_seperator = ':'

  for record in record_list:
    join_key = key_seperator.join([record[s] for s in join_key_list])
    if join_key in joinable_dict:
      joinable_dict[join_key].append(record)
    else:
      joinable_dict[join_key] = [record]
  return joinable_dict

def _combine_dicts(record_dict, new_data_dict, new_data_key):
  record_list = []
  for join_key, record_value in record_dict.iteritems():
    for pivot in record_value:
      joined_dict = {}
      if join_key in new_data_dict:
        for new_data_value in new_data_dict[join_key]:
          joined_dict = dict(pivot.items() + new_data_value.items())
          record_list.append(joined_dict)
      else:
        if new_data_key == None:
          continue
        joined_dict = dict(pivot.items() + {new_data_key: []}.items())
        record_list.append(joined_dict)
  return record_list


def combine(product_file,
            spl_extract,
            rx_extract,
            unii_extract,
            upc_extract,
            json_out):
  json_output_file = open(json_out, 'w')

  product = csv.DictReader(open(product_file), delimiter='\t')
  label = read_json_file(open(spl_extract))
  rxs = read_json_file(open(rx_extract))
  unii = read_json_file(open(unii_extract))
  upcs = read_json_file(open(upc_extract))

  all_products = []
  for row in product:
    clean_product = {}
    #the productid has a date on the front of it, so need to split
    clean_product['id'] = row['PRODUCTID'].split('_')[1]
    clean_product['application_number'] = row['APPLICATIONNUMBER']
    clean_product['product_type'] = row['PRODUCTTYPENAME']
    clean_product['generic_name'] = row['NONPROPRIETARYNAME']
    clean_product['manufacturer_name'] = row['LABELERNAME']
    clean_product['brand_name'] = row['PROPRIETARYNAME']
    clean_product['brand_name_suffix'] = row['PROPRIETARYNAMESUFFIX']
    clean_product['product_ndc'] = row['PRODUCTNDC']
    clean_product['dosage_form'] = row['DOSAGEFORMNAME']
    clean_product['route'] = row['ROUTENAME']
    clean_product['substance_name'] = row['SUBSTANCENAME']

    all_products.append(clean_product)

  joinable_labels = {}

  for spl_data in label:
    clean_label = {}
    clean_label['spl_set_id'] = spl_data['spl_set_id']
    clean_label['id'] = spl_data['id']
    clean_label['spl_version'] = spl_data['spl_version']
    clean_label['is_original_packager'] = spl_data['is_original_packager']
    clean_label['spl_product_ndc'] = spl_data['ProductNDCs']
    clean_label['original_packager_product_ndc'] = \
       spl_data['OriginalPackagerProductNDSs']
    clean_label['package_ndc'] = spl_data['PackageNDCs']

    joinable_labels[clean_label['id']] = [clean_label]

  # Adding labels
  joinable_products = _joinable_dict(all_products, ['id'])
  record_list = _combine_dicts(joinable_labels, joinable_products, None)

  # Adding UNII
  joinable_record_dict = _joinable_dict(record_list, ['id'])
  joinable_unii = _joinable_dict(unii, ['spl_id'])
  record_list = _combine_dicts(joinable_record_dict,
                               joinable_unii,
                               'unii_indexing')
  # Adding RXNorm
  joinable_record_dict = _joinable_dict(record_list,
                                        ['spl_set_id', 'spl_version'])
  joinable_rxnorm = _joinable_dict(rxs, ['spl_set_id', 'spl_version'])
  record_list = _combine_dicts(joinable_record_dict, joinable_rxnorm, 'rxnorm')

  # Adding UPC
  joinable_record_dict = _joinable_dict(record_list, ['spl_set_id'])
  joinable_upc = _joinable_dict(upcs, ['set_id'])
  record_list = _combine_dicts(joinable_record_dict, joinable_upc, 'upc')

  for row in record_list:
    json.dump(row, json_output_file, encoding='utf-8-sig')
    json_output_file.write('\n')

#!/usr/bin/python

"""
Combines the extracted information into the annotation table for harmonization.

Inputs:
        1) NDC product.txt
        2) SPL extract
        3) RX Norm extract
        4) UNII extract

Process: Serially merge each of the input files

        1) Combine NDC product.txt and SPL Extract
           Join Key: id
        2) Combine Output of step 1 with RX Norm Extract
           Join Key: spl_set_id and spl_version
        3) Combine Output of step 2 with UNII Extract
           Join Key: spl_set_id
"""

import csv
import simplejson as json

def read_json_file(json_file):
  for line in json_file:
    yield json.loads(line)

def combine(product_file,
            spl_extract,
            rx_extract,
            unii_extract,
            json_out):
  json_output_file = open(json_out, 'w')

  product = csv.DictReader(open(product_file), delimiter='\t')
  label = read_json_file(open(spl_extract))
  rx = read_json_file(open(rx_extract))
  unii = read_json_file(open(unii_extract))

  all_products = []
  for row in product:
    clean_product = {}
    #the productid has a date on the front of it, so need to split
    clean_product['id'] = row['PRODUCTID'].split('_')[1]
    clean_product['application_number'] = row['APPLICATIONNUMBER']
    clean_product['product_type'] = row['PRODUCTTYPENAME']
    clean_product['generic_name'] = row['NONPROPRIETARYNAME']
    clean_product['manufacturer_name'] = row['LABELERNAME']

    #TODO(hansnelsen): add suffix as a separate field in output
    clean_product['brand_name'] = (row['PROPRIETARYNAME'] + ' ' +
                                   row['PROPRIETARYNAMESUFFIX'])
    clean_product['product_ndc'] = row['PRODUCTNDC']
    clean_product['dosage_form'] = row['DOSAGEFORMNAME']
    clean_product['route'] = row['ROUTENAME']
    clean_product['substance_name'] = row['SUBSTANCENAME']

    all_products.append(clean_product)

  all_labels = {}
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

    all_labels[clean_label['id']] = clean_label

  all_uniis = {}
  for unii_data in unii:
    hid = unii_data['spl_id']
    all_uniis[hid] = {'unii_indexing': unii_data['unii_indexing']}

  combo = []
  for product in all_products:
    p = {}
    product_id = product['id']
    if product_id in all_labels:
      label_with_unii = dict(product.items() + all_labels[product_id].items())
      if product_id in all_uniis:
        p = dict(label_with_unii.items() + all_uniis[product_id].items())

      else:
        p = dict(label_with_unii.items() + {'unii_indexing': []}.items())

      combo.append(p)

  combo2 = []
  rx_dict = {}
  for this_rx in rx:
    xid = this_rx['spl_set_id'] + ':' + this_rx['spl_version']
    rx_dict[xid] = this_rx

  combo_dict = {}
  for row in combo:
    cid = row['spl_set_id'] + ':' + row['spl_version']
    if cid in combo_dict:
      combo_dict[cid].append(row)
    else:
      combo_dict[cid] = [row]

  for combo_key, combo_value in combo_dict.iteritems():
    for pivot in combo_value:
      tmp_dict = {}
      if combo_key in rx_dict:
        tmp_dict = dict(pivot.items() + rx_dict[combo_key].items())
      else:
        tmp_dict = dict(pivot.items() + {'rxnorm': []}.items())
      combo2.append(tmp_dict)

  for row in combo2:
    json.dump(row, json_output_file, encoding='utf-8-sig')
    json_output_file.write('\n')

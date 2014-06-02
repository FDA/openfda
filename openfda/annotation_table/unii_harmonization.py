#!/usr/bin/python

"""
Extract fields from the SPL Pharmalogical Class Indexing Files.

Note: the set_id in these XMLs is NOT the set_id in the standard SPL,
there is nothing that relates to the set_id in the XML file!

Inputs:
        1) Directory filled with xml files that were extracted from
           pharmacologic_class_indexing_spl_files.zip
        2) NDC/Product.txt file
        3) JSON to which we dump the extract
"""

import csv
import extract_unii
import fnmatch
import os
import simplejson as json

from openfda import parallel_runner
from openfda.spl import extract

def harmonization_extract_worker(args):
  filename = args[0]
  name_queue = args[1]
  try:
    tree = extract.parse_xml(filename)
    harmonized = {}
    unii_list = []
    intermediate = []

    harmonized['unii'] = extract_unii.extract_unii(tree)
    harmonized['set_id'] = extract_unii.extract_set_id(tree)
    harmonized['name'] = extract_unii.extract_unii_name(tree)
    # zipping together two arrays, since they came from the same xpath locations
    # these are the NUI codes and their respective names
    # we might be able to get the names from somewhere else and avoid the zip
    intermediate = zip(extract_unii.extract_unii_other_code(tree),
                       extract_unii.extract_unii_other_name(tree))
    header = ['number', 'name']
    harmonized['va'] = [dict(zip(header, s)) for s in intermediate]
    unii_list.append(harmonized)
    name_queue.put(unii_list)
  except Exception as inst:
    print filename + 'has a problem'
    print inst

def harmonize_unii(out_file, product_file, class_index_dir):
  out = open(out_file, 'w')
  meta_file = csv.DictReader(open(product_file, 'rb'), delimiter='\t')

  ndc_dict = {}
  for row in meta_file:
    # Building the ndc_dict which is the out_fileer data structure of the final
    # loop. A little weird because there are duplicate set_id entries in the
    # Product file. Setting the key of the ndc_dict as the substance name
    # and then checking to see if the set_id is already in value list of
    # that key.
    this_spl_id = row['PRODUCTID'].split('_')[1]
    this_substance = row['SUBSTANCENAME']

    if this_spl_id in ndc_dict:
      tmp_substance = [s.lstrip() for s in this_substance.split(';')]
      ndc_dict[this_spl_id] = set(tmp_substance + ndc_dict[this_spl_id])
      ndc_dict[this_spl_id] = list(ndc_dict[this_spl_id])
    else:
      ndc_dict[this_spl_id] = [s.lstrip() for s in this_substance.split(';')]

  xmls = []
  for root, _, filenames in os.walk(class_index_dir):
    for filename in fnmatch.filter(filenames, '*.xml'):
      xmls.append(os.path.join(root, filename))

  rows = parallel_runner.parallel_extract(xmls, harmonization_extract_worker)

  combo = []

  # Handles the many-to-many relationship of ingredients to products.
  unii_pivot = {}
  for key, value in ndc_dict.iteritems():
    for substance_list in value:
      for unii_extract_dict in rows:
        if unii_extract_dict[0]['name'] in substance_list:
          if key in unii_pivot:
            unii_pivot[key].append(unii_extract_dict[0])
          else:
            unii_pivot[key] = [unii_extract_dict[0]]

  for key, value in unii_pivot.iteritems():
    output_dict = {}
    output_dict['spl_id'] = key
    output_dict['unii_indexing'] = value[0]
    combo.append(output_dict)

  for row in combo:
    out.write(json.dumps(row) + '\n')

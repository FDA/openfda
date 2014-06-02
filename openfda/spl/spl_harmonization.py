#!/usr/bin/python

"""
Generates SPL Harmonization file

Inputs: spl_dir, which refers to a directory that contains
the extracted SPL xmls.
"""

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
    harmonized['spl_set_id'] = extract.extract_set_id(tree)
    harmonized['id'] = extract.extract_id(tree)
    harmonized['spl_version'] = extract.extract_version_number(tree)
    harmonized['is_original_packager'] = extract.is_original_packager(tree)
    harmonized['ProductNDCs'] = extract.extract_product_ndcs(tree)
    harmonized['OriginalPackagerProductNDSs'] = \
      extract.extract_original_packager_product_ndcs(tree)
    harmonized['PackageNDCs'] = extract.extract_package_ndcs(tree)
    name_queue.put(harmonized)

  except Exception as inst:
    print filename + 'has a problem'
    print inst


def harmonize_spl(spl_dir, json_out):
  json_output_file = open(json_out, 'w')

  xmls = []
  # grab only original xml files
  for root, _, filenames in os.walk(spl_dir):
    for filename in fnmatch.filter(filenames, '*.xml'):
      xmls.append(os.path.join(root, filename))

  rows = parallel_runner.parallel_extract(xmls, harmonization_extract_worker)
  for output in rows:
    json_output_file.write(json.dumps(output) + '\n')

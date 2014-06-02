#!/usr/bin/python

"""
Generate RXNorm Harmonization file from SPL RxNorm mapping file.
"""

import csv
import simplejson as json

def harmonize_rxnorm(rx, json_out):
  rx_file = open(rx, 'rb')
  json_output_file = open(json_out, 'w')

  rxnorm_dict = csv.DictReader(rx_file, delimiter='|')

  pivot = {}

  for row in rxnorm_dict:
    row_dict = {}
    row_dict['rxcui'] = row['RXCUI']
    row_dict['rxstring'] = row['RXSTRING']
    row_dict['rxtty'] = row['RXTTY']
    # Fabricating a combined key that will be split later
    this_id = row['SETID'] + ':' + row['SPL_VERSION']

    if this_id in pivot:
      pivot[this_id].append(row_dict)
    else:
      pivot[this_id] = [row_dict]

  output = []
  for key, value in pivot.items():
    final = {}
    final['rxnorm'] = value
    # Split the fabricated key
    split_key = key.split(':')
    final['spl_set_id'] = split_key[0]
    final['spl_version'] = split_key[1]
    output.append(final)

  for row in output:
    json_output_file.write(json.dumps(row)  + '\n')



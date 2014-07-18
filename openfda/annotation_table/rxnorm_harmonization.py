#!/usr/bin/python
"""
Generate RXNorm Harmonization file

Inputs: rxnorm_mappings.txt
        JSON file to which you dump results

Output: --json_out or -o file specified in the parameter
        {
          'properties': {
            'spl_setid': {
              'type': 'string'
            },
            'spl_version': {
              'type': 'string'
            },
            'rxnorm': {
              'description' : 'An array of dictionaries',
              'properties': {
                'rxcui': {
                  'type': 'string'
                },
                'rxstring': {
                  'type': 'string'
                },
                'rxtty': {
                  'type': 'string'
                }
              }
            }
          }
        }

"""
import csv
import simplejson as json

def harmonize_rxnorm(rx, json_out):
  out = open(json_out, 'w')
  rx_file = open(rx, 'rb')
  rxnorm_dict = csv.DictReader(rx_file, delimiter='|')

  pivot = {}

  for row in rxnorm_dict:
    row_dict = {}
    row_dict['rxcui'] = row['RXCUI']
    row_dict['rxstring'] = row['RXSTRING']
    row_dict['rxtty'] = row['RXTTY']
    # fabricating a combined key that will be split later
    this_id = row['SETID'] + ':' + row['SPL_VERSION']

    if this_id in pivot:
      pivot[this_id].append(row_dict)
    else:
      pivot[this_id] = [row_dict]


  output = []
  for key, value in pivot.items():
    final = {}
    # final['rxcui_rxstring_rxtty'] = ','.join([str(s) for s in value])
    final['rxnorm'] = value
    #split the fabricated key
    split_key = key.split(':')
    final['spl_set_id'] = split_key[0]
    final['spl_version'] = split_key[1]
    output.append(final)

  for row in output:
    out.write(json.dumps(row)  + '\n')



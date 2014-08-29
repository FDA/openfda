#!/usr/bin/python

''' A Utility to generate a mapping file for the MAUDE dataset (device recalls)
'''

import os
from os.path import dirname, join
import simplejson as json

# The sample file must be the widest possible, since we are generating the
# mapping from it. The standard output of join_maude.py is to remove unused
# date fields. Currently have to comment out the date removal portion of the
# join_maude.py code to generate this file. Luckily the structure is stable.
# TODO(hansnelsen): find a way to automate the generation of this file.
RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
# Wide sample means that it includes all of the date fields, even if null
WIDE_SAMPLE_JSON = join(RUN_DIR, 'maude/data/wide_sample.json')
json_file = open(WIDE_SAMPLE_JSON, 'r').read().strip()
json_list = []
json_list.append(json.loads(json_file))

NESTED = ['mdr_text', 'patient', 'device']

# This is the base_mapping dictionary. Everything builds upon this dictionary.
# Note: a pointer dictionary is used in spots for readability.
base_mapping = {"maude":{"properties": {}}}
base_mapping['maude']['_source'] = {"includes": ["*"], "excludes": ["*_exact"]}
base_mapping['maude']['_source']['excludes'].append('patient.*_exact')
base_mapping['maude']['_source']['excludes'].append('device.*_exact')
base_mapping['maude']['_source']['excludes'].append('mdr_text.*_exact')

for json_dict in json_list:
  for key_name, value in json_dict.items():
    # Resetting these types to avoid some pointer wonkiness
    date_type =  {"type": "date", "format": "basic_date||date"}
    exact_type  = {"type": "string", "index": "not_analyzed"}
    multifield_type =  {"type": "multi_field", "fields": {}}
    string_type = {"type": "string", "index": "analyzed"}

    # There are three logic branches we are accounting for:
    # Dates, the subdocuments and everything else.
    # Everything else breaks into lists vs multifield
    if 'date' in key_name and 'flag' not in key_name:
     base_mapping['maude']['properties'][key_name] = date_type

    elif key_name in NESTED:
      nested_type = {"properties": {}}
      base_mapping['maude']['properties'][key_name] = nested_type
      # Only need to do first one from this array to make the mapping
      for k, v in value[0].items():
        # Dates
        if 'date' in k and 'flag' not in k:
          pointer = {}
          pointer = base_mapping['maude']['properties'][key_name]['properties']
          pointer[k] = date_type
        # Multifield
        else:
          new_key = {}
          pointer = {}
          multifield_type =  {"type": "multi_field", "fields": {}}
          pointer = base_mapping['maude']['properties'][key_name]['properties']
          if type(v) == type([]):
            pointer[k] = exact_type
          else:
            pointer[k] = multifield_type
            new_key[k] = string_type
            new_key['exact'] = exact_type
            pointer[k]['fields'] = new_key

    else:
      # Lists
      if '_exact' in key_name or type(value) == type([]):
        if key_name not in base_mapping['maude']['properties'].keys():
          new_key = {}
          if '_exact' in key_name:
            base_mapping['maude']['properties'][key_name] = exact_type
          else:
            base_mapping['maude']['properties'][key_name] = string_type
      # Multifield
      else:
        if key_name not in base_mapping['maude']['properties'].keys():
          new_key = {}
          base_mapping['maude']['properties'][key_name] = multifield_type
          new_key[key_name] = string_type
          new_key['exact'] = exact_type
          base_mapping['maude']['properties'][key_name]['fields'] = new_key

print json.dumps(base_mapping, sort_keys=True, indent=4)

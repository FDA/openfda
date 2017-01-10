#!/usr/local/bin/python
#
# NDA Collision Analysis
#
# Usage: ./nda.py | sort -n

import re

def DocIdMeta():
  f = open('/Users/matt/iodine/openfda-data/ndc/product.txt')
  split_header = f.next().strip().split('\t')

  docid_meta = {}
  for line in f:
    line = line.strip()
    split_line = line.split('\t')

    docid = split_line[0].split('_')[1]
    if docid in docid_meta:
      meta = docid_meta[docid]
    else:
      meta = {}

    for i in range(len(split_line)):
      name = split_header[i].lower()
      value = split_line[i]
      if name in meta:
        if type(meta[name]) == type([]):
          if value not in meta[name]:
            meta[name].append(value)
        else:
          if meta[name] != value:
            meta[name] = [ meta[name], value]
      else:
        meta[name] = value

    docid_meta[docid] = meta

  return docid_meta



docid_meta = DocIdMeta()

numeric_app_to_full = {}

docids = docid_meta.keys()
for docid in docids:
  apps = docid_meta[docid]['applicationnumber']
  if type(apps) != type([]):
    apps = [ apps ]
  for app in apps:
    if 'part' in app:
      continue
    if not app:
      continue

    if 'BLA' in app:
      print app.replace('BLA', ''), 'BLA'
    if app.find('NDA') == 0:
      print app.replace('NDA', ''), 'NDA'
    if app.find('ANDA') == 0:
      print app.replace('ANDA', ''), 'ANDA'

    numeric_app = re.sub('[^\d]', '', app)

    if not numeric_app.isdigit():
      continue

    # This removes leading zeros
    numeric_app = int(numeric_app)

    if numeric_app not in numeric_app_to_full:
      numeric_app_to_full[numeric_app] = {}
    if app not in numeric_app_to_full[numeric_app]:
      numeric_app_to_full[numeric_app][app] = 1
    else:
      numeric_app_to_full[numeric_app][app] += 1

numeric_apps = numeric_app_to_full.keys()
numeric_apps.sort()

for numeric_app in numeric_apps:
  if len(numeric_app_to_full[numeric_app]) > 1:
    print numeric_app, numeric_app_to_full[numeric_app]



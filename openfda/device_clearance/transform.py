#!/usr/bin/env python

''' A set of utility functions for converting CDRH 510K Device Clearance data
    from its native Pipe Separated Value (PSV) format into JSON so that it can
    be loaded into Elasticsearch.
'''

import arrow
import collections
import csv
import logging
import simplejson as json

from openfda import common, device_common

# Dates are special case for Elasticsearch, so we need a list of them.
DATE_KEYS = ['date_received', 'decision_date']
RENAME_MAP = {
  'classadvisecomm': 'advisory_committee',
  'sspindicator': 'ssp_indicator',
  'knumber': 'k_number',
  'devicename': 'device_name',
  'zip': 'zip_code',
  'stateorsumm': 'statement_or_summary',
  'decisiondate': 'decision_date',
  'expeditedreview': 'expedited_review_flag',
  'type': 'clearance_type',
  'reviewadvisecomm': 'review_advisory_committee',
  'productcode': 'product_code',
  'thirdparty': 'third_party_flag',
  'street1': 'address_1',
  'street2': 'address_2',
  'datereceived': 'date_received',
  'decision': 'decision_code'
}

IGNORE = ['sspindicator']

ADVISORY_COMMITTEE = device_common.MED_SPECIALTY_ADVISORY_COMMITTEE

code_file = './openfda/device_clearance/data/decision_codes.csv'
CODE_LIST = csv.reader(open(code_file, 'r'))
DECISION_CODES = collections.defaultdict(str)

for row in CODE_LIST:
  key, value = row[0], row[1]
  DECISION_CODES[key] = value

def get_description(data):
  data = data.replace('SESE', 'SE')
  if len(data) > 2:
    data.replace('SE', '')
  if data in DECISION_CODES:
    return DECISION_CODES[data.strip()]

  return 'Unknown'

def _cleaner(k, v):
  ''' A helper function that is used for renaming dictionary keys and
      formatting dates.
  '''
  k = k.lower()
  if k in IGNORE: return None
  if k in RENAME_MAP:
    k = RENAME_MAP[k]
  if v != None:
    if k in DATE_KEYS:
      dt = arrow.get(v, 'MM/DD/YYYY').format('YYYY-MM-DD')
      return (k, dt)
  if k == 'decision_code':
    ek, ev = 'decision_description', get_description(v)
    return [(k, v.strip()), (ek, ev)]
  if k == 'advisory_committee':
    ek, ev = 'advisory_committee_description', ADVISORY_COMMITTEE[v]
    return [(k, v.strip()), (ek, ev)]
  else: return (k, v.strip())
  return (k, v)

def transform_device_clearance(csv_dict):
  return common.transform_dict(csv_dict, _cleaner)
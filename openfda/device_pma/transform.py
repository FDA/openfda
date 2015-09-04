#!/usr/bin/env python

''' A set of functions for converting CDRH Pre-Market Approval data from Pipe
    Separated Value (PSV) format to JSON so that it can be loaded into
    Elasticsearch.
'''

import arrow
import csv
import simplejson as json

from openfda import common, device_common

# A list of keys to ignore from the source file.
IGNORE = []
# A list of date keys to reformat
DATE_KEYS = ['fedregnoticedate', 'decisiondate', 'datereceived']

# Changing names to match the openFDA naming standard
# key = source name, value = replacement name
RENAME_MAP = {
  'productcode': 'product_code',
  'fedregnoticedate': 'fed_reg_notice_date',
  'reviewgrantedyn': 'expedited_review_flag',
  'advisorycommittee': 'advisory_committee',
  'decisiondate': 'decision_date',
  'supplementnumber': 'supplement_number',
  'genericname': 'generic_name',
  'supplementreason': 'supplement_reason',
  'pmanumber': 'pma_number',
  'docketnumber': 'docket_number',
  'tradename': 'trade_name',
  'supplementtype': 'supplement_type',
  'aostatement': 'ao_statement',
  'decisioncode': 'decision_code',
  'datereceived': 'date_received'
}

ADVISORY_COMMITTEE = device_common.MED_SPECIALTY_ADVISORY_COMMITTEE

def _cleaner(k, v):
  ''' A helper function that is used for removing and renaming dictionary keys.
      Dates are also reformated for Elasticsearch and removed if null.
  '''
  k = k.lower()
  if k in IGNORE: return None
  if k in DATE_KEYS:
    if not v: return None
    v = arrow.get(v, 'MM/DD/YYYY').format('YYYY-MM-DD')
  if k in RENAME_MAP:
    k = RENAME_MAP[k]
  if k == 'advisory_committee':
    ek, ev = 'advisory_committee_description', ADVISORY_COMMITTEE[v]
    return [(k, v), (ek, ev)]

  return (k, v)

def transform_device_pma(csv_dict):
  return common.transform_dict(csv_dict, _cleaner)

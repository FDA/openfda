'''
Given a set of test descriptions from tests_config.json and an endpoint,
generate a default test file for the endpoint.

Usage:

python scripts/generate_tests.py <endpoint> > ./openfda/deploy/tests/<endpoint>/test_endpoint.py

While this is intended as a one-off test generator, it maybe useful if many similar
tests need to be generated for an endpoint, or to create a set of tests for a
new endpoint.
'''

import json
import os
import sys
import requests

data  = json.loads(open('./openfda/deploy/tests_config.json').read())[0]
endpoint = sys.argv[1]
types = data[endpoint]

total_idx = iter(range(100))
count_idx = iter(range(100))

def print_total(name, query, count=None):
  if not count:
    data = requests.get('http://localhost:8000%s' % query).json()
    count = data['meta']['results']['total']

  print '''
def test_%s_%d():
  assert_total('%s', %d)
''' % (name, total_idx.next(), query, count),

def print_count(name, query):
  cmd = 'curl -s "http://localhost:8000%s" | jq -r ".results[].term"' % query
  counts = os.popen(cmd).read().split('\n')[:5]
  print '''
def test_%s_%d():
  assert_count_top('%s', %s)
''' % (name, count_idx.next(), query, counts),

print '''from nose.tools import *
import requests

from openfda.tests.api_test_helpers import *
from nose.tools import *
'''

for name, tests in types.items():
  if name == 'openfda_key_check':
    for t in tests:
      print_total('openfda', t)

  if name == 'not_analyzed_check':
    for t in tests:
      print_total('not_analyzed', t)

  if name == 'date_check':
    for t in tests:
      print_total('date', t)

  if name == 'date_range_check':
    for t in tests:
      print_total('date_range', t)

  if name == 'exact_count':
    for t in tests:
      print_count('exact_count', t)

  if name == 'openfda_exact_count':
    for t in tests:
      print_count('openfda_exact_count', t)

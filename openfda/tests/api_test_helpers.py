import collections
import requests
import os
from nose.tools import assert_greater, assert_greater_equal, eq_, ok_
from unittest.case import SkipTest

ENDPOINT = os.getenv('OPENFDA_ENDPOINT_URL', 'http://localhost:8000')
Count = collections.namedtuple('Count', ['term', 'count'])


class CountList(list):
  def __repr__(self):
    return '\n'.join([repr(c) for c in self])


def extract_counts(results):
  counts = CountList()
  for r in results:
    if 'term' in r:
      counts.append(Count(r['term'], r['count']))
    else:
      counts.append(Count(r['time'], r['count']))
  return counts


def assert_total(query, minimum):
  meta, results = fetch(query)
  assert_greater_equal(meta['results']['total'], minimum,
    'Query %s had fewer results than expected.  %s < %s' % (
      query, meta['results']['total'], minimum
    ))


def assert_total_exact(query, total):
  meta, results = fetch(query)
  eq_(meta['results']['total'], total,
      'Query %s had different number of results than expected.  %s != %s' % (
        query, meta['results']['total'], total
      ))


def assert_count_top(query, expected_terms, N=10):
  '''Verify that all of the terms in `terms` are found in the top-N count results.'''
  meta, counts = fetch_counts(query)
  count_terms = set([count.term for count in counts[:N]])
  for term in expected_terms:
    assert term in count_terms, 'Query %s missing expected term %s; terms=%s' % (
        query, term, '\n'.join(list(count_terms))
    )

def assert_count_contains(query, expected_terms):
  meta, counts = fetch_counts(query)
  count_terms = set([count.term for count in counts])

  for term in expected_terms:
    assert term in count_terms, 'Query %s missing expected term %s; terms=%s' % (
      query, term, '\n'.join(count_terms)
    )


def assert_unii(query, expected_result):
  meta, results = fetch(query)
  unii_list = []
  if 'openfda' in list(results[0].keys()):
    for result in results[0]['openfda']['unii']:
      unii_list.append(result)
  elif 'patient' in list(results[0].keys()):
    for result in results[0]['patient']['drug'][0]['openfda']['unii']:
      unii_list.append(result)
  eq_(sorted(unii_list), sorted(expected_result))


def fetch(query):
  print('Fetching %s' % query)
  data = requests.get(ENDPOINT + query).json()
  return data.get('meta'), data.get('results')


def fetch_headers(query):
  print('Fetching headers for %s' % query)
  data = requests.get(query)
  return data.headers


def expect_error(query):
  print('Fetching %s' % query)
  data = requests.get(ENDPOINT + query).json()
  return data.get('error')


def json(query):
  print('Getting %s' % query)
  data = requests.get(ENDPOINT + query).json()
  return data


def fetch_counts(query):
  meta, results = fetch(query)
  return meta, extract_counts(results)


def not_circle(fn):
  'Skip this test when running under CI.'
  def _fn(*args, **kw):
    if 'CIRCLECI' in os.environ:
      raise SkipTest
    fn(*args, **kw)

  return _fn

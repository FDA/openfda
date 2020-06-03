from openfda.tests import api_test_helpers


def assert_enough_results(query, count):
  meta, results = api_test_helpers.fetch(query)
  assert meta['results']['total'] > count, \
    'Query %s returned fewer results than expected: %d vs %d' % (
      query, count, meta['results']['total'])


def test_all_green():
  status = api_test_helpers.json('/status')
  print status
  for ep in status:
    assert ep['status'] == 'GREEN'


def test_counts():
  status = api_test_helpers.json('/status')
  assert ((x for x in status if x['endpoint'] == 'deviceevent').next())['documents'] >= 9400000
  assert ((x for x in status if x['endpoint'] == 'devicerecall').next())['documents'] >= 65000
  assert ((x for x in status if x['endpoint'] == 'deviceclass').next())['documents'] >= 6176
  assert ((x for x in status if x['endpoint'] == 'devicereglist').next())['documents'] >= 227602
  assert ((x for x in status if x['endpoint'] == 'deviceclearance').next())['documents'] >= 145930
  assert ((x for x in status if x['endpoint'] == 'devicepma').next())['documents'] >= 33000
  assert ((x for x in status if x['endpoint'] == 'deviceudi').next())['documents'] >= 2480000
  assert ((x for x in status if x['endpoint'] == 'drugevent').next())['documents'] >= 11512000
  assert ((x for x in status if x['endpoint'] == 'druglabel').next())['documents'] >= 160000
  assert ((x for x in status if x['endpoint'] == 'foodevent').next())['documents'] >= 83000
  assert ((x for x in status if x['endpoint'] == 'deviceenforcement').next())['documents'] >= 10000
  assert ((x for x in status if x['endpoint'] == 'drugenforcement').next())['documents'] >= 5000
  assert ((x for x in status if x['endpoint'] == 'foodenforcement').next())['documents'] >= 1000
  assert ((x for x in status if x['endpoint'] == 'animalandveterinarydrugevent').next())['documents'] >= 919100

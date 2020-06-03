from nose.tools import *
import requests

from openfda.tests.api_test_helpers import *
from nose.tools import *


def test_not_analyzed_0():
  assert_total('/device/recall.json?search=firm_fei_number:3001236905', 12)


def test_exact_count_0():
  assert_count_top('/device/recall.json?count=root_cause_description.exact',
                   ['Device Design', 'Nonconforming Material/Component', 'Process Control', 'Software Design(Device)',
                    'Other'])


def test_date_1():
  assert_total('/device/recall.json?search=event_date_terminated:20050922', 17)


def test_date_range_2():
  assert_total('/device/recall.json?search=event_date_terminated:([20050922+TO+20051231])', 700)


def test_openfda_3():
  assert_total('/device/recall.json?search=openfda.regulation_number:878.4370', 1004)


def test_dupe_removal_fda_392():
  assert_total_exact('/device/recall.json?search=product_res_number.exact:Z-1844-2017', 1)
  assert_total_exact('/device/recall.json?search=product_res_number.exact:Z-0592-2011', 1)
  meta, results = fetch('/device/recall.json?search=product_res_number.exact:Z-0592-2011')
  eq_(results[0]['res_event_number'], '56864')
  eq_(results[0]['product_res_number'], 'Z-0592-2011')
  eq_(results[0]['firm_fei_number'], '3000206154')
  eq_(results[0]['event_date_terminated'], '2010-12-14')
  eq_(results[0]['root_cause_description'], 'Error In Labeling')
  eq_(results[0]['product_code'], 'KKX')
  eq_(results[0].get('pma_numbers'), None)
  eq_(results[0]['k_numbers'], ['K050322'])
  eq_(results[0]['other_submission_description'], '')

  eq_(results[0]['openfda']['device_name'], 'Drape, Surgical')
  eq_(results[0]['openfda']['medical_specialty_description'], 'General, Plastic Surgery')
  eq_(results[0]['openfda']['device_class'], '2')
  eq_(results[0]['openfda']['regulation_number'], '878.4370')


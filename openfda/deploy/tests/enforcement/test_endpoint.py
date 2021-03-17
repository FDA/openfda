from nose.tools import *
import requests

from openfda.tests.api_test_helpers import *
from nose.tools import *


EXACT_FIELDS = ['openfda.application_number.exact', 'openfda.brand_name.exact',
                'openfda.generic_name.exact', 'openfda.manufacturer_name.exact',
                'openfda.nui.exact', 'openfda.package_ndc.exact', 'openfda.pharm_class_cs.exact',
                'openfda.pharm_class_epc.exact', 'openfda.pharm_class_moa.exact', 'openfda.pharm_class_pe.exact',
                'openfda.product_ndc.exact', 'openfda.product_type.exact', 'openfda.route.exact', 'openfda.rxcui.exact',
                'openfda.spl_id.exact', 'openfda.spl_set_id.exact',
                'openfda.substance_name.exact', 'openfda.unii.exact']

def test_exact_field_queries_after_fda_253():
  for field in EXACT_FIELDS:
    print(field)
    meta, counts = fetch_counts('/drug/enforcement.json?count=%s&limit=1' % field)
    eq_(len(counts), 1)

def test_no_reports_before_2012():
  assert_total('/drug/enforcement.json?search=report_date:[19600101+TO+20120630]', 0)
  assert_total('/drug/enforcement.json?search=report_date:[1960-01-01+TO+2012-06-30]', 0)


def test_not_analyzed_0():
  assert_total('/drug/enforcement.json?search=openfda.spl_set_id.exact:fee7d073-0b99-48f2-7985-0d8cf970894b', 1)


def test_date_1():
  assert_total('/food/enforcement.json?search=recall_initiation_date:20120910', 143)


def test_date_2():
  assert_total('/drug/enforcement.json?search=recall_initiation_date:20140827', 120)


def test_date_3():
  assert_total('/device/enforcement.json?search=recall_initiation_date:20120910', 3)


def test_date_range_4():
  assert_total('/device/enforcement.json?search=recall_initiation_date:([20120910+TO+20121231])', 607)


def test_date_range_5():
  assert_total('/drug/enforcement.json?search=recall_initiation_date:([20140827+TO+20141231])', 863)


def test_date_range_6():
  assert_total('/food/enforcement.json?search=recall_initiation_date:([20120910+TO+20121231])', 140)


def test_openfda_7():
  assert_total('/drug/enforcement.json?search=openfda.is_original_packager:true', 885)


def test_drug_enforcement_unii():
  assert_unii('/drug/enforcement.json?search=recall_number.exact:"D-1616-2014"',
              ['0E43V0BB57', 'QTG126297Q'])


def test_no_dupes():
  assert_total_exact('/device/enforcement.json?search=recall_number.exact:Z-0031-2018', 1)


def test_no_whitespace_in_zip_codes():
  meta, results = fetch('/device/enforcement.json?search=recall_number.exact:Z-0031-2018')
  eq_(results[0]['postal_code'], '91342-3577')

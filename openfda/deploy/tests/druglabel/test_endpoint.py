from nose.tools import *
import requests

from openfda.tests.api_test_helpers import *
from nose.tools import *


def test_not_analyzed_0():
  assert_total('/drug/label.json?search=id:d08e6fbf-9162-47cd-9cf6-74ea24d48214', 1)


def test_exact_count_0():
  assert_count_top('/drug/label.json?count=openfda.brand_name.exact', ['Oxygen', 'Ibuprofen', 'Gabapentin', 'Allergy Relief', 'Amoxicillin'])


def test_date_1():
  assert_total('/drug/label.json?search=effective_time:20140320', 15)


def test_date_range_2():
  assert_total('/drug/label.json?search=effective_time:([20140320+TO+20141231])', 4000)
  assert_total('/drug/label.json?search=effective_time:([2014-03-20+TO+2014-12-31])', 4000)


def test_openfda_3():
  assert_total('/drug/label.json?search=openfda.is_original_packager:true', 53000)


def test_package_ndc_unii():
  assert_unii('/drug/label.json?search=openfda.substance_name:Methoxsalen', ['U4VJ29L7BQ'])


def test_drug_label_unii():
  assert_unii('/drug/label.json?search=openfda.substance_name:ofloxacin', ['A4P49JAZ9H'])


def test_unii_multiple_substances():
  assert_unii('/drug/label.json?search=set_id:0002a6ef-6e5e-4a34-95d5-ebaab222496f',
  [
    "39M11XPH03", "1G4GK01F67", "S7V92P67HO", "X7BCI5P86H", "6YR2608RSU"
  ])

def test_count_after_es2_migration():
  assert_count_top('/drug/label.json?count=openfda.product_type.exact', ['HUMAN OTC DRUG','HUMAN PRESCRIPTION DRUG'], 2)

  meta, counts = fetch_counts('/drug/label.json?count=openfda.product_type.exact')
  assert_greater(counts[0].count, counts[1].count)


def test_multiple_upc():
  meta, results = fetch(
    '/drug/label.json?search=openfda.spl_set_id.exact:0b0be196-0c62-461c-94f4-9a35339b4501')
  eq_(len(results), 1)
  upcs = results[0]['openfda']['upc']
  upcs.sort()
  eq_(['0300694200305', '0300694210304', '0300694220303'], upcs)

def test_single_upc():
  meta, results = fetch(
    '/drug/label.json?search=openfda.spl_set_id.exact:00006ebc-ec2b-406c-96b7-a3cc422e933f')
  eq_(len(results), 1)
  upcs = results[0]['openfda']['upc']
  eq_(['0740640391006'], upcs)

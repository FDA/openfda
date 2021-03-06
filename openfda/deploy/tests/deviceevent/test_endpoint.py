from openfda.tests.api_test_helpers import *
from nose.tools import *


def test_exact_count():
  assert_count_top('/device/event.json?count=type_of_report.exact', ['Initial submission', 'Followup'])


def test_date():
  assert_total('/device/event.json?search=date_received:20090217', 1)


def test_date_range():
  assert_total('/device/event.json?search=date_received:([20090217+TO+20091231])', 1)


def test_openfda():
  assert_total('/device/event.json?search=device.openfda.regulation_number:892.1650', 1)


def test_single_product_problem():
  meta, results = fetch(
    '/device/event.json?search=mdr_report_key:1693964')
  eq_(len(results), 1)

  event = results[0]
  eq_(event["product_problems"], ["Absorption"])


def test_multiple_product_problems():
  meta, results = fetch(
    '/device/event.json?search=mdr_report_key:7014702')
  eq_(len(results), 1)

  event = results[0]
  problems = sorted(event["product_problems"], key=lambda k: k)
  eq_(problems, ["Ambient Noise Problem", "Fracture", "High impedance", "Over-Sensing", "Pacing Problem"])


def test_foitext_present():
  meta, results = fetch(
    '/device/event.json?search=mdr_report_key:10131013')
  eq_(len(results), 1)

  event = results[0]
  mdrtext = sorted(event["mdr_text"], key=lambda k: k["mdr_text_key"])
  eq_(len(mdrtext), 2)
  eq_(mdrtext[0]["text"], u"INVESTIGATION COMPLETED ON A SMITHS MEDICAL FLUID WARMING|LEVEL 1 HOTLINE LOW FLOW SYSTEMS - HL-90 COMPLAINT OF TEMPERATURE FLUCTUATIONS WAS VERIFIED DURING TESTING. THIS WAS ISOLATED TO A FAULTY PCB (PRIMARY CIRCUIT BOARD). THE DEVICE WAS FOUND TO HAVE WEAR AND TEAR UPON ARRIVAL FOR INVESTIGATION. FRONT COVER, TANK AND LINE CORD. OUTDATED PCB AND POWER SWITCH. NO ACTION WAS TAKEN TO REPAIR DEVICE AS DEEMED BEYOND REPAIR AND WILL BE SCRAPPED. NO CAUSE OF EVENT WAS ESTABLISHED.")
  eq_(mdrtext[1]["text"], u"INFORMATION RECEIVED A FLUID WARMING|LEVEL 1 HOTLINE LOW FLOW SYSTEMS - HL-90 TEMPERATURE WAS BOUNCING AROUND AND SHOWING AT 45 DEGREES CELSIUS BUT WAS AT 20 DEGREES CELSIUS. NO PATIENT ADVERSE EVENTS REPORTED.")

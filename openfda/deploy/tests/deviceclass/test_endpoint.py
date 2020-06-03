from nose.tools import *
import requests

from openfda.tests.api_test_helpers import *
from nose.tools import *

def test_exact_count():
  assert_count_contains('/device/classification.json?count=device_name.exact',
    [ 'Endoscopic Video Imaging System/Component, Gastroenterology-Urology',
      'Catheter Introducer Kit'
      ])

def test_exact_count_desc():
  assert_count_top('/device/classification.json?count=medical_specialty_description.exact',
  [ 'Unknown',
    'Clinical Chemistry',
    'Gastroenterology, Urology',
    'General, Plastic Surgery',
    'Microbiology'
  ])

def test_openfda():
  assert_total('/device/classification.json?search=openfda.registration_number:3005809810', 1)

def test_not_analyzed():
  assert_total('/device/classification.json?search=regulation_number:878.4200', 1)

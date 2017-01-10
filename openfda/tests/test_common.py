import unittest
from openfda import common
class TestCommonMethods(unittest.TestCase):

  def test_extract_date(self):
    assert common.extract_date("20121110") == '2012-11-10'
    assert common.extract_date("201211103422") == '2012-11-10'
    assert common.extract_date("20121610") == '2012-01-10'
    assert common.extract_date("20120010") == '2012-01-10'
    assert common.extract_date("20121132") == '2012-11-01'
    assert common.extract_date("2012000001") == '2012-01-01'
    assert common.extract_date("20561132") == '1900-11-01'
    assert common.extract_date("18001132") == '1900-11-01'
    assert common.extract_date("") == '1900-01-01'
    assert common.extract_date(None) == '1900-01-01'

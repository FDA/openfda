#!/usr/bin/python

import os
import unittest
import xmltodict

from openfda.res import extract
from openfda.test_common import open_data_file


class ExtractUnitTest(unittest.TestCase):
  'RES Extract Unit Test'

  def setUp(self):
    pass

  def test_extract_ndc_4_4_2(self):
    expected_ndc = '0024-0590-10'
    xml = open_data_file('ndc-4-4-2.xml')
    xml_dict = xmltodict.parse(xml)
    extracted_ndcs = extract.extract_ndc_from_recall(xml_dict['recall-number'])
    self.assertEqual(expected_ndc, extracted_ndcs[0],
                     'ndc-4-4-2.xml')

  def test_extract_ndc_5_3_2(self):
    expected_ndc = '10631-106-08'
    xml = open_data_file('ndc-5-3-2.xml')
    xml_dict = xmltodict.parse(xml)
    extracted_ndcs = extract.extract_ndc_from_recall(xml_dict['recall-number'])
    self.assertEqual(expected_ndc, extracted_ndcs[0],
                     'ndc-5-3-2.xml')

  def test_extract_ndc_5_4_2(self):
    expected_ndc = '00591-0369-01'
    xml = open_data_file('ndc-5-4-2.xml')
    xml_dict = xmltodict.parse(xml)
    extracted_ndcs = extract.extract_ndc_from_recall(xml_dict['recall-number'])
    self.assertEqual(expected_ndc, extracted_ndcs[0],
                     'ndc-5-4-2.xml')

  def test_extract_upc_with_spaces(self):
    expected_upcs = [ '361958010115' ]
    xml = open_data_file('upc-with-spaces.xml')
    xml_dict = xmltodict.parse(xml)
    extracted_upcs = extract.extract_upc_from_recall(xml_dict['recall-number'])
    self.assertListEqual(expected_upcs, extracted_upcs,
                         'upc-with-spaces.xml')

  def test_extract_upc_with_dashes(self):
    # ensure the best by date "041913 12265 1" is not included
    expected_upcs = [ '698997806158' ]
    xml = open_data_file('upc-with-dashes.xml')
    xml_dict = xmltodict.parse(xml)
    extracted_upcs = extract.extract_upc_from_recall(xml_dict['recall-number'])
    self.assertListEqual(expected_upcs, extracted_upcs,
                         'upc-with-dashes.xml')

  def test_extract_upc_with_spaces_and_dashes(self):
    expected_upcs = [ '306037039397' ]
    xml = open_data_file('upc-with-spaces-and-dashes.xml')
    xml_dict = xmltodict.parse(xml)
    extracted_upcs = extract.extract_upc_from_recall(xml_dict['recall-number'])
    self.assertListEqual(expected_upcs, extracted_upcs,
                         'upc-with-spaces-and-dashes.xml')

  def test_extract_upc_more_than_one_labeler(self):
    # Note 300436122 is not included (invalid length) even though the record
    # indicates it's a UPC.
    expected_upcs = [
      '300676122304',
      '032251004292',
      '400910131502',
      '300670675103',
      '300670668150',
      '300670675301',
      '300670675400',
      '300670675004',
      '300670675127',
      '300676255736',
      '300670674106',
      '300670674304',
      '681131739276',
      '681131739283',
      '050428068595',
      '050428046784',
      '050428065747',
      '050428579350',
      '050428046791',
      '050428065822',
      '050428160282'
    ]

    xml = open_data_file('upc-more-than-one-labeler.xml')
    xml_dict = xmltodict.parse(xml)
    extracted_upcs = extract.extract_upc_from_recall(xml_dict['recall-number'])
    self.assertListEqual(sorted(expected_upcs), sorted(extracted_upcs) )


if __name__ == '__main__':
  unittest.main()

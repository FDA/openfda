#!/usr/bin/env python

import os
import unittest

from openfda.annotation_table import extract_unii as extract
from openfda.spl import extract as extract_spl

class UNIIExtractMethoxsalenUnitTest(unittest.TestCase):
  'UNII Extract Unit Test'

  def setUp(self):
    self.tree = extract_spl.parse_xml(os.path.dirname(__file__) +
      '/data/METHOXSALEN.xml')

  def test_extract_setid(self):
    expected_setid = 'ea1a6225-0eb9-4743-9601-23d5795936a3'
    extracted_setid = extract.extract_set_id(self.tree)
    self.assertEqual(expected_setid, extracted_setid, extracted_setid)

  def test_extract_unii(self):
    expected_unii = 'U4VJ29L7BQ'
    extracted_unii = extract.extract_unii(self.tree)
    self.assertEqual(expected_unii, extracted_unii, extracted_unii)

  def test_extract_unii_name(self):
    expected_unii_name = 'METHOXSALEN'
    extracted_unii_name = extract.extract_unii_name(self.tree)
    self.assertEqual(expected_unii_name,
                     extracted_unii_name,
                     extracted_unii_name)

  def test_extract_unii_other_code(self):
    expected_unii_other_code = ['N0000010217',
                                'N0000175984',
                                'N0000009801',
                                'N0000175879',
                                'N0000007909']
    extracted_unii_other_code = extract.extract_unii_other_code(self.tree)
    self.assertListEqual(expected_unii_other_code,
                         extracted_unii_other_code,
                         extracted_unii_other_code)

  def test_extract_unii_other_name(self):
    expected_unii_other_name = ['Photoabsorption [MoA]',
                                'Photoactivated Radical Generator [EPC]',
                                'Photosensitizing Activity [PE]',
                                'Psoralen [EPC]',
                                'Psoralens [Chemical/Ingredient]']
    extracted_unii_other_name = extract.extract_unii_other_name(self.tree)
    self.assertListEqual(expected_unii_other_name,
                         extracted_unii_other_name,
                         extracted_unii_other_name)

if __name__ == '__main__':
  unittest.main()


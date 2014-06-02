#!/usr/bin/env python

import os
import unittest

from openfda.spl import extract
from openfda.test_common import data_filename


class SPLExtractHumiraUnitTest(unittest.TestCase):
  'SPL Humira Extract Unit Test'

  def setUp(self):
    self.tree = extract.parse_xml(data_filename('humira.xml'))

  def test_extract_title(self):
    expected_title = \
      ('These highlights do not include all the information needed to use '
       'HUMIRA safely and effectively. See full prescribing information for '
       'HUMIRA. HUMIRA (adalimumab) injection, for subcutaneous use Initial '
       'U.S. Approval: 2002')
    extracted_title = extract.extract_title(self.tree)
    self.assertEqual(expected_title, extracted_title,
                     extracted_title)

  def test_extract_id(self):
    expected_id = '2c9fb32d-4b1b-b5da-4bdf-6b06908ba8b3'
    extracted_id = extract.extract_id(self.tree)
    self.assertEqual(expected_id, extracted_id,
                     extracted_id)

  def test_extract_setid(self):
    expected_setid = '608d4f0d-b19f-46d3-749a-7159aa5f933d'
    extracted_setid = extract.extract_set_id(self.tree)
    self.assertEqual(expected_setid, extracted_setid,
                     extracted_setid)

  def test_extract_effective_time(self):
    expected_effective_time = '2013-09-30'
    extracted_effective_time = extract.extract_effective_time(self.tree)
    self.assertEqual(expected_effective_time, extracted_effective_time,
                     extracted_effective_time)

  def test_extract_version_number(self):
    expected_version_number = '1560'
    extracted_version_number = extract.extract_version_number(self.tree)
    self.assertEqual(expected_version_number, extracted_version_number,
                     extracted_version_number)

  def test_extract_display_name(self):
    expected_display_name = 'HUMAN PRESCRIPTION DRUG LABEL'
    extracted_display_name = extract.extract_display_name(self.tree)
    self.assertEqual(expected_display_name, extracted_display_name,
                     extracted_display_name)

  def test_extract_duns(self):
    expected_duns = '078458370'
    extracted_duns = extract.extract_duns(self.tree)
    self.assertEqual(expected_duns, extracted_duns,
                     extracted_duns)

  def test_extract_product_ndcs(self):
    expected_ndcs = ['0074-3799', '0074-9374', '0074-4339', '0074-3797']
    extracted_ndcs = extract.extract_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_is_original_packager(self):
    self.assertEqual(True, extract.is_original_packager(self.tree),
                     True)

  def test_extract_original_packager_product_ndcs(self):
    expected_ndcs = []
    extracted_ndcs = extract.extract_original_packager_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_extract_package_ndcs(self):
    expected_ndcs = ['0074-3799-02', '0074-3799-71', '0074-9374-02',
                     '0074-9374-71', '0074-4339-02', '0074-4339-06',
                     '0074-4339-07', '0074-4339-71', '0074-4339-73',
                     '0074-3797-01']
    extracted_ndcs = extract.extract_package_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

class SPLExtractLipitorOrigUnitTest(unittest.TestCase):
  'SPL Lipitor Original Packager Extract Unit Test'

  def setUp(self):
    self.tree = extract.parse_xml(data_filename('lipitor-orig.xml'))

  def test_extract_product_ndcs(self):
    expected_ndcs = ['0071-0155', '0071-0156', '0071-0157', '0071-0158']
    extracted_ndcs = extract.extract_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_is_original_packager(self):
    self.assertEqual(True, extract.is_original_packager(self.tree),
                     True)

  def test_extract_original_packager_product_ndcs(self):
    expected_ndcs = []
    extracted_ndcs = extract.extract_original_packager_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_extract_package_ndcs(self):
    expected_ndcs = ['0071-0155-23', '0071-0155-34', '0071-0155-40',
                     '0071-0155-10', '0071-0155-97', '0071-0156-23',
                     '0071-0156-94', '0071-0156-40', '0071-0156-10',
                     '0071-0156-96', '0071-0157-23', '0071-0157-73',
                     '0071-0157-88', '0071-0157-40', '0071-0157-97',
                     '0071-0158-23', '0071-0158-73', '0071-0158-88',
                     '0071-0158-92']
    extracted_ndcs = extract.extract_package_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)


class SPLExtractLipitorRepackUnitTest(unittest.TestCase):
  'SPL Lipitor Repackager Extract Unit Test'

  def setUp(self):
    self.tree = extract.parse_xml(data_filename('lipitor-repack.xml'))

  def test_extract_product_ndcs(self):
    expected_ndcs = ['55289-800']
    extracted_ndcs = extract.extract_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_is_original_packager(self):
    self.assertEqual(False, extract.is_original_packager(self.tree),
                     False)

  def test_extract_original_packager_product_ndcs(self):
    expected_ndcs = ['0071-0156']
    extracted_ndcs = extract.extract_original_packager_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_extract_package_ndcs(self):
    expected_ndcs = ['55289-800-30']
    extracted_ndcs = extract.extract_package_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

class SPLExtractCoughUnitTest(unittest.TestCase):
  'SPL Lipitor Repackager Extract Unit Test'

  def setUp(self):
    self.tree = extract.parse_xml(data_filename('cough.xml'))

  def test_extract_product_ndcs(self):
    expected_ndcs = ['0067-6344']
    extracted_ndcs = extract.extract_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_is_original_packager(self):
    self.assertEqual(True, extract.is_original_packager(self.tree),
                     True)

  def test_extract_original_packager_product_ndcs(self):
    expected_ndcs = []
    extracted_ndcs = extract.extract_original_packager_product_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

  def test_extract_package_ndcs(self):
    expected_ndcs = ['0067-6344-04', '0067-6344-08']
    extracted_ndcs = extract.extract_package_ndcs(self.tree)
    self.assertListEqual(expected_ndcs, extracted_ndcs,
                         extracted_ndcs)

class SPLExtractNoTitleUnitTest(unittest.TestCase):
  'SPL With No Title Extract Unit Test'

  def setUp(self):
    self.tree = extract.parse_xml(data_filename('no-title.xml'))

  def test_is_original_packager(self):
    self.assertEqual(False, extract.is_original_packager(self.tree),
                     False)

  def test_extract_title(self):
    expected_title = ''
    extracted_title = extract.extract_title(self.tree)
    self.assertEqual(expected_title, extracted_title,
                     extracted_title)

if __name__ == '__main__':
  unittest.main()


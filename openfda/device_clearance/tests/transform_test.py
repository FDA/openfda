#!/usr/bin/env python
# coding=utf-8
import unittest

from openfda.device_clearance import transform


class TestGetDescription(unittest.TestCase):

  def test_plain_code(self):
    assert transform.get_description('SE') == 'Substantially Equivalent'
    assert transform.get_description('NE') == 'Not Substantially Equivalent'

  def test_doubled_se_is_collapsed(self):
    assert transform.get_description('SESE') == 'Substantially Equivalent'

  def test_se_prefixed_code_resolves_to_the_suffix(self):
    # decision_codes.csv holds two-character codes, so a longer code such as
    # SESK is the SE prefix plus the real code
    assert transform.get_description('SESK') == 'Substantially Equivalent - Kit'
    assert transform.get_description('SESD') == 'Substantially Equivalent with Drug'

  def test_unknown_code(self):
    assert transform.get_description('ZZ') == 'Unknown'


if __name__ == '__main__':
  unittest.main()

#!/usr/bin/python

import os
import simplejson as json
import unittest

from openfda.faers import annotate
from openfda.test_common import open_data_file


class AnnotateUnitTest(unittest.TestCase):
  'FAERS Annotate Unit Test'

  def setUp(self):
    pass

  def test_annotate_event(self):
    version = 10000
    harmonized_file = open_data_file('harmonized.small.json')
    harmonized_dict = annotate.read_harmonized_file(harmonized_file)

    event = json.load(open_data_file('event.preannotation.json'))
    annotate.AnnotateEvent(event, version, harmonized_dict)

    # Note that assertMultiLineEqual doesn't give pretty diffs so we do dict
    # comparison of the parsed JSON instead.
    #
    # To update event.postannotation.json, uncoment the following line and run:
    # nosetests -v -s openfda/faers > openfda/faers/tests/data/event.postannotation.json
    #print json.dumps(event, indent=2, sort_keys=True)

    expected = json.load(open_data_file('event.postannotation.json'))

    # Setting maxDiff to None means that there is no maximum length of diffs.
    self.maxDiff = None
    self.assertDictEqual(expected, event)

  def test_normalization(self):
    self.assertEqual(annotate.normalize_product_name('HUMIRa.'), 'humira')
    self.assertEqual(annotate.normalize_product_name('HUMIRA'), 'humira')
    self.assertEqual(annotate.normalize_product_name('FOLIC ACID'), 'folic acid')


if __name__ == '__main__':
  unittest.main()

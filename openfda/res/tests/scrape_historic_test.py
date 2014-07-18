#!/usr/bin/python

import unittest
from openfda.res import scrape_historic
import os
import simplejson as json

class ScrapeHistoricUnitTest(unittest.TestCase):
  'Scrape Historic Unit Test'

  def set_up(self):
    pass

  def test_scrape_june__13__2012(self):
    mydir = os.path.dirname(__file__)
    html = open(mydir + '/data/ucm308307.htm')
    expected_json = open(mydir + '/data/ucm308307.json').read()
    scraped_list = scrape_historic.scrape_report(html.read())
    actual_json = '\n'.join([json.dumps(s) for s in scraped_list])
    self.assertEqual(expected_json, actual_json, mydir + '/data/ucm308307.json')

  def test_scrape_one_recall(self):
    mydir = os.path.dirname(__file__)
    recall = open(mydir + '/data/one-recall.txt').read().strip()
    expected_recall_json = open(mydir + '/data/one-recall.json').read().strip()
    actual_recall_json = json.dumps(scrape_historic.scrape_one_recall(recall))
    self.assertEqual(expected_recall_json, actual_recall_json, mydir + '/data/one-recall.json')

if __name__ == '__main__':
  unittest.main()

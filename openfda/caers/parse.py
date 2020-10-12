#!/usr/local/bin/python

from __future__ import print_function
import csv
import simplejson
from six.moves import range

reader = csv.reader(open('caers.csv'))
header = next(reader)

PRETTY_FIELDS = [ 'report_number', 'create_date', 'event_start_date', 'primary_product_name', 'meddra_soc',
                  'event_outcome', 'age', 'age_unit', 'gender', 'mandatory_flag', 'voluntary_flag']

for row in reader:
  json_row = {}
  for i in range(len(row)):
    json_row[PRETTY_FIELDS[i]] = row[i]

  print('{ "index" : { "_index" : "caers", "_type" : "event" }}')
  print(simplejson.dumps(json_row))

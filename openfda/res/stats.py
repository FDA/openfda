#!/usr/local/bin/python

# get per field stats

import glob
import xmltodict

num_recalls = 0

tags_with_upc = {}
tags_with_ndc = {}

def handle_recall(_, recall):
  for (tag, value) in recall.items():
    if tag and value:
      if 'NDC' in value:
        #print '=' * 100
        #print tag, value
        tags_with_ndc[tag] = tags_with_ndc.get(tag, 0) + 1
      if 'UPC' in value:
        #print '=' * 100
        #print tag, value
        tags_with_upc[tag] = tags_with_upc.get(tag, 0) + 1

  return True

for xml_filename in glob.glob('./data/*.xml'):
  xmltodict.parse(open(xml_filename), item_depth=2, item_callback=handle_recall)

print 'UPC', tags_with_upc
print 'NDC', tags_with_ndc
#!/usr/local/bin/python

import extract
import sys

tree = extract.parse_xml(sys.argv[1])

methods_to_call = []
for method in dir(extract):
  if method.find('Extract') == 0:
    methods_to_call.append(method)

methods_to_call.sort()

for method in methods_to_call:
  print method.replace('Extract', '') + ':', eval('extract.%(method)s(tree)' % locals())

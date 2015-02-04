#!/usr/local/bin/python
"""
Extract NDC and UPC from freetext fields in RES
"""

import ean
import re

# http://en.wikipedia.org/wiki/National_Drug_Code
#
# Supports NDC10 and NDC11
NDC_RE = (r'\d{10}|\d{11}|\d{4}-\d{4}-\d{2}|\d{5}-\d{3}-\d{2}|'
          r'\d{5}-\d{4}-\d{2}|'
          r'\d{5}-\d{4}-\d{1}')

# http://en.wikipedia.org/wiki/Universal_Product_Code
#
# In RES, sometimes the UPC is given as 0 76847 61701 5 or
# 6-98997-80615-8 or 3 0603-7039-39 7
#
# non-matching group consisting of a digit optionally followed by
# a space or a dash, all repeated 12 times.
#
# Note: We can be very flexible in parsing UPCs as it's easy to verify
# if they are valid.
UPC_RE = r'(?:\d[\- ]?){12}'

# TODO(mattmo): Load in a whitelist of valid NDCs from the NDC dataset or
# from SPL.
def is_valid_ndc(ndc):
  return True

# TODO(mattmo): Use whitelist to map NDCs without dashes to NDCs with dashes.
def clean_ndc(ndc):
  return ndc

def extract_ndc(text):
  raw_ndcs = re.findall(NDC_RE, text)
  ndcs = {}
  for raw_ndc in raw_ndcs:
    ndc = clean_ndc(raw_ndc)
    if is_valid_ndc(ndc):
      ndcs[ndc] = True
  return ndcs.keys()

def extract_ndc_from_recall(recall):
  desc = recall['product-description']
  if 'code-info' in recall:
    desc += ' ' + recall['code-info']
  return extract_ndc(desc)

def is_valid_upc(upc):
  return ean.upca_valid(upc)

def clean_upc(upc):
  # Remove spaces
  upc = upc.replace(' ', '')

  # Remove dashes
  upc = upc.replace('-', '')

  return upc

def extract_upc(text):
  raw_upcs = re.findall(UPC_RE, text)
  upcs = {}
  for raw_upc in raw_upcs:
    upc = clean_upc(raw_upc)
    if is_valid_upc(upc):
      upcs[upc] = True
  return upcs.keys()

def extract_upc_from_recall(recall):
  desc = recall['product-description']
  if 'code-info' in recall:
    desc += ' ' + recall['code-info']
  return extract_upc(desc)

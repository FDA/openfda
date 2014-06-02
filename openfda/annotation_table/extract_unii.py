#!/usr/bin/python

"""
Extract fields from the SPL Pharmalogical Class Indexing Files.
"""

from openfda.spl import extract

# http://www.fda.gov/ForIndustry/DataStandards/
# StructuredProductLabeling/ucm162061.htm
UNII_OID = '2.16.840.1.113883.4.9'
UNII_OTHER_OID = '2.16.840.1.113883.3.26.1.5'

def extract_set_id(tree):
  return extract.first_match_or_empty_string(
    tree.getroot().xpath('/document/setId/@root'))

def extract_unii(tree):
  unii_xpath = '//id[@root="%s"]/@extension' % UNII_OID
  return extract.first_match_or_empty_string(tree.getroot().xpath(unii_xpath))

def extract_unii_name(tree):
  unii_name_xpath = '//identifiedSubstance/name/descendant::text()'
  return extract.first_match_or_empty_string(
    tree.getroot().xpath(unii_name_xpath))

def extract_unii_other_code(tree):
  unii_other_xpath = \
  '//generalizedMaterialKind/code[@codeSystem="%s"]/@code' % UNII_OTHER_OID
  return tree.getroot().xpath(unii_other_xpath)

def extract_unii_other_name(tree):
  unii_other_xpath = \
  '//generalizedMaterialKind/code[@codeSystem="%s"]/@displayName' \
  % UNII_OTHER_OID
  return tree.getroot().xpath(unii_other_xpath)

#!/usr/local/bin/python
"""
Extract UNII fields from the Pharmalogical_Class_Indexing_SPL_Files

The top part of this file is copied from extract.py, so obviously these can
be combined at some point. Kept separate for debugging purposes.

Functions:
          Remove the namespace from the XML
          Parse the XML
          First Match or Give an Empty String back
          Get UNII name
          Get UNII id
          Get UNII other codes (NUI)
          Get UNII other code names
"""

# TODO(mattmo): Logging to a file when fields are not present. Should refactor a

from lxml import etree
import StringIO
import re


# http://www.fda.gov/ForIndustry/DataStandards/StructuredProductLabeling/ucm162061.htm
UNII_OID = '2.16.840.1.113883.4.9'
UNII_OTHER_OID = '2.16.840.1.113883.3.26.1.5'

def remove_namespace(xml_file):
  """SPL have namespaces that don't play nice with python's libxml. For now
  we just remove namespaces to work around this."""
  # TODO(mattmo): Figure out a better solution to this.
  # http://stackoverflow.com/questions/5572247/how-to-find-xml-elements-
  # via-xpath-in-python-in-a-namespace-agnostic-way
  lines = xml_file.read()
  lines = re.sub(r'<document [^>]+>', '<document>', lines)
  lines = re.sub(r'\w+:type="[^"]+"', '', lines)
  return StringIO.StringIO(lines)

def parse_xml(xml_file):
  p = etree.XMLParser(huge_tree=True)
  tree = etree.parse(remove_namespace(open(xml_file)), parser=p)
  return tree

def first_match_or_empty_string(matches):
  if len(matches) > 0:
    return matches[0]
  else:
    return ''

def extract_set_id(tree):
  return first_match_or_empty_string(
    tree.getroot().xpath('/document/setId/@root'))

def extract_unii(tree):
  """Extracts the UNII from Pharmalogical Indexing XML"""
  unii_xpath = '//id[@root="%s"]/@extension' % UNII_OID
  return first_match_or_empty_string(tree.getroot().xpath(unii_xpath))

def extract_unii_name(tree):
  """Extracts the name of the UNII from the Pharmalogical Indexing XML"""
  unii_name_xpath = '//identifiedSubstance/name/descendant::text()'
  return first_match_or_empty_string(tree.getroot().xpath(unii_name_xpath))

def extract_unii_other_code(tree):
  """Extract the codes for other ingredients"""
  unii_other_xpath = \
  '//generalizedMaterialKind/code[@codeSystem="%s"]/@code' % UNII_OTHER_OID
  return tree.getroot().xpath(unii_other_xpath)

def extract_unii_other_name(tree):
  """Extract the display names for other ingredients"""
  unii_other_xpath = \
  '//generalizedMaterialKind/code[@codeSystem="%s"]/@displayName' \
  % UNII_OTHER_OID
  return tree.getroot().xpath(unii_other_xpath)

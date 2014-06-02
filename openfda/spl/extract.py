#!/usr/bin/python

"""
Extract SetId, Id, NDC from fields in SPL

TODO(mattmo): Logging to a file when fields are not present.
"""

from lxml import etree
import StringIO
import re

# http://www.fda.gov/ForIndustry/DataStandards/StructuredProductLabeling/ucm162061.htm
DUNS_OID = '1.3.6.1.4.1.519.1'
NDC_OID = '2.16.840.1.113883.6.69'
UNII_OID = '2.16.840.1.113883.4.9'
UNII_OTHER_OID = '2.16.840.1.113883.3.26.1.5'

def remove_namespace(xml_file):
  """SPL files have namespaces that don't play nice with python's libxml.
  For now we just remove namespaces to work around this."""
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

def extract_title(tree):
  return ' '.join(tree.getroot().xpath('/document/title/descendant::text()'))

def extract_id(tree):
  return first_match_or_empty_string(
    tree.getroot().xpath('/document/id/@root'))

def extract_set_id(tree):
  return first_match_or_empty_string(
    tree.getroot().xpath('/document/setId/@root'))

def extract_effective_time(tree):
  raw_date = first_match_or_empty_string(
    tree.getroot().xpath('/document/effectiveTime/@value'))
  if not raw_date:
    return ''
  year, month, day = raw_date[:4], raw_date[4:6], raw_date[6:]
  return year + "-" + month + "-" + day

def extract_version_number(tree):
  return first_match_or_empty_string(
    tree.getroot().xpath('/document/versionNumber/@value'))

def extract_display_name(tree):
  return first_match_or_empty_string(
    tree.getroot().xpath('/document/code/@displayName'))

def extract_duns(tree):
  duns_xpath = '//id[@root="%s"]/@extension' % DUNS_OID
  return first_match_or_empty_string(tree.getroot().xpath(duns_xpath))

def is_original_packager(tree):
  "Returns true if this SPL's manufacturer is the original packager."
  return len(extract_original_packager_product_ndcs(tree)) == 0

def extract_product_ndcs(tree):
  "Extracts the partial (labelercode-productcode) NDCs."
  ndc_xpath = '//manufacturedProduct/code[@codeSystem="%s"]/@code' % NDC_OID
  return tree.getroot().xpath(ndc_xpath)

def extract_original_packager_product_ndcs(tree):
  """Extracts the partial (labelercode-productcode) NDCs for the original
  packager. Only populated if this SPL is for a repackager."""
  ndc_xpath = \
    '//definingMaterialKind/code[@codeSystem="%s"]/@code' % NDC_OID
  return tree.getroot().xpath(ndc_xpath)

def extract_package_ndcs(tree):
  """Extracts the full labelercode-productcode-packagecode NDCs"""
  ndc_xpath = \
    '//containerPackagedProduct/code[@codeSystem="%s"]/@code' % NDC_OID
  return tree.getroot().xpath(ndc_xpath)

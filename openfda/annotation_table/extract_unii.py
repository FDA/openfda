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

import re
from io import BytesIO

import pandas as pd
from lxml import etree

# http://www.fda.gov/ForIndustry/DataStandards/StructuredProductLabeling/ucm162061.htm
UNII_OID = "2.16.840.1.113883.4.9"
UNII_OTHER_OID1 = "2.16.840.1.113883.3.26.1.5"
UNII_OTHER_OID2 = "2.16.840.1.113883.6.177"
UNII_OTHER_OID3 = "2.16.840.1.113883.6.345"


def remove_namespace(xml_file):
    """SPL have namespaces that don't play nice with python's libxml. For now
    we just remove namespaces to work around this."""
    # TODO(mattmo): Figure out a better solution to this.
    # http://stackoverflow.com/questions/5572247/how-to-find-xml-elements-
    # via-xpath-in-python-in-a-namespace-agnostic-way
    lines = xml_file.read()
    lines = re.sub(r"<document [^>]+>", "<document>", lines)
    lines = re.sub(r'\w+:type="[^"]+"', "", lines)
    return BytesIO(lines.encode())


def parse_xml(xml_file):
    p = etree.XMLParser(huge_tree=True)
    tree = etree.parse(remove_namespace(open(xml_file)), parser=p)
    return tree


def first_match_or_empty_string(matches):
    if len(matches) > 0:
        return matches[0]
    else:
        return ""


def extract_set_id(tree):
    return first_match_or_empty_string(tree.getroot().xpath("/document/setId/@root"))


def load_unii_from_csv(file):
    unii_rows = {}
    df = pd.read_csv(
        open(file, "r", encoding="utf-8", errors="ignore"),
        header=0,
        names=["Name", "Type", "UNII", "PT"],
        sep="\t",
        index_col=False,
        encoding="utf-8",
        dtype=str,
        low_memory=False,
        na_values=[],
        keep_default_na=False,
    )
    for row in df.itertuples():
        unii_rows[row.Name.lower()] = {
            "va": [],
            "name": row.Name,
            "set_id": "",
            "unii": row.UNII,
        }
        unii_rows[row.PT.lower()] = {
            "va": [],
            "name": row.PT,
            "set_id": "",
            "unii": row.UNII,
        }
    return unii_rows


def extract_unii(tree):
    """Extracts the UNII from Pharmalogical Indexing XML"""
    unii_xpath = '//id[@root="%s"]/@extension' % UNII_OID
    return first_match_or_empty_string(tree.getroot().xpath(unii_xpath))


def extract_unii_name(tree):
    """Extracts the name of the UNII from the Pharmalogical Indexing XML"""
    unii_name_xpath = "//identifiedSubstance/name/descendant::text()"
    return first_match_or_empty_string(tree.getroot().xpath(unii_name_xpath))


def extract_unii_other_code(tree):
    """Extract the codes for other ingredients"""
    unii_other_xpath = (
        '//generalizedMaterialKind/code[@codeSystem="%s" or @codeSystem="%s" or @codeSystem="%s"]/@code'
        % (UNII_OTHER_OID1, UNII_OTHER_OID2, UNII_OTHER_OID3)
    )
    return tree.getroot().xpath(unii_other_xpath)


def extract_unii_other_name(tree):
    """Extract the display names for other ingredients"""
    unii_other_xpath = (
        '//generalizedMaterialKind/code[@codeSystem="%s" or @codeSystem="%s" or @codeSystem="%s"]/@displayName'
        % (UNII_OTHER_OID1, UNII_OTHER_OID2, UNII_OTHER_OID3)
    )
    return tree.getroot().xpath(unii_other_xpath)

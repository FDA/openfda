#!/usr/bin/python

"""
Scrapes historic HTML RES reports into the JSON format used for the new reports.

Note that the following fields in this new record format are populated:

product-type
recalling-firm
distribution-pattern
classification
product-description
code-info
product-quantity
reason-for-recall
report-date

And the following fields in this new record format are *not* populated:

event-id
status
city
state
country
voluntary-mandated
initial-firm-notification
recall-initiation-date

Example new record:

{"product-type": "Biologics", "event-id": "40631", "status": "Terminated", "recalling-firm": "Belle Bonfils Memorial Blood Center", "city": "Denver", "state": "CO", "country": "US", "voluntary-mandated": "Voluntary: Firm Initiated", "initial-firm-notification": "E-Mail", "distribution-pattern": "Switzerland, CO", "classification": "Class II", "product-description": "Red Blood Cells Leukocytes Reduced", "code-info": "9049505", "product-quantity": "1 unit", "reason-for-recall": "Blood products, collected from a donor with a history of living in an HIV-O risk area, were distributed.", "recall-initiation-date": "08/22/2005", "report-date": "07/11/2012"}

Note: It appears that the historic RES HTML is hand coded into the FDA CMS. The HTML structure has changed over the years. Thus
"""

import datetime
import logging
import re
import simplejson
from bs4 import BeautifulSoup
from openfda.common import strip_unicode

SECTION_MAPPING = {
  'PRODUCT': 'product-description',
  'RECALLING FIRM/MANUFACTURER': 'recalling-firm',
  'REASON': 'reason-for-recall',
  'VOLUME OF PRODUCT IN COMMERCE': 'product-quantity',
  'DISTRIBUTION': 'distribution-pattern',
  'CODE': 'code-info',
}

PRODUCT_TYPES = [
  'BIOLOGICS',
  'FOODS',
  'DRUGS',
  'DEVICES',
  'VETERINARY MEDICINE',
]

CLASSIFICATIONS = [
  'I',
  'II',
  'III'
]


def scrape_report(html):
  soup = BeautifulSoup(html)
  # If the page contains a timeout warning, then skip it
  title = soup.find(text='Gateway Timeout')
  if title:
    return []

  product_type = None
  classification = None
  report_date = None

  reports = []

  middle_column_div = soup.find('div',
                                attrs={'class': re.compile(r'middle-column')})
  middle_column_text = strip_unicode(middle_column_div.get_text())

  end_index = middle_column_text.find('END OF ENFORCEMENT REPORT')
  middle_column_text = middle_column_text[:end_index]

  # The FDA begins a pilot program seeking to expedite notifications of human
  # drug product recalls to the public. The agency will modify the drug product
  # section of the Enforcement Report17, published every Wednesday, to include
  # actions that have been determined to be recalls, but that remain in the
  # process of being classified as a Class I, II, or III action. Such recalls
  # will be listed under the heading, "Recalls Pending Classification: DRUGS."
  # They will be reposted with their classification once that determination has
  # been made. Send comments or suggestions to CDERRecallPilot@fda.hhs.gov.
  # http://www.fda.gov/Safety/Recalls/EnforcementReports/ucm285341.htm

  recall_group_re = (r'RECALLS AND FIELD CORRECTIONS: ([A-Z ]+) - CLASS '
                     r'(I+)|RECALLS PENDING CLASSIFICATION: (\w+)')
  recall_groups = re.split(recall_group_re, middle_column_text)

  for recall_group in recall_groups:
    if recall_group in PRODUCT_TYPES:
      product_type = recall_group
      product_type = product_type[0] + product_type[1:].lower()
      if product_type == 'Foods':
        # Be consistent with the XML format, which uses 'Food' rather than 'Foods'
        product_type = 'Food'
      continue

    if recall_group in CLASSIFICATIONS:
      classification = 'Class ' + recall_group
      continue

    if not recall_group:
      continue

    raw_recalls = re.split('_________+', recall_group)
    for raw_recall in raw_recalls:
      m = re.findall('Enforcement Report for (.+)', raw_recall)
      if m:
        text = m[0]
        # Outlier Cases, Handled explicitly since data set is static
        text = text.replace('Aprl', 'April')\
                   .replace('February 23 ', 'February 23, 2005')\
                   .replace('(PDF - 874KB)', '')\
                   .replace(',', '')\
                   .strip()

        # June 13 2012
        try:
          dt = datetime.datetime.strptime(text, '%B %d %Y')
          report_date = dt.strftime('%m/%d/%Y')
        except:
          logging.info('Malformed Date String: ' + raw_recall)
          logging.info(r'Expecting Date Format (%B %d %Y), i.e. June 13 2012')

      if 'PRODUCT' in raw_recall:
        # Newlines are not consistently used across reports
        raw_recall = raw_recall.replace('\n', '')
        recall = scrape_one_recall(raw_recall)
        recall['product-type'] = product_type
        recall['classification'] = classification
        recall['report-date'] = report_date
        reports.append(recall)

  return reports

def scrape_one_recall(recall):
  recall_remaining = recall
  recall_section = {}

  while recall_remaining:
    last_section = None
    last_section_index = -1

    for section in SECTION_MAPPING.keys():
      section_index = recall_remaining.rfind(section)
      if section_index == -1:
        continue

      # Detect 'PRODUCT' within 'VOLUME OF PRODUCT IN COMMERCE'
      if (section == 'PRODUCT' and
          recall_remaining.rfind('VOLUME OF PRODUCT IN COMMERCE') ==
          section_index - len('VOLUME OF ')):
        continue

      if last_section_index < section_index:
        last_section = section
        last_section_index = section_index

    # No sections found, so we're done
    if last_section is None:
      return recall_section

    offset_section_index = last_section_index + len(last_section)
    last_section_text = recall_remaining[offset_section_index:].strip()
    if ' The FDA begins a pilot program' in last_section_text:
      i = last_section_text.find(' The FDA begins a pilot program')
      last_section_text = last_section_text[0:i]

    recall_section[SECTION_MAPPING[last_section]] = last_section_text

    recall_remaining = recall_remaining[0:last_section_index]

  return recall_section

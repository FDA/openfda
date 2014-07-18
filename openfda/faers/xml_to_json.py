#!/usr/bin/python

import collections
import glob
import logging
from os.path import basename, dirname
import re
import traceback

import xmltodict

from openfda import parallel
import simplejson as json


class MergeSafetyReportsReducer(parallel.Reducer):
  def reduce(self, key, values, output):
    # keys are case numbers, values are (timestamp, json_data)
    _, json_value = sorted(values)[-1]
    output.Put(key, json_value)
      

def timestamp_from_filename(filename):
  '''Returns a string YEAR.QUARTER extracted from ``filename``.

  (The timestamp doesn't need to be an integer, just something that can
  be ordered correctly).
  '''
  match = re.search(r'.*/.*_([0-9]+)q([0-4])/.*', filename)
  year, quarter = int(match.group(1)), int(match.group(2))
  return '%04d.%02d' % (year, quarter)


def parse_demo_file(demo_filename):
  '''Parse a FAERS demo file.

  Returns a dictionary mapping from safety report ID to case number.
  '''
  result = {}
  with open(demo_filename) as f:
    f.readline() # skip header
    for line in f.read().split('\n'):
      if not line:
        continue
      parts = line.split('$')
      safety_report_id = parts[0]
      case_number = parts[1]
      result[safety_report_id] = case_number

  return result


class ExtractSafetyReportsMapper(parallel.Mapper):
  '''Extract safety reports from ``input_filename``.

  This additionally looks up the case number for a given safety report ID
  using the AERS/FAERS ascii files.

  The resulting reports are converted to JSON.

  For each report, a 3-tuple (timestamp, case_number, json_str) is
  added to ``report_queue``.
  '''
  def map_shard(self, map_input, map_output):
    inputs = list(map_input)
    assert len(inputs) == 1
    input_filename = inputs[0][0]
    logging.info('Extracting reports from %s', input_filename)
    file_timestamp = timestamp_from_filename(input_filename)
    logging.info('File timestamp: %s', file_timestamp)

    input_dir = dirname(dirname(dirname(input_filename)))
    input_base = basename(dirname(dirname(input_filename)))
    
    # report id to case number conversion only needed for AERS SGM files
    # not FAERS XML files
    input_is_sgml = input_filename.find('SGML') != -1
    if input_is_sgml:
      ascii_base = input_base.replace('SGML', 'ASCII')
      ascii_glob = '%s/%s/*/DEMO*.[Tt][Xx][Tt]' % (input_dir, ascii_base)
      ascii_file = glob.glob(ascii_glob)[0]
      id_to_case = parse_demo_file(ascii_file)

    def handle_safety_report(_, safety_report):
      '''Handle a single safety_report entry.'''
      try:
        # Skip the small number of records without a patient section
        if 'patient' not in safety_report.keys():
          return True

        # Have drug and reaction in a list (even if they are just one element)
        if type(safety_report['patient']['drug']) != type([]):
          safety_report['patient']['drug'] = [safety_report['patient']['drug']]
        if type(safety_report['patient']['reaction']) != type([]):
          safety_report['patient']['reaction'] = [
            safety_report['patient']['reaction']]

        # add timestamp for kibana
        try:
          d = safety_report['receiptdate']
          if d:
            safety_report['@timestamp'] = d[0:4] + '-' + d[4:6] + '-' + d[6:8]
        except:
          pass

        # print json.dumps(safety_report, sort_keys=True,
        #   indent=2, separators=(',', ':'))
        report_id = safety_report['safetyreportid']

        # strip "check" digit
        report_id = report_id.split('-')[0]
        if input_is_sgml:
          case_number = id_to_case[report_id]
        else:
          case_number = report_id

        safety_report['@case_number'] = case_number

        report_json = json.dumps(safety_report) + '\n'
        map_output.add(case_number, (file_timestamp, report_json))
        return True
      except:
        # We sometimes encounter bad records.
        # Ignore them and continue processing.
        logging.info('Traceback in file: %s' % input_filename)
        traceback.print_exc()
        logging.info('Bad record: %s' % repr(safety_report))
        logging.info('Continuing...')
        return True
      
    try:
      xmltodict.parse(open(input_filename),
                      item_depth=2,
                      item_callback=handle_safety_report)
    except:
      pass

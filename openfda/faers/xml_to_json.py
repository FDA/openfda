#!/usr/bin/python

import collections
import glob
import logging
from os.path import basename, dirname
import pprint
import re
import traceback

import arrow
import xmltodict

from openfda import parallel


class MergeSafetyReportsReducer(parallel.Reducer):
  def __init__(self):
    self.report_counts = collections.defaultdict(int)
    parallel.Reducer.__init__(self)

  def reduce(self, key, values, output):
    # keys are case numbers, values are (timestamp, json_data)
    timestamp, report = sorted(values)[-1]
    self.report_counts[timestamp] += 1
    output.put(key, (timestamp, report))

  def reduce_finished(self):
    return self.report_counts


def timestamp_from_filename(filename):
  '''Returns a timestamp corresponding to the year/quarter in `filename`. '''
  match = re.search(r'/([0-9]+)q([0-4])/', filename)
  year, quarter = int(match.group(1)), int(match.group(2))
  return arrow.get('%04d.%02d' % (year, quarter), 'YYYY.MM').timestamp


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

def case_insensitive_glob(pattern):
  def either(c):
    return '[%s%s]'%(c.lower(),c.upper()) if c.isalpha() else c
  return glob.glob(''.join(map(either,pattern)))

class ExtractSafetyReportsMapper(parallel.Mapper):
  '''Extract safety reports from ``input_filename``.

  This additionally looks up the case number for a given safety report ID
  using the AERS/FAERS ascii files.

  The resulting reports are converted to JSON.

  For each report, a 3-tuple (timestamp, case_number, json_str) is
  added to ``report_queue``.
  '''
  def __init__(self, max_records_per_file=-1):
    # For testing, this can be set to a number > 0, which will stop the
    # extraction process early.
    self.max_records_per_file = max_records_per_file
    self._record_count = 0

  def map_shard(self, map_input, map_output):
    inputs = list(map_input)
    assert len(inputs) == 1
    input_filename = inputs[0][0]
    logging.info('Extracting reports from %s', input_filename)
    file_timestamp = timestamp_from_filename(input_filename)
    logging.info('File timestamp: %s', file_timestamp)

    # report id to case number conversion only needed for AERS SGM files
    # not FAERS XML files
    input_is_sgml = input_filename.lower().find('sgml') != -1
    id_to_case = None

    if input_is_sgml:
      input_dir = dirname(dirname(input_filename))
      ascii_files = case_insensitive_glob('%s/*/demo*.txt' % (input_dir))
      if ascii_files:
        logging.info('Found DEMO file %s', ascii_files[0])
        id_to_case = parse_demo_file(ascii_files[0])
      else:
        logging.info('No DEMO file for input %s', input_filename)


    def handle_safety_report(_, safety_report):
      '''Handle a single safety_report entry.'''
      try:
        self._record_count += 1
        if self.max_records_per_file > 0 and self._record_count >= self.max_records_per_file:
          return False

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
        if id_to_case:
          case_number = id_to_case[report_id]
        else:
          case_number = report_id

        safety_report['@case_number'] = case_number

        map_output.add(case_number, (file_timestamp, safety_report))
        return True
      except Exception:
        # We sometimes encounter bad records.
        # Ignore them and continue processing.
        logging.info('Traceback in file: %s' % input_filename)
        traceback.print_exc()
        logging.warn('Report was: %s', pprint.pformat(safety_report))
        logging.info('Continuing...')
        return True

    try:
      xmltodict.parse(open(input_filename),
                      item_depth=2,
                      item_callback=handle_safety_report)
    except:
      pass

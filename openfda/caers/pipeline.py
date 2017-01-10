import csv
import collections
import glob
import logging
import os
from os.path import basename, dirname, join
import re
import sys

import arrow
import elasticsearch
import luigi
import simplejson as json
import urllib2

from openfda import common, config, elasticsearch_requests, index_util, parallel
from openfda.tasks import AlwaysRunTask

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = config.data_dir('caers')
common.shell_cmd('mkdir -p %s', BASE_DIR)

S3_BUCKET = 's3://openfda-data-caers/'
S3_LOCAL_DIR = config.data_dir('caers/s3_sync')
# TODO(hansnelsen): initiate and resolve naming convention for this file and
#                   s3 bucket. Currently, the file is downloaded from
#                   s3://openfda-lonnie/caers/ (the naming of this file is
#                   not consistent). The pipeline engineer downloads it, renames
#                   it and then uploaded manually to the above bucket.
CAERS_FILE = 'caers.csv'
logging.info(S3_LOCAL_DIR, 'dir')
common.shell_cmd('mkdir -p %s', S3_LOCAL_DIR)

RENAME_MAP = {
  'RA_Report #': 'report_number',
  'RA_CAERS Created Date': 'date_created',
  'AEC_Event Start Date': 'date_started',
  'PRI_Product Role': 'role',
  'PRI_Reported Brand/Product Name': 'name_brand',
  'PRI_FDA Industry Code': 'industry_code',
  'PRI_FDA Industry Name': 'industry_name',
  'CI_Age at Adverse Event': 'age',
  'CI_Age Unit': 'age_unit',
  'CI_Gender': 'gender',
  'AEC_One Row Outcomes': 'outcomes',
  'SYM_One Row Coded Symptoms': 'reactions'
}

# Lists of keys used by the cleaner function in the CSV2JSONMapper() and the
# _transform() in CSV2JSONReducer()
CONSUMER = ['age', 'age_unit', 'gender']
PRODUCTS = ['role', 'name_brand', 'industry_code', 'industry_name']
EXACT = ['outcomes', 'reactions']
DATES = ['date_created', 'date_started']


class SyncS3(luigi.Task):
  bucket = S3_BUCKET
  local_dir = S3_LOCAL_DIR

  def output(self):
    return luigi.LocalTarget(self.local_dir)

  def flag_file(self):
    return os.path.join(self.local_dir, '.last_sync_time')

  def complete(self):
    'Only run S3 sync once per day.'
    return os.path.exists(self.flag_file()) and (
      arrow.get(os.path.getmtime(self.flag_file())) > arrow.now().floor('day'))

  def run(self):
    common.cmd(['aws',
                '--profile=' + config.aws_profile(),
                's3',
                'sync',
                self.bucket,
                self.local_dir])

    with open(self.flag_file(), 'w') as out_f:
      out_f.write('')


class CSV2JSONMapper(parallel.Mapper):
  @staticmethod
  def cleaner(k, v):
    ''' Callback function passed into transform_dict. Takes a key/value tuple
        and either passes them through, does a transformation either or drops
        both (by returning None).

        In this case: renaming all fields, returning None on empty keys to
          avoid blowing up downstream transforms, formating dates and creating
          _exact fields.
    '''
    if k in RENAME_MAP:
      k = RENAME_MAP[k]

    if v is None:
      return (k, None)

    if k in DATES:
      if v:
        v = arrow.get(v).format('YYYYMMDD')
      else:
        return None

    if k in EXACT:
      nk = k + '_exact'
      return [(k, v), (nk, v)]

    return (k, v)

  def map(self, key, value, output):
    new_value = common.transform_dict(value, self.cleaner)
    new_key = new_value['report_number']

    output.add(new_key, new_value)


class CSV2JSONReducer(parallel.Reducer):
  def _transform(self, value):
    ''' Takes several rows for the same report_number and merges them into
        a single report object, which is the final JSON representation,
        barring any anotation steps that may follow.
    '''
    result = {
      'report_number': None,
      'date_created': None,
      'date_started': None,
      'consumer': {},
      'products': [],
      'reactions': {}, # reactions and outcomes get pivoted to arrays of keys
      'reactions_exact': {},
      'outcomes': {},
      'outcomes_exact': {}
    }

    track_consumer = []
    for row in value:
      product = {k:v for k, v in row.items() if k in PRODUCTS}
      consumer = {k:v for k, v in row.items() if k in CONSUMER}
      reactions = row.get('reactions', []).split(',')
      outcomes = row.get('outcomes', []).split(',')

      # Setting the results
      result['report_number'] = row['report_number']
      result['date_created'] = row['date_created']
      if 'date_started' in row:
        result['date_started'] = row['date_started']

      result['products'].append(product)

      # The design assumes that the same consumer present in each report, so
      # fail if that assumption is not true
      previous_consumer = result.get('consumer', {})
      if previous_consumer:
        assert consumer == previous_consumer, \
          'There are none-matching consumers for the same report_number %s' % \
            row['report_number']
      result['consumer'] = consumer
      
      # Eliminating duplicate reactions
      for reaction in reactions:
        reaction = reaction.strip()
        result['reactions'][reaction] = True
        result['reactions_exact'][reaction] = True
      
      # Eliminating duplicate outcomes
      for outcome in outcomes:
        outcome = outcome.strip()
        result['outcomes'][outcome] = True
        result['outcomes_exact'][outcome] = True

    # Now that each list is unique, revert to list of strings
    result['reactions'] = result['reactions'].keys()
    result['reactions_exact'] = result['reactions_exact'].keys()
    result['outcomes'] = result['outcomes'].keys()
    result['outcomes_exact'] = result['outcomes_exact'].keys()

    return result

  def reduce(self, key, values, output):
    output.put(key, self._transform(values))


class CSV2JSON(luigi.Task):
  def requires(self):
    return SyncS3()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'json.db'))

  def run(self):
    file_name = join(self.input().path, CAERS_FILE)
    parallel.mapreduce(
      parallel.Collection.from_glob(file_name,
        parallel.CSVDictLineInput(delimiter=',', quoting=csv.QUOTE_MINIMAL)),
      mapper=CSV2JSONMapper(),
      reducer=CSV2JSONReducer(),
      output_prefix=self.output().path,
      num_shards=10)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'foodevent'
  type_name = 'rareport'
  mapping_file = './schemas/foodevent_mapping.json'
  data_source = CSV2JSON()
  use_checksum = False
  optimize_index = True


if __name__ == '__main__':
  luigi.run()

#!/usr/bin/python

''' Device pipeline for downloading, joining and loading registration and
    listings into elasticsearch.

    The input data for registrations is normalized, so we need to do
    a number of joins to get everything in one place. Further adding to the
    complexity, there are a number of two pass joins, meaning we stream on a
    common key from the mapper, then perform a fine-grained join on specific
    keys in the reducer. Below is the documentation of each mapper join key.
    Please look at each reducer for specifics on each secondary join.

    Tables are simply csv file names, lower-cased, with no extension, e.g.
    Owner_Operator.csv translates to table `owner_operator`.

    In some cases a join takes place and then the result is joined to another
    data set. In those case the table name will begin with `intermediate_`, e.g.
    owner operator is joined to contact address and official correspondent in
    order to make an intermediate dataset, `intermediate_owner_operator`, that
    is then joined to the final result.

    intermediate_owner_operator = join('owner_operator',
                                       'contact_addresses',
                                       'official_correspondent')
                                  ON 'contact_id'

    intermediate_registration = join('intermediate_owner_operator',
                                     'registration',
                                     'us_agent'
                                ON 'reg_key'

    intermediate_establishment_listing = join('listing_estabtypes',
                                              'estabtypes')
                                         ON 'establishment_type_id'

    intermediate_registration_listing = join('remapped_registration_listing',
                                             'listing_pcd',
                                             'listing_proprietary_name')
                                        ON 'key_val'

    final_result = join('intermediate_registration_listing',
                        'intermediate_establishment_listing',
                        'intermediate_registration')
                   ON 'reg_key'
'''

import csv
import collections
import glob
import logging
import os
from os.path import basename, dirname, join
import re
import sys

import arrow
from bs4 import BeautifulSoup
import elasticsearch
import luigi
import pandas
import requests
import simplejson as json
import urllib2

from openfda import common, config, elasticsearch_requests, index_util, parallel
from openfda import download_util
from openfda.tasks import AlwaysRunTask
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
  DeviceAnnotateMapper)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = config.data_dir('registration')
common.shell_cmd('mkdir -p %s', BASE_DIR)
# A directory for holding files that track Task state
META_DIR = config.data_dir('registration/meta')
common.shell_cmd('mkdir -p %s', META_DIR)

DEVICE_REG_PAGE = ('http://www.fda.gov/MedicalDevices/'
                   'DeviceRegulationandGuidance/HowtoMarketYourDevice/'
                   'RegistrationandListing/ucm134495.htm')

S3_BUCKET = 's3://openfda-data-reglist/'
S3_LOCAL_DIR = config.data_dir('registration/s3_sync')

common.shell_cmd('mkdir -p %s', S3_LOCAL_DIR)

REMAPPED_FILES = {
 'registration_listing.txt': 'remapped_registration_listing.txt',
 }

# TODO(hansnelsen): analyze and add the following files to the pipeline, schema,
#                   es mapping, documentation.
# For now, we will just exclude them.
EXCLUDED_FILES = [
  'Manu_ID_by_Imp.txt',
  'Non_Reg_Imp_ID_by_Manu.txt',
  'Reg_Imp_ID_by_Manu.txt'
]

# TODO(hansnelsen): copied from spl/pipeline.py, consolidate to a common place.
#                   This version has been slightly altered, so we will need to
#                   do this refactor once all of the S3 requirements are in.
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

def remap_supplemental_files(original, supplemental, output_file):
  orig = pandas.read_csv(original, sep='|')
  supp = pandas.read_csv(supplemental, sep='|')
  orig.columns = map(str.lower, orig.columns)
  supp.columns = map(str.lower, supp.columns)

  combined = pandas.merge(orig, supp, how='left')

  combined['premarket_submission_number'] = \
    combined['premarket_submission_number'].astype('str')

  submission = combined['premarket_submission_number']

  combined['pma_number'] = submission.map(common.get_p_number)
  combined['k_number'] = submission.map(common.get_k_number)

  combined.drop('premarket_submission_number', axis=1, inplace=True)

  # to_csv() will prepend an extra delimiter to the CSV header row unless you
  # specifiy `index=False`
  combined.to_csv(output_file, sep='|', index=False)

  return

def construct_join_key(data, join_keys):
  ''' A helper function to construct a join key from dictionary values.
  '''
  return ':'.join([v for k, v in data.items() if k in join_keys and v != None])

class JoinMapper(parallel.Mapper):
  def __init__(self, tables, join_keys):
    parallel.Mapper.__init__(self)
    self.tables = tables
    self.join_keys = join_keys

  def map(self, key, value, output):
    if not isinstance(value, list): value = [value]
    table = key.split(':')[0]
    if table in self.tables:
      for row in value:
        jk = construct_join_key(row, self.join_keys)
        if jk:
          output.add(jk, (table, row))

class DownloadDeviceRegistrationAndListings(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/raw'))

  def run(self):
    zip_urls = []
    soup = BeautifulSoup(urllib2.urlopen(DEVICE_REG_PAGE).read())
    for a in soup.find_all(href=re.compile('.*.zip')):
      zip_urls.append(a['href'])
    if not zip_urls:
      logging.info('No Registration Zip Files Found At %s' % DEVICE_REG_PAGE)
    for zip_url in zip_urls:
      filename = zip_url.split('/')[-1]
      common.download(zip_url, join(self.output().path, filename))

class ExtractAndCleanDownloadsReg(luigi.Task):
  ''' Unzip each of the download files and remove all the non-UTF8 characters.
      Unzip -p streams the data directly to iconv which then writes to disk.
  '''
  # These files have floats, e.g. 123.0 instead of 123, on the join keys, which
  # causes problems downstream.
  problem_files = [
    'registration_listing.txt',
    'remapped_registration_listing.txt',
    'Listing_Proprietary_Name.txt'
  ]

  def requires(self):
    return [DownloadDeviceRegistrationAndListings(), SyncS3()]

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/extracted'))

  def run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    input_dir = self.input()[0].path
    supplemental_dir = self.input()[1].path
    download_util.extract_and_clean(input_dir, 'ISO-8859-1', 'UTF-8', 'txt')

    # One of the files needs to be remapped from one column (submission_number)
    # to two columns (pma_number and k_number) depending on the prefix.
    file_name = 'registration_listing.txt'
    output_file = join(output_dir, 'remapped_' + file_name)
    remap_supplemental_files(join(output_dir, file_name),
                             join(supplemental_dir, file_name),
                             output_file)

    # There are a handful of files with floats for keys
    # This step can be removed once it is fixed on the source system.
    for fix_file in self.problem_files:
      with open(join(output_dir, fix_file), 'r') as needs_fixing:
        lines = needs_fixing.readlines()
      with open(join(output_dir, fix_file), 'w') as gets_fixing:
        for line in lines:
          gets_fixing.write(re.sub(r'\.0', '', line))

class TXT2JSONMapper(parallel.Mapper):
  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename
    return parallel.Mapper.map_shard(self, map_input, map_output)

  def map(self, key, value, output):
    def transform_dates(coll):
      DATE_KEYS = ['created_date']
      def _replace_date(k, v):
        if k is None: return
        k = k.lower()
        if v is None: return (k, v)
        if k in DATE_KEYS: return (k,  arrow.get(v, 'MM/DD/YYYY')\
                                            .format('YYYY-MM-DD'))
        if isinstance(v, list): return (k, v)
        return (k, v.strip())
      return common.transform_dict(coll, _replace_date)

    new_value = transform_dates(value)
    table_name = basename(self.filename).lower().replace('.txt', '')
    new_key = table_name + ':' + key
    output.add(new_key, new_value)

class TXT2JSON(luigi.Task):
  def requires(self):
    return ExtractAndCleanDownloadsReg()

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/json.db'))

  def run(self):
    input_dir = self.input().path
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', dirname(output_dir))

    NEEDS_HEADERS = {
      'estabtypes.txt': ['establishment_type_id', 'description']
    }

    inputs = []
    for input_file in glob.glob(input_dir + '/*.txt'):
      if basename(input_file) in REMAPPED_FILES:
        continue
      if basename(input_file) in EXCLUDED_FILES:
        continue
      header_key = basename(input_file)
      fieldnames = NEEDS_HEADERS.get(header_key, None)
      inputs.append(
        parallel.Collection.from_glob(
          input_file, parallel.CSVDictLineInput(delimiter='|',
                                                fieldnames=fieldnames,
                                                quoting=csv.QUOTE_NONE,
                                                escapechar='\\')))

    parallel.mapreduce(
        inputs=inputs,
        mapper=TXT2JSONMapper(),
        reducer=parallel.IdentityReducer(),
        output_prefix=self.output().path)

class OwnerOperatorJoinReducer(parallel.Reducer):
  def _join(self, values):
    intermediate = parallel.pivot_values(values)
    result = []
    for row in intermediate['owner_operator']:
      final = dict(row)
      final['official_correspondent'] = {}
      final['contact_address'] = {}
      # Should only be one address, but the intermediate interface is a list
      # so we will just grab the first item from the list.
      contact_address_data = intermediate.get('contact_addresses', None)
      if contact_address_data:
        final['contact_address'] = contact_address_data[0]

      left_key = final['reg_key']

      for data in intermediate.get('official_correspondent', []):
        right_key = data['reg_key']
        if right_key == left_key:
          final['official_correspondent'] = data
      result.append(final)

    return result

  def reduce(self, key, values, output):
    new_key = 'intermediate_owner_operator:' + key
    val = self._join(values)
    if val: output.put(new_key, val)

class JoinOwnerOperator(luigi.Task):
  def requires(self):
    return TXT2JSON()

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/owner_operator.db'))

  def run(self):
    tables = ['owner_operator', 'contact_addresses', 'official_correspondent']
    join_keys = ['contact_id']
    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input().path),
      mapper=JoinMapper(tables=tables, join_keys=join_keys),
      reducer=OwnerOperatorJoinReducer(),
      output_prefix=self.output().path,
      num_shards=10)

class RegistrationJoinReducer(parallel.Reducer):
  def _join(self, values):
    address_keys = [
      'address_line_1',
      'address_line_2',
      'city',
      'state_id',
      'zip_code',
      'postal_code',
      'iso_country_code'
    ]

    intermediate = parallel.pivot_values(values)

    # The US Agent Address is in the registration dataset, we need to pluck it
    # out and merge it with each us agent record.
    us_agent_address = {}
    for row in intermediate.get('registration', []):
      _type = row.get('address_type_id', None)
      if _type == 'U':
        us_agent_address = {k:v for k, v in row.items() if k in address_keys}

    # There are 0 or 1 US Agents assigned to a facility
    us_agent = {}
    agent_data = intermediate.get('us_agent', [])
    if agent_data:
      us_agent = dict(agent_data[0].items() + us_agent_address.items())

    # There is 0 or 1 owner operators
    owner_operator = {}
    owner_operator_data = intermediate.get('intermediate_owner_operator', [])
    if owner_operator_data:
      owner_operator = owner_operator_data[0]

    result = []
    for row in intermediate.get('registration', []):
      _type = row.get('address_type_id', None)
      # We only want `Facility` records, i.e. skip all the us agent addresses
      if _type == 'F':
        final = dict(row)
        final['us_agent'] = us_agent
        final['owner_operator'] = owner_operator
        result.append(final)

    return result

  def reduce(self, key, values, output):
    new_key = 'intermediate_registration:' + key
    val = self._join(values)
    if val: output.put(new_key, val)

class JoinRegistration(luigi.Task):
  def requires(self):
    return [TXT2JSON(), JoinOwnerOperator()]

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/registration.db'))

  def run(self):
    tables = ['intermediate_owner_operator', 'registration', 'us_agent']
    join_keys = ['reg_key']
    db_list = [s.path for s in self.input()]

    parallel.mapreduce(
      parallel.Collection.from_sharded_list(db_list),
      mapper=JoinMapper(tables=tables, join_keys=join_keys),
      reducer=RegistrationJoinReducer(),
      output_prefix=self.output().path,
      num_shards=10)

class JoinEstablishmentTypesReducer(parallel.Reducer):
  def _join(self, values):
    intermediate = parallel.pivot_values(values)
    result = []
    # There should be only one estblishment type streamed
    est_type = intermediate.get('estabtypes', [])[0]
    for data in intermediate.get('listing_estabtypes', []):
      final = dict(data.items() + est_type.items())
      result.append(final)
    return result

  def reduce(self, key, values, output):
    key_prefix = 'intermediate_establishment_listing:' + key
    val = self._join(values)
    if val:
      for i, row in enumerate(val):
        new_key = key_prefix + ':' + str(i)
        output.put(new_key, row)

class JoinEstablishmentTypes(luigi.Task):
  def requires(self):
    return TXT2JSON()

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/establishment_listing.db'))

  def run(self):
    tables = ['listing_estabtypes', 'estabtypes']
    join_keys = ['establishment_type_id']
    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input().path),
      mapper=JoinMapper(tables=tables, join_keys=join_keys),
      reducer=JoinEstablishmentTypesReducer(),
      output_prefix=self.output().path,
      num_shards=10)

class ListingJoinReducer(parallel.Reducer):
  def _join(self, values):
    intermediate = parallel.pivot_values(values)
    result = []

    for row in intermediate.get('remapped_registration_listing', []):
      final = dict(row)
      final['products'] = intermediate.get('listing_pcd', [])
      final['proprietary_name'] = intermediate.get('listing_proprietary_name', [])
      result.append(final)

    return result

  def reduce(self, key, values, output):
    new_key = 'intermediate_registration_listing:' + key
    val = self._join(values)
    output.put(new_key, val)

class JoinListings(luigi.Task):
  def requires(self):
    return TXT2JSON()

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/registration_listing.db'))

  def run(self):
    tables = [
      'remapped_registration_listing',
      'listing_pcd',
      'listing_proprietary_name']
    join_keys = ['key_val']

    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input().path),
      mapper=JoinMapper(tables=tables, join_keys=join_keys),
      reducer=ListingJoinReducer(),
      output_prefix=self.output().path,
      num_shards=10)

class JoinAllReducer(parallel.Reducer):
  def _join(self, values):
    intermediate = parallel.pivot_values(values)
    result = []

    for data in intermediate.get('intermediate_registration_listing', []):
      final = dict(data)
      final['establishment_type'] = []
      final['registration'] = []
      final['proprietary_name'] = []

      # De-dup the proprietary names
      for prop_name in data.get('proprietary_name', []):
        name = prop_name.get('proprietary_name', None)
        if name and name not in final['proprietary_name']:
          final['proprietary_name'].append(name)

      est_join = ['registration_listing_id']
      reg_join = ['reg_key']

      est_left_key = construct_join_key(data, est_join)
      reg_left_key = construct_join_key(final, reg_join)

      # Grab just the descriptions of the establishment type
      for row in intermediate.get('intermediate_establishment_listing', []):
        est_right_key = construct_join_key(row, est_join)
        if est_left_key == est_right_key:
          final['establishment_type'].append(row['description'])

      # There is only one registered facility
      registrant = {}
      facility = intermediate.get('intermediate_registration', [])
      if facility:
        registrant = facility[0]
      final['registration'] = registrant

      result.append(final)

    return result

  def reduce(self, key, values, output):
    # There are  lot of keys that we do not need once all the joining has been
    # done, so we can now transform the output and remove the join keys and
    # changes some of the names to be in line with naming conventions.

    IGNORE = ['key_val',
      'address_id',
      'address_type_id',
      'contact_id',
      'reg_key',
      'listing_prop_id',
      'listing_prop_name_id',
      'registration_listing_id',
      'establishment'
    ]

    RENAME_MAP = {
      'address_line1': 'address_1',
      'address_line2': 'address_2',
      'reg_status_id': 'status_code',
      'state_id': 'state_code'
    }

    EXPANSION_MAP = {
      'establishment_type': 'establishment_type_exact',
      'proprietary_name': 'proprietary_name_exact'
    }

    def _prune(k, v):
      ''' A helper function used for removing and renaming dictionary keys.
      '''
      if k in IGNORE: return None
      if k in RENAME_MAP:
        k = RENAME_MAP[k]
      if k in EXPANSION_MAP:
        ek, ev = EXPANSION_MAP[k], v
        return [(k, v), (ek, ev)]
      return (k, v)

    key_prefix = 'final_result:' + key
    val = self._join(values)
    if val:
      for i, row in enumerate(val):
        new_key = key_prefix + ':' + str(i)
        new_value = common.transform_dict(row, _prune)
        output.put(new_key, new_value)

class JoinAll(luigi.Task):
  def requires(self):
    return [JoinListings(), JoinEstablishmentTypes(), JoinRegistration()]

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/final.db'))

  def run(self):
    tables = [
      'intermediate_registration_listing',
      'intermediate_establishment_listing',
      'intermediate_registration'
    ]
    join_keys = ['reg_key']
    db_list = [s.path for s in self.input()]

    parallel.mapreduce(
      parallel.Collection.from_sharded_list(db_list),
      mapper=JoinMapper(tables=tables, join_keys=join_keys),
      reducer=JoinAllReducer(),
      output_prefix=self.output().path,
      num_shards=10)

class RegistrationAnnotateDevice(DeviceAnnotateMapper):
  ''' The registration document has a unique placement requirement, so we need
      to override the `harmonize()` method to place the `openfda` section on
      each product in the `products` list within the document.
  '''
  def harmonize(self, data):
    result = dict(data)
    k_number = data.get('k_number', None)
    pma_number = data.get('pma_number', None)

    product_list = []
    for row in result.get('products', []):
      d = dict(row)
      harmonized = self.filter(row, k_num=k_number, p_num=pma_number)
      if harmonized:
        d['openfda'] = self.flatten(harmonized)
      else:
        d['openfda'] = {}
      product_list.append(d)
    result['products'] = product_list
    return result

  def filter(self, data, k_num=None, p_num=None):
    product_code = data['product_code']
    harmonized = self.harmonized_db.get(product_code, None)
    if harmonized:
      if self.table in harmonized:
        del harmonized[self.table]
      if k_num:
        k_numbers = list(harmonized['510k'])
        harmonized['510k'] = [d for d in k_numbers if d['k_number'] == k_num]
      else:
        harmonized['510k'] = []

      if p_num:
        pma_numbers = list(harmonized['device_pma'])
        harmonized['device_pma'] = \
          [d for d in pma_numbers if d['pma_number'] == p_num]
      else:
        harmonized['device_pma'] = []

      return harmonized
    return None

class AnnotateDevice(luigi.Task):
  def requires(self):
    return [Harmonized2OpenFDA(), JoinAll()]

  def output(self):
    return luigi.LocalTarget(config.data_dir('registration/annotate.db'))

  def run(self):
    harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      mapper=RegistrationAnnotateDevice(harmonized_db=harmonized_db),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=10)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'devicereglist'
  type_name = 'registration'
  mapping_file = './schemas/registration_mapping.json'
  data_source = AnnotateDevice()
  use_checksum = False
  optimize_index = True


if __name__ == '__main__':
  luigi.run()

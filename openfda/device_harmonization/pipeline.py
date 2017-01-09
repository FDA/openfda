#!/usr/bin/env python
''' A pipeline for joining together each of the device datasets into one
    harmonization database to be used by each pipeline's annotation step.

    The output of this process is a leveldb that contains a key of
    `product_code` and a value that is a dictionary that contains a key/list
    pair for each data source.
    e.g.
      ABC:
      {
        'registration': [dict1, dict2, ...],
        'device_pma': [dict1, dict2, ...],
        '510k': [dict1, dict2, ...],
        'classification': [dict1, dict2, ...],
      }

    The keys for each dictionary in the list are contained in the lists within
    the PLUCK_MAP.

    The `classification` list should always be filled and should only have one
    item.

    The rest of the keys are optional. It is usually the case that either
    the device_pma OR 510k is populated, since they each represent alternate
    business processes for the same effect, approval.

'''

import collections
import logging
import os
from os.path import basename, dirname, join
import sys

import luigi
import simplejson as json

from openfda import common, config, parallel
from openfda.parallel import mapreduce, Collection, Mapper, PivotReducer

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
DATA_DIR = config.data_dir('device_harmonization')
common.shell_cmd('mkdir -p %s', DATA_DIR)

PLUCK_MAP = {
  'device_pma': [
    'applicant',
    'trade_name',
    'generic_name',
    'decision_date',
    'advisory_committee',
    'pma_number',
    'product_code',
    'advisory_committee',
  ],
  '510k': [
    'applicant',
    'decision_date',
    'clearance_type',
    'k_number',
    'device_name',
    'decision_description',
    'advisory_committee',
    'review_advisory_committee',
    'third_party_flag',
    'expedited_review_flag',
    'product_code'
  ],
  'classification': [
    'device_name',
    'implant_flag',
    'review_code',
    'gmp_exempt_flag',
    'submission_type_id',
    'review_panel',
    'medical_specialty',
    'medical_specialty_description',
    'third_party_flag',
    'life_sustain_support_flag',
    'regulation_number',
    'device_class',
    'product_code'
  ],
  'registration': [
    'proprietary_name',
    'establishment_type',
    'product_code',
    'created_date',
    'registration.owner_operator.firm_name',
    'registration.registration_number',
    'registration.fei_number',
    'registration.iso_country_code',
    'registration.status_code',
    'registration.name',
    'registration.reg_expiry_date_year'
  ]
}


def pluck(pluck_list, data):
  ''' A helper function for extracting a specific subset of keys from a
      dictionary.
  '''
  # TODO(hansnelsen): If there is collision on a document key names, a better
  #                   way of naming needs to be found. It just so happens that
  #                   there is no collision on a key name for any particular
  #                   table.
  result = {}
  data = common.ObjectDict(data)
  for key in pluck_list:
    new_key = key.split('.')[-1]
    try:
      result[new_key] = data.get_nested(key)
    except:
      logging.warn('Could not get key: '+key)
  return result


class JoinMapper(Mapper):
  ''' A mapper that extracts the `product_code` from each input and streams
      out the value with `product_code` as the key. Each value object is
      transformed via `pluck()` before it is streamed to the reducer.

  '''
  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename
    self.table = basename(dirname(dirname(self.filename)))

    assert (self.table in PLUCK_MAP), \
      'Could not find %s in PLUCK_MAP' % self.table

    self.pluck_list = PLUCK_MAP[self.table]

    return Mapper.map_shard(self, map_input, map_output)

  def map(self, key, value, output):
    if not isinstance(value, list): value = [value]
    for val in value:
      # Registration is a special case, in that it contains many product codes
      # that we want to stream out of the Mapper.
      # As such, we want to hoist each product out and merge it with all of
      # its respective registration data.
      if self.table == 'registration':
        products = val.get('products', [])
        for product in products:
          new_key = product['product_code']
          new_value = dict(val.items() + product.items())
          if new_key:
            output.add(new_key, (self.table, pluck(self.pluck_list, new_value)))
      else:
        new_key = val['product_code']
        if new_key:
          output.add(new_key, (self.table, pluck(self.pluck_list, val)))


class DeviceAnnotateMapper(parallel.Mapper):
  def __init__(self, harmonized_db):
    parallel.Mapper.__init__(self)
    self.harmonized_db = harmonized_db

  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename
    self.table = basename(dirname(dirname(self.filename)))
    # TODO(hansnelsen): clean up the inconsistent naming of device_clearance,
    #                   it should be device_clearance every where.
    if self.table == 'device_clearance': self.table = '510k'
    return parallel.Mapper.map_shard(self, map_input, map_output)

  def flatten(self, harmonized):
    ''' Helper function which flattens a filtered harmonized db entry.

        Moves any key/values from the `classification` dictionary to the
        top-level and anything else gets added to a list of values.
    '''
    result = collections.defaultdict(set)
    for key, val in harmonized.items():
      if key == 'classification':
        try:
          for k, v in val[0].items():
            result[k] = v
        except IndexError:
          continue
      else:
        for row in val:
          for k, v in row.items():
            result[k].add(v)

    return {k:list(v) if isinstance(v, set) else v for k, v in result.items()}

  def filter(self, data, lookup=None):
    ''' This is the standard filter for Device Annnotation. It may be
        appropriate to override this when the data does not have a
        `product_code` and/or there is special logic for the filter.

        A filter should only filter out items from the harmonized_db entry,
        not alter it. The standard case is to remove the current table from
        the harmonized_db entry so there is not duplicate data in `openfda`.
    '''
    product_code = data['product_code']
    harmonized = self.harmonized_db.get(product_code, None)
    if harmonized:
      if self.table in harmonized:
        del harmonized[self.table]
      return harmonized
    return None

  def harmonize(self, data):
    ''' A function for placing the `openfda` section in the appropriate place.
        The default is just to put it at the top level. If there are more
        unique requirements, this function should be over-ridden in a custom
        mapper.

        The `harmonize()` should call `self.filter()` and `self.flatten()`
        appropriately.
    '''
    result = dict(data)
    harmonized = self.filter(result)
    if harmonized:
      result['openfda'] = self.flatten(harmonized)
    else:
      result['openfda'] = {}
    return result

  def map(self, key, value, output):
    if not value: return
    if not isinstance(value, list): value = [value]
    for val in value:
      new_value = self.harmonize(val)
      output.add(self.filename + ':' + key, new_value)


class DeviceHarmonization(luigi.Task):
  ''' There will be some namespace collision since the inputs come from the
      files listed below and the output of this process will be consumed by
      each of these files during their respective annotation steps. It seems
      cleaner to contain all the namespace ugliness in one place, here.
  '''
  def requires(self):
    from openfda.classification import pipeline as classification_pipeline
    from openfda.device_clearance import pipeline as clearance_pipeline
    from openfda.device_pma import pipeline as device_pma_pipeline
    from openfda.registration import pipeline as registration_pipeline
    return [classification_pipeline.Classification2JSON(),
            clearance_pipeline.Clearance2JSON(),
            device_pma_pipeline.Pma2JSON(),
            registration_pipeline.JoinAll()]

  def output(self):
    return luigi.LocalTarget(join(DATA_DIR, 'harmonized.db'))

  def run(self):
    db_list = [s.path for s in self.input()]
    mapreduce(
      Collection.from_sharded_list(db_list),
      mapper=JoinMapper(),
      reducer=PivotReducer(),
      output_prefix=self.output().path,
      num_shards=10)


class Harmonized2OpenFDAMapper(parallel.Mapper):
  ''' A simple mapper that strips out all unwanted keys from the harmonized.db
  '''
  def __init__(self):
    parallel.Mapper.__init__(self)
    # openfda_map represents the fields that will end up in the openfda
    # section of a document during the annotation step.
    self.openfda_map = {
      'device_pma': [
        'pma_number'
      ],
      '510k': [
        'k_number'
      ],
      'classification': [
        'device_name',
        'regulation_number',
        'device_class',
        'medical_specialty_description'
      ],
      'registration': [
        'registration_number',
        'fei_number',
        'k_number',
        'pma_number'
      ]
    }

  def openfda_pluck(self, table, data):
    ''' A helper function for extracting a specific subset of keys from a
        dictionary.
    '''
    return {k: v for k, v in data.items() if k in self.openfda_map[table]}

  def map(self, key, value, output):
    new_value = collections.defaultdict(list)
    for table_name, rows in value.items():
      for row in rows:
        new_value[table_name].append(self.openfda_pluck(table_name, row))
    output.add(key, new_value)


class Harmonized2OpenFDA(luigi.Task):
   def requires(self):
     return DeviceHarmonization()

   def output(self):
     return luigi.LocalTarget(join(DATA_DIR, 'openfda.db'))

   def run(self):
     mapreduce(Collection.from_sharded(self.input().path),
       mapper=Harmonized2OpenFDAMapper(),
       reducer=parallel.IdentityReducer(),
       output_prefix=self.output().path,
       num_shards=1)

if __name__ == '__main__':
  luigi.run()

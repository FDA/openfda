#!/usr/local/bin/python

'''
Pipeline for converting CSV nsde data to JSON and importing into Elasticsearch.
'''

import logging
import os
import sys
from os.path import join, dirname

import arrow
import luigi
import pandas as pd

from openfda import common, config, parallel, index_util

reload(sys)
sys.setdefaultencoding('utf-8')

NSDE_DOWNLOAD = \
  'https://www.fda.gov/downloads/ForIndustry/DataStandards/StructuredProductLabeling/UCM363568.zip'


NSDE_EXTRACT_DB = 'nsde/nsde.db'


class DownloadNSDE(luigi.Task):
  csv_file_name = "Comprehensive NDC SPL Data Elements File.csv"

  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(config.data_dir('nsde/raw/nsde_raw.json'))

  def run(self):
    logging.basicConfig(level=logging.INFO)

    zip_filename = config.data_dir('nsde/raw/nsde.zip')
    output_dir = config.data_dir('nsde/raw')
    os.system('mkdir -p %s' % output_dir)
    common.download(NSDE_DOWNLOAD, zip_filename)
    os.system('unzip -o %(zip_filename)s -d %(output_dir)s' % locals())

    csv_file = join(output_dir, self.csv_file_name)
    logging.info("Reading csv file: %s", (csv_file))
    os.system('mkdir -p %s' % dirname(self.output().path))
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    df.to_json(self.output().path, orient='records')
    with open(self.output().path, "w") as f:
      for row in df.iterrows():
        row[1].to_json(f)
        f.write("\n")


class NSDE2JSONMapper(parallel.Mapper):
  rename_map = {
    "Item Code": "package_ndc",
    "NDC11": "package_ndc11",
    "Marketing Category": "marketing_category",
    "Marketing Start Date": "marketing_start_date",
    "Marketing End Date": "marketing_end_date",
    "Billing Unit": "billing_unit",
    "Proprietary Name": "proprietary_name",
    "Dosage Form": "dosage_form",
    "Application Number or Citation": "application_number_or_citation",
    "Product Type": "product_type"
  }

  def map(self, key, value, output):
    def _cleaner(k, v):
      ''' Helper function to rename keys and purge any keys that are not in
          the map.
      '''
      # See https://github.com/FDA/openfda-dev/issues/6
      # Handle bad Unicode characters that may come fron NDC product.txt.
      if isinstance(v, str):
        v = unicode(v, 'utf8', 'ignore').encode()

      if k in self.rename_map and v is not None:
        if "Date" in k:
          return (self.rename_map[k], str(int(v)))
        if "Proprietary Name" in k:
          return (self.rename_map[k], v.title())
        else:
          return (self.rename_map[k], v)

    new_value = common.transform_dict(value, _cleaner)
    output.add(key, new_value)


class NSDE2JSON(luigi.Task):
  def requires(self):
    return DownloadNSDE()

  def output(self):
    return luigi.LocalTarget(config.data_dir(NSDE_EXTRACT_DB))

  def run(self):
    parallel.mapreduce(
        parallel.Collection.from_glob(
          self.input().path, parallel.JSONLineInput()),
        mapper=NSDE2JSONMapper(),
        reducer=parallel.IdentityReducer(),
        output_prefix=self.output().path,
        num_shards=1)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'othernsde'
  type_name = 'othernsde'
  mapping_file = './schemas/othernsde_mapping.json'
  data_source = NSDE2JSON()
  use_checksum = False
  optimize_index = True


if __name__ == '__main__':
  luigi.run()

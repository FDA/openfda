#!/usr/bin/python

"""
Execution pipeline for generating the openfda harmonization json file
(aka the annotation table).
"""

import logging
import luigi
import os
from os.path import dirname, join
import sys

from openfda.annotation_table import combine_harmonization
from openfda.annotation_table import rxnorm_harmonization
from openfda.annotation_table import unii_harmonization
from openfda.spl import spl_harmonization

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))

# Make sure this is set to the proper symlink (needs space)
# TODO(mattmo): Make a flag option.
BASE_DATA_DIR = '/mnt/ssd0/'

# TODO(hansnelsen): Refactor setup_data.sh into luigi.
class PlaceHolderDataInit(luigi.Task):
  def __init__(self):
    luigi.Task.__init__(self)

  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget('/tmp/harmonization_data_setup.done')

  def run(self):
    os.system(join(RUN_DIR, 'annotation_table/setup_data.sh'))
    os.system('touch "%s"' % self.output().path)

class DownloadSPL(luigi.Task):
  def requires(self):
    return PlaceHolderDataInit()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR, 'spl/raw'))

  def run(self):
    pass

class DownloadNDC(luigi.Task):
  def requires(self):
    return PlaceHolderDataInit()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR, 'ndc/raw'))

  def run(self):
    pass

class DownloadUNII(luigi.Task):
  def requires(self):
    return PlaceHolderDataInit()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR, 'unii/raw'))

  def run(self):
    pass

class DownloadRXNorm(luigi.Task):
  def requires(self):
    return PlaceHolderDataInit()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR, 'rxnorm/raw'))

  def run(self):
    pass

class ExtractZipNDC(luigi.Task):
  def requires(self):
    return DownloadNDC()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR, 'ndc/extracted/product.txt'))

  def run(self):
    pass

class ExtractZipRXNorm(luigi.Task):
  def requires(self):
    return DownloadRXNorm()
  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR,
      'rxnorm/extracted/rxnorm_mappings.txt'))
  def run(self):
    pass

class ExtractZipUNII(luigi.Task):
  def requires(self):
    return DownloadUNII()
  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR, 'unii/extracted'))
  def run(self):
    pass

class ExtractZipSPL(luigi.Task):
  def requires(self):
    return DownloadSPL()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR, 'spl/extracted'))

  def run(self):
    pass

class RXNormHarmonizationJSON(luigi.Task):
  def requires(self):
    return ExtractZipRXNorm()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR,
      'harmonization/rxnorm_extract.json'))

  def run(self):
    rxnorm_file = self.input().path
    output_file = self.output().path
    rxnorm_harmonization.harmonize_rxnorm(rxnorm_file, output_file)

class UNIIHarmonizationJSON(luigi.Task):
  def requires(self):
    return [ExtractZipNDC(), ExtractZipUNII()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR,
      'harmonization/unii_extract.json'))

  def run(self):
    ndc_file = self.input()[0].path
    unii_dir = self.input()[1].path
    output_file = self.output().path
    unii_harmonization.harmonize_unii(output_file, ndc_file, unii_dir)

class SPLHarmonizationJSON(luigi.Task):
  def requires(self):
    return ExtractZipSPL()

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR,
      'harmonization/spl_extract.json'))

  def run(self):
    spl_path = self.input().path
    output_file = self.output().path
    spl_harmonization.harmonize_spl(spl_path, output_file)

class CombineHarmonization(luigi.Task):
  def requires(self):
    return [ SPLHarmonizationJSON(), UNIIHarmonizationJSON(),
             ExtractZipNDC(), RXNormHarmonizationJSON() ]

  def output(self):
    return luigi.LocalTarget(join(BASE_DATA_DIR,
      'harmonization/harmonized.json'))

  def run(self):
    spl_file = self.input()[0].path
    unii_file = self.input()[1].path
    ndc_file = self.input()[2].path
    rxnorm_file = self.input()[3].path
    json_output_file = self.output().path
    combine_harmonization.combine(ndc_file,
                                  spl_file,
                                  rxnorm_file,
                                  unii_file,
                                  json_output_file)

if __name__ == '__main__':
  logging.basicConfig(
    stream=sys.stderr,
    format='%(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s',
    level=logging.DEBUG)

  luigi.run()

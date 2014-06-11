#!/usr/bin/python

"""
Execution pipeline for generating the openfda harmonization json file
(aka the annotation table).
"""

from bs4 import BeautifulSoup
import glob
import logging
import luigi
import multiprocessing
import os
from os.path import basename, dirname, join
import re
import subprocess
import sys
import urllib2

from openfda.annotation_table import combine_harmonization
from openfda.annotation_table import rxnorm_harmonization
from openfda.annotation_table import unii_harmonization
from openfda.spl import spl_harmonization


RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data/'


SPL_DOWNLOADS = [
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part1.zip',
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part2.zip',
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part1.zip',
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part2.zip',
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part3.zip',
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_homeopathic.zip'
]

PHARM_CLASS_DOWNLOAD = \
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/pharmacologic_class_indexing_spl_files.zip'

RXNORM_DOWNLOAD = \
  'ftp://public.nlm.nih.gov/nlmdata/.dailymed/rxnorm_mappings.zip'

NDC_DOWNLOAD_PAGE = \
  'http://www.fda.gov/drugs/informationondrugs/ucm142438.htm'


def download(url, output_filename):
  os.system('mkdir -p %s' % dirname(output_filename))
  os.system("curl '%s' > '%s'" % (url, output_filename))


class DownloadSPL(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'spl/raw'))

  def run(self):
    for url in SPL_DOWNLOADS:
      filename = join(self.output().path, url.split('/')[-1])
      download(url, filename)


class DownloadNDC(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'ndc/raw/ndc_database.zip'))

  def run(self):
    zip_url = None
    soup = BeautifulSoup(urllib2.urlopen(NDC_DOWNLOAD_PAGE).read())
    for a in soup.find_all(href=re.compile('.*.zip')):
      if 'NDC Database File' in a.text:
        zip_url = 'http://www.fda.gov' + a['href']
        break

    if not zip_url:
      logging.fatal('NDC database file not found!')

    download(zip_url, self.output().path)


class DownloadUNII(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'unii/raw/pharmacologic_class.zip'))

  def run(self):
    download(PHARM_CLASS_DOWNLOAD, self.output().path)


class DownloadRXNorm(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'rxnorm/raw/rxnorm_mappings.zip'))

  def run(self):
    download(RXNORM_DOWNLOAD, self.output().path)


def ListZipFilesInZip(zip_filename):
  return subprocess.check_output("unzip -l %s | grep zip | awk '{print $4}'" % zip_filename,
                                 shell=True).strip().split('\n')


def ListXMLFileInZip(zip_filename):
  return subprocess.check_output("unzip -l %s | grep xml | awk '{print $4}'" % zip_filename,
                                 shell=True).strip()


def ExtractXMLFromNestedZip(zip_filename, output_dir):
  for child_zip_filename in ListZipFilesInZip(zip_filename):
    temp_zip_filename = '%(output_dir)s/child.zip' % locals()

    cmd = 'unzip -p %(zip_filename)s %(child_zip_filename)s > %(temp_zip_filename)s' % locals()
    os.system(cmd)

    xml_filename = ListXMLFileInZip(temp_zip_filename)
    cmd = 'unzip -p %(temp_zip_filename)s %(xml_filename)s > %(output_dir)s/%(xml_filename)s' % locals()
    os.system(cmd)

    os.system('rm %(temp_zip_filename)s' % locals())


class ExtractNDC(luigi.Task):
  def requires(self):
    return DownloadNDC()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'ndc/extracted/product.txt'))

  def run(self):
    zip_filename = self.input().path
    output_filename = self.output().path
    os.system('mkdir -p %s' % dirname(self.output().path))
    cmd = 'unzip -p %(zip_filename)s product.txt > %(output_filename)s' % locals()
    os.system(cmd)


class ExtractRXNorm(luigi.Task):
  def requires(self):
    return DownloadRXNorm()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR,
      'rxnorm/extracted/rxnorm_mappings.txt'))

  def run(self):
    zip_filename = self.input().path
    output_filename = self.output().path
    os.system('mkdir -p %s' % dirname(self.output().path))
    cmd = 'unzip -p %(zip_filename)s rxnorm_mappings.txt > %(output_filename)s' % locals()
    os.system(cmd)


class ExtractUNII(luigi.Task):
  def requires(self):
    return DownloadUNII()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'unii/extracted'))

  def run(self):
    zip_filename = self.input().path
    output_dir = self.output().path
    os.system('mkdir -p %s' % output_dir)
    ExtractXMLFromNestedZip(zip_filename, output_dir)


class ExtractSPL(luigi.Task):
  def requires(self):
    return DownloadSPL()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'spl/extracted'))

  def run(self):
    pool = multiprocessing.Pool(processes=8)

    src_dir = self.input().path
    for zip_filename in glob.glob(src_dir + '/*.[Zz][Ii][Pp]'):
      zip_dir = basename(zip_filename).split('.')[0] + '_xml'
      output_dir = join(self.output().path, zip_dir)
      os.system('mkdir -p %s' % output_dir)
      pool.apply_async(ExtractXMLFromNestedZip,
                       args=(zip_filename, output_dir))

    pool.close()
    pool.join()


class RXNormHarmonizationJSON(luigi.Task):
  def requires(self):
    return ExtractRXNorm()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR,
      'harmonization/rxnorm_extract.json'))

  def run(self):
    rxnorm_file = self.input().path
    output_file = self.output().path
    os.system('mkdir -p %s' % dirname(self.output().path))
    rxnorm_harmonization.harmonize_rxnorm(rxnorm_file, output_file)


class UNIIHarmonizationJSON(luigi.Task):
  def requires(self):
    return [ExtractNDC(), ExtractUNII()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR,
      'harmonization/unii_extract.json'))

  def run(self):
    ndc_file = self.input()[0].path
    unii_dir = self.input()[1].path
    output_file = self.output().path
    os.system('mkdir -p %s' % dirname(self.output().path))
    unii_harmonization.harmonize_unii(output_file, ndc_file, unii_dir)


class SPLHarmonizationJSON(luigi.Task):
  def requires(self):
    return ExtractSPL()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR,
      'harmonization/spl_extract.json'))

  def run(self):
    spl_path = self.input().path
    output_file = self.output().path
    os.system('mkdir -p %s' % dirname(self.output().path))
    spl_harmonization.harmonize_spl(spl_path, output_file)


class CombineHarmonization(luigi.Task):
  def requires(self):
    return [ SPLHarmonizationJSON(), UNIIHarmonizationJSON(),
             ExtractNDC(), RXNormHarmonizationJSON() ]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR,
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

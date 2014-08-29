#!/usr/bin/python

''' A pipeline for loading SPL data into ElasticSearch
'''

import glob
import logging
import multiprocessing
import os
from os.path import join, dirname
import requests
import simplejson as json
import sys

import luigi

from openfda.annotation_table.pipeline import CombineHarmonization, ExtractSPL
from openfda.spl import annotate


RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = './data'
META_DIR = join(BASE_DIR, 'spl/meta')

SPL_JS = join(RUN_DIR, 'spl/spl_to_json.js')
LOINC = join(RUN_DIR, 'spl/data/sections.csv')

# Ensure meta directory is available for task tracking
os.system('mkdir -p %s' % META_DIR)

class SPL2JSON(luigi.Task):
  def requires(self):
    return ExtractSPL()

  def output(self):
    return [luigi.LocalTarget(join(META_DIR, 'spl2json.done')),
            luigi.LocalTarget(join(BASE_DIR, 'spl/json/all.json'))]

  def run(self):
    input_dir = self.input().path
    output_file = self.output()[1].path
    output_dir = dirname(output_file)
    os.system('mkdir -p %s' % output_dir)
    spl_js = SPL_JS
    loinc = LOINC
    pool = multiprocessing.Pool(processes=16)
    for xml in glob.glob(input_dir + '/*/*/*.[Xx][Mm][Ll]'):
      json = xml.replace('.xml', '.json')
      cmd = 'node %(spl_js)s %(xml)s %(loinc)s > %(json)s' % locals()
      pool.apply_async(os.system, (cmd,))
    pool.close()
    pool.join()

    # Combine all of the json files into one
    cmd = 'find %(input_dir)s -name "*.json" ' % locals()
    cmd += "-exec cat {} >> %(output_file)s \;" % locals()
    os.system(cmd)
    os.system('touch "%s"' % self.output()[0].path)

class AnnotateJSON(luigi.Task):
  def requires(self):
    return [SPL2JSON(), CombineHarmonization()]

  def output(self):
    return [luigi.LocalTarget(join(META_DIR, 'annotatejson.done')),
            luigi.LocalTarget(join(BASE_DIR, 'spl/annotated'))]

  def run(self):
    input_file = self.input()[0][1].path
    harmonized_file = self.input()[1].path
    output_dir = self.output()[1].path
    output_file = join(output_dir, 'all.json')
    os.system('mkdir -p %s' % output_dir)
    label = annotate.AnnotateMapper(harmonized_file)
    label(input_file, output_file)
    os.system('touch "%s"' % self.output()[0].path)

class ResetElasticSearch(luigi.Task):
  def requires(self):
    return AnnotateJSON()

  def output(self):
    return luigi.LocalTarget(join(META_DIR, 'resetelasticsearch.done'))

  def run(self):
    cmd = join(RUN_DIR, 'spl/clear_and_load_mapping.sh')
    logging.info('Running Shell Script:' + cmd)
    os.system(cmd)
    os.system('touch "%s"' % self.output().path)

# TODO(hansnelsen): Look to consolidate this code into a shared library. It is
# used by all pipeline.py. RES and SPL are slightly different, in that it
# writes to JSON instead of LevelDB
def load_json(input_file):
  label_json = open(input_file, 'r')
  json_batch = []
  def _post_batch():
    response = \
    requests.post('http://localhost:9200/druglabel/spl/_bulk',
                  data='\n'.join(json_batch))
    del json_batch[:]
    if response.status_code != 200:
      logging.info('Bad response: %s', response)

  for row in label_json:
    label_raw = json.loads(row)
    json_batch.append('{ "index" : {} }')
    json_batch.append(json.dumps(label_raw))
    if len(json_batch) > 1000:
      _post_batch()

  _post_batch()

class LoadJSON(luigi.Task):
  def requires(self):
    return [ResetElasticSearch(), AnnotateJSON()]

  def output(self):
    return luigi.LocalTarget(join(META_DIR, 'loadjson.done'))

  def run(self):
    input_json = glob.glob(self.input()[1][1].path + '/all.json')
    for filename in input_json:
      load_json(filename)
    os.system('touch "%s"' % self.output().path)

if __name__ == '__main__':
  fmt_string = '%(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s'
  logging.basicConfig(stream=sys.stderr,
                      format=fmt_string,
                      level=logging.DEBUG)
  luigi.run()

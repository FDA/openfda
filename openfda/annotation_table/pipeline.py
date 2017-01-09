#!/usr/bin/env python

"""
Execution pipeline for generating the openfda harmonization json file
(aka the annotation table).
"""

from bs4 import BeautifulSoup
import collections
import glob
import logging
import luigi
import os
from os.path import basename, dirname, join
import re
import subprocess
import urllib2
import urlparse

import arrow
import simplejson as json

from openfda.tasks import DependencyTriggeredTask
from openfda import common, config, parallel, spl
from openfda.annotation_table import unii_harmonization
from openfda.spl import process_barcodes, extract

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
data_dir = config.data_dir('harmonization')
BATCH_DATE = arrow.utcnow().ceil('week').format('YYYYMMDD')
BASE_DIR = config.data_dir('harmonization/batches/%s' % BATCH_DATE)
SPL_S3_DIR = config.data_dir('spl/s3_sync')
TMP_DIR = config.tmp_dir()

common.shell_cmd('mkdir -p %s', data_dir)
common.shell_cmd('mkdir -p %s', BASE_DIR)
common.shell_cmd('mkdir -p %s', TMP_DIR)

SPL_SET_ID_INDEX = join(BASE_DIR, 'spl_index.db')
DAILYMED_PREFIX = 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/'

PHARM_CLASS_DOWNLOAD = \
  DAILYMED_PREFIX + 'pharmacologic_class_indexing_spl_files.zip'

RXNORM_DOWNLOAD = \
  DAILYMED_PREFIX + 'rxnorm_mappings.zip'

NDC_DOWNLOAD_PAGE = \
  'http://www.fda.gov/drugs/informationondrugs/ucm142438.htm'

# The database names play a central role in terms of output and the joiner.
# For instance: SPL_EXTRACT_DB is used as both an output of a download/json/etc
# pipeline AND it is used as input into a join mapreduce() AND it is used
# as a source identifier on a mapper output record for the actural JoinReducer.
SPL_EXTRACT_DB = 'spl_extract.db'
NDC_EXTRACT_DB = 'ndc.db'
UNII_EXTRACT_DB = 'unii.db'
RXNORM_EXTRACT_DB = 'rxnorm.db'
UPC_EXTRACT_DB = 'upc.db'


class DownloadNDC(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'ndc/raw/ndc_database.zip'))

  def run(self):
    zip_url = None
    soup = BeautifulSoup(urllib2.urlopen(NDC_DOWNLOAD_PAGE).read(), 'lxml')
    for a in soup.find_all(href=re.compile('.*.zip')):
      if 'NDC Database File' in a.text:
        zip_url = urlparse.urljoin('http://www.fda.gov', a['href'])
        break

    if not zip_url:
      logging.fatal('NDC database file not found!')

    common.download(zip_url, self.output().path)


class DownloadUNII(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'unii/raw/pharmacologic_class.zip'))

  def run(self):
    common.download(PHARM_CLASS_DOWNLOAD, self.output().path)


class DownloadRXNorm(luigi.Task):
  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'rxnorm/raw/rxnorm_mappings.zip'))

  def run(self):
    common.download(RXNORM_DOWNLOAD, self.output().path)


def list_zip_files_in_zip(zip_filename):
  return subprocess.check_output("unzip -l %s | \
                                  grep zip | \
                                  awk '{print $4}'" % zip_filename,
                                  shell=True).strip().split('\n')


def ExtractXMLFromNestedZip(zip_filename, output_dir, exclude_images=True):
  for child_zip_filename in list_zip_files_in_zip(zip_filename):
    base_zip = basename(child_zip_filename)
    target_dir = base_zip.split('.')[0]
    cmd = 'unzip -j -d %(output_dir)s/%(target_dir)s \
                       %(zip_filename)s \
                       %(child_zip_filename)s' % locals()
    os.system(cmd)

    cmd = 'unzip %(output_dir)s/%(target_dir)s/%(base_zip)s -d \
                   %(output_dir)s/%(target_dir)s' % locals()
    if exclude_images:
      cmd += ' -x *.jpg'

    os.system(cmd)
    os.system('rm %(output_dir)s/%(target_dir)s/%(base_zip)s' % locals())


class ExtractNDC(luigi.Task):
  def requires(self):
    return DownloadNDC()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'ndc/extracted/product.txt'))

  def run(self):
    zip_filename = self.input().path
    output_filename = self.output().path
    os.system('mkdir -p %s' % dirname(self.output().path))
    cmd = 'unzip -p %(zip_filename)s product.txt > \
                    %(output_filename)s' % locals()
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
    cmd = 'unzip -p %(zip_filename)s rxnorm_mappings.txt > \
                    %(output_filename)s' % locals()
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


class RXNorm2JSONMapper(parallel.Mapper):
  def map(self, key, value, output):
    keepers = ['rxcui', 'rxstring', 'rxtty','setid', 'spl_version']
    rename_map = {
      'setid': 'spl_set_id'
    }
    def _cleaner(k, v):
      k = k.lower()
      if k in keepers:
        if k in rename_map:
          return (rename_map[k], v)
        return (k, v)
    new_value = common.transform_dict(value, _cleaner)
    new_key = new_value['spl_set_id'] + ':'+ new_value['spl_version']
    output.add(new_key, new_value)


class RXNormReducer(parallel.Reducer):
  def reduce(self, key, values, output):
    out = {}
    out['spl_set_id'] = values[0]['spl_set_id']
    out['spl_version'] = values[0]['spl_version']
    out['rxnorm'] = []
    for v in values:
      out['rxnorm'].append({ 'rxcui' : v['rxcui'],
                             'rxstring' : v['rxstring'],
                             'rxtty' : v['rxtty'] })
    output.put(key, out)


class RXNorm2JSON(luigi.Task):
  ''' The Rxnorm data needs to be rolled up to the SPL_SET_ID level, so first
      we clean the data and group it with a ListReducer.
  '''
  def requires(self):
    return ExtractRXNorm()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, RXNORM_EXTRACT_DB))

  def run(self):
    parallel.mapreduce(
        parallel.Collection.from_glob(
          self.input().path, parallel.CSVDictLineInput(delimiter='|')),
        mapper=RXNorm2JSONMapper(),
        reducer=RXNormReducer(),
        output_prefix=self.output().path,
        num_shards=10)

# TODO(hansnelsen): refactor this task into a join reduction. Leaving it in
#                   place for now since the logic is a bit harry. Ideally, we
#                   go directly to leveldb, avoiding the JSON step.
class UNIIHarmonizationJSON(luigi.Task):
  def requires(self):
    return [ExtractNDC(), ExtractUNII()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'unii_extract.json'))

  def run(self):
    ndc_file = self.input()[0].path
    unii_dir = self.input()[1].path
    output_file = self.output().path
    os.system('mkdir -p %s' % dirname(self.output().path))
    unii_harmonization.harmonize_unii(output_file, ndc_file, unii_dir)


class UNII2JSON(luigi.Task):
  def requires(self):
    return UNIIHarmonizationJSON()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, UNII_EXTRACT_DB))

  def run(self):
    parallel.mapreduce(
        parallel.Collection.from_glob(
          self.input().path, parallel.JSONLineInput()),
        mapper=parallel.IdentityMapper(),
        reducer=parallel.IdentityReducer(),
        output_prefix=self.output().path,
        num_shards=1)


class NDC2JSONMapper(parallel.Mapper):
  rename_map = {
    'PRODUCTID': 'id',
    'APPLICATIONNUMBER': 'application_number',
    'PRODUCTTYPENAME': 'product_type',
    'NONPROPRIETARYNAME': 'generic_name',
    'LABELERNAME': 'manufacturer_name',
    'PROPRIETARYNAME': 'brand_name',
    'PROPRIETARYNAMESUFFIX': 'brand_name_suffix',
    'PRODUCTNDC': 'product_ndc',
    'DOSAGEFORMNAME': 'dosage_form',
    'ROUTENAME': 'route',
    'SUBSTANCENAME': 'substance_name',
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

      if k == 'PRODUCTID':
        v = v.split('_')[1]
      if k in self.rename_map:
        return (self.rename_map[k], v)

    new_value = common.transform_dict(value, _cleaner)
    output.add(key, new_value)

# TODO(hansnelsen): Refactor the UNII steps to not need NDC File. Once done,
#                   Everything can use this step. As it stands now, it is only
#                   used by the JoinAll() task.
class NDC2JSON(luigi.Task):
  def requires(self):
    return ExtractNDC()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'ndc', NDC_EXTRACT_DB))

  def run(self):
    parallel.mapreduce(
        parallel.Collection.from_glob(
          self.input().path, parallel.CSVDictLineInput(delimiter='\t')),
        mapper=NDC2JSONMapper(),
        reducer=parallel.IdentityReducer(),
        output_prefix=self.output().path,
        num_shards=1)


class SPLSetIDMapper(parallel.Mapper):
  ''' Creates an index that is used for both getting the latest version of the
      SPL AND for mapping the id to set_id, so that there can be a single
      reduction key (spl_set_id). The same SPL raw file is indexed twice, with
      a different prefix and a different key, because different downstream
      pipelines use different keys. We want to give all pipelines a way to get
      an SPL_SET_ID.
  '''
  def __init__(self, index_db):
    parallel.Mapper.__init__(self)
    self.index_db = index_db

  def map(self, key, value, output):
    set_id, version, _id = value['set_id'], value['version'], value['id']

    _index = {
      '_version': version,
      '_id': _id,
      '_set_id': set_id,
      '_key': key
    }

    # Limiting the output to the known universe of SPL IDs that exist in the
    # main join file (NDC).
    if _id in self.index_db:
      output.add('_set_id:' + set_id, (version, _index))
      output.add('_id:' + _id, (version, _index))


class SPLSetIDIndex(luigi.Task):
  ''' Creates an index used for SPL version resolution and ID/SET_ID mapping.
      This task has a cross-dependency upon the SPL pipeline. It returns a list
      of every json.db every made by the SPL pipeline.
  '''
  def requires(self):
    from openfda.spl.pipeline import PrepareBatch
    return [PrepareBatch(), NDC2JSON()]

  def output(self):
    return luigi.LocalTarget(SPL_SET_ID_INDEX)

  def run(self):
    ndc_spl_id_index = {}
    ndc_db = self.input()[1].path
    logging.info('Joining data from NDC DB: %s', ndc_db)
    db = parallel.ShardedDB.open(ndc_db)
    db_iter = db.range_iter(None, None)

    # We want each SPL ID that is in the NDC file so that we always use the
    # same SPL file for both ID and SET_ID based joins.
    for (key, val) in db_iter:
      ndc_spl_id_index[val['id']] = True


    parallel.mapreduce(
      parallel.Collection.from_sharded_list([batch.path for batch in self.input()[0]]),
      mapper=SPLSetIDMapper(index_db=ndc_spl_id_index),
      reducer=parallel.ListReducer(),
      output_prefix=self.output().path,
      num_shards=16)


class CurrentSPLMapper(parallel.Mapper):
  def _extract(self, filename):
    ''' Moving this code from the `spl_harmonization` file, since it is the
        only part of that file that is needed now that we have converted to a
        map reduction.`
    '''
    try:
      tree = extract.parse_xml(filename)
      harmonized = {}
      harmonized['spl_set_id'] = spl.extract.extract_set_id(tree)
      harmonized['id'] = spl.extract.extract_id(tree)
      harmonized['spl_version'] = spl.extract.extract_version_number(tree)
      harmonized['is_original_packager'] = \
        spl.extract.is_original_packager(tree)
      harmonized['spl_product_ndc'] = spl.extract.extract_product_ndcs(tree)
      harmonized['original_packager_product_ndc'] = \
        spl.extract.extract_original_packager_product_ndcs(tree)
      harmonized['package_ndc'] = spl.extract.extract_package_ndcs(tree)
      return harmonized
    except:
      logging.warn('ERROR processing SPL data: %s', filename)
      return None

  def map(self, key, value, output):
    if '_set_id:' in key:
      version, _index = sorted(value)[-1]
      xml_file = _index['_key']
      new_value = self._extract(xml_file)
      if new_value is not None:
        new_key = key.replace('_set_id:', '')
        output.add(new_key, new_value)


class GenerateCurrentSPLJSON(luigi.Task):
  ''' All SPL files on S3 (IDs), we only want the most recent versions. This
      task generates a spl_extract.db that is only the current version.
  '''
  def requires(self):
    return SPLSetIDIndex()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, SPL_EXTRACT_DB))

  def run(self):
    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input().path),
      mapper=CurrentSPLMapper(),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path)


class ExtractUPCFromSPL(luigi.Task):
  ''' A task that will generate otc-bars.xml for ANY SPL IDs that does not have
      one yet. There is a simple output file to represent that this process has
      run for this batch cycle.
  '''
  def requires(self):
    # Just need a task that forces an S3 sync, which GenerateCurrentSPLJSON does
    return GenerateCurrentSPLJSON()

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'barcode_update.done'))

  def run(self):
    for filename in glob.glob(SPL_S3_DIR + '/*/*.xml'):
      src_dir = dirname(filename)
      barcode_target = join(src_dir, 'barcodes')
      xml_out = join(barcode_target, 'otc-bars.xml')
      json_out = xml_out.replace('.xml', '.json')

      if not os.path.exists(xml_out):
        common.shell_cmd('mkdir -p %s', barcode_target)
        logging.info('Zbarimg on directory %s', src_dir)
        cmd = 'find %(src_dir)s -name "*.jpg" -size +0\
                                -exec zbarimg -q --xml {} \; > \
                    %(xml_out)s' % locals()
        os.system(cmd)

      if common.is_older(json_out, xml_out):
        logging.info('%s does not exist, producing...', json_out)
        process_barcodes.XML2JSON(xml_out)
      else:
        logging.debug('%s already exists, skipping', xml_out)
    common.shell_cmd('touch %s', self.output().path)


class UpcMapper(parallel.Mapper):
  def map(self, key, value, output):
    if value is None: return
    _id = value['id']
    # otc-bars.json contains new-line delimited JSON strings.  we output each
    # line along with it's id.
    upc_json = join(SPL_S3_DIR, _id, 'barcodes/otc-bars.json')
    if os.path.exists(upc_json):
      upcs = open(upc_json, 'r')
      for upc in upcs:
        upc_dict = json.loads(upc)
        output.add(upc_dict['id'], upc_dict)


class UpcXml2JSON(luigi.Task):
  def requires(self):
    return [ExtractUPCFromSPL(), GenerateCurrentSPLJSON()]

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, UPC_EXTRACT_DB))

  def run(self):
    input_glob = glob.glob(SPL_S3_DIR + '/*/barcodes/otc-bars.json')
    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      mapper=UpcMapper(),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path)

# The index key has a prefix to avoid collision, we need to data structure
# that tells us which prefix maps to which database and which value to pull
# out of that database in order to get the set_id from the index.
# For instance NDC_EXTRACT_DB:
#   If '_id', means we looking for an index key that starts with '_id:'
#   If 'id', grab the id from the value in the map()
#   We then concatenate these to find the index entry.
#   Which then results in a spl_set_id, which is our new output key.
ID_PREFIX_DB_MAP = {
  SPL_EXTRACT_DB: ('_set_id', 'spl_set_id'),
  NDC_EXTRACT_DB: ('_id', 'id'),
  UNII_EXTRACT_DB: ('_id', 'spl_id'),
  RXNORM_EXTRACT_DB: ('_set_id', 'spl_set_id'),
  UPC_EXTRACT_DB: ('_id', 'id')
}


class JoinAllMapper(parallel.Mapper):
  def __init__(self, index_db):
    parallel.Mapper.__init__(self)
    self.index_db = index_db

  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename
    return parallel.Mapper.map_shard(self, map_input, map_output)

  def get_set_id(self, lookup_value):
    ''' Helper function to pull the set_id from the first entry in the index.
    '''
    if lookup_value in self.index_db:
      index = self.index_db[lookup_value]
      set_id = index['_set_id']
      return set_id
    else:
      return None

  def map(self, key, value, output):
    db_name = basename(dirname(self.filename))
    prefix, lookup_key = ID_PREFIX_DB_MAP[db_name]
    if not value:
      logging.warn('Bad value for map input: %s, %s', db_name, key)
      return
    if not isinstance(value, list): value = [value]
    for val in value:
      lookup_value = prefix + ':' + val[lookup_key]
      set_id = self.get_set_id(lookup_value)
      if set_id:
        output.add(set_id, (db_name, val))
      else:
        logging.warn('Missing set id for %s', lookup_value)


class JoinAllReducer(parallel.Reducer):
  ''' A custom joiner for combining all of the data sources into a single
      unique record.
  '''
  def _join(self, values):
    # The mapper output values: (database_file, dictionary value)
    # Each database_file lines up with an instance variable so that we can
    # grab the appropriate data for the appropriate join.
    intermediate = collections.defaultdict(list)
    for row in values:
      db_name, val = row
      intermediate[db_name].append(val)

    result = []
    for ndc_row in intermediate[NDC_EXTRACT_DB]:
      for spl_row in intermediate[SPL_EXTRACT_DB]:
        result.append(dict(ndc_row.items() + spl_row.items()))

    final_result = []

    for row in result:
      final = dict(row)

      # If there is unii_indexing, there is only one record, so we make sure
      # something is there and grab the first one.
      final['unii_indexing'] = {}
      unii_data = intermediate.get(UNII_EXTRACT_DB, None)
      if unii_data:
        final['unii_indexing'] = unii_data[0]['unii_indexing']

      final['rxnorm'] = []
      for row in intermediate.get(RXNORM_EXTRACT_DB, []):
        for data in row['rxnorm']:
          final['rxnorm'].append(data)

      # TODO(hansnelsen): clean this up, it saves either a string or an empty
      #                   list. It is bad, but it will require a refactor of
      #                   annotation steps in each pipeline, so leaving it for
      #                   now.
      # If upc data exists, there is only one, so we make sure it is there and
      # then grab the first one.
      final['upc'] = []
      upc_data = intermediate.get(UPC_EXTRACT_DB, None)
      if upc_data:
        final['upc'] = upc_data[0]['upc']

      final_result.append(final)

    return final_result

  def reduce(self, key, values, output):
    # we're writing to a JSONLine output, which ignores
    # out key field and simply writes one value per line.
    for row in self._join(values):
      output.put(key, row)
    else:
      logging.warn('No data for key: %s, values: %s', key, values)


class CombineHarmonization(DependencyTriggeredTask):
  def requires(self):
    return { 'ndc': NDC2JSON(),
             'spl': GenerateCurrentSPLJSON(),
             'unii': UNII2JSON(),
             'rxnorm': RXNorm2JSON(),
             'upc' : UpcXml2JSON(),
             'setid': SPLSetIDIndex() }

  def output(self):
    return luigi.LocalTarget(join(BASE_DIR, 'harmonized.json'))

  def run(self):
    index_db = {}
    setid_db = self.input()['setid'].path
    logging.info('Joining data from setid DB: %s', setid_db)
    db = parallel.ShardedDB.open(setid_db)
    db_iter = db.range_iter(None, None)

    for (key, val) in db_iter:
      # Only need one entry from the index, pruning here to simplify mapper
      # The index is also used to find the latest version of SPL, so it is a bit
      # clunky to use in this particular case.
      index_db[key] = val[0][1]

    db_list = [
      self.input()[_db].path for _db in ['ndc', 'spl', 'unii', 'rxnorm', 'upc']
    ]

    logging.info('DB %s', db_list)
    parallel.mapreduce(
      parallel.Collection.from_sharded_list(db_list),
      mapper=JoinAllMapper(index_db=index_db),
      reducer=JoinAllReducer(),
      output_prefix=self.output().path,
      output_format=parallel.JSONLineOutput())

if __name__ == '__main__':
  luigi.run()

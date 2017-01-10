#!/usr/bin/env python

'''
Helper functions for managing ElasticSearch indices.
'''

import contextlib
import hashlib
import logging
import math
import os
from os.path import basename, join
import sys
import requests
import time

import arrow
import elasticsearch
import elasticsearch.helpers
import luigi
import czipfile as zipfile

from openfda import config, elasticsearch_requests, parallel
from openfda.common import BatchHelper
from openfda.tasks import AlwaysRunTask

try:
  import simplejson as json
except:
  print >>sys.stderr, 'Failed to import simplejson.  This might result in slowdowns.'
  import json


def optimize_index(index_name, wait_for_merge=1):
  # elasticsearch client throws a timeout error when waiting for an optimize
  # command to finish, so we use requests instead
  logging.info('Optimizing: %s (this may take a while)', index_name)
  resp = requests.post('http://%s/%s/_optimize?max_num_segments=1&wait_for_merge=%d' %
    (config.es_host(), index_name, wait_for_merge), timeout=100000)
  resp.raise_for_status()


def dump_index(es,
               index,
               endpoint,
               target_dir,
               cleaner=None,
               query=None,
               chunks=100000):
  '''
  Dump the index `index` to disk as compressed JSON files. If a query is passed
  into the function, then it is filters out those records before the dump. If
  chunks is passed into the function, then a new file is written for every
  chunk.
  '''
  endpoint_list = endpoint[1:].split('/')
  update_date = arrow.utcnow().format('YYYY-MM-DD')

  def _write_chunk(target_file, data):
    ''' Helper function for writing out zip file chunks.
    '''
    file_to_zip = basename(target_file).replace('.zip', '')
    try:
      with contextlib.closing(zipfile.ZipFile(target_file, 'w')) as out_f:
        zip_info = zipfile.ZipInfo(file_to_zip, time.localtime()[0:6])
        zip_info.external_attr = 0777 << 16L
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        out_f.writestr(zip_info, json.dumps(data, indent=2) + '\n')
    except:
      logging.error("Failed to write a chunk to: "+target_file)
      raise

  def _make_file_name(target_dir, idx, total):
    ''' Helper function for making uniform file names.
    '''
    prefix = '-'.join(endpoint_list)
    return join(target_dir, prefix + '-%04d-of-%04d.json.zip' % (idx, total))

  def _make_display_name(endpoint, name):
    ''' Isolating the thorny naming logic for the display name in a helper
        function. The naming is solely present for url cleanliness.
        Possible outputs (examples):
          No partition and no shards: `Device pma data`
          No partition but sharded: `Device pma (part 1 of 2)`
          Partition but no shards: `2014 Q1 (all)`
          Partition with shards: `2014 Q1 (part 1 of 2)`
    '''
    partition = ''
    parts = name.split('/')
    file_name = parts[-1]
    pieces = file_name.split('-of-')
    part, total = int(pieces[0][-4:]), int(pieces[1][:4])
    part_text = ' (all)'

    if len(parts) > 1:
      partition = ' '.join(parts[0:-1])

    # Currently only support quartly partitions, so we can safely split on `q`
    if partition:
      if 'q' in partition:
        partition = ' q'.join(partition.split('q'))
        partition = ' {partition}'.format(partition=partition).title()
      elif 'all' in partition.lower():
        partition = 'All other data'

    if total > 1:
      part_text = ' (part {part} of {total})'.format(**locals())
    elif total == 1 and not partition:
      part_text = ' data'

    if partition:
      endpoint = ''

    return '{endpoint}{partition}{part_text}'.format(**locals()).strip()

  def _make_partition(file_name, endpoint, total):
    ''' Helper function for making the partition object that is appended to
        manifest.partitions list.
    '''
    # http://download.open.fda.gov/device/event/1992q4/device-event-0001-of-0001.json.zip
    # Sometimes the filename includes a partition, so we take everything after
    # the endpoint and not just the basename
    name = file_name.split(endpoint)[-1]
    url = 'http://download.open.fda.gov' + endpoint + name
    file_size = os.path.getsize(file_name)/1024/1024.0

    return {
      'display_name': _make_display_name(endpoint, name),
      'file': url,
      'size_mb': '%0.2f' % file_size,
      'records': total
    }

  # Fetch the total number of documents for the query
  # This is used for both logging and the skip and limit values which are
  # added to the dump as to simulate the API output.
  total_docs = es.count(index=index, body=query)
  total_docs = total_docs['count']
  total_zips = int(math.ceil(total_docs * 1.0/chunks))
  domain, subdomain = endpoint_list

  # Tracking data structure that will eventually be written to ES for the
  # downloads API.
  manifest = {
    domain: {
      subdomain: {
        'export_date': update_date,
        'total_records': total_docs,
        'partitions': []
      }
    }
  }

  # All files are wrapped in the API format, so that the data interface remains
  # constant between the API results and the download results.
  disclaimer = ('Do not rely on openFDA to make decisions regarding medical care. '
                'While we make every effort to ensure that data is accurate, you '
                'should assume all results are unvalidated. We may limit or otherwise '
                'restrict your access to the API in line with our Terms of Service.')

  # Bespoke disclaimer for caers API, if there are any more of these, then they
  # should be externalized
  caers_disclaimer = ('Do not rely on openFDA to make decisions regarding '
    'medical care. While we make every effort to ensure that data is accurate, '
    'you should assume all results are unvalidated. We may limit or otherwise '
    'restrict your access to the API in line with our Terms of Service. '
    'Submission of an adverse event report does not constitute an admission '
    'that a product caused or contributed to an event. The information in '
    'these reports has not been scientifically or otherwise verified as to a '
    'cause and effect relationship and cannot be used to estimate incidence '
    '(occurrence rate) or to estimate risk.')

  return_dict = {
    'meta': {
      'disclaimer': caers_disclaimer if index == 'foodevent' else disclaimer,
      'terms': 'https://open.fda.gov/terms/',
      'license': 'https://open.fda.gov/license/',
      'last_updated': update_date,
      'results': {
        'skip': 0,
        'limit': chunks,
        'total': total_docs
      }
    },
    'results': []
  }

  logging.info('Dumping %s (%d documents) to %s', index, total_docs, target_dir)
  os.system('mkdir -p "%s"' % target_dir)
  docs_written = 0
  file_idx = 1

  target_file = _make_file_name(target_dir, file_idx, total_zips)

  results = []
  for result in elasticsearch.helpers.scan(es,
                                           query=query,
                                           raise_on_error=True,
                                           scroll='30m',
                                           index=index):

    # Grab a response from ES and clean it with the callback passed into the
    # main function. This is so we can avoid exposing internal keys in the
    # exported data.
    source_data = json.loads(json.dumps(result['_source']), object_hook=cleaner)
    results.append(source_data)
    docs_written = len(results)

    # Write out a new file everytime we have fetched a chunks' worth.
    if docs_written % chunks == 0:
      chunks_written = int(return_dict['meta']['results']['skip'] + chunks)
      total_written = (chunks_written * 1.0)/total_docs
      logging.info('Writing %20s, %.2f%% done', index, 100. * total_written)
      return_dict['results'] = results

      _write_chunk(target_file, return_dict)
      manifest[domain][subdomain]['partitions'].append(
        _make_partition(target_file, endpoint, len(results))
      )

      results = []
      return_dict['meta']['results']['skip'] += chunks
      file_idx += 1
      target_file = _make_file_name(target_dir, file_idx, total_zips)

  # Take care of any final results
  if results:
    return_dict['results'] = results
    return_dict['meta']['results']['limit'] = len(results)
    _write_chunk(target_file, return_dict)
    manifest[domain][subdomain]['partitions'].append(
      _make_partition(target_file, endpoint, len(results))
    )

  # Write out manifest file that will feed into the downloads API
  with open(join(target_dir, 'manifest.json'), 'w') as manifest_out:
    json.dump(manifest, manifest_out)


def index_without_checksum(es, index, doc_type, batch):
  '''
  A function for bulk loading a batch into an index without versioning.

  This operation assumes that the index was newly created (or erased) prior to
  indexing.
  '''
  create_batch = []
  for doc in batch:
    create_batch.append({ '_index' : index,
                          '_type' : doc_type,
                          '_source' : doc })

  create_count, errors = elasticsearch.helpers.bulk(es, create_batch,
      raise_on_error=False,
      raise_on_exception=False)
  for item in errors:
    create_resp = item['create']
    if create_resp['status'] >= 400:
      logging.error('Unexpected conflict in creating documents: %s', create_resp)

  logging.info('%s create batch complete: %s created %s errors',
               index, create_count, len(errors))


def index_with_checksum(es, index, doc_type, batch):
  '''
  Index documents in `batch`, inserting them into `index`.

  As an optimization, a SHA1 checksum is computed for each document.  If a
  document already exists with the same checksum, we skip re-indexing the
  document.
  '''
  if not batch:
    return

  # skip documents which have a matching checksum.  the checksum of a document
  # is the checksum of the sha1 string of the JSON representation (where the
  # "@checksum" field is left blank.
  batch_with_checksum = []
  for docid, doc in batch:
    doc['@checksum'] = ''
    checksum = hashlib.sha1(json.dumps(doc, sort_keys=True)).hexdigest()
    doc['@checksum'] = checksum
    batch_with_checksum.append((docid, checksum, doc))

  # mapping from docid to checksum
  existing_docs = {}
  for match in es.mget(body={ 'ids' : [docid for (docid, _, _) in batch_with_checksum] },
                       index=index,
                       doc_type=doc_type,
                       _source=False,
                       fields='@checksum')['docs']:
    if 'error' in match:
      logging.warn('ERROR during query: %s', match['error'])
      continue

    if not match['found']:
      continue

    # some mappings convert integer ids into strings: we want to
    # match the documents in either case, so we use a string id here
    # explicitly
    matching_id = str(match['_id'])
    existing_docs[matching_id] = match['fields']['@checksum'][0]

  logging.debug('%d existing documents found', len(existing_docs))
  # filter out documents with matching checksums
  filtered_batch = []
  skipped = 0
  for docid, checksum, doc in batch_with_checksum:
    es_checksum = existing_docs.get(str(docid), None)
    if es_checksum == checksum:
      logging.debug('Skipping docid %s, already exists.', docid)
      skipped += 1
    else:
      logging.debug('Checksum mismatch for %s, [ %s vs %s ]', docid, checksum, es_checksum)
      filtered_batch.append((docid, checksum, doc))

  batch = filtered_batch

  index_batch = []
  for docid, checksum, doc in batch:
    index_batch.append(
      { '_op_type' : 'index',
        '_id' : docid,
        '_index' : index,
        '_type' : doc_type,
        '_source' : doc })

  index_count, errors = elasticsearch.helpers.bulk(es, index_batch,
      raise_on_error=False,
      raise_on_exception=False)

  for item in errors:
    update_resp = item['index']
    logging.error('Failed to index: %s', update_resp)

  logging.info('%s update batch complete: %s total, %s indexed, %s skipped, %s errors',
               index, len(index_batch) + skipped, len(index_batch), skipped, len(errors))


def get_mapping(es, index):
  ''' Get the Elasticsearch mapping for an index.
  '''
  indices = es.indices

  return indices.get_mapping(index=index)


def copy_mapping(es, source_index, target_index):
  '''
  Copy the mappings and settings from `source_index` to `target_index`.

  `target_index` must not exist.  This operation does not copy any data.
  '''
  idx = es.indices
  mapping = idx.get_mapping(index=source_index)
  settings = idx.get_settings(index=source_index)[source_index]

  assert not es.indices.exists(target_index), (
    'Trying to copy mapping to an already existing index: %s' % target_index)

  idx.create(target_index,
             body={ 'settings' : settings['settings'] })

  for doc_type, schema in mapping[source_index]['mappings'].items():
    idx.put_mapping(index=target_index, doc_type=doc_type, body=schema)


class ResetElasticSearch(AlwaysRunTask):
  '''Create a new index with the given type and mapping.'''
  target_host = luigi.Parameter()
  target_index_name = luigi.Parameter()
  target_type_name = luigi.Parameter()
  target_mapping_file = luigi.Parameter()
  delete_index = luigi.Parameter(default=False)

  def _run(self):
    logging.info('Create index and loading mapping: %s/%s',
                 self.target_index_name, self.target_type_name)
    es = elasticsearch.Elasticsearch(self.target_host, timeout=120)
    if self.delete_index:
      elasticsearch_requests.clear_and_load(es,
                                        self.target_index_name,
                                        self.target_type_name,
                                        self.target_mapping_file)
    else:
      elasticsearch_requests.load_mapping(es,
                                          self.target_index_name,
                                          self.target_type_name,
                                          self.target_mapping_file)


class LoadJSONMapper(parallel.Mapper):
  def __init__(self, es_host, index_name, type_name, docid_key, incremental):
    self.index_name = index_name
    self.type_name = type_name
    self.docid_key = docid_key
    self.incremental = incremental
    self.es_host = es_host

  def _map_incremental(self, map_input):
    es = elasticsearch.Elasticsearch(self.es_host, timeout=120)
    with BatchHelper(index_with_checksum, es, self.index_name, self.type_name) as helper:
      for key, doc in map_input:
        assert self.docid_key in doc, 'Document is missing id field: %s' % json.dumps(doc, indent=2)
        doc_id = doc[self.docid_key]
        helper.add((doc_id, doc))

  def _map_non_incremental(self, map_input):
    es = elasticsearch.Elasticsearch(self.es_host, timeout=120)
    with BatchHelper(index_without_checksum, es, self.index_name, self.type_name) as helper:
      for key, doc in map_input:
        helper.add(doc)

  def map_shard(self, map_input, map_output):
    if self.incremental:
      self._map_incremental(map_input)
    else:
      self._map_non_incremental(map_input)


class LoadJSONBase(AlwaysRunTask):
  '''
  Load JSON into elasticsearch, using the given index, type and mapping.
  '''

  'The index and type to load into.'
  index_name = None
  type_name = None
  mapping_file = None

  'Load data incrementally, using a checksum?'
  use_checksum = False

  'For checksum loads: the field to use for document ids.'
  docid_key = 'docid'

  'A task specifying the data source to load into ES.'
  data_source = None

  'If specified, this task will be ordered after `previous_task`'
  previous_task = luigi.Parameter(default=None)

  'The number of processes to use when loading JSON into ES'
  load_json_workers = luigi.Parameter(default=4)

  'Should we optimize the index after adding documents.'
  optimize_index = False

  def _data(self):
    '''
    Return a task that supplies the data files to load into ES.

    By default, this is just `data_source`, but this can be overridden
    for more complex situations (such as a depending on a parameter)
    '''
    return self.data_source

  def _schema(self):
    return ResetElasticSearch(
        target_host=config.es_host(),
        target_index_name=self.index_name,
        target_type_name=self.type_name,
        target_mapping_file=self.mapping_file,
        delete_index=not self.use_checksum)

  def requires(self):
    deps = {
      'schema': self._schema(),
      'data': self._data(),
    }

    if self.previous_task:
      deps['previous_task'] = self.previous_task

    return deps

  def __repr__(self):
    # if we have an ordering on this set of tasks, the default representation
    # can become overwhelmingly verbose; we truncate it here.
    return 'LoadJSON(data=%s)' % self._data()

  def _run(self):
    json_dir = self.input()['data'].path

    mapper = LoadJSONMapper(
      config.es_host(),
      index_name=self.index_name,
      type_name=self.type_name,
      docid_key=self.docid_key,
      incremental=self.use_checksum
    )

    parallel.mapreduce(
      parallel.Collection.from_sharded(json_dir),
      mapper=mapper,
      reducer=parallel.IdentityReducer(),
      output_format=parallel.NullOutput(),
      map_workers=self.load_json_workers,
      num_shards=1,
      output_prefix=config.tmp_dir('%s/load-json' % self.index_name)
    )

    # update metadata index
    elasticsearch_requests.update_process_datetime(
      config.es_client(), self.index_name, arrow.utcnow().format('YYYY-MM-DD')
    )

    # optimize index, if requested
    if self.optimize_index:
      optimize_index(self.index_name, wait_for_merge=False)


    # update metadata index again. Trying to solve mystery of missing "last_update_date" entries...
    elasticsearch_requests.update_process_datetime(
      config.es_client(), self.index_name, arrow.utcnow().format('YYYY-MM-DD')
    )

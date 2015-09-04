#!/usr/bin/env python

'''
Helper functions for managing ElasticSearch indices.
'''

import hashlib
import logging
import os
from os.path import join
import sys
import time
import yaml

import arrow
import elasticsearch
import elasticsearch.helpers
import gflags
import luigi
from gzip import GzipFile

from openfda import app, config, elasticsearch_requests, parallel

try:
  import simplejson as json
except:
  print >>sys.stderr, 'Failed to import simplejson.  This might result in slowdowns.'
  import json

DEFAULT_BATCH_SIZE=100

def dump_index(es, index, target_dir, max_uncompressed_size=1e9):
  '''
  Dump the index `index` to disk as compressed JSON files.

  The maximum _uncompressed_ size of each file is specified via
  `max_uncompressed_size`.  Files are compressed via gzip, and
  written as {0.json.gz, 1.json.gz....}
  '''
  # fetch the total number of documents from the index, for logging
  total_docs = es.indices.stats(index=index)['indices'].values()[0]['total']['docs']['count']

  logging.info('Dumping %s (%d documents) to %s', index, total_docs, target_dir)
  os.system('mkdir -p "%s"' % target_dir)
  lines = []
  docs_written = 0
  num_bytes = 0
  file_idx = 0

  target_file = join(target_dir, '%05d.json.gz' % file_idx)
  out_f = os.popen('pigz -c > "%s"' % target_file, 'w', 1048576)

  for result in elasticsearch.helpers.scan(es, query={}, raise_on_error=True, index=index):
    doc_data = json.dumps(result['_source']) + '\n'
    out_f.write(doc_data)

    docs_written += 1
    num_bytes += len(doc_data)
    if docs_written % 10000 == 0:
      logging.info('Writing %20s, %.2f%% done', index, 100. * docs_written / total_docs)

    if num_bytes > max_uncompressed_size:
      out_f.close()

      logging.info('Wrote %s (%d MB)', target_file, num_bytes / 1e6)
      file_idx += 1
      target_file = join(target_dir, '%05d.json.gz' % file_idx)
      out_f = os.popen('pigz -c > "%s"' % target_file, 'w')
      num_bytes = 0

  out_f.close()


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

  create_count, errors = elasticsearch.helpers.bulk(es, create_batch, raise_on_exception=False)
  for item in errors:
    create_resp = item['create']
    if create_resp['status'] >= 400:
      logging.warn('Unexpected conflict in creating documents: %s', create_resp)

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

  index_count, errors = elasticsearch.helpers.bulk(es, index_batch, raise_on_exception=False)

  for item in errors:
    update_resp = item['index']
    logging.error('Failed to index: %s', update_resp)

  logging.info('%s update batch complete: %s total, %s indexed, %s skipped, %s errors',
               index, len(index_batch) + skipped, len(index_batch), skipped, len(errors))


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


class AlwaysRunTask(luigi.Task):
  '''
  A task that should always run once.

  This is generally used for tasks that are idempotent, and for which the
  "done" state is difficult to determine: e.g. initializing an index, or
  downloading files.

  N.B. Luigi creates *new instances* of tasks when performing completeness
  checks.  This means we can't store our completeness as a local boolean,
  and instead have to use a file on disk as a flag.

  To allow subclasses to use `output` normally, we don't use an `output` file
  here and instead manage completion manually.
  '''
  nonce_timestamp = luigi.Parameter(default=arrow.utcnow().format('YYYY-MM-DD-HH-mm-ss'))

  def __init__(self, *args, **kw):
    luigi.Task.__init__(self, *args, **kw)

  def requires(self):
    return []

  def _nonce_file(self):
    import hashlib
    digest = hashlib.sha1(repr(self)).hexdigest()
    return '/tmp/always-run-task/%s-%s' % (self.nonce_timestamp, digest)

  def output(self):
    return luigi.LocalTarget(self._nonce_file())

  def run(self):
    self._run()

    os.system('mkdir -p "/tmp/always-run-task"')
    with open(self._nonce_file(), 'w') as nonce_file:
      nonce_file.write('finished.')



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
    es = elasticsearch.Elasticsearch(self.target_host)
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

class BatchHelper(object):
  '''
  Convenience class for batching operations.

  When more than `batch_size` operations have been added to the batch, `fn`
  will be invoked with `args` and `kw`.  The actual data must be expected
  via the `batch` argument.

  Call `flush()` when no more documents will be added.
  '''
  def __init__(self, fn, *args, **kw):
    self._fn = fn
    self._args = args
    self._kw = kw
    self._batch = []

  def flush(self):
    self._fn(*self._args, batch=self._batch, **self._kw)
    del self._batch[:]

  def add(self, obj):
    self._batch.append(obj)
    if len(self._batch) > DEFAULT_BATCH_SIZE:
      self.flush()

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.flush()


class LoadJSONMapper(parallel.Mapper):
  def __init__(self, es_host, index_name, type_name, docid_key, incremental):
    self.index_name = index_name
    self.type_name = type_name
    self.docid_key = docid_key
    self.incremental = incremental
    self.es_host = es_host

  def _map_incremental(self, map_input):
    es = elasticsearch.Elasticsearch(self.es_host)
    with BatchHelper(index_with_checksum, es, self.index_name, self.type_name) as helper:
      for key, doc in map_input:
        assert self.docid_key in doc, 'Document is missing id field: %s' % json.dumps(doc, indent=2)
        doc_id = doc[self.docid_key]
        helper.add((doc_id, doc))

  def _map_non_incremental(self, map_input):
    es = elasticsearch.Elasticsearch(self.es_host)
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
      output_prefix=config.tmp_dir('%s/load-json' % self.index_name)
    )

    # update metadata index
    elasticsearch_requests.update_process_datetime(
      config.es_client(), self.index_name, arrow.utcnow().format('YYYY-MM-DD')
    )

class OptimizeIndex(AlwaysRunTask):
  index_name = luigi.Parameter()
  def _run(self):
    logging.info('Optimizing %s', self.index_name)
    es = config.es_client()
    es.indices.optimize(
      index=self.index_name,
      max_num_segments=1
    )

def main(argv):
  FLAGS = gflags.FLAGS
  es = elasticsearch.Elasticsearch(FLAGS.host)
  if FLAGS.operation == 'rollback':
    rollback_index_transaction(es, FLAGS.index)
  elif FLAGS.operation == 'commit':
    commit_index_transaction(es, FLAGS.index)
  elif FLAGS.operation == 'dump_index':
    dump_index(es, FLAGS.index, FLAGS.dump_directory)
  else:
    raise Exception, 'Unknown operation: %s' % FLAGS.operation


if __name__ == '__main__':
  gflags.DEFINE_string('dump_directory', '/tmp/index-data',
                       'Location to write exported index data.')
  gflags.DEFINE_string('host', 'http://localhost:9200', 'ElasticSearch host.')
  gflags.DEFINE_string('index', None, 'Index to operate on.')
  gflags.DEFINE_enum('operation', None, ['rollback', 'commit', 'dump_index'], 'Operation to perform')
  gflags.MarkFlagAsRequired('index')
  gflags.MarkFlagAsRequired('operation')

  app.run()

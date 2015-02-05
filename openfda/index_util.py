#!/usr/bin/env python

'''
Helper functions for managing ElasticSearch indices.
'''
import logging

import elasticsearch
import elasticsearch.helpers
import gflags
import luigi

from openfda import app, parallel

DEFAULT_BATCH_SIZE=100

def copy_documents(es, source_index, dest_index, doc_type, docids):
  '''Copy the given list of `docids` from `source_index` to `dest_index`.

  This operation fails if any of the documents already exist in `dest_index`.
  '''
  logging.debug('Copying %d documents from %s to %s',
               len(docids), source_index, dest_index)
  for docid in docids:
    logging.debug('Copying: %s', docid)
    document = es.get(index=source_index, doc_type=doc_type, id=docid)
    try:
      es.create(index=dest_index, doc_type=doc_type, id=docid,
                body=document['_source'],
                version=document['_version'],
                version_type='external_gt')

    except elasticsearch.ConflictError:
      logging.warn(
      'A duplicate document id (%s) was found while indexing.  ' +
      'The version originally present in the index will be kept.', docid)
        )

def create_or_spill(es, index, doc_type, batch):
  '''
  Create index entries in `index.base` for the list of documents in `batch`.

  If an index entry already exists, the existing document is copied to
  `index.spill`, and then overwritten in `index.base`.
  '''
  if not batch:
    return

  base = index + '.base'
  spill = index + '.spill'

  assert es.indices.exists(spill), \
    ('A transaction has not been started for index: %s' % index,
     'A transaction must be started (using `start_index_transaction`) before '
     'using this function.')

  # We first check for documents that already exist, and have a version
  # higher than our own: there is no reason to try inserting them.
  existing_versions = {}
  for match in es.mget(body={ 'ids' : [docid for (docid, _, _) in batch] },
                       index=base,
                       doc_type=doc_type,
                       _source=False,
                       fields='@version')['docs']:
    if not match['found']: continue
    existing_versions[match['_id']] = match['_version']

  filtered_batch = []
  skipped = 0
  for docid, version, doc in batch:
    if existing_versions.get(docid, -1) >= version:
      logging.debug('Skipping docid %s, version already exists.', docid)
      skipped += 1
    else:
      filtered_batch.append((docid, version, doc))

  batch = filtered_batch

  # Now try to create any remaining documents.  This will fail if an
  # existing document exists with a lower version than ours.
  create_batch = []
  for docid, version, doc in batch:
    create_batch.append(
      { '_op_type' : 'create',
        '_id' : docid,
        '_index' : base,
        '_type' : doc_type,
        '_version' : version,
        '_version_type' : 'external_gt',
        '_source' : doc })

  create_count, errors = elasticsearch.helpers.bulk(es, create_batch)
  conflicts = set()
  for item in errors:
    create_resp = item['create']
    # conflict occurred
    if create_resp['status'] == 409:
      conflicts.add(create_resp['_id'])
    elif create_resp['status'] >= 400:
      logging.info('Bad result: %s', create_resp)

  # Copy conflicting documents to the spill index
  copy_documents(es, base, spill, doc_type, conflicts)

  # Overwrite documents in the base index
  update_batch = []
  for docid, version, doc in batch:
    if not docid in conflicts: continue
    update_batch.append({
        '_index' : base,
        '_id' : docid,
        '_type' : doc_type,
        '_version' : version,
        '_version_type' : 'external_gt',
        '_source' : doc })

  update_count, errors = elasticsearch.helpers.bulk(es, update_batch)
  old_version_count = 0
  for item in errors:
    update_resp = item['index']
    # conflict occurred (this should effectively never occur, as we check
    # for newer versions above)
    if update_resp['status'] == 409:
      old_version_count += 1
    elif update_resp['status'] >= 400:
      logging.info('Bad result:', update_resp)

  logging.info('Update finished: %s created, %s updated, %s skipped, %s conflicts, %s errors',
               create_count, update_count, skipped, old_version_count,
               len(errors) - old_version_count)


def copy_mapping(es, source_index, target_index):
  '''
  Copy the mappings and settings from `source_index` to `target_index`.

  `target_index` must not exist.  This operation does not copy any data.
  '''
  idx = es.indices
  mapping = idx.get_mapping(index=source_index)
  settings = idx.get_settings(index=source_index)[source_index]

  idx.create(target_index,
             body={ 'settings' : settings['settings'] })

  for doc_type, schema in mapping[source_index]['mappings'].items():
    idx.put_mapping(index=target_index, doc_type=doc_type, body=schema)


def start_index_transaction(es, index_name, epoch):
  '''
  Start an indexing "transaction" for `index_name`

  This creates a spill index to hold documents that are changed during
  indexing and updates the index aliases to filter out any new data.

  After the new index is tested, `commit_index_transaction` may be
  called to finalize the index.  If the indexing operation was
  unsucessful, it may be aborted by using `rollback_index_transaction`.
  '''
  logging.info('Starting index transaction for %s', index_name)

  assert not es.indices.exists(index_name + '.spill'), \
    'An existing indexing transaction is already in progress.\n'\
    'It must be rolled back or commited before a new transaction can start.'

  base = index_name + '.base'
  spill = index_name + '.spill'

  serving = index_name
  staging = index_name + '.staging'

  copy_mapping(es, base, spill)
  es.indices.update_aliases({
     'actions' : [
        { 'add' : { 'alias' : staging, 'index' : base } },
        { 'add' : {
          'alias' : serving,
          'index' : [spill, base],
          'filter' : { 'range' : { '@epoch' : { 'gte' : 0, 'lte' : epoch}}}
        } }
    ]
  })


def rollback_index_transaction(es, index_name):
  '''
  Revert an in-progress index operation, restoring the index to it's original state.
  '''
  base = index_name + '.base'
  spill = index_name + '.spill'
  serving = index_name

  logging.info('Rolling back index: %s', index_name)
  elasticsearch.helpers.reindex(es, spill, base)
  es.indices.update_aliases({
     'actions' : [
        { 'remove' : { 'alias' : serving, 'index' : [spill, base] } },
        { 'add' : {
          'alias' : serving,
          'index' : base,
        } }
    ]
  })
  es.indices.delete(index=spill)



def commit_index_transaction(es, index_name):
  '''
  Commits an indexing operation.

  Any changes to the index will be made final.  After this completes, a
  new indexing operation may be started.
  '''
  logging.info('Committing index for %s', index_name)
  base = index_name + '.base'
  serving = index_name
  staging = index_name + '.staging'
  es.indices.update_aliases({
     'actions' : [
        { 'remove' : { 'alias' : staging, 'index' : base } },
        { 'add' : { 'alias' : serving, 'index' : base, } }
    ]
  })
  es.indices.delete(index_name + '.spill')

class AlwaysRunTask(luigi.Task):
  '''
  A task that should always run once.

  This is generally used for tasks that are idempotent, and for which the
  "done" state is difficult to determine: e.g. initializing an index, or
  downloading files.
  '''
  def __init__(self, *args, **kw):
    luigi.Task.__init__(self, *args, **kw)
    self._complete = False

  def requires(self):
    return []

  def run(self):
    self._run()
    self._complete = True

  def complete(self):
    return self._complete

class LoadJSONMapper(parallel.Mapper):
  def __init__(self,
               es_host,
               index,
               data_type,
               epoch,
               docid_key='docid',
               version_key='version'):
    self.es_host = es_host
    self.index = index
    self.data_type = data_type
    self.epoch = epoch
    self.docid_key = docid_key
    self.version_key = version_key

  def map_shard(self, map_input, map_output):
    es = elasticsearch.Elasticsearch(self.es_host)
    json_batch = []
    for key, doc in map_input:
      doc['@epoch'] = self.epoch
      doc_id = doc[self.docid_key]
      version = doc[self.version_key]
      json_batch.append((doc_id, version, doc))
      if len(json_batch) > DEFAULT_BATCH_SIZE:
        create_or_spill(es, self.index, self.data_type, json_batch)
        del json_batch[:]
    create_or_spill(es, self.index, self.data_type, json_batch)

def main(argv):
  FLAGS = gflags.FLAGS
  es = elasticsearch.Elasticsearch(FLAGS.host)
  if FLAGS.operation == 'rollback':
    rollback_index_transaction(es, FLAGS.index)
  elif FLAGS.operation == 'commit':
    commit_index_transaction(es, FLAGS.index)
  else:
    raise Exception, 'Unknown operation: %s' % FLAGS.operation


if __name__ == '__main__':
  gflags.DEFINE_string('host', 'http://localhost:9200', 'ElasticSearch host.')
  gflags.DEFINE_string('index', None, 'Index to operate on.')
  gflags.DEFINE_enum('operation', None, ['rollback', 'commit'], 'Operation to perform')
  gflags.MarkFlagAsRequired('index')
  gflags.MarkFlagAsRequired('operation')

  app.run()

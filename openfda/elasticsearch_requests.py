''' A bundle of ElasticSearch functions used throughout the pipelines
'''
import logging
import os
import types
from os.path import join, dirname

import elasticsearch
import simplejson as json

METADATA_INDEX = 'openfdametadata'
METADATA_TYPE = 'last_run'

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
INDEX_SETTINGS = join(RUN_DIR, 'schemas/indexing.json')

def clear_and_load(es, index_name, type_name, mapping_file):
  try:
    es.indices.delete(index=index_name)
    logging.info('Deleting index %s', index_name)
  except elasticsearch.ElasticsearchException:
    logging.info('%s does not exist, nothing to delete', index_name)

  load_mapping(es, index_name, type_name, mapping_file)


def load_mapping(es, index_name, type_name, mapping_file_or_dict):
  if not isinstance(mapping_file_or_dict, types.DictType):
    mapping = open(mapping_file_or_dict, 'r').read().strip()
    mapping_dict = json.loads(mapping)
  else:
    mapping_dict = mapping_file_or_dict

  if es.indices.exists(index_name):
    logging.info('Index %s already exists, skipping creation.', index_name)
    return

  try:
    settings_dict = json.loads(open(INDEX_SETTINGS).read())
    # Ignore "index already exists" error
    es.indices.create(index=index_name,
                      body=settings_dict, ignore=400)

    es.indices.put_mapping(index=index_name,
                           doc_type=type_name,
                           body=mapping_dict)
    es.indices.clear_cache(index=index_name)
  except:
    logging.fatal('Something has gone wrong making the mapping for %s', type_name,
                   exc_info=1)
    raise

def update_process_datetime(es, doc_id, timestamp):
  ''' Updates the last_update_date for the document id passed into function.
    The document id in will be the name of another index in the cluster.
  '''
  _map = {
    'last_run': {
      'properties': {
         'last_update_date': {
           'type': 'date',
           'format': 'dateOptionalTime'
         }
      }
    }
  }

  load_mapping(es, METADATA_INDEX, METADATA_TYPE, _map)
  new_doc = { 'last_update_date': timestamp }
  logging.info("Updating last_update_date -  [index=%s, type=%s, id=%s, time=%s]", METADATA_INDEX, METADATA_TYPE, doc_id, timestamp)
  es.index(index=METADATA_INDEX,
           doc_type=METADATA_TYPE,
           id=doc_id,
           body=new_doc)

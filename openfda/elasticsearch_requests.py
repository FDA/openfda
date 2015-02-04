''' A bundle of ElasticSearch functions used throughout the pipelines
'''
import logging
import os
from os.path import join, dirname

import elasticsearch

import simplejson as json


RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
INDEX_SETTINGS = join(RUN_DIR, 'schemas/indexing.json')

def clear_and_load(es, index_name, type_name, mapping_file):
  try:
    es.indices.delete(index=index_name)
    logging.info('Deleting index %s', index_name)
  except elasticsearch.ElasticsearchException:
    logging.info('%s does not exist, nothing to delete', index_name)

  load_mapping(es, index_name, type_name, mapping_file)


def load_mapping(es, index_name, type_name, mapping_file):
  mapping = open(mapping_file, 'r').read().strip()
  mapping_dict = json.loads(mapping)

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

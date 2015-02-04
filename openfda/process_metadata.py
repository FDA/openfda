#!/usr/local/bin/python

import logging
from pyelasticsearch import ElasticSearch, exceptions

def update_process_datetime(doc_id, timestamp):
  ''' Updates the last_update_date for the document id passed into function.
    The document id in will be the name of another index in the cluster.
  '''
  connection_string = 'http://localhost:9200'
  process_index = 'openfdametadata'
  _type = 'last_run'
  _map = {}
  _map[_type] = {}
  _map[_type]['properties'] = {}
  _map[_type]['properties']['last_update_date'] = {}
  _map[_type]['properties']['last_update_date']['type'] = 'date'
  _map[_type]['properties']['last_update_date']['format'] = 'dateOptionalTime'

  es = ElasticSearch(connection_string)
  try:
    es.create_index(process_index)
    logging.info('Creating index %s', process_index)
  except exceptions.IndexAlreadyExistsError as e:
    logging.info('%s already exists', process_index)

  try:
    es.put_mapping(process_index, doc_type=_type, mapping=_map)
    logging.info('Successfully created mapping')
  except:
    logging.fatal('Could not create the mapping')

  new_doc = {}
  new_doc['last_update_date'] = timestamp
  es.index(process_index,
           doc_type=_type,
           id=doc_id,
           doc=new_doc,
           overwrite_existing=True)

""" A bundle of ElasticSearch functions used throughout the pipelines
"""

import logging
import os
from os.path import join, dirname

import elasticsearch
import simplejson as json

METADATA_INDEX = "openfdametadata"
METADATA_TYPE = "_doc"

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
INDEX_SETTINGS = join(RUN_DIR, "schemas/indexing.json")


def clear_and_load(es, index_name, type_name, mapping_file):
    try:
        es.indices.delete(index=index_name)
        logging.info("Deleting index %s", index_name)
    except elasticsearch.ElasticsearchException:
        logging.info("%s does not exist, nothing to delete", index_name)

    load_mapping(es, index_name, type_name, mapping_file)


def load_mapping(es, index_name, type_name, mapping_file_or_dict):
    if es.indices.exists(index_name):
        logging.info("Index %s already exists, skipping creation.", index_name)
        return

    if not isinstance(mapping_file_or_dict, dict):
        mapping = open(mapping_file_or_dict, "r").read().strip()
        mapping_dict = json.loads(mapping)
    else:
        mapping_dict = mapping_file_or_dict

    try:
        settings_dict = json.loads(open(INDEX_SETTINGS).read())["settings"]
        es.indices.create(
            index=index_name,
            mappings=mapping_dict,
            settings=settings_dict,
            include_type_name=True,
        )
        es.indices.clear_cache(index=index_name)
    except:
        logging.fatal(
            "Something has gone wrong making the mapping for %s", type_name, exc_info=1
        )
        raise


def update_process_datetime(es, doc_id, last_run_date, last_update_date):
    """Updates the last_run_date and last_update_date for the document id passed into function.
    The document id in will be the name of another index in the cluster.
    """
    _map = {
        "_doc": {
            "properties": {
                "last_update_date": {
                    "type": "date",
                    "format": "strict_date_optional_time",
                },
                "last_run_date": {
                    "type": "date",
                    "format": "strict_date_optional_time",
                },
            }
        }
    }

    load_mapping(es, METADATA_INDEX, METADATA_TYPE, _map)
    new_doc = {"last_update_date": last_update_date, "last_run_date": last_run_date}
    logging.info(
        "Updating last_run_date & last_update_date -  [index=%s, type=%s, id=%s, last_run_date=%s, last_update_date=%s]",
        METADATA_INDEX,
        METADATA_TYPE,
        doc_id,
        last_run_date,
        last_update_date,
    )
    es.index(index=METADATA_INDEX, doc_type=METADATA_TYPE, id=doc_id, body=new_doc)

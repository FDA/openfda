#!/usr/bin/env python

import json
import logging
import os
import sys
import time

import elasticsearch

from openfda import index_util

ES_HOST = os.getenv("ES_HOST", default="localhost:9200")


def p(obj):
    return json.dumps(obj, indent=2, sort_keys=True)


def test_fresh_index():
    es = elasticsearch.Elasticsearch(ES_HOST)
    try:
        es.indices.delete("index_util_test1")
    except:
        pass
    es.indices.create("index_util_test1")
    time.sleep(0.1)

    docs = [
        {
            "user": "power",
            "message": "hello1",
        },
        {"user": "power", "message": "hello2"},
        {"user": "power", "message": "hello3"},
        {"user": "power", "message": "hello4"},
    ]

    doc_type = "user_message"
    batch = list(zip(list(range(4)), docs))
    index_util.index_with_checksum(es, "index_util_test1", doc_type, batch)

    for i in range(4):
        fetched = es.get(index="index_util_test1", id=i, doc_type="user_message")
        doc = dict(fetched["_source"])
        assert docs[i] == doc, (p(docs[i]), p(fetched))


def test_replace_some_docs():
    es = elasticsearch.Elasticsearch(ES_HOST)
    test_fresh_index()

    # change 2 documents of our 4, and check they are updated.
    docs = [
        {
            "user": "power",
            "message": "hello5",
        },
        {"user": "power", "message": "hello2"},
        {
            "user": "power",
            "message": "hello6",
        },
        {"user": "power", "message": "hello4"},
    ]

    doc_type = "user_message"
    batch = list(zip(list(range(4)), docs))
    index_util.index_with_checksum(es, "index_util_test1", doc_type, batch)

    for i in range(4):
        fetched = es.get(index="index_util_test1", id=i, doc_type="user_message")
        doc = dict(fetched["_source"])
        assert docs[i] == doc, (p(docs[i]), p(fetched))


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    test_fresh_index()
    test_replace_some_docs()

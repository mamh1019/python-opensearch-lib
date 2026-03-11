#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Nested object mapping sample. Run from project root: python -m samples.nested_object
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from elasticsearch import ElasticSearch

INDEX_NAME = "sample_nested"
ID_COLS = ["user_id"]
MAPPINGS = {
    "mappings": {
        "properties": {
            "user_id": {"type": "long"},
            "tags": {
                "type": "nested",
                "properties": {
                    "name": {"type": "keyword"},
                    "applied_date": {"type": "date", "format": "yyyy-MM-dd"},
                },
            },
        }
    }
}

es = ElasticSearch.init_with_auth("localhost")

res = es.create_index(INDEX_NAME, MAPPINGS, if_exist_drop=True)
print("create_index:", res)

doc = {
    "user_id": 1,
    "tags": [
        {"name": "premium", "applied_date": "2024-07-01"},
        {"name": "verified", "applied_date": "2024-07-02"},
    ],
}
res = es.insert(INDEX_NAME, doc, ID_COLS)
print("insert:", res)

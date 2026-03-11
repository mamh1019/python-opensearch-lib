#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Create index sample. Run from project root: python -m samples.create_index
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from elasticsearch import ElasticSearch

INDEX_NAME = "sample_index"
MAPPINGS = {
    "mappings": {
        "properties": {
            "item_id": {"type": "long"},
            "sku": {"type": "keyword"},
            "title": {"type": "text"},
            "category": {"type": "keyword"},
            "price": {"type": "long"},
            "created": {"type": "date", "format": "yyyy-MM-dd"},
            "tags": {"type": "keyword"},
        }
    }
}

es = ElasticSearch.init_with_auth("localhost")
res = es.create_index(INDEX_NAME, MAPPINGS, if_exist_drop=True)
print(res)

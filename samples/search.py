#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Search sample. Run from project root: python -m samples.search
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from elasticsearch import ElasticSearch

INDEX_NAME = "sample_index"

es = ElasticSearch.init_with_auth("localhost")

query = {
    "query": {
        "bool": {
            "must": [{"term": {"category": "electronics"}}],
            "should": [
                {"term": {"tags": "sale"}},
                {"term": {"tags": "bestseller"}},
            ],
            "minimum_should_match": 1,
        }
    }
}
res = es.search(query=query, index=INDEX_NAME)
print(res)

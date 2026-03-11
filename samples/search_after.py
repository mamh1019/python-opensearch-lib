#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Search with search_after (PIT-based paging) sample.
Run from project root: python -m samples.search_after
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from elasticsearch import ElasticSearch

INDEX_NAME = "sample_index"

es = ElasticSearch.init_with_auth("localhost")

query = {
    "query": {"match_all": {}},
    "sort": [{"item_id": "asc"}, {"sku": "asc"}],
}
res = es.search_after(query=query, index=INDEX_NAME)
print(res)

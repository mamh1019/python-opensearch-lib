#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Insert single document sample. Run from project root: python -m samples.insert
Requires sample index: run create_index.py first.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from elasticsearch import ElasticSearch

INDEX_NAME = "sample_index"
ID_COLS = ["item_id", "sku"]

es = ElasticSearch.init_with_auth("localhost")

doc = {
    "item_id": 1,
    "sku": "SKU-001",
    "title": "Sample product",
    "category": "electronics",
    "price": 29900,
    "created": "2024-03-01",
    "tags": ["sale", "bestseller"],
}
res = es.insert(INDEX_NAME, doc, ID_COLS)
print(res)

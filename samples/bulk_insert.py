#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bulk insert sample. Run from project root: python -m samples.bulk_insert
Requires sample index: run create_index.py first.
"""
import os
import sys
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from elasticsearch import ElasticSearch

INDEX_NAME = "sample_index"
ID_COLS = ["item_id", "sku"]

CATEGORIES = ["electronics", "books", "clothing", "home"]
TAGS = ["sale", "bestseller", "new", "clearance"]

es = ElasticSearch.init_with_auth("localhost")

docs = []
for i in range(1000, 1050):
    docs.append(
        {
            "item_id": i,
            "sku": f"SKU-{i:06d}",
            "title": f"Product {i}",
            "category": random.choice(CATEGORIES),
            "price": random.randint(1000, 100000),
            "created": "2024-03-01",
            "tags": random.sample(TAGS, k=random.randint(0, 2)),
        }
    )

res = es.bulk_insert(INDEX_NAME, docs, ID_COLS)
print(res)

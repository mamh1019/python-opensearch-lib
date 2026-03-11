#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OpenSearch SQL plugin sample. Run from project root: python -m samples.sql
Requires sample index with data.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from elasticsearch import ElasticSearch

es = ElasticSearch.init_with_auth("localhost")

sql = 'SELECT * FROM sample_index WHERE category = "electronics" LIMIT 10'
res = es.sql.query(sql)
print(res)

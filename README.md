# python-opensearch-lib

OpenSearch Python client wrapper. Supports Pandas DataFrame integration, bulk operations, search_after/scroll paging, SQL queries, and aggregations.

## Requirements

- Python 3.10+
- opensearch-py
- pandas

## Installation

```bash
pip install opensearch-py pandas
```

## Usage

```python
from elasticsearch import ElasticSearch

es = ElasticSearch.init_with_auth("localhost")

# Create index
es.create_index("my_index", mappings, if_exist_drop=True)

# Insert document
es.insert("my_index", doc, id_cols=["item_id", "sku"])

# Search
result = es.search(query={"query": {"match_all": {}}}, index="my_index")

# SQL query
df = es.sql.query('SELECT * FROM my_index WHERE category = "electronics"')
```

## Features

- **Index**: create_index, exists_index, delete_by_query, truncate
- **Documents**: insert, update, bulk_insert, bulk_update
- **Search**: search, search_page, search_after, search_scroll, search_buckets, search_composite_buckets
- **SQL**: OpenSearch SQL plugin
- **Result**: `ResultType.DATAFRAME` or `ResultType.RECORD`

## AWS OpenSearch

If `host` contains `amazonaws`, SSL and basic auth are applied automatically.

```python
es = ElasticSearch.init_with_auth(
    host="your-domain.us-east-1.es.amazonaws.com",
    user_id="user",
    password="password"
)
```

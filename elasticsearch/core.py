#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import pandas as pd
from opensearchpy import OpenSearch
from typing import Union, List
from enum import Enum


class ResultType(Enum):
    DATAFRAME = 1
    RECORD = 2


class ElasticSearch:
    def __init__(
        self, host, client: OpenSearch, result_type: ResultType = ResultType.RECORD
    ):
        self.host: str = host
        self.client: OpenSearch = client
        self.select_size = 10000
        self.result_type: ResultType = result_type
        # inner classes
        self.sql = self._SQL(self)

    @classmethod
    def init_with_auth(
        cls,
        host: str,
        user_id: str = None,
        password: str = None,
        result_type: ResultType = ResultType.RECORD,
    ):
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html
        """
        if "amazonaws" in host:  # production
            client = OpenSearch(
                hosts=[{"host": host, "port": 443}],
                http_compress=True,
                http_auth=(user_id, password),
                use_ssl=True,
                verify_certs=True,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
            )
        else:  # test
            client = OpenSearch(
                hosts=[{"host": host, "port": 9200}], http_compress=True
            )

        return cls(host, client, result_type)

    def __del__(self):
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.close
        """
        if self.client:
            self.client.close()

    ##########################################################################################
    ## helpers
    ##########################################################################################
    def generate_doc_id(self, cols: Union[str, List[str]], doc: dict) -> str:
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html
        """
        id_obj_type = type(cols)
        if id_obj_type == list:
            id_obj = "".join(map(lambda col: str(doc[col]), cols))
        elif id_obj_type == str:
            id_obj = doc[id_obj]
        else:
            raise Exception("invalid document id type")

        return self.generate_id_str(id_obj)

    def generate_id_str(self, id_str: str):
        hash_object = hashlib.md5(id_str.encode())
        return hash_object.hexdigest()

    def normallize(
        self, hits: list, result_type: ResultType
    ) -> Union[pd.DataFrame, dict]:
        if len(hits) <= 0:
            match result_type:
                case ResultType.DATAFRAME:
                    return pd.DataFrame()
                case _:
                    return {}
        result = [hit["_source"] for hit in hits]
        match result_type:
            case ResultType.DATAFRAME:
                return pd.DataFrame(result)
            case _:
                return result

    ##########################################################################################
    ## indices
    ##########################################################################################
    def create_index(
        self, index: str, mappings: dict, if_exist_drop: bool = False
    ) -> dict:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.create
        """
        if self.exists_index(index=index):
            if if_exist_drop is True:
                res = self.client.indices.delete(index=index)
                if res["acknowledged"] is True:
                    print(f"Index '{index}' deleted.")
                else:
                    raise Exception(f"Index '{index}' drop failed.")
            else:
                raise Exception(f"Index '{index}' already exists.")

        return self.client.indices.create(index=index, body=mappings)

    def exists_index(self, index: str) -> bool:
        return self.client.indices.exists(index=index)

    def delete_by_query(self, index: str, query: dict) -> dict:
        """
        특정 조건(query)에 맞는 문서들을 삭제한다.
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.delete_by_query
        """
        try:
            response = self.client.delete_by_query(
                index=index,
                body={
                    "query": query,
                },
            )
            print(
                f"[delete_by_query][{index}] {response.get('deleted', 0)} records deleted"
            )
            return response
        except Exception as e:
            print(f"[delete_by_query][{index}] error: {e}")
            raise

    def count(self, index: str, query: dict = None) -> int:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.count
        """
        if query is None:
            query = {"query": {"match_all": {}}}
        result = self.client.count(index=index, body=query)
        return result["count"]

    def insert(self, index, doc: dict, id_cols: list, parent_id: str = None) -> dict:
        if not self.exists_index(index):
            raise Exception(f"index {index} not exists")

        # create unique id
        _id = self.generate_doc_id(id_cols, doc)

        if parent_id is None:
            return self.client.index(index=index, body=doc, id=_id)
        else:
            return self.client.index(index=index, body=doc, id=_id, routing=parent_id)

    def update(self, index, doc_id: str, doc: dict) -> dict:
        body = {"doc": doc}
        return self.client.update(index=index, id=doc_id, body=body)

    def bulk_insert(
        self,
        index,
        rows: list[dict],
        id_cols: list,
        index_refresh: bool = False,
        upsert: bool = True,
    ) -> dict:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.bulk
        """
        if not self.exists_index(index):
            raise Exception(f"index {index} not exists")

        res = {"inserted": 0, "updated": 0}
        if len(rows) <= 0:
            return res

        # OpenSearch bulk indexing 성능을 위해 문서를 chunk 단위로 나눠 전송
        docs_arr = []
        docs = []
        counter = 0
        for row in rows:
            _id = self.generate_doc_id(id_cols, doc=row)
            docs.append({"update": {"_index": index, "_id": _id}})
            docs.append({"doc": row, "doc_as_upsert": upsert})

            counter += 1
            if (counter % 2000) == 0:
                docs_arr.append(docs)
                docs = []
                counter = 0

        if len(docs) > 0:
            docs_arr.append(docs)
            docs = []

        for data in docs_arr:
            response = self.client.bulk(body=list(data), refresh=index_refresh)
            if response["errors"]:
                print(response)
                for item in response["items"]:
                    for k, v in item.items():
                        print(f"[ERROR] {k} -> {v['error']['reason']}")
            else:
                updated_count = 0
                inserted_count = 0
                for item in response["items"]:
                    action_result = list(item.values())[0]
                    if action_result["result"] == "created":
                        inserted_count += 1
                    elif action_result["result"] == "updated":
                        updated_count += 1

                res["inserted"] += inserted_count
                res["updated"] += updated_count
                print(
                    f"Bulk operation completed successfully. processed inserted cnt {inserted_count}. updated cnt {updated_count}"
                )
        return res

    def bulk_update(
        self, index, rows: list[dict], id_cols: list, index_refresh: bool = False
    ) -> dict:
        """문서가 있을 때만 업데이트"""
        return self.bulk_insert(index, rows, id_cols, index_refresh, upsert=False)

    def truncate(self, index) -> bool:
        """특정 인덱스의 모든 문서를 삭제"""
        try:
            response = self.client.delete_by_query(
                index=index, body={"query": {"match_all": {}}}  # 모든 문서를 선택
            )
            print(f"삭제 완료: {response}")
            return True
        except Exception as e:
            print(f"문서 삭제 중 오류 발생: {e}")
            return False

    ##########################################################################################
    ## Search (search_after, sroll api, paging api)
    ## https://opensearch.org/docs/latest/search-plugins/searching-data/paginate/
    ##########################################################################################
    def search(self, query: dict, index: str = None, scroll: str = None) -> dict:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.search
        """
        if index is not None:
            if scroll:
                result = self.client.search(index=index, body=query, scroll=scroll)
            else:
                result = self.client.search(index=index, body=query)
        else:
            result = self.client.search(body=query)

        return result

    def search_after(self, query: dict, index: str) -> Union[pd.DataFrame, dict]:
        result: list = []
        pit_id = self.create_pit(index)
        query["size"] = self.select_size
        query["pit"] = {"id": pit_id}

        response = self.search(query)
        response_cnt = len(response["hits"]["hits"])
        if response_cnt > 0:
            result += response["hits"]["hits"]
            if response_cnt >= self.select_size:
                cnt = 0
                while True:
                    query["search_after"] = response["hits"]["hits"][-1]["sort"]
                    response = self.search(query=query)
                    response_cnt = len(response["hits"]["hits"])
                    if response_cnt > 0:
                        result += response["hits"]["hits"]

                    if response_cnt < self.select_size:
                        break

                    # defense - maximum 500,000 rows
                    cnt += 1
                    if cnt > 100:
                        self.delete_pit(pit_id)
                        raise Exception("too many query request")
        self.delete_pit(pit_id)
        return self.normallize(result, self.result_type)

    def search_page(self, query: dict, index: str, page: int = 0, size: int = 100):
        if size > self.select_size:
            raise Exception("exceed page size")

        query["size"] = size
        query["from"] = page * size
        result = self.search(query=query, index=index)

        return self.normallize(result["hits"]["hits"], self.result_type)

    def search_scroll(self, query: dict, index: str) -> Union[pd.DataFrame, dict]:
        """
        OpenSearch Scroll API를 사용하여 데이터 검색
        """
        result = []
        scroll_timeout = "1m"
        query["size"] = self.select_size

        # 초기 검색 요청
        response = self.search(query=query, index=index, scroll=scroll_timeout)

        scroll_id = response["_scroll_id"]
        response_cnt = len(response["hits"]["hits"])

        if response_cnt > 0:
            result += response["hits"]["hits"]
            cnt = 0
            while response_cnt > 0:
                response = self.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
                response_cnt = len(response["hits"]["hits"])
                if response_cnt > 0:
                    result += response["hits"]["hits"]

                scroll_id = response["_scroll_id"]

                # defense - maximum 500,000 rows
                cnt += 1
                if cnt > 100:
                    self.clear_scroll(scroll_id)
                    raise Exception("too many query request")

        self.clear_scroll(scroll_id)
        return self.normallize(result, self.result_type)

    def search_buckets(self, query: dict, index: str) -> pd.DataFrame:
        """
        집계 함수 일 때만 사용. agg 쿼리는 size 가 없으므로 페이징 할 필요가 없음
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html
        """
        if "aggs" not in query:
            raise Exception("invalid aggregations query")
        if len(query["aggs"].keys()) > 1:
            raise Exception("Only one aggregation group should be included in aggs.")

        res = pd.DataFrame()
        response = self.search(query, index)
        if "aggregations" not in response:
            return res

        def parse_buckets(buckets: list, pk: str, parent_keys: dict = {}):
            """가장 하위 노드를 찾아 상위 키를 연결. SQL과 동일한 결과로 리턴 시킴"""
            res = []
            last_node = True
            for bucket in buckets:
                for k, v in bucket.items():
                    if type(v) is dict and "buckets" in v:
                        last_node = False
                        parent_keys[pk] = (
                            bucket["key_as_string"]
                            if "key_as_string" in bucket
                            else bucket["key"]
                        )
                        res.extend(parse_buckets(v["buckets"], k, parent_keys))
                        break
                    elif type(v) is dict and "value" in v:
                        bucket[k] = v["value"]

            if last_node is True:
                for bucket in buckets:
                    _pk = parent_keys.copy()
                    _pk[pk] = bucket["key"]
                    bucket.update(_pk)

                    if "key" in bucket:
                        del bucket["key"]

                    res.append(bucket)

            return res

        aggs = response["aggregations"]

        for _pk, agg in aggs.items():
            items = parse_buckets(agg["buckets"], _pk)
            if len(items) > 0:
                return pd.DataFrame(items)

        return res

    def search_composite_buckets(self, query: dict, index: str) -> pd.DataFrame:
        """
        Composite aggregation을 사용하여 10,000개 이상의 문서를 검색할 때 사용.

        Composite aggregation은 중첩 구조가 아니므로 bucket 내에 bucket이 없습니다.
        결과로 나온 `after_key`를 활용해 페이징 처리합니다.

        Parameters
        ----------
        query : dict
            실행할 OpenSearch 쿼리.
        index : str
            검색할 인덱스 이름.

        Returns
        -------
        dict
            검색 결과의 응답 객체.

        참고
        ----
        https://opensearch.org/docs/latest/aggregations/bucket/index/
        """
        if "aggs" not in query:
            raise Exception("invalid aggregations query")
        if len(query["aggs"].keys()) > 1:
            raise Exception("Only one aggregation group should be included in aggs.")

        res = pd.DataFrame()
        response = self.search(query, index)
        if "aggregations" not in response:
            return res

        def flatten_json_value(bucket: dict):
            record = {}
            for k, v in bucket.items():
                if type(v) is dict and "doc_count" in v:
                    record.update(flatten_json_value(v))
                elif type(v) is dict and "value" in v:
                    record[k] = v["value"]
            return record

        def parse_buckets(buckets: list):
            """가장 하위 노드를 찾아 상위 키를 연결. SQL과 동일한 결과로 리턴 시킴"""

            res = []

            for bucket in buckets:
                record = {}
                for k1, v1 in bucket.items():
                    if k1 in ["doc_count"]:
                        continue

                    if k1 == "key":
                        for kk, kv in v1.items():
                            record[kk] = kv

                    if type(v1) is dict and "doc_count" in v1:
                        record.update(flatten_json_value(v1))
                    elif type(v1) is dict and "value" in v1:
                        record[k1] = v1["value"]

                res.append(record)

            return res

        aggs = response["aggregations"]
        for k, agg in aggs.items():
            items = parse_buckets(agg["buckets"])

            if len(items) > 0:
                res = pd.DataFrame(items)
            while "after_key" in aggs[k]:
                print(query)
                query["aggs"][k]["composite"]["after"] = agg["after_key"]
                response = self.search(query, index)
                if "aggregations" not in response:
                    return res
                aggs = response["aggregations"]
                for k, agg in aggs.items():
                    items = parse_buckets(agg["buckets"])
                    if len(items) > 0:
                        res = pd.concat(
                            [res, pd.DataFrame(items)], ignore_index=True
                        ).reset_index(drop=True)
            break

        return res

    def search_by_id(self, index: str, ids: Union[str | list]) -> dict:
        if type(ids) is not list:
            query_ids = [self.generate_id_str(ids)]
        else:
            query_ids = [self.generate_id_str(id_str) for id_str in ids]
        query = {"query": {"ids": {"values": query_ids}}}

        response = self.search(query, index)
        result = self.normallize(response["hits"]["hits"], ResultType.RECORD)

        if type(ids) is not list:
            if len(result) > 0:
                return result[0]

        return result

    ##########################################################################################
    ## Point in time API
    ## https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    ##########################################################################################
    def create_pit(self, index: str, keep_alive: str = "2m") -> str:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.create_pit
        """
        response = self.client.create_pit(index, params={"keep_alive": keep_alive})
        pit_id = response["pit_id"]
        return pit_id

    def delete_pit(self, pit_id: str) -> None:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.delete
        """
        response = self.client.delete_pit(body={"pit_id": pit_id})
        for pit in response["pits"]:
            if pit["successful"] is not True:
                raise Exception(f'pit_id ({pit["pit_id"]}) close failed ')

    ##########################################################################################
    ## Scroll API
    ## https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html
    ##########################################################################################
    def scroll(self, scroll_id: str, scroll: str) -> any:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.scroll
        """
        return self.client.scroll(scroll_id=scroll_id, scroll=scroll)

    def clear_scroll(self, scroll_id: str) -> any:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/clients/opensearch_client.html#opensearchpy.OpenSearch.clear_scroll
        """
        self.client.clear_scroll(scroll_id=scroll_id)

    ##########################################################################################
    ## SQL Query
    ##########################################################################################
    class _SQL:
        """
        https://opensearch-project.github.io/opensearch-py/api-ref/transport.html#opensearchpy.Transport.perform_request
        """

        def __init__(self, outer):
            self.outer = outer
            self.client: OpenSearch = (
                self.outer.client if hasattr(self.outer, "client") else None
            )
            self.select_size = (
                self.outer.select_size if hasattr(self.outer, "select_size") else 10
            )  # es default value

        def query(self, query) -> pd.DataFrame:
            response = self.client.transport.perform_request(
                "POST",
                "/_plugins/_sql",
                body={"query": query, "fetch_size": self.select_size},
            )
            columns = response["schema"]
            data = response["datarows"]
            cursor = response.get("cursor")

            cnt = 0
            while cursor:
                response = self.client.transport.perform_request(
                    "POST", "/_plugins/_sql", body={"cursor": cursor}
                )
                data.extend(response["datarows"])
                cursor = response.get("cursor")

                # defense
                cnt += 1
                if cnt > 100:
                    raise Exception("too many query request")

            return pd.DataFrame(data, columns=[col["name"] for col in columns])

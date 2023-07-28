from elasticsearch import Elasticsearch
import time

es = Elasticsearch(hosts=["172.16.8.39:9200", "172.16.8.40:9200", "172.16.8.41:9200"],
                   http_auth=("elastic", "Elastic123a#Bt"), timeout=500)

repeat_data_count = {
    "size": 0,
    "aggs": {
        "uuids": {
            "terms": {
                "field": "uuid",
                "min_doc_count": 2,
                "size": 10000
            }
        }
    }
}


def es_query(index, query):
    response = es.search(index=index, body=query)
    aggregations = response["aggregations"]["uuids"]["buckets"]

    uuids = []
    for bucket in aggregations:
        uuid = bucket["key"]
        data_count = bucket["doc_count"]
        #print(f"uuid: {uuid} ===> 共 {data_count} 条")
        uuids.append(uuid)

    return uuids


def es_query_uuid(index, uuid):
    query_uuid = {
        "query": {
            "match": {
                "uuid": uuid
            }
        },
        "_source": ["uuid", "collect_url"]
    }

    response = es.search(index=index, body=query_uuid)
    hits = response["hits"]["hits"]

    return hits


def es_delete_data(index, uuid):
    query = {
        "query": {
            "match": {
                "uuid": uuid
            }
        }
    }
    response = es.delete_by_query(index=index, body=query)
    deleted_count = response["deleted"]
    if (deleted_count == 1):
        print(f"uuid: {uuid} ===> 删除成功!")

    time.sleep(2)


def es_delete(hits):
    data_dict = {}
    for data in hits:
        _index = data["_index"]
        _id = data["_id"]

        _source = data["_source"]
        collect_url = _source["collect_url"]
        # print(f"_index: {_index}, _id: {_id}, collect_url: {collect_url}")

        if _index not in data_dict:
            data_dict[_index] = {}
            uid = "id"
            url = "url"
            data_dict[_index][uid] = _id
            data_dict[_index][url] = collect_url

    # for index,values in data_dict.items():
    #     uid = values["id"]
    #     url = values["url"]
    #     print(f"_index: {index} ==> uid: {uid} ==> url: {url}")

    # 先判断 info_pre_2023 是否在字典中,存在就判断url是否包含http,不包含就删除
    info_index = "info_pre_2023"
    if info_index in data_dict.keys():
        info_url = data_dict[info_index]["url"]
        info_uuid = data_dict[info_index]["id"]
        sub_str = "http"
        if info_url is not None and info_url.startswith(sub_str):
            for all_key in data_dict.keys():
                if all_key != info_index:
                    key_id = data_dict[all_key]["id"]
                    key_url = data_dict[all_key]["url"]
                    print(f"_index: {all_key} ==> uuid: {key_id} ==> collect_url: {key_url}")
                    es_delete_data(all_key, info_uuid)
        else:
            key_url = data_dict[info_index]["url"]
            print(f"_index: {info_index} ==> uuid: {info_uuid} ==> collect_url: {key_url}")
            es_delete_data(info_index, info_uuid)
            del data_dict[info_index]
            index_keep = None
            for data_index in data_dict.keys():
                if index_keep is None:
                    index_keep = data_index
                else:
                    other_id = data_dict[data_index]["id"]
                    other_url = data_dict[data_index]["url"]
                    print(f"_index: {data_index} ==> uuid: {other_id} ==> collect_url: {other_url}")
                    es_delete_data(data_index, info_uuid)
    else:
        key_keep = None
        for data_key in data_dict.keys():
            if key_keep is None:
                key_keep = data_key
            else:
                delete_id = data_dict[data_key]["id"]
                delete_url = data_dict[data_key]["url"]
                print(f"_index: {data_key} ==> uuid: {delete_id} ==> collect_url: {delete_url}")
                es_delete_data(data_key, delete_id)


if __name__ == "__main__":
    es_index = "info_pre"
    uuid_list = es_query(es_index,repeat_data_count)
    for uuid in uuid_list:
        hits = es_query_uuid(es_index, uuid)
        es_delete(hits)

    # hits = es_query_uuid(es_index, "0082c9dad0875a3059b90e3b18a05902")
    # es_delete(hits)
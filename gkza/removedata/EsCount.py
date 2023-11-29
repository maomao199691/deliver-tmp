from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=["172.16.8.39:9200", "172.16.8.40:9200", "172.16.8.41:9200"],
                   http_auth=("elastic", "Elastic123a#Bt"), timeout=500)

repeat_data_count = {
    "size": 0,
    "aggs": {
        "uuids": {
            "terms": {
                "field": "uuid",
                "min_doc_count": 2,
                "size": 1000
            }
        }
    }
}

def es_query(index, query):
    response = es.search(index=index, body=query)
    aggregations = response["aggregations"]["uuids"]["buckets"]

    count = len(aggregations)

    print(f"重复uuid 共有 {count} 个")

if __name__ == '__main__':
    es_index = "info_pre"
    es_query(index=es_index, query=repeat_data_count)
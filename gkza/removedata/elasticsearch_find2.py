from elasticsearch import Elasticsearch
import csv

es = Elasticsearch(hosts=["172.16.8.39:9200", "172.16.8.40:9200", "172.16.8.41:9200"],
                   http_auth=("elastic", "Elastic123a#Bt"),timeout=500)

"""
获取所有的数据源以及数量
"""
source_count = {
  "track_total_hits": 2147483647,
  "size": 0,
  "aggs": {
    "terms-task": {
      "terms": {
        "field": "source.keyword",
        "size": 1000000
      }
    }
  }
}

"""
查询是否有自己采集的数据
"""
data_count={
  "query": {
    "bool": {
      "must": [
        {"term": {
          "source.keyword": {
            "value": "国家自然科学基金"
          }
        }},
        {
          "range": {
            "collect_time": {
              "gte": 1687017600000,
              "lte": 1689609600000
            }
          }
        }
      ]
    }
  }
}

es_index = "policies_pre"

def get_one_month(json):
    res = es.count(index=es_index,body=json)
    print(data_count['query']['bool']['must'][0]['term']['source.keyword']['value'])
    return res['count']



if __name__ == "__main__":
    res = es.search(index=es_index, body=source_count)

    buckets = res['aggregations']['terms-task']['buckets']
    file = open('D:\\data\\allSource6.csv', 'w', encoding='utf-8', newline="\n")
    writer = csv.writer(file)
    f = 0
    for kv in buckets:
        f = f+1
        source = kv['key']
        count = kv['doc_count']
        data_count['query']['bool']['must'][0]['term']['source.keyword']['value']=source
        one_count = get_one_month(data_count)
        lists = [source, count,one_count]
        writer.writerow(lists)
        mo = f % 4
        if mo == 0:
            file.flush()

    file.close()
    es.close()

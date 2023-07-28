from elasticsearch import Elasticsearch
import pandas as pd

es = Elasticsearch(hosts=["172.16.8.39:9200", "172.16.8.40:9200", "172.16.8.41:9200"],
                   http_auth=("elastic", "Elastic123a#Bt"), timeout=500)

query = {
"size": 6000,
  "query": {
    "bool": {
      "should": [
        {"match": {
          "alias_title": "开源数据"
        }},
        {
          "match": {
            "alias_full_text": "开源数据"
          }
        }
      ]
    }
  },
  "_source": ["source","collect_url","colum1","colum2","colum3","pub_time","art_title","authors","keywords","abstracts","full_text","collect_time"]
}

if __name__ == '__main__':
  es_index = "info_pre"
  response = es.search(index=es_index, body=query)

  hits = response["hits"]["hits"]

  data_list = []
  for hit in hits:
    data = hit["_source"]
    source = data.get("source", None)
    collect_url = data["collect_url"]
    colum1 = data.get("colum1", None)
    colum2 = data.get("colum2", None)
    colum3 = data.get("colum3", None)
    pub_time = data.get("pub_time", None)
    art_title = data.get("art_title", None)
    authors = data.get("authors", None)
    keywords = data.get("keywords", None)
    abstracts = data.get("abstracts", None)
    full_text = data.get("full_text", None)
    acq_time = data.get("collect_time", None)


    data_dict = {
      "source": source,
      "collect_url": collect_url,
      "colum1": colum1,
      "colum2": colum2,
      "colum3": colum3,
      "pub_time": pub_time,
      "art_title": art_title,
      "authors": authors,
      "keywords": keywords,
      "abstracts": abstracts,
      "full_text": full_text,
      "collect_time": acq_time
    }
    data_list.append(data_dict)

  df = pd.DataFrame(data_list)
  df.to_excel("E:\hadoop\data\ExcelOut\es_info.xlsx", index=False)

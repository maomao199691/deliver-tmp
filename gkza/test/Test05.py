import gkza.util.EsClientUtil as es

if __name__ == '__main__':

    hosts = ["172.16.8.110:9200", "172.16.8.111:9200", "172.16.8.112:9200"]
    username = "elastic"
    password = "Elastic123a#Bt"

    client = es.getEsClient(hosts=hosts,username=username,password=password)

    query_data = {
                  "query": {
                    "match_all": {}
                  },
                  "size": 1000
                }

    es_index = "nraq2_external_dev"

    response = client.search(index=es_index, body=query_data, scroll="1m")

    data_list = response["hits"]["hits"]
    for data in data_list:

        _id = data["_id"]
        _source = data["_source"]

        # print("----> " , _id)
        content = _source["content"]
        url = _source["url"]
        if "www.chinanews.com" not in url:
            if len(content) < 10:
                print("===> ", _id)
from elasticsearch import Elasticsearch

def getEsClient(hosts, username, password):

    es = Elasticsearch(hosts=hosts,http_auth=(username, password), timeout=500)

    return es
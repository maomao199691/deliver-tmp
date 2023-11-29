from pyspark.sql import SparkSession
import json
import redis
from redis_comm import get_value_from_cache,r
import pandas as pd

#r = redis.StrictRedis(host="172.16.8.42",port=30079,db=0, password="123456")

# 创建SparkSession
spark = SparkSession.builder.appName("Process JSON Files").getOrCreate()

spark.conf.set("spark.executorEnv.PYTHONPATH", "D:\App\python-3.7.7\python.exe")

# 文件夹路径
#folder_path = "/path/to/folder"
folder_path = "F:\\专利\\patent\\a_json_20230517\\data000"

# 递归获取所有JSON文件的路径
json_file_paths = spark.sparkContext.textFile(folder_path + "/*.json")


#mydata = []

# 逐行处理JSON数据
def process_json(json_line):
    try:
        json_data = json.loads(json_line)
        # 处理单行JSON数据
        # 例如：访问特定字段、进行计算等操作
        # print(json_data["applicants"])
        if json_data["applicants"] is not None and len(json_data["applicants"]) > 0:
            for applicants in json_data["applicants"]:
                enterprise = r.get("enterprise_id_" + applicants["id"])
                if enterprise is None:
                    continue
                tmpmap = {}
                tmpmap["enterprise_id"] = applicants["id"]
                tmpmap["patent_id"] = json_data["_id"]
                tmpmap["country_code"] = json_data["_id"][0:2]
                tmpmap["country_name"] = get_value_from_cache("data_gjjc", tmpmap["country_code"])

                #mydata.append(tmpmap)
                # print(tmpmap)
                # print("存在：" + applicants["id"])
                return tmpmap
        else:
            if json_data["family_number"]:
                enterprises = r.smembers('family_number_id_' + json_data["family_number"])
                if len(enterprises) > 0:
                    # print("family_number:" + enterprises)
                    for enterprise in enterprises:
                        tmpmap = {}
                        tmpmap["enterprise_id"] = str(enterprise)
                        tmpmap["patent_id"] = json_data["_id"]
                        tmpmap["country_code"] = json_data["_id"][0:2]
                        # tmpmap["country_name"] = get_value_from_cache("data_gjjc", tmpmap["country_code"])
                        tmpmap["country_name"] = ""
                        #mydata.append(tmpmap)
                        return tmpmap

            if json_data["inpadoc_number"]:
                enterprises = r.smembers('inpadoc_number_id_' + json_data["inpadoc_number"])
                if len(enterprises) > 0:
                    # print("inpadoc_number:" + enterprises)
                    for enterprise in enterprises:
                        tmpmap = {}
                        tmpmap["enterprise_id"] = str(enterprise)
                        tmpmap["patent_id"] = json_data["_id"]
                        tmpmap["country_code"] = json_data["_id"][0:2]
                        # tmpmap["country_name"] = get_value_from_cache("data_gjjc", tmpmap["country_code"])
                        tmpmap["country_name"] = ""
                        #mydata.append(tmpmap)
                        return tmpmap
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as ex:
        print(f"Error processing JSON data: {ex}")

# 对每条JSON数据应用处理函数
dictRdd = json_file_paths.map(process_json)


data_list = dictRdd.collect()
for item in data_list:
    print(item)

# df_pd = pd.DataFrame(dictRdd, columns=["enterprise_id", "patent_id", "country_code", "country_name"])
#
# #df = spark.createDataFrame(dictRdd, ["enterprise_id", "patent_id", "country_code", "country_name"])
# #dictRdd.map(lambda x: str(x))
# df = spark.createDataFrame(df_pd)
#
# df.show()
#df.write.csv("E:\\data")

# 停止SparkSession
spark.stop()
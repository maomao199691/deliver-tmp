from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("test") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.local.dir", "E:/tmp/spark")\
    .enableHiveSupport().getOrCreate()

folder_path = "F:\\专利\\patent\\a_json_20230517\\data003"

json_file_paths = spark.sparkContext.textFile(folder_path + "/*.json")

def process_json(json_line):
    try:
        json_data = json.loads(json_line)
        print(json_data)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as ex:
        print(f"Error processing JSON data: {ex}")
    print("====>>")

dictRdd = json_file_paths.map(process_json)

data_list = dictRdd.collect()
for item in data_list:
    print(item)

print("执行结束")
spark.stop()
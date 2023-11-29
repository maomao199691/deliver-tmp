from pyspark.sql import SparkSession

def getSession():
    spark = SparkSession.builder.appName("test") \
        .master("local[*]") \
        .config("hive.metastore.uris", "thrift://hadoop6:9083") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .enableHiveSupport().getOrCreate()

    return spark
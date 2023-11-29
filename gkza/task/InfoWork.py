from pyspark.sql import SparkSession
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def run():
    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .config("hive.metastore.uris", "thrift://hadoop6:9083")\
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .enableHiveSupport().getOrCreate()

    work_id = "aa8399b21897cf16517cc72f5463942e"
    start_date = 2023072200
    end_date = 2023072223

    fields = ["work_id", "art_id", "art_title", "authors", "full_text", "collect_url", "publisher", "pub_time", "pub_date",
              "pub_year", "source", "source_type", "file_name", "data_type", "collect_way", "for_project", "domain"]

    new_columns = ["任务id" ,"唯一id", "标题", "作者", "正文", "采集地址", "发布者", "发布时间", "发布日期", "发布年", "来源", "来源类型",
                   "文件名称", "数据类型", "采集方式", "应用项目", "域名"]

    field_str = ",".join(fields)

    sql = "SELECT {field} FROM special_database.info_collect WHERE cdate between {start} and {end} and work_id='{workId}'"\
        .format(field=field_str, start=start_date, end=end_date,workId=work_id)

    logger.info(">>>>>>读取hive中......<<<<<<")

    hive_df = spark.sql(sql)

    logger.info(">>>>>>读取hive完毕<<<<<<")

    #hive_df.show()

    df = hive_df.toDF(*new_columns)

    pandas_df = df.toPandas()

    writer = pd.ExcelWriter("E:\hadoop\data\ExcelOut\pandas.xlsx", engine='openpyxl')

    pandas_df.to_excel(writer, sheet_name='Sheet1', index=False)

    writer.save()

    logger.info(">>>>>>写入Excel完毕<<<<<<")
from pyspark.sql import SparkSession
import pandas as pd
import gkza.test.SparkUdf as ud
import gkza.base.SparkSessionUtil as sp
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()



if __name__ == '__main__':
    spark = sp.getSession()

    #spark.udf.register("deimage", ud.delete_image)

    sql = """select * from database_01.enterprise_table limit 100;"""

    logger.info(">>>>>>hive读取中<<<<<<<")

    hive_df = spark.sql(sql)

    logger.info(">>>>>>读取hive完毕<<<<<<")

    hive_df.show()

    spark.stop
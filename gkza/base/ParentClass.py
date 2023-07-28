from gkza.util.SparkUtil import getSparkSeession
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

class ParentClass:
    def __init__(self, table_name, work_id, start_date, end_date, fields, new_columns):
        self.table_name = table_name
        self.work_id = work_id
        self.start_date = start_date
        self.end_date = end_date
        self.fields = fields
        self.new_columns = new_columns

    def read_hive(self):
        session = getSparkSeession()

        field_str = ",".join(self.fields)

        sql = "SELECT {field} FROM special_database.{table} WHERE cdate between {start} and {end} and work_id='{workId}'"\
        .format(field=field_str,table=self.table_name, start=self.start_date, end=self.end_date,workId=self.work_id)

        logger.info(">>>>>>读取hive中......<<<<<<")

        hive_df = session.sql(sql)

        logger.info(">>>>>>读取hive完毕<<<<<<")

        df = hive_df.toDF(*self.new_columns)

        pandas_df = df.toPandas()

        return pandas_df

    @staticmethod
    def write_excel(df, path):
        writer = pd.ExcelWriter(path, engine='openpyxl')
        df.to_excel(writer, sheet_name="Sheet1", index=False)
        writer.save()

        logger.info(">>>>>>写入Excel完毕<<<<<<")
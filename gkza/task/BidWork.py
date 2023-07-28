from gkza.base.ParentClass import ParentClass
from gkza.resources.config import bid_table_name, bid_work_id, bid_start, bid_end, bid_fields, bid_columns,bid_path

class bidwork(ParentClass):

    def __init__(self):
        super().__init__(bid_table_name, bid_work_id, bid_start, bid_end, bid_fields, bid_columns)

    def run(self):
        data_df = super().read_hive()

        ParentClass.write_excel(data_df, bid_path)


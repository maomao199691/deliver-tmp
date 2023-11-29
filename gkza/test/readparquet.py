import pandas as pd
from pyarrow.parquet import ParquetFile

# # 读取 Snappy 压缩的 Parquet 文件
# table = ParquetFile('E:\\part-00000-3416ca3d-44e2-4016-ac83-676ccd01750d.c000.snappy.parquet')
#
# # 将数据转换为 Pandas DataFrame
# df = table.to_pandas()
#
# # 设置 Pandas 显示选项以显示所有列和行
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# pd.set_option('display.width', None)  # 可选：设置输出的宽度
#
# # 打印 DataFrame 内容
# print(df)

df = pd.read_parquet('E:\data\part-00001-afdb0afe-7d2f-4924-8c46-8fe6f704812f.c000.snappy.parquet')

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)  # 可选：设置输出的宽度


# data_count = df.count()
# # 打印 DataFrame 内容
# print(' ===> ', data_count)

info = df.info()
print(info)


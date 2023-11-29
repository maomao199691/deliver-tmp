import pandas as pd

if __name__ == '__main__':

    hdfs_path = 'hdfs://hadoop10:8020/enterprise-test/河北_-_2.xlsx'

    df = pd.read_excel(hdfs_path)


    print(df.head())
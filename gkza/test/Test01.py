import gkza.util.MysqlUtil as mysl
import pandas as pd

def get_domain(url):
    if url:
        if "//" in url:
            split = url.split("//")
            s = None
            try:
                s = split[1]
            except Exception as e:
                print("===>", s)
                return None

            if s.startswith("//"):
                s = s[2:]

            if "/" in s:
                i = s.index("/")
                sub_str = s[:i]
                if ":" in sub_str:
                    index = sub_str.index(":")
                    if index > 0:
                        sub_str = sub_str[:index]

                return sub_str

            i1 = s.find(":")
            if i1 > 0:
                s = s[:i1]

            return s

        if "/" in url:
            i = url.index("/")
            domain = url[:i]
            i1 = domain.index(":")
            if i1 > 0:
                domain = domain[:i1]

            return domain

    return url

if __name__ == '__main__':
    file_path = 'E:\data\锐寻数据总结.xlsx'

    df = pd.read_excel(file_path, sheet_name=10, header=0)
    filter_df = df.query('`数据采集渠道` != "空白"')

    mysql_conn = mysl.connect_to_mysql("172.16.8.46", "root", "123456", "data_handle")

    cursor = mysql_conn.cursor()
    for index, row in df.iterrows():

        table_name = "credential_collect"
        data_source = row["数据源"]
        if pd.isnull(data_source):
            data_source = None
        is_inc = row["是否在持续采"]
        collect_way = row["数据采集渠道"]
        collect_url = row["链接地址"]

        domain = None
        if pd.isnull(collect_url):
            collect_url = None
        else:
            domain = get_domain(str(collect_url))

        sql = "INSERT INTO ruixun_source (`table_name`, `data_source`, `is_inc`, `collect_way`, `collect_url`, `domain`) values(%s, %s, %s, %s, %s, %s)"

        values = (table_name, data_source, is_inc, collect_way, collect_url, domain)

        cursor.execute(sql, values)

    mysql_conn.commit()
    mysl.close_mysql_connection(mysql_conn)
import gkza.util.MysqlUtil as mysl

if __name__ == '__main__':
    mysql_conn = mysl.connect_to_mysql("172.16.8.46", "root", "123456", "data_handle")

    cursor = mysql_conn.cursor()

    query_sql = """select domain
from ruixun_source where table_name = 'info_collect' and is_inc = '持续采集' and collect_way = '爬虫采集' and domain != 'mp.weixin.qq.com';"""

    query_sql1 = """select distinct domain
from ruixun_source
where table_name = 'project_collect' and is_inc = '持续采集' and collect_way = '爬虫采集';"""

    cursor.execute(query_sql1)
    df = cursor.fetchall()

    where_sql = "("
    for row in df:
        domain_str = row[0]
        where_sql += f"\"{domain_str}\","

    where_sql += ")"

    print("===> ", where_sql)


    mysql_conn.commit()
    mysl.close_mysql_connection(mysql_conn)
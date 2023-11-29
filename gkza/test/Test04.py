import gkza.util.HiveConnUtil as myhive
import gkza.util.MysqlUtil as mql

if __name__ == '__main__':

    conn = mql.connect_to_mysql("172.16.8.46", "root", "123456", "data_handle")

    cursor = conn.cursor()
    #
    # query = "select * from support_domain_count where table_name = 'info_collect' and `delete` = 0"
    #
    # cursor.execute(query)
    #
    # result = cursor.fetchall()
    #
    # for row in result:
    #     print(row)
    #
    # mql.close_mysql_connection()

    info_sql1 = """select domain,source_type,cdate,count(*) as count
from(select domain,source_type,floor(cdate/100) as cdate from info_collect
where  domain != 'mp.weixin.qq.com') tmp group by domain,source_type, cdate"""

    info_sql2 = """select source,source_type,cdate,count(*) as count
from(select domain,source_type,floor(cdate/100) as cdate from info_collect
where  domain = 'mp.weixin.qq.com') tmp group by domain,source_type, cdate"""

    hive_conn = myhive.HiveConnector(host='hadoop8', port=10000, username='root', password='hive', database='special_database')

    hive_conn.connect()

    result = hive_conn.execute_query(info_sql1)

    # 先删除数据
    # delete_sql = "DELETE from support_domain_count where table_name='info_collect' and type = 1 and `delete`=0;"
    # cursor.execute(delete_sql)
    # print("数据删除成功")

    insert_sql = "INSERT INTO support_domain_count (`domain`, `cdate`, `count`, `type`, `table_name`, `source_type`, `delete`) VALUES (%s, %s, %s, %s, %s, %s, %s) " \
                 "ON DUPLICATE KEY UPDATE `domain` = VALUES(`domain`), `cdate` = VALUES(`cdate`), `count` = VALUES(`count`), `type` = VALUES(`type`), `table_name` = VALUES(`table_name`), " \
                 "`source_type` = VALUES(`source_type`), `delete` = VALUES(`delete`)"
    for row in result:
        print(row)

    hive_conn.close_conn()
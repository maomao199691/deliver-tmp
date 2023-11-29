import gkza.util.MyPyMysqlUtil as myl


if __name__ == '__main__':

        conn = myl.connect_to_mysql("172.16.8.75", 4000, "root", "=pT4M9%5Y3k6s0R#@N", "cei_data")

        try:
           cursor = conn.cursor()

           sql = """select * from cei_data_area limit 20"""

           cursor.execute(sql)

           results = cursor.fetchall()

           for row in results:
               print(row)

        finally:
                myl.close_mysql_connection(conn)

    # with myl.get_connection("172.16.8.75", 4000, "root", "=pT4M9%5Y3k6s0R#@N", "cei_data") as conn:
    #     with conn.cursor() as cur:
    #         cur.execute("select * from cei_data_area limit 100")
    #         df = cur.fetchall()
    #         for row in df:
    #             print(row)

    # conn = myl.get_connection("172.16.8.75", 4000, "root", "=pT4M9%5Y3k6s0R#@N", "cei_data")
    # cur = conn.cursor()
    # cur.execute("select * from cei_data_area limit 100")
    # df = cur.fetchall()
    # for row in df:
    #     print(row)
    #
    # myl.close_database_connection(conn)





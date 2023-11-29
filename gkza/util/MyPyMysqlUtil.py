import pymysql
import logging

def connect_to_mysql(host, port, username, password, database):
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            db=database
        )
        if connection.open:
            logging.info("成功连接到MySQL数据库")
            return connection
    except pymysql.Error as error:
        logging.error("连接到MySQL数据库时出现错误:", error)
        return None

def close_mysql_connection(connection):
    if connection.open:
        connection.close()
        logging.info("已关闭与MySQL数据库的连接")


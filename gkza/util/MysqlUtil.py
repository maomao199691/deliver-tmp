import mysql.connector
import logging

def connect_to_mysql(host, username, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=database
        )
        if connection.is_connected():
            logging.info("成功连接到MySQL数据库")
            return connection
    except mysql.connector.Error as error:
        logging.error("连接到MySQL数据库时出现错误:", error)
        return None

def close_mysql_connection(connection):
    if connection.is_connected():
        connection.close()
        logging.info("已关闭与MySQL数据库的连接")
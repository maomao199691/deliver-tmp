import mysql.connector
from mysql.connector import MySQLConnection
import logging

def get_connection(host, port, user, password, database,autocommit: bool = True) -> MySQLConnection:

    db_conf = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "autocommit": autocommit,
        # mysql-connector-python will use C extension by default,
        # to make this example work on all platforms more easily,
        # we choose to use pure python implementation.
        "use_pure": True,
    }

    # if config.ca_path:
    #     db_conf["ssl_verify_cert"] = True
    #     db_conf["ssl_verify_identity"] = True
    #     db_conf["ssl_ca"] = config.ca_path
    try:
        conn = mysql.connector.connect(**db_conf)
        if conn.is_connected():
            logging.info("成功连接到数据库")
            return conn
    except mysql.connector.Error as error:
        logging.error("连接数据库异常: ", error)
        return None

def close_database_connection(connection):
    if connection.is_connected():
        connection.close()
        logging.info("已关闭与数据库的连接")

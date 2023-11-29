from pyhive import hive
import logging
from colorama import init, Fore, Style

# 初始化 colorama 库
init()

# 创建 logger
logger = logging.getLogger(__name__)

# 设置 logger 的级别
logger.setLevel(logging.INFO)

# 创建控制台处理程序
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建格式化器
formatter = logging.Formatter(Fore.BLUE + '%(asctime)s - %(levelname)s - %(message)s' + Style.RESET_ALL)

# 将格式化器应用于处理程序
console_handler.setFormatter(formatter)

# 将处理程序添加到 logger
logger.addHandler(console_handler)

class HiveConnector:
    def __init__(self, host, port, username, password, database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = hive.connect(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                auth='CUSTOM'
            )
            self.cursor = self.connection.cursor()
            logger.info("成功连接到Hive")
        except Exception as e:
            logger.error("连接到Hive时出现错误:", str(e))

    def execute_query(self, sql):
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            logger.error("查询错误: ", str(e))

    def close_conn(self):
        if self.conn:
            self.conn.close()
            logger.info("连接关闭")
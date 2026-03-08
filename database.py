# src/utils/database.py
import sqlite3
import threading
import logging
from pathlib import Path
import pandas as pd

from src.utils.config_loader import get_db_path
from src.utils.logging_config import get_logger


class DatabaseManager:
    _write_lock = threading.Lock()
    _instance = None  # 单例模式
    
    def __new__(cls, *args, **kwargs):
        """单例模式：确保全局只有一个数据库连接管理器"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_path=None, logger=None):
        # 避免重复初始化
        if self._initialized:
            return
            
        self.db_path = Path(db_path or get_db_path())
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        self._ensure_db_exists()
        self._initialized = True
    
    def _ensure_db_exists(self):
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.db_path.exists():
                conn = sqlite3.connect(str(self.db_path))
                conn.close()
                self.logger.info(f"创建新数据库文件: {self.db_path}")
        except Exception as e:
            self.logger.error(f"确保数据库存在失败: {e}")
    
    def connect(self):
        if self.connection is None:
            try:
                self.connection = sqlite3.connect(
                    str(self.db_path),
                    check_same_thread=False,  # 允许跨线程，但用锁保护写
                    timeout=60.0              # 超时加长，避免短暂锁定
                )
                self.connection.execute("PRAGMA journal_mode = WAL")
                self.connection.execute("PRAGMA foreign_keys = ON")
                self.connection.execute("PRAGMA busy_timeout = 100")  # 忙时等待1秒
                self.connection.row_factory = sqlite3.Row
                self.logger.debug(f"数据库连接成功: {self.db_path}")
            except Exception as e:
                self.logger.error(f"数据库连接失败: {e}")
                self.connection = None
    
    def close(self):
        if self.connection:
            try:
                self.connection.close()
                self.logger.debug("数据库连接已关闭")
            except Exception as e:
                self.logger.error(f"关闭连接失败: {e}")
            finally:
                self.connection = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            try:
                if exc_type is None:
                    self.connection.commit()
                    self.logger.info("事务提交成功")
                else:
                    self.connection.rollback()
                    self.logger.warning("事务回滚")
            except Exception as e:
                self.logger.error(f"事务处理异常: {e}")
            finally:
                self.close()
                self.logger.debug("数据库连接关闭")

    def execute_query(self, query, params=()):
        if not self.connection:
            self.connect()
        if not self.connection:
            return None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"查询执行失败: {query} | {e}")
            return None

    def query_one(self, query, params=()):
        """查询单条记录"""
        results = self.execute_query(query, params)
        if results:
            return dict(results[0])
        return None

    def query_df(self, query, params=None):
        """查询并返回DataFrame"""
        if not self.connection:
            self.connect()
        if not self.connection:
            raise RuntimeError("无法获取数据库连接")
        try:
            if params:
                df = pd.read_sql_query(query, self.connection, params=params)
            else:
                df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            self.logger.error(f"执行查询失败: {query}\n参数: {params}\n错误: {e}")
            raise

    def insert_or_replace(self, table_name: str, columns: list, values_list: list) -> int:
        if not values_list:
            return 0
        if not self.connection:
            self.connect()
        if not self.connection:
            self.logger.error("无法插入数据：无有效连接")
            return 0
        
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT OR REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        with self._write_lock:
            try:
                cursor = self.connection.cursor()
                cursor.executemany(query, values_list)
                self.logger.debug(f"成功插入/替换 {len(values_list)} 条记录到 {table_name}")
                return len(values_list)
            except Exception as e:
                self.connection.rollback()
                self.logger.error(f"批量插入失败 {table_name}: {e}", exc_info=True)
                return 0


# 标准列定义
OPTION_STANDARD_COLUMNS = [
    "期权合约代码",
    "交易日期",
    "开盘价",
    "最高价",
    "最低价",
    "收盘价",
    "前结算价",
    "结算价",        
    "收盘涨跌",
    "结算涨跌",
    "成交量",
    "持仓量",
    "行权量",
    "持仓变化",
    "成交额",
    "行权价",
    "期权类型",
]

OPTION_COLUMN_TYPES = {
    "期权合约代码": "TEXT",
    "交易日期": "TEXT",
    "开盘价": "REAL",
    "最高价": "REAL",
    "最低价": "REAL",
    "收盘价": "REAL",
    "前结算价": "REAL",
    "结算价": "REAL",
    "收盘涨跌": "REAL",
    "结算涨跌": "REAL",
    "成交量": "INTEGER",
    "持仓量": "INTEGER",
    "行权量": "INTEGER",
    "持仓变化": "INTEGER",
    "成交额": "REAL",
    "行权价": "INTEGER",
    "期权类型": "TEXT",
}

# ============================================================
# ✅ 关键修复：创建全局数据库实例
# ============================================================

# 全局数据库实例（单例模式）
db = DatabaseManager()

# 便捷函数
def get_db():
    """获取全局数据库实例"""
    return db

def create_standard_option_table(db_path: str = None, table_name: str = None):
    if table_name is None:
        raise ValueError("table_name 必传")
    db_path = db_path or get_db_path()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    col_defs = ", ".join([f'"{col}" {OPTION_COLUMN_TYPES[col]}' for col in OPTION_STANDARD_COLUMNS])
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {col_defs},
        PRIMARY KEY ("期权合约代码", "交易日期")
    )
    """
    cursor.execute(sql)
    
    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}("交易日期")')
    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_contract ON {table_name}("期权合约代码")')
    
    conn.commit()
    conn.close()


FU_STANDARD_COLUMNS = [
    "期货合约",
    "交易日期",
    "开盘价",
    "最高价",
    "最低价",
    "收盘价",
    "结算价",
    "成交量",
    "持仓量"
]

FU_COLUMN_TYPES = {
    "期货合约": "TEXT",
    "交易日期": "TEXT",
    "开盘价": "REAL",
    "最高价": "REAL",
    "最低价": "REAL",
    "收盘价": "REAL",
    "结算价": "REAL",
    "成交量": "INTEGER",
    "持仓量": "INTEGER"
}

def create_standard_futures_table(db_path: str = None, table_name: str = None):
    if table_name is None:
        raise ValueError("table_name 必传（动态期货表）")
    db_path = db_path or get_db_path()  

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    col_defs = ", ".join([f'"{col}" {FU_COLUMN_TYPES[col]}' for col in FU_STANDARD_COLUMNS])
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {col_defs},
        PRIMARY KEY ("期货合约", "交易日期")
    )
    """
    cursor.execute(sql)
    
    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}("交易日期")')
    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_contract ON {table_name}("期货合约")')
    
    conn.commit()
    conn.close()

def create_stock_table(db_path: str = None):
    db_path = db_path or get_db_path()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock (
            "指数名称" TEXT NOT NULL,
            "交易日期" TEXT NOT NULL,                
            "开盘价" REAL,
            "最高价" REAL, 
            "最低价" REAL, 
            "收盘价" REAL NOT NULL, 
            "成交量" INTEGER,                               
            PRIMARY KEY ("指数名称", "交易日期")
        );
    """)

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_date ON stock ("交易日期")')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_name ON stock ("指数名称")')
    conn.commit()
    conn.close()

def create_ivmr_table(db_path: str = None):
    db_path = db_path or get_db_path()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ivmr (
            "期权合约代码" TEXT NOT NULL,
            "交易日期" TEXT NOT NULL,
            "交易所" TEXT NOT NULL,                
            iv REAL,                               
            hv20 REAL,                               
            delta REAL,
            gamma REAL,
            theta REAL,
            vega REAL,
            rho REAL,
            "理论价格" REAL,                       
            "内在价值" REAL,                  
            "时间价值" REAL,                  
            "市场价差" REAL,                   
            ivmr3 REAL,
            ivmr7 REAL,
            ivmr15 REAL,
            ivmr30 REAL,
            ivmr90 REAL,
            ivmr REAL,                             
            PRIMARY KEY ("期权合约代码", "交易日期", "交易所")
        );
    """)

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ivmr_date ON ivmr ("交易日期")')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ivmr_name ON ivmr ("期权合约代码")')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ivmr_date_exchange ON ivmr ("交易日期", "交易所")')
    conn.commit()
    conn.close()
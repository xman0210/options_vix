# src/utils/update_utils.py
import json
import os
import pandas as pd
import numpy as np
from typing import Callable, List
from datetime import date, timedelta, datetime
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config_loader import get_db_path, load_config, get_project_root
CONFIG = load_config()
from .database import DatabaseManager, OPTION_STANDARD_COLUMNS, FU_STANDARD_COLUMNS


# ==================== 交易日缓存（全局共享） ====================
_CACHED_TRADING_DAYS = None
_CACHED_TRADING_DAYS_STR = None

INITIAL_START_DATE = date.fromisoformat(CONFIG["updater"]["common"].get("initial_start_date", "2025-01-02"))
FORCE_RECENT_DAYS = CONFIG["updater"]["common"].get("force_recent_days_on_full", 15)  # 全量时强制覆盖最近N个交易日

def to_ak_date(d: date) -> str:
    return d.strftime("%Y%m%d")

def safe_num(val, to_int=False):
    try:
        num = float(val)
        return int(num) if to_int else num
    except (ValueError, TypeError):
        return None

def get_row_value(row, candidates: List[str]):
    for cand in candidates:
        if cand in row and pd.notna(row[cand]):
            return row[cand]
    return None

def refresh_trading_days() -> None:
    global _CACHED_TRADING_DAYS, _CACHED_TRADING_DAYS_STR
    _CACHED_TRADING_DAYS = None
    _CACHED_TRADING_DAYS_STR = None
    get_trading_days()

# ==================== 交易日缓存（全局共享，全项目复用） ==================================
def get_trading_days(end_date: date = None, force_refresh: bool = False) -> List[date]:
    global _CACHED_TRADING_DAYS, _CACHED_TRADING_DAYS_STR

    if force_refresh or _CACHED_TRADING_DAYS is None:
        trade_day_file = CONFIG["paths"]["trade_day_file"]
        try:
            with open(trade_day_file, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)

            if isinstance(raw_data, dict) and "trading_days" in raw_data:
                date_strs = raw_data["trading_days"]
            elif isinstance(raw_data, list):
                date_strs = raw_data
            else:
                raise ValueError(f"交易日文件 {trade_day_file} 格式错误，必须是列表或 {'trading_days': [...]}")

            _CACHED_TRADING_DAYS = []
            _CACHED_TRADING_DAYS_STR = []  # 同时生成 YYYYMMDD 格式
            for s in date_strs:
                try:
                    d = datetime.strptime(s, "%Y-%m-%d").date()
                    _CACHED_TRADING_DAYS.append(d)
                    _CACHED_TRADING_DAYS_STR.append(d.strftime("%Y%m%d"))
                except ValueError:
                    print(f"无效日期字符串跳过: {s}")

            if not _CACHED_TRADING_DAYS:
                raise ValueError("交易日列表为空")
            
            print(f"交易日文件加载成功，共 {len(_CACHED_TRADING_DAYS)} 个交易日（最后: {_CACHED_TRADING_DAYS[-1] if _CACHED_TRADING_DAYS else '无'}）")
        except Exception as e:
            # 失败时返回空列表，避免系统崩溃
            _CACHED_TRADING_DAYS = []
            _CACHED_TRADING_DAYS_STR = []

    if end_date is None:
        return _CACHED_TRADING_DAYS[:]

    return [d for d in _CACHED_TRADING_DAYS if d <= end_date]

def get_prev_trading_day(target_date: date) -> date:
    """返回 target_date 的前一个交易日"""
    trading_days = get_trading_days(target_date)
    for d in reversed(trading_days):
        if d < target_date:
            return d
    raise ValueError(f"无 {target_date} 之前的交易日")

def get_next_trading_day(target_date: date) -> date:
    """返回 target_date 的后一个交易日"""
    trading_days = get_trading_days(target_date + timedelta(days=60))
    for d in trading_days:
        if d > target_date:
            return d
    raise ValueError(f"无 {target_date} 之后的交易日")

def get_latest_db_date(
    table_name: str,
    date_column: str = "交易日期"
) -> str | None:
    db_path = get_db_path()
    query = f'SELECT MAX("{date_column}") FROM {table_name}'
    
    try:
        with DatabaseManager(db_path) as db_mgr:
            result = db_mgr.execute_query(query)
            if result and result[0][0] is not None:
                return result[0][0]  # 返回字符串，如 '2025-12-28'
            return None
    except Exception as e:
        print(f"查询 {table_name} 最新日期失败: {e}")
        return None

# ==================== 通用缺失日期查（updater + risker 复用） ====================
def get_missing_dates(table_name: str, date_column: str, candidate_dates: List[str]) -> List[str]:
    if not candidate_dates:
        return []

    db_path = get_db_path()
    placeholders = ",".join(["?"] * len(candidate_dates))
    query = f'SELECT DISTINCT "{date_column}" FROM {table_name} WHERE "{date_column}" IN ({placeholders})'
    
    existing_dates = set()
    try:
        with DatabaseManager(db_path) as db_mgr:
            result = db_mgr.execute_query(query, tuple(candidate_dates))
            if result:
                existing_dates = {row[0] for row in result if row[0]}
        print(f"{table_name} 已存在 {len(existing_dates)} 个日期，候选 {len(candidate_dates)} 个")
    except Exception as e:
        print(f"查询 {table_name} 已存在日期失败: {e}")
        return candidate_dates[:]  # 保守策略：全部视为缺失

    missing = [d for d in candidate_dates if d not in existing_dates]
    return missing

# ==================== 核心日期判定函数 ====================
def determine_end_date_for_updater(current_time: datetime = None) -> date:
    """严格遵守18:00分界，返回可信的最新交易日期"""
    if current_time is None:
        current_time = datetime.now()

    today = current_time.date()
    trading_days = get_trading_days(today)

    if current_time.hour >= 18:
        return today if today in trading_days else get_prev_trading_day(today)
    else:
        return get_prev_trading_day(today)

# ==================== 插入函数（保持不变） ====================
def insert_standard_option_records(db_mgr: DatabaseManager, table_name: str, records: List[dict]) -> int:
    if not records:
        return 0
    
    columns = OPTION_STANDARD_COLUMNS
    values_list = []
    for record in records:
        values = [record.get(col) for col in columns]
        values_list.append(tuple(values))
    
    return db_mgr.insert_or_replace(table_name, columns, values_list)

def insert_standard_futures_records(db_mgr: DatabaseManager, table_name: str, records: List[dict]) -> int:
    if not records:
        return 0
    
    columns = FU_STANDARD_COLUMNS
    values_list = []
    for record in records:
        values = [record.get(col) for col in columns]
        values_list.append(tuple(values))
    
    print(f"{table_name} 准备插入 {len(values_list)} 条记录")
    inserted = db_mgr.insert_or_replace(table_name, columns, values_list)
    print(f"{table_name} 插入完成，返回 {inserted} 条")

    return inserted



# ==================== 计算脚本公共工具函数 ================================
# ==================== 通用 TTE 计算（risker 专用工具） ====================
def calculate_tte(expiry_date_str: str, current_date_str: str) -> float:
    """
    精确交易日 TTE（年化，含当日 / 252）
    """
    trading_days = get_trading_days()
    expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
    current_date = datetime.strptime(current_date_str, "%Y-%m-%d").date()
    
    try:
        current_idx = trading_days.index(current_date)
        expiry_idx = trading_days.index(expiry_date)
        remaining_days = expiry_idx - current_idx + 1
        return remaining_days / 252.0
    except ValueError:
        return np.nan

# ==================== 通用回补框架（risker） ===========================
def backfill_missing_dates(
    table_name: str,
    compute_one_day_func: Callable[[str], None],
    start_date_str: str,
    end_date_str: str = None
):
    """
    通用回补（查缺失日期 + 计算）
    """
    candidate_dates = [d.strftime("%Y-%m-%d") for d in get_trading_days() if start_date_str <= d.strftime("%Y-%m-%d") <= (end_date_str or datetime.now().strftime("%Y-%m-%d"))]
    missing = get_missing_dates(table_name, "交易日期", candidate_dates)
    
    for date_str in tqdm(missing, desc="回补进度"):
        compute_one_day_func(date_str)

"""
标的资产映射工具 - 仅处理CFFEX特殊规则
✅ 商品期权：品种代码完全一致（CU→CU, SI→SI）
✅ CFFEX：IO→IF, MO→IM 等（7行映射）
✅ 返回小写用于表名，查询时转大写匹配数据库
"""
_CFFEX_MAPPING = {"io": "if", "mo": "im", "ho": "ih", "eo": "ic", "t": "ts", "tf": "tf", "tl": "t"}

def get_underlying_product(option_product: str, exchange: str) -> str:
    """
    获取标的期货品种代码（小写）→ 用于生成表名 fu_{code}
    数据库查询时需用 .upper() 匹配 product 字段（IF/CU/SI）
    """
    opt = option_product.strip().lower()
    return _CFFEX_MAPPING.get(opt, opt) if exchange.lower() == "cffex" else opt







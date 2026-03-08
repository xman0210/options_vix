# data_loader.py
"""
数据加载与过滤模块
职责：执行三条件精准过滤，返回待计算合约及完整统计
"""
import pandas as pd
import logging
from config_loader import ConfigLoader
from database import db  # 假设已有数据库连接模块

logger = logging.getLogger(__name__)

def load_filtered_positions(trade_date: str, exchange: str) -> tuple[pd.DataFrame, dict]:
    """
    执行三条件过滤，返回待计算合约DataFrame及统计字典
    
    三条件：
    1. 合约名在 {exchange}_op_name 配置列表中
    2. 合约到期日在 {exchange}_op_expiry_dates 配置列表中
    3. op_{exchange} 表中当日存在有效数据（settle_price非空）
    
    返回:
        (contracts_df, stats_dict)
        stats_dict 包含5个核心指标：
        - config_contract_count: 配置ris_name字符集中的合约总数
        - contracts_meet_expiry: 满足条件1+2的合约数（ris_name ∩ expiry_date配置）
        - op_table_contract_count: op_表中当日有数据的合约数（且在ris_name内）
        - final_contract_count: 三条件交集后的待计算合约数
        - success_count: 预留字段（由计算层填充）
    """
    config = ConfigLoader()
    
    # === 条件1：加载配置合约名 ===
    valid_names_key = f"{exchange}_op_name"
    valid_names = set(config.get(valid_names_key, []))
    config_contract_count = len(valid_names)
    
    # === 条件2：加载配置到期日 ===
    valid_expiry_key = f"{exchange}_op_expiry_dates"
    valid_expiry = set(config.get(valid_expiry_key, []))
    config_expiry_count = len(valid_expiry)  # 辅助参考值
    
    # 配置校验
    if not valid_names:
        logger.warning(f"⚠️ {exchange.upper()}配置缺失 {valid_names_key}，跳过计算")
        return pd.DataFrame(), _build_stats(0, 0, 0, 0, config_contract_count, config_expiry_count)
    if not valid_expiry:
        logger.warning(f"⚠️ {exchange.upper()}配置缺失 {valid_expiry_key}，跳过计算")
        return pd.DataFrame(), _build_stats(0, 0, 0, 0, config_contract_count, config_expiry_count)
    
    # === 统计满足条件1+2的合约数（核心指标2）===
    # 从contract_master查询：合约名在ris_name中 且 到期日在配置expiry_date中
    if not valid_names or not valid_expiry:
        contracts_meet_expiry_count = 0
    else:
        meet_expiry_query = f"""
            SELECT contract_code 
            FROM contract_master 
            WHERE exchange = '{exchange}'
              AND contract_code IN ({','.join([f"'{c}'" for c in valid_names])})
              AND expiry_date IN ({','.join([f"'{d}'" for d in valid_expiry])})
        """
        try:
            meet_expiry_df = db.query(meet_expiry_query)
            contracts_meet_expiry = set(meet_expiry_df['contract_code'].tolist())
            contracts_meet_expiry_count = len(contracts_meet_expiry)
        except Exception as e:
            logger.error(f"查询满足条件1+2的合约失败: {e}")
            contracts_meet_expiry_count = 0
            contracts_meet_expiry = set()
    
    # === 条件3：op表当日有数据的合约（且在ris_name内）===
    op_table = f"op_{exchange}"
    if not valid_names:
        op_table_contract_count = 0
        valid_from_op = set()
    else:
        op_query = f"""
            SELECT DISTINCT contract_code 
            FROM {op_table} 
            WHERE trade_date = '{trade_date}'
              AND contract_code IN ({','.join([f"'{c}'" for c in valid_names])})
        """
        try:
            op_contracts_df = db.query(op_query)
            valid_from_op = set(op_contracts_df['contract_code'].tolist())
            op_table_contract_count = len(valid_from_op)
        except Exception as e:
            logger.error(f"查询{op_table}表失败: {e}")
            op_table_contract_count = 0
            valid_from_op = set()
    
    # === 三条件交集：待计算合约 ===
    target_contracts = contracts_meet_expiry & valid_from_op
    final_contract_count = len(target_contracts)
    
    # === 查询详细数据（仅三条件交集合约）===
    if target_contracts:
        positions_query = f"""
            SELECT 
                p.contract_code,
                p.position,
                COALESCE(p.contract_type, 'option') as contract_type,
                c.expiry_date,
                c.strike_price,
                c.option_type,
                c.multiplier,
                o.settle_price
            FROM positions p
            JOIN contract_master c 
                ON p.contract_code = c.contract_code 
                AND c.exchange = '{exchange}'
            JOIN {op_table} o 
                ON p.contract_code = o.contract_code 
                AND o.trade_date = '{trade_date}'
            WHERE p.trade_date = '{trade_date}'
              AND p.exchange = '{exchange}'
              AND p.contract_code IN ({','.join([f"'{c}'" for c in target_contracts])})
              AND o.settle_price IS NOT NULL
              AND o.settle_price > 0
        """
        try:
            positions = db.query(positions_query)
            # 确保关键字段非空
            positions = positions.dropna(subset=['expiry_date', 'settle_price'])
        except Exception as e:
            logger.error(f"查询待计算合约详细数据失败: {e}")
            positions = pd.DataFrame()
            final_contract_count = 0
    else:
        positions = pd.DataFrame()
        if final_contract_count > 0:
            logger.info(f"ℹ️ {exchange.upper()}无满足三条件的合约 | 配置名:{config_contract_count} | 满足到期日:{contracts_meet_expiry_count} | op表存在:{op_table_contract_count}")
    
    return positions, _build_stats(
        config_contract_count,
        contracts_meet_expiry_count,
        op_table_contract_count,
        final_contract_count,
        config_contract_count,
        config_expiry_count
    )

def _build_stats(cfg_cnt, meet_exp_cnt, op_cnt, final_cnt, orig_cfg_cnt, exp_cfg_cnt):
    """构建标准化统计字典"""
    return {
        'config_contract_count': orig_cfg_cnt,      # 指标1：配置ris_name总数
        'contracts_meet_expiry': meet_exp_cnt,      # 指标2：满足条件1+2的合约数
        'op_table_contract_count': op_cnt,          # 指标3：op表存在合约数
        'final_contract_count': final_cnt,          # 指标4：待计算合约数
        'success_count': 0,                         # 指标5：成功计算数（预留）
        '_config_expiry_count': exp_cfg_cnt         # 辅助：配置到期日数量（非合约数）
    }
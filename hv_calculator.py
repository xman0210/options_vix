#src/calc/hv_calculator.py
"""
历史波动率(HV)计算模块 - 主力/次主力合约加权
关键概念：
- 主力合约：持仓量最大的合约（流动性最好）
- 次主力合约：持仓量第二大的合约（通常是下月主力）
- 换月判断：动态基于持仓量变化，当新合约持仓量超过原主力时切换
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class ContractInfo:
    """合约信息"""
    contract_code: str
    trade_date: str
    close: float
    open_interest: int  # 持仓量（判断主力的唯一标准）
    volume: int         # 成交量（辅助参考）

class HVCalculator:
    """
    历史波动率计算器 - 双合约加权
    
    核心逻辑：
    1. 每日识别主力（持仓最大）和次主力（持仓第二大）
    2. 计算加权价格 = 主力价格*0.7 + 次主力价格*0.3
    3. 检测换月并做价格平滑处理
    4. 20日对数收益率计算年化波动率
    """
    
    WINDOW = 20         # 20个交易日窗口
    PRIMARY_WEIGHT = 0.7    # 主力权重
    SECONDARY_WEIGHT = 0.3  # 次主力权重
    MIN_HISTORY_DAYS = 25   # 最小历史数据要求（20+5缓冲）
    
    def __init__(self, db_manager, trade_days: List[str]):
        self.db = db_manager
        self.trade_days = trade_days
        self._contract_cache: Dict[str, List[ContractInfo]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_hv(
        self, 
        product: str, 
        trade_date: str,
        table_prefix: str = "fu"
    ) -> Optional[float]:
        """
        计算指定品种在指定日期的20日历史波动率
        
        Args:
            product: 品种代码（小写，如'if', 'cu', 'io'）
            trade_date: 计算日期 (YYYY-MM-DD)
            table_prefix: 期货表前缀
        
        Returns:
            年化历史波动率（小数形式，如0.25表示25%），失败返回None
        """
        # 定位当前日期在交易日列表中的位置
        try:
            target_idx = self.trade_days.index(trade_date)
        except ValueError:
            self.logger.error(f"日期 {trade_date} 不在交易日列表中")
            return None
        
        # 需要至少25个交易日数据（20日窗口+5日缓冲）
        if target_idx < self.MIN_HISTORY_DAYS:
            self.logger.warning(f"历史数据不足: {trade_date} 仅第{target_idx}个交易日")
            return None
        
        # 获取计算窗口所需的历史交易日（最近25天）
        window_days = self.trade_days[target_idx - self.MIN_HISTORY_DAYS : target_idx + 1]
        
        # 构建加权连续价格序列
        continuous_prices = self._build_weighted_price_series(
            product, window_days, table_prefix
        )
        
        if len(continuous_prices) < self.WINDOW + 1:
            self.logger.warning(f"{product} 有效价格数据不足: {len(continuous_prices)} < {self.WINDOW + 1}")
            return None
        
        # 取最近20+1个价格计算对数收益率
        recent_prices = continuous_prices[-(self.WINDOW + 1):]
        
        # 计算对数收益率: ln(P_t / P_{t-1})
        log_returns = np.diff(np.log(recent_prices))
        
        if len(log_returns) < self.WINDOW:
            self.logger.warning(f"{product} 收益率数据不足")
            return None
        
        # 计算年化波动率
        daily_volatility = np.std(log_returns, ddof=1)  # 样本标准差
        annual_volatility = daily_volatility * np.sqrt(252)
        
        self.logger.debug(
            f"{product} {trade_date} HV20={annual_volatility:.4f} "
            f"(日均波动{daily_volatility:.6f}*√252)"
        )
        
        # 合理性检查
        if not np.isfinite(annual_volatility) or annual_volatility <= 0:
            return None
        
        return float(annual_volatility)
    
    def _build_weighted_price_series(
        self,
        product: str,
        trade_days: List[str],
        table_prefix: str
    ) -> np.ndarray:
        """
        构建加权连续价格序列，处理合约换月
        
        Returns:
            加权价格数组（已处理换月平滑）
        """
        weighted_prices = []
        last_primary_contract = None
        price_ratio = 1.0  # 换月价格调整比率
        
        for date_str in trade_days:
            # 识别当日主力和次主力
            primary, secondary = self._identify_main_contracts(
                product, date_str, table_prefix
            )
            
            if primary is None:
                continue  # 当日无数据，跳过
            
            # 检测换月（主力合约变更）
            if last_primary_contract and primary.contract_code != last_primary_contract:
                # 发生换月，计算价格调整比率
                switch_ratio = self._calculate_switch_ratio(
                    last_primary_contract,
                    primary.contract_code,
                    product,
                    date_str,
                    table_prefix
                )
                
                if switch_ratio:
                    # 调整历史价格序列（乘以新比率）
                    price_ratio *= switch_ratio
                    self.logger.info(
                        f"{product} 换月平滑: {last_primary_contract} -> {primary.contract_code} "
                        f"比率={switch_ratio:.6f}, 累计比率={price_ratio:.6f}"
                    )
            
            # 计算当日加权价格
            if secondary:
                weighted_price = (
                    primary.close * self.PRIMARY_WEIGHT + 
                    secondary.close * self.SECONDARY_WEIGHT
                )
            else:
                weighted_price = primary.close
            
            # 应用换月调整
            adjusted_price = weighted_price * price_ratio
            weighted_prices.append(adjusted_price)
            
            last_primary_contract = primary.contract_code
        
        return np.array(weighted_prices)
    
    def _identify_main_contracts(
        self,
        product: str,
        trade_date: str,
        table_prefix: str
    ) -> Tuple[Optional[ContractInfo], Optional[ContractInfo]]:
        """
        识别指定日期的主力和次主力合约
        
        判定标准：
        - 主力 = 持仓量最大的合约
        - 次主力 = 持仓量第二大的合约
        
        Returns:
            (主力合约信息, 次主力合约信息)
        """
        # 查询当日该品种所有合约
        contracts = self._query_product_contracts(product, trade_date, table_prefix)
        
        if not contracts:
            return None, None
        
        # 按持仓量降序排序
        sorted_contracts = sorted(
            contracts, 
            key=lambda x: x.open_interest, 
            reverse=True
        )
        
        primary = sorted_contracts[0]
        secondary = sorted_contracts[1] if len(sorted_contracts) > 1 else None
        
        self.logger.debug(
            f"{product} {trade_date} | "
            f"主力:{primary.contract_code}(持仓{primary.open_interest:,}, "
            f"收盘{primary.close}) | "
            f"次主力:{secondary.contract_code if secondary else '无'}"
            f"{f'(持仓{secondary.open_interest:,})' if secondary else ''}"
        )
        
        return primary, secondary
    
    def _calculate_switch_ratio(
        self,
        old_contract: str,
        new_contract: str,
        product: str,
        switch_date: str,
        table_prefix: str
    ) -> Optional[float]:
        """
        计算合约换月时的价格调整比率
        
        方法：查找两合约在换月前后3天的重叠数据，计算平均价差比率
        
        Returns:
            价格调整比率（新合约价格/旧合约价格），失败返回None
        """
        try:
            switch_idx = self.trade_days.index(switch_date)
        except ValueError:
            return None
        
        # 取换月前后各3天（共7天窗口）
        overlap_start = max(0, switch_idx - 3)
        overlap_end = min(len(self.trade_days), switch_idx + 4)
        overlap_days = self.trade_days[overlap_start:overlap_end]
        
        old_prices = []
        new_prices = []
        
        for date_str in overlap_days:
            # 查询旧合约价格
            old_price = self._query_contract_price(
                old_contract, product, date_str, table_prefix
            )
            # 查询新合约价格
            new_price = self._query_contract_price(
                new_contract, product, date_str, table_prefix
            )
            
            if old_price is not None and new_price is not None:
                old_prices.append(old_price)
                new_prices.append(new_price)
        
        # 需要至少2个有效价格对
        if len(old_prices) >= 2:
            # 使用中位数比率（更稳健）
            ratios = [new / old for new, old in zip(new_prices, old_prices)]
            median_ratio = float(np.median(ratios))
            
            self.logger.debug(
                f"换月比率计算: {old_contract}->{new_contract} "
                f"样本{len(ratios)}个, 中位数比率={median_ratio:.6f}"
            )
            
            return median_ratio
        
        return None
    
    def _query_product_contracts(
        self, 
        product: str, 
        trade_date: str,
        table_prefix: str
    ) -> List[ContractInfo]:
        """查询某品种某日所有合约数据"""
        table_name = f"{table_prefix}_{product}"
        
        sql = f"""
            SELECT 期货合约 as contract_code, 收盘价 as close, 
                   持仓量 as open_interest, 成交量 as volume
            FROM {table_name}
            WHERE 交易日期 = ? AND 期货合约 LIKE ?
            ORDER BY 持仓量 DESC
        """
        
        try:
            df = self.db.query_df(sql, [trade_date, f"{product.upper()}%"])
            
            contracts = []
            for _, row in df.iterrows():
                try:
                    contracts.append(ContractInfo(
                        contract_code=str(row['contract_code']),
                        trade_date=trade_date,
                        close=float(row['close']),
                        open_interest=int(row['open_interest']),
                        volume=int(row['volume'])
                    ))
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"数据转换失败 {row}: {e}")
                    continue
            
            return contracts
            
        except Exception as e:
            self.logger.error(f"查询合约数据失败 {table_name} {trade_date}: {e}")
            return []
    
    def _query_contract_price(
        self,
        contract_code: str,
        product: str,
        trade_date: str,
        table_prefix: str
    ) -> Optional[float]:
        """查询指定合约某日收盘价"""
        table_name = f"{table_prefix}_{product}"
        
        sql = f"""
            SELECT 收盘价 as close
            FROM {table_name}
            WHERE 期货合约 = ? AND 交易日期 = ?
        """
        
        try:
            result = self.db.query_one(sql, [contract_code, trade_date])
            if result and result.get('close') is not None:
                return float(result['close'])
            return None
        except Exception as e:
            self.logger.error(f"查询合约价格失败 {contract_code}: {e}")
            return None
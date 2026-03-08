"""
中金所风险计算器 - 股指期权
特性：IO/MO/HO 使用 BSM 模型（欧式期权）
"""
import pandas as pd
from typing import Dict
from src.calc.risker.base_risker import RiskCalculatorBase


class RisCFFEX(RiskCalculatorBase):
    """中金所风险计算器"""
    
    exchange_name = "CFFEX"
    
    def _extract_product(self, contract_code: str) -> str:
        """
        提取品种代码
        IO2503C4000 -> io (沪深300)
        MO2503C6000 -> mo (中证1000)
        HO2503C2500 -> ho (上证50)
        """
        cleaned = ''.join(filter(str.isalpha, contract_code)).lower()
        return cleaned[:2] if len(cleaned) >= 2 else cleaned
    
    def _calculate_single_contract(self, row: pd.Series) -> Dict:
        """
        CFFEX特殊处理：股指期权为欧式，使用BSM模型
        """
        # 基类通用计算已足够，CFFEX无需特殊处理
        # 如需特殊处理（如分红率调整），可在此扩展
        return super()._calculate_single_contract(row)
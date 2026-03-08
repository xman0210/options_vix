"""
郑商所风险计算器 - 化工/农产品期权
特性：纯碱、玻璃等使用Black76
"""
import pandas as pd
from src.calc.risker.base_risker import RiskCalculatorBase


class RisCZCE(RiskCalculatorBase):
    """郑商所风险计算器"""
    
    exchange_name = "CZCE"
    
    def _extract_product(self, contract_code: str) -> str:
        """
        提取品种代码
        SA2505C1400 -> sa (纯碱)
        FG2505C1200 -> fg (玻璃)
        """
        cleaned = ''.join(filter(str.isalpha, contract_code)).lower()
        return cleaned[:2] if len(cleaned) >= 2 else cleaned
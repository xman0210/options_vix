#src/calc/moneyness_utils.py
"""
期权行权价类型(moneyness)计算工具
提供平值、实值、虚值判断及IVMR斜率计算
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Literal, Tuple
from dataclasses import dataclass
from enum import Enum

class MoneynessType(Enum):
    """行权价类型"""
    DEEP_ITM = "deep_itm"      # 深度实值 (|delta| > 0.8)
    ITM = "itm"                # 实值 (0.5 < |delta| <= 0.8)
    ATM = "atm"                # 平值 (0.4 <= |delta| <= 0.6)
    OTM = "otm"                # 虚值 (0.2 <= |delta| < 0.4)
    DEEP_OTM = "deep_otm"      # 深度虚值 (|delta| < 0.2)

@dataclass
class StrikeIV:
    """带行权价类型的IV数据"""
    strike: float
    iv: float
    delta: float
    option_type: Literal['call', 'put']
    moneyness: MoneynessType
    moneyness_label: str  # 中文标签

class MoneynessCalculator:
    """行权价类型计算器"""
    
    # Delta阈值定义
    DELTA_BOUNDS = {
        'deep_itm': 0.8,
        'itm_lower': 0.5,
        'atm_lower': 0.4,
        'atm_upper': 0.6,
        'otm_upper': 0.2
    }
    
    @classmethod
    def classify(
        cls, 
        delta: float, 
        option_type: Literal['call', 'put']
    ) -> Tuple[MoneynessType, str]:
        """
        根据Delta值判断行权价类型
        
        Returns:
            (MoneynessType, 中文标签)
        """
        abs_delta = abs(delta)
        
        if abs_delta > cls.DELTA_BOUNDS['deep_itm']:
            mtype = MoneynessType.DEEP_ITM
            label = "深度实值"
        elif abs_delta > cls.DELTA_BOUNDS['itm_lower']:
            mtype = MoneynessType.ITM
            label = "实值"
        elif cls.DELTA_BOUNDS['atm_lower'] <= abs_delta <= cls.DELTA_BOUNDS['atm_upper']:
            mtype = MoneynessType.ATM
            label = "平值"
        elif abs_delta >= cls.DELTA_BOUNDS['otm_upper']:
            mtype = MoneynessType.OTM
            label = "虚值"
        else:
            mtype = MoneynessType.DEEP_OTM
            label = "深度虚值"
        
        # 看涨/看跌标注
        direction = "认购" if option_type == 'call' else "认沽"
        
        return mtype, f"{label}{direction}"
    
    @classmethod
    def calculate_ivmr_with_moneyness(
        cls,
        options_df: pd.DataFrame,
        underlying_price: float,
        risk_free_rate: float = 0.019
    ) -> pd.DataFrame:
        """
        计算带行权价类型的IVMR系列值
        
        Args:
            options_df: 期权数据，需包含 strike, iv, delta, option_type, expiry_date
            underlying_price: 标的资产价格
            risk_free_rate: 无风险利率
        
        Returns:
            添加IVMR列和moneyness分类的DataFrame
        """
        df = options_df.copy()
        
        # 1. 添加行权价类型分类
        df[['moneyness_type', 'moneyness_label']] = df.apply(
            lambda row: pd.Series(cls.classify(row['delta'], row['option_type'])),
            axis=1
        )
        
        # 2. 计算标准化行权价（moneyness = K/S）
        df['moneyness_ratio'] = df['strike'] / underlying_price
        
        # 3. 分离认购和认沽，分别计算IVMR斜率
        calls = df[df['option_type'] == 'call'].copy()
        puts = df[df['option_type'] == 'put'].copy()
        
        # 4. 计算不同行权价区域的IVMR斜率
        df['ivmr3'] = cls._calc_regional_slope(df, 3)   # 近月斜率
        df['ivmr7'] = cls._calc_regional_slope(df, 7)   # 1周斜率
        df['ivmr15'] = cls._calc_regional_slope(df, 15) # 半月斜率
        df['ivmr30'] = cls._calc_regional_slope(df, 30) # 月度斜率
        df['ivmr90'] = cls._calc_regional_slope(df, 90) # 季度斜率
        
        # 5. 综合IVMR（加权平均）
        df['ivmr'] = (
            df['ivmr3'] * 0.35 + 
            df['ivmr7'] * 0.25 + 
            df['ivmr15'] * 0.20 + 
            df['ivmr30'] * 0.15 + 
            df['ivmr90'] * 0.05
        )
        
        # 6. 按行权价类型分组统计IVMR
        df['ivmr_by_type'] = cls._calc_slope_by_moneyness_type(df)
        
        return df
    
    @classmethod
    def _calc_regional_slope(cls, df: pd.DataFrame, days: int) -> float:
        """
        计算特定期限的IV-行权价斜率（线性回归）
        
        使用ATM附近的期权进行回归，避免深度虚值/实值期权的流动性偏差
        """
        # 筛选ATM附近的期权（用于计算斜率）
        atm_options = df[
            df['moneyness_type'].isin([MoneynessType.ATM, MoneynessType.ITM, MoneynessType.OTM])
        ].copy()
        
        if len(atm_options) < 3:
            return np.nan
        
        # 按行权价排序
        atm_options = atm_options.sort_values('strike')
        
        # 线性回归: IV = alpha + beta * strike
        x = atm_options['strike'].values
        y = atm_options['iv'].values
        
        # 加权回归（ATM期权权重更高）
        weights = 1.0 / (1.0 + np.abs(atm_options['moneyness_ratio'].values - 1.0))
        
        try:
            # 使用numpy的polyfit进行加权线性回归
            coeffs = np.polyfit(x, y, 1, w=weights)
            slope = coeffs[0]  # IV随行权价的变化率
            
            # 标准化：转换为每1%行权价变化的IV变化（波动率点）
            atm_strike = x[len(x)//2]  # 中位数行权价作为参考
            normalized_slope = slope * atm_strike  # 转换为相对变化
            
            return normalized_slope
        except (np.linalg.LinAlgError, ValueError):
            return np.nan
    
    @classmethod
    def _calc_slope_by_moneyness_type(cls, df: pd.DataFrame) -> pd.Series:
        """
        按行权价类型分组计算IVMR特征
        
        返回JSON格式的详细分类斜率
        """
        results = []
        
        for opt_type in ['call', 'put']:
            type_df = df[df['option_type'] == opt_type]
            
            type_result = {
                'option_type': opt_type,
                'atm_iv': type_df[type_df['moneyness_type'] == MoneynessType.ATM]['iv'].mean(),
                'itm_skew': cls._calc_skew(type_df, MoneynessType.ITM, MoneynessType.ATM),
                'otm_skew': cls._calc_skew(type_df, MoneynessType.OTM, MoneynessType.ATM),
                'deep_otm_premium': cls._calc_deep_premium(type_df)
            }
            results.append(type_result)
        
        # 为每行添加对应的分类数据
        def get_row_data(row):
            for r in results:
                if r['option_type'] == row['option_type']:
                    return r
            return {}
        
        return df.apply(lambda row: get_row_data(row), axis=1)

    @classmethod
    def _calc_skew(
        cls, 
        df: pd.DataFrame, 
        target_type: MoneynessType, 
        ref_type: MoneynessType
    ) -> float:
        """计算某类型相对于ATM的IV偏差"""
        target_iv = df[df['moneyness_type'] == target_type]['iv'].mean()
        ref_iv = df[df['moneyness_type'] == ref_type]['iv'].mean()
        
        if pd.notna(target_iv) and pd.notna(ref_iv) and ref_iv > 0:
            return (target_iv - ref_iv) / ref_iv  # 百分比偏差
        return np.nan
    
    @classmethod
    def _calc_deep_premium(cls, df: pd.DataFrame) -> float:
        """计算深度虚值期权的IV溢价（尾部风险指标）"""
        deep_otm = df[df['moneyness_type'] == MoneynessType.DEEP_OTM]['iv'].mean()
        atm = df[df['moneyness_type'] == MoneynessType.ATM]['iv'].mean()
        
        if pd.notna(deep_otm) and pd.notna(atm) and atm > 0:
            return (deep_otm - atm) / atm
        return np.nan

# 便捷函数
def add_moneyness_classification(
    df: pd.DataFrame, 
    underlying_price: float,
    delta_col: str = 'delta',
    option_type_col: str = 'option_type'
) -> pd.DataFrame:
    """
    为DataFrame添加行权价类型分类列
    
    Args:
        df: 期权数据
        underlying_price: 标的价格（用于计算moneyness ratio）
        delta_col: Delta列名
        option_type_col: 期权类型列名
    
    Returns:
        添加moneyness_type, moneyness_label, moneyness_ratio列的DataFrame
    """
    df = df.copy()
    
    # 计算moneyness ratio
    if 'strike' in df.columns:
        df['moneyness_ratio'] = df['strike'] / underlying_price
    
    # 分类
    classifications = df.apply(
        lambda row: MoneynessCalculator.classify(
            row[delta_col], 
            row[option_type_col]
        ),
        axis=1
    )
    df['moneyness_type'] = [c[0] for c in classifications]
    df['moneyness_label'] = [c[1] for c in classifications]
    
    return df
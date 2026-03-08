#src/calc/ivmr_calculator.py
"""
IVMR (Implied Volatility Mean Reversion) 计算模块

核心概念：
- IVMR = 单个期权合约隐含波动率的时间序列线性回归斜率
- 反映该合约IV的趋势变化（均值回归特性）
- 多时间窗口：3, 7, 15, 30, 60, 90日及全周期

计算方法：
对单个合约的IV序列做线性回归：IV_t = alpha + beta * t
IVMR = beta（斜率），表示IV每日变化趋势
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class IVMRResult:
    """IVMR计算结果"""
    contract_code: str
    trade_date: str
    ivmr3: Optional[float]   # 3日斜率（最近3日IV趋势）
    ivmr7: Optional[float]   # 7日斜率
    ivmr15: Optional[float]  # 15日斜率
    ivmr30: Optional[float]  # 30日斜率
    ivmr60: Optional[float]  # 60日斜率
    ivmr90: Optional[float]  # 90日斜率
    ivmr: Optional[float]    # 加权综合IVMR
    r_squared: Optional[float]  # 拟合优度（最长窗口）
    data_points: int         # 实际使用数据点数
    current_iv: Optional[float]   # 当前IV值


class IVMRCalculator:
    """
    IVMR计算器
    
    IVMR含义：
    - 正值：IV近期上升趋势（可能继续涨或均值回归下跌）
    - 负值：IV近期下降趋势（可能继续跌或均值回归上涨）
    - 接近0：IV稳定，无明显趋势
    """
    
    # 时间窗口配置（天数 -> 最小要求数据点 -> 权重）
    TIME_WINDOWS = {
        'ivmr3': {'days': 3, 'min_points': 3, 'weight': 0.20},
        'ivmr7': {'days': 7, 'min_points': 5, 'weight': 0.25},
        'ivmr15': {'days': 15, 'min_points': 10, 'weight': 0.25},
        'ivmr30': {'days': 30, 'min_points': 20, 'weight': 0.20},
        'ivmr60': {'days': 60, 'min_points': 40, 'weight': 0.07},
        'ivmr90': {'days': 90, 'min_points': 60, 'weight': 0.03},
    }
    
    def __init__(self, db_manager, trade_days: List[str]):
        self.db = db_manager
        self.trade_days = trade_days
        self._iv_cache: Dict[str, pd.DataFrame] = {}  # 合约IV历史缓存
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_ivmr(
        self,
        contract_code: str,
        trade_date: str,
        exchange: str
    ) -> IVMRResult:
        """
        计算单个期权合约的IVMR系列值
        
        Args:
            contract_code: 期权合约代码，如"IO2503C4000"
            trade_date: 计算日期 (YYYY-MM-DD)
            exchange: 交易所代码 (cffex, shfe等)
        
        Returns:
            IVMRResult包含各时间窗口斜率和综合值
        """
        # 1. 获取该合约的历史IV序列
        iv_history = self._get_iv_history(contract_code, exchange, trade_date)
        
        current_iv = iv_history['iv'].iloc[-1] if not iv_history.empty else None
        
        if len(iv_history) < 3:
            self.logger.debug(f"{contract_code} 历史IV数据不足: {len(iv_history)}天")
            return IVMRResult(
                contract_code=contract_code,
                trade_date=trade_date,
                ivmr3=None, ivmr7=None, ivmr15=None,
                ivmr30=None, ivmr60=None, ivmr90=None,
                ivmr=None, r_squared=None,
                data_points=len(iv_history),
                current_iv=current_iv
            )
        
        # 2. 计算各时间窗口的斜率
        slopes = {}
        valid_slopes = []  # 用于加权平均
        
        for field, config in self.TIME_WINDOWS.items():
            slope, r2, points = self._calc_window_slope(
                iv_history, 
                config['days'],
                config['min_points']
            )
            slopes[field] = slope
            
            # 记录有效斜率和权重
            if slope is not None and np.isfinite(slope):
                valid_slopes.append((field, slope, config['weight']))
        
        # 3. 计算综合IVMR（加权平均，仅使用有效值）
        composite_ivmr = self._calc_composite_ivmr(valid_slopes)
        
        # 4. 计算整体R²（使用最长可用窗口）
        max_window = min(90, len(iv_history))
        _, overall_r2, _ = self._calc_window_slope(iv_history, max_window, 10)
        
        self.logger.debug(
            f"{contract_code} IVMR: 综合={composite_ivmr:.6f if composite_ivmr else None}, "
            f"3日={slopes.get('ivmr3'):.6f if slopes.get('ivmr3') else None}, "
            f"7日={slopes.get('ivmr7'):.6f if slopes.get('ivmr7') else None}, "
            f"R²={overall_r2:.4f if overall_r2 else None}"
        )
        
        return IVMRResult(
            contract_code=contract_code,
            trade_date=trade_date,
            ivmr3=slopes.get('ivmr3'),
            ivmr7=slopes.get('ivmr7'),
            ivmr15=slopes.get('ivmr15'),
            ivmr30=slopes.get('ivmr30'),
            ivmr60=slopes.get('ivmr60'),
            ivmr90=slopes.get('ivmr90'),
            ivmr=composite_ivmr,
            r_squared=overall_r2,
            data_points=len(iv_history),
            current_iv=current_iv
        )
    
    def _get_iv_history(
        self,
        contract_code: str,
        exchange: str,
        end_date: str,
        max_days: int = 100
    ) -> pd.DataFrame:
        """
        获取合约的历史IV数据
        
        Returns:
            DataFrame with columns: [trade_date, iv, settle_price, delta, strike]
            按交易日期升序排列
        """
        # 检查缓存
        cache_key = f"{exchange}_{contract_code}"
        if cache_key in self._iv_cache:
            cached = self._iv_cache[cache_key]
            # 过滤到end_date
            history = cached[cached['trade_date'] <= end_date].copy()
            if len(history) >= max_days * 0.8:  # 缓存足够新
                return history.tail(max_days)
        
        # 计算起始日期
        try:
            end_idx = self.trade_days.index(end_date)
        except ValueError:
            self.logger.error(f"日期 {end_date} 不在交易日列表")
            return pd.DataFrame()
        
        start_idx = max(0, end_idx - max_days)
        start_date = self.trade_days[start_idx]
        
        # 查询数据库
        table_name = f"op_{exchange}"
        sql = f"""
            SELECT 
                交易日期 as trade_date,
                隐含波动率 as iv,
                结算价 as settle_price,
                Delta as delta,
                行权价 as strike,
                期权类型 as option_type
            FROM {table_name}
            WHERE 期权合约代码 = ? 
              AND 交易日期 BETWEEN ? AND ?
              AND 隐含波动率 IS NOT NULL
              AND 隐含波动率 > 0
            ORDER BY 交易日期 ASC
        """
        
        try:
            df = self.db.query_df(sql, [contract_code, start_date, end_date])
            
            # 数据类型转换
            df['iv'] = pd.to_numeric(df['iv'], errors='coerce')
            df['delta'] = pd.to_numeric(df['delta'], errors='coerce')
            df['settle_price'] = pd.to_numeric(df['settle_price'], errors='coerce')
            
            # 清洗：删除IV无效的数据
            df = df.dropna(subset=['iv'])
            
            # 更新缓存
            if not df.empty:
                self._iv_cache[cache_key] = df.copy()
            
            return df
            
        except Exception as e:
            self.logger.error(f"查询IV历史失败 {contract_code}: {e}")
            return pd.DataFrame()
    
    def _calc_window_slope(
        self,
        iv_history: pd.DataFrame,
        window_days: int,
        min_points: int
    ) -> Tuple[Optional[float], Optional[float], int]:
        """
        计算指定窗口的线性回归斜率
        
        回归模型: IV_t = alpha + beta * t
        其中 t = 0, 1, 2, ..., n-1（时间序列序号）
        
        Returns:
            (斜率beta, R², 使用数据点数)
            斜率表示每日IV变化量（如0.001表示每天涨0.1个百分点）
        """
        if len(iv_history) < min_points:
            return None, None, len(iv_history)
        
        # 取最近window_days的数据
        recent_data = iv_history.tail(window_days).copy()
        
        if len(recent_data) < min_points:
            return None, None, len(recent_data)
        
        # 构建时间序列（0, 1, 2, ...）
        x = np.arange(len(recent_data))
        y = recent_data['iv'].values
        
        # 线性回归（最小二乘法）
        # 解方程: y = beta * x + alpha
        x_mean = np.mean(x)
        y_mean = np.mean(y)
        
        # 计算斜率 beta = Cov(x,y) / Var(x)
        numerator = np.sum((x - x_mean) * (y - y_mean))
        denominator = np.sum((x - x_mean) ** 2)
        
        if denominator == 0:
            return 0.0, 1.0 if len(set(y)) == 1 else 0.0, len(x)
        
        beta = numerator / denominator
        alpha = y_mean - beta * x_mean
        
        # 计算R²
        y_pred = alpha + beta * x
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y_mean) ** 2)
        
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        
        # 返回标准化斜率（IV每日变化率，相对于当前IV水平）
        if y_mean > 0:
            normalized_beta = beta / y_mean  # 相对变化率
        else:
            normalized_beta = beta
        
        return float(normalized_beta), float(r_squared), len(x)
    
    def _calc_composite_ivmr(
        self,
        valid_slopes: List[Tuple[str, float, float]]
    ) -> Optional[float]:
        """
        计算加权综合IVMR
        
        当某些窗口数据不足时，重新归一化权重
        
        Args:
            valid_slopes: [(字段名, 斜率值, 原始权重), ...]
        
        Returns:
            加权平均IVMR，无有效数据返回None
        """
        if not valid_slopes:
            return None
        
        # 重新归一化权重
        total_weight = sum(w for _, _, w in valid_slopes)
        if total_weight == 0:
            return None
        
        # 加权平均
        weighted_sum = sum(slope * (weight / total_weight) for _, slope, weight in valid_slopes)
        
        return float(weighted_sum) if np.isfinite(weighted_sum) else None


# 便捷函数
def calculate_contract_ivmr(
    contract_code: str,
    trade_date: str,
    exchange: str,
    db_manager,
    trade_days: List[str]
) -> Dict[str, Optional[float]]:
    """
    计算单个合约的IVMR（便捷函数）
    
    Returns:
        {
            'ivmr3': float or None,
            'ivmr7': float or None,
            'ivmr15': float or None,
            'ivmr30': float or None,
            'ivmr60': float or None,
            'ivmr90': float or None,
            'ivmr': float or None,      # 综合值
            'ivmr_r2': float or None,   # 拟合优度
            'ivmr_points': int,         # 数据点数
            'current_iv': float or None # 当前IV
        }
    """
    calc = IVMRCalculator(db_manager, trade_days)
    result = calc.calculate_ivmr(contract_code, trade_date, exchange)
    
    return {
        'ivmr3': result.ivmr3,
        'ivmr7': result.ivmr7,
        'ivmr15': result.ivmr15,
        'ivmr30': result.ivmr30,
        'ivmr60': result.ivmr60,
        'ivmr90': result.ivmr90,
        'ivmr': result.ivmr,
        'ivmr_r2': result.r_squared,
        'ivmr_points': result.data_points,
        'current_iv': result.current_iv
    }
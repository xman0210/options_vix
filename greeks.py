# src/calc/model/greeks.py

import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
import math
from typing import Tuple, Optional

def black_scholes_merton_price(S: float, K: float, T: float, r: float, q: float, sigma: float, flag: str) -> float:
    """BSM 理论价格（解析公式）
    flag: 'call' or 'put'
    """
    # 参数验证
    if not all(np.isfinite([S, K, T, r, q, sigma])):
        return np.nan
    
    if S <= 0 or K <= 0:
        return np.nan
    
    if T <= 0:
        return max(S - K, 0) if flag == 'call' else max(K - S, 0)
    
    if sigma <= 0:
        pv = np.exp(-r * T)
        if flag == 'call':
            return max((S * np.exp(-q * T) - K * np.exp(-r * T)), 0)
        else:
            return max((K * np.exp(-r * T) - S * np.exp(-q * T)), 0)
    
    try:
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if flag == 'call':
            price = S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:
            price = K * np.exp(-r * T) * norm.cdf(-d2) - S * np.exp(-q * T) * norm.cdf(-d1)
        
        return max(price, 0)  # 确保价格非负
    except (ValueError, OverflowError):
        return np.nan

def black76_price(F: float, K: float, T: float, r: float, sigma: float, flag: str) -> float:
    """Black76 期货期权定价模型
    F: 期货价格
    flag: 'call' or 'put'
    """
    # 参数验证
    if not all(np.isfinite([F, K, T, r, sigma])):
        return np.nan
    
    if F < 0 or K <= 0:
        return np.nan
    
    if T <= 0:
        return max(F - K, 0) if flag == 'call' else max(K - F, 0)
    
    if sigma <= 0:
        pv = np.exp(-r * T)
        if flag == 'call':
            return max((F - K) * pv, 0)
        else:
            return max((K - F) * pv, 0)
    
    try:
        d1 = (np.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        pv = np.exp(-r * T)
        
        if flag == 'call':
            price = pv * (F * norm.cdf(d1) - K * norm.cdf(d2))
        else:
            price = pv * (K * norm.cdf(-d2) - F * norm.cdf(-d1))
        
        return max(price, 0)  # 确保价格非负
    except (ValueError, OverflowError):
        return np.nan

def baw_american_call_price(S: float, K: float, T: float, r: float, q: float, sigma: float) -> float:
    """Barone-Adesi Whaley 美式看涨期权定价（简化实现）"""
    if not all(np.isfinite([S, K, T, r, q, sigma])):
        return np.nan
    
    if T <= 0:
        return max(S - K, 0)
    
    if sigma <= 0:
        return max(S * np.exp(-q * T) - K * np.exp(-r * T), 0)
    
    if q == 0 or r <= 0:
        return black_scholes_merton_price(S, K, T, r, q, sigma, 'call')
    
    try:
        n = 2 * r / (sigma**2)
        m = 2 * (r - q) / (sigma**2)
        q2 = (-(n - 1) + np.sqrt((n - 1)**2 + 4 * m)) / 2
        
        if q2 <= 0:
            return black_scholes_merton_price(S, K, T, r, q, sigma, 'call')
        
        S_star = K / (1 - (K * (1 - np.exp(-q * T)) * q2) / (S * (q2 - 1)))
        european_price = black_scholes_merton_price(S, K, T, r, q, sigma, 'call')
        
        if S < S_star:
            A2 = (S_star * (q2 - 1)) / (q2 * K * np.exp(-r * T))
            baw_price = european_price + A2 * ((S / S_star) ** q2) * (S_star - K)
            return max(baw_price, S - K)  # 不低于内在价值
        else:
            return S - K
    except (ValueError, OverflowError):
        return black_scholes_merton_price(S, K, T, r, q, sigma, 'call')

def baw_american_put_price(S: float, K: float, T: float, r: float, q: float, sigma: float) -> float:
    """Barone-Adesi Whaley 美式看跌期权定价（简化实现）"""
    if not all(np.isfinite([S, K, T, r, q, sigma])):
        return np.nan
    
    if T <= 0:
        return max(K - S, 0)
    
    if sigma <= 0:
        return max(K * np.exp(-r * T) - S * np.exp(-q * T), 0)
    
    if r == 0:
        return black_scholes_merton_price(S, K, T, r, q, sigma, 'put')
    
    try:
        n = 2 * r / (sigma**2)
        m = 2 * (r - q) / (sigma**2)
        q1 = (-(n - 1) - np.sqrt((n - 1)**2 + 4 * m)) / 2
        
        if q1 >= 0:
            return black_scholes_merton_price(S, K, T, r, q, sigma, 'put')
        
        S_star = K / (1 + (K * (1 - np.exp(-r * T)) * q1) / (S * (q1 - 1)))
        european_price = black_scholes_merton_price(S, K, T, r, q, sigma, 'put')
        
        if S > S_star:
            A1 = (-S_star * (q1 - 1)) / (q1 * K * np.exp(-r * T))
            baw_price = european_price + A1 * ((S / S_star) ** q1) * (K - S_star)
            return max(baw_price, K - S)  # 不低于内在价值
        else:
            return K - S
    except (ValueError, OverflowError):
        return black_scholes_merton_price(S, K, T, r, q, sigma, 'put')

def implied_volatility(price: float, S: float, K: float, T: float, r: float, q: float, 
                      flag: str, model: str = 'bsm', F: Optional[float] = None, 
                      tol: float = 1e-6, max_iter: int = 100) -> float:
    """根据指定模型反推隐含波动率
    
    增强版：处理边界情况和数值稳定性，支持交易所规则允许的价格为0的虚值期权
    
    model: 'bsm', 'black76', 'baw'
    F: 期货价格（仅black76模型需要）
    """
    # 严格的参数验证
    try:
        price = float(price)
        S = float(S) if S is not None else 0.0
        K = float(K)
        T = float(T)
        r = float(r)
        q = float(q)
        if F is not None:
            F = float(F)
    except (ValueError, TypeError):
        return np.nan
    
    # 基础检查
    if not np.isfinite(price) or not np.isfinite(K) or not np.isfinite(T):
        return np.nan
    
    if T <= 0 or K <= 0:
        return np.nan
    
    # 【关键修复】：处理价格为0的情况（交易所规则允许虚值期权结算价为0）
    if price < 0:
        return np.nan
    
    # 确定使用的标的资产价格和计算内在价值
    if model == 'black76':
        if F is None or not np.isfinite(F) or F < 0:
            return np.nan
        underlying = F
        pv = np.exp(-r * T)
        intrinsic = max((F - K) * pv, 0) if flag == 'call' else max((K - F) * pv, 0)
    elif model == 'bsm':
        if not np.isfinite(S) or S <= 0:
            return np.nan
        underlying = S
        pv = np.exp(-r * T)
        intrinsic = max((S * np.exp(-q * T) - K * pv), 0) if flag == 'call' else max((K * pv - S * np.exp(-q * T)), 0)
    else:  # baw
        if not np.isfinite(S) or S <= 0:
            return np.nan
        underlying = S
        intrinsic = max(S - K, 0) if flag == 'call' else max(K - S, 0)
    
    # 【关键修复】：价格为0时的处理逻辑
    if price == 0:
        # 虚值期权（内在价值为0）结算价为0是交易所允许的正常情况
        if intrinsic <= 0:
            # 返回0作为标记值，表示虚值期权无IV（或IV趋于无穷大）
            return 0.0
        else:
            # 实值期权价格为0是数据错误，无法计算IV
            return np.nan
    
    # 检查无套利条件：市场价格不能低于内在价值（允许微小误差）
    if price < intrinsic * 0.99:  # 允许1%的误差
        return np.nan
    
    # 定义目标函数
    def objective(sigma):
        if sigma <= 0:
            return np.inf
        try:
            if model == 'bsm':
                return black_scholes_merton_price(S, K, T, r, q, sigma, flag) - price
            elif model == 'black76':
                return black76_price(F, K, T, r, sigma, flag) - price
            elif model == 'baw':
                if flag == 'call':
                    return baw_american_call_price(S, K, T, r, q, sigma) - price
                else:
                    return baw_american_put_price(S, K, T, r, q, sigma) - price
            else:
                return np.nan
        except (ValueError, OverflowError):
            return np.nan
    
    # 检查边界值
    sigma_low, sigma_high = 1e-6, 5.0
    
    try:
        obj_low = objective(sigma_low)
        obj_high = objective(sigma_high)
        
        # 检查是否为NaN
        if not np.isfinite(obj_low) or not np.isfinite(obj_high):
            return np.nan
        
        # 检查符号
        if obj_low * obj_high > 0:
            # 同号情况：选择最接近的边界
            if abs(obj_low) < abs(obj_high):
                # 低波动率更接近目标
                if obj_low > 0:
                    # 即使sigma=0价格也高于市场价，返回最小IV
                    return sigma_low
                else:
                    return np.nan
            else:
                # 高波动率更接近目标
                if obj_high < 0:
                    # 即使sigma=500%价格也低于市场价，返回最大IV
                    return sigma_high
                else:
                    return np.nan
        
        # 使用brentq求解
        iv = brentq(objective, sigma_low, sigma_high, xtol=tol, maxiter=max_iter)
        
        # 验证结果合理性
        if not np.isfinite(iv) or iv < sigma_low or iv > sigma_high:
            return np.nan
        
        return iv
        
    except (ValueError, RuntimeError):
        return np.nan

def calculate_bsm_greeks(chain_df: pd.DataFrame, S: float, r: float, q: float = 0.02) -> pd.DataFrame:
    """计算BSM模型希腊值
    chain_df 需列: market_price, strike, tte, option_type ('call'/'put')
    返回新增: iv, theo_price, delta, gamma, theta, vega, rho
    """
    df = chain_df.copy()
    
    # 确保数值类型
    df['market_price'] = pd.to_numeric(df['market_price'], errors='coerce')
    df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
    df['tte'] = pd.to_numeric(df['tte'], errors='coerce')
    
    # 【关键修复】：过滤无效数据，但允许价格为0（交易所规则允许虚值期权结算价为0）
    valid_mask = (
        df['market_price'].notna() & df['strike'].notna() & df['tte'].notna() &
        (df['market_price'] >= 0) &  # 改为 >= 0，允许结算价为0
        (df['strike'] > 0) & 
        (df['tte'] > 0)
    )
    df = df[valid_mask].copy()
    
    if df.empty:
        return df
    
    # 【新增】：计算内在价值，用于后续处理价格为0的情况
    df['intrinsic_value'] = df.apply(
        lambda row: max(S - row['strike'], 0) if row['option_type'] == 'call' 
                   else max(row['strike'] - S, 0),
        axis=1
    )
    
    # 【新增】：标记价格有效性
    # 价格>0：正常情况；价格==0且内在价值==0：虚值期权结算价为0（交易所规则允许）
    # 价格==0且内在价值>0：实值期权价格为0，数据错误
    df['price_valid'] = (df['market_price'] > 0) | ((df['market_price'] == 0) & (df['intrinsic_value'] == 0))
    
    # 过滤掉数据错误的情况（实值期权价格为0）
    df = df[df['price_valid']].copy()
    
    if df.empty:
        return df
    
    # IV 反推
    df['iv'] = df.apply(
        lambda row: implied_volatility(
            row['market_price'], S, row['strike'], row['tte'], r, q, row['option_type'], model='bsm'
        ),
        axis=1
    )
    
    # 【关键修复】：过滤IV无效的行，但保留IV=0的情况（对应价格为0的虚值期权）
    df = df[df['iv'].notna()].copy()  # 只检查notna，不检查>0
    
    if df.empty:
        return df
    
    # 【新增】：处理IV=0的情况（价格为0的虚值期权）
    # 对于IV=0的合约，理论价格等于内在价值，Greeks为0（除delta外）
    def calc_theo_price(row):
        if row['iv'] == 0:
            # 虚值期权价格为0，理论价格等于内在价值（也为0）
            return row['intrinsic_value']
        else:
            return black_scholes_merton_price(S, row['strike'], row['tte'], r, q, row['iv'], row['option_type'])
    
    df['theo_price'] = df.apply(calc_theo_price, axis=1)
    
    # 分离IV>0和IV=0的合约分别计算Greeks
    df_with_iv = df[df['iv'] > 0].copy()
    df_zero_iv = df[df['iv'] == 0].copy()
    
    # 计算IV>0合约的Greeks
    if not df_with_iv.empty:
        sqrt_tte = np.sqrt(df_with_iv['tte'])
        df_with_iv['d1'] = (np.log(S / df_with_iv['strike']) + (r - q + 0.5 * df_with_iv['iv'] ** 2) * df_with_iv['tte']) / (df_with_iv['iv'] * sqrt_tte)
        df_with_iv['d2'] = df_with_iv['d1'] - df_with_iv['iv'] * sqrt_tte
        
        mask_call = df_with_iv['option_type'] == 'call'
        
        # Delta
        df_with_iv['delta'] = np.where(mask_call, 
                                      np.exp(-q * df_with_iv['tte']) * norm.cdf(df_with_iv['d1']),
                                      -np.exp(-q * df_with_iv['tte']) * norm.cdf(-df_with_iv['d1']))
        
        # Gamma (call/put 相同)
        df_with_iv['gamma'] = np.exp(-q * df_with_iv['tte']) * norm.pdf(df_with_iv['d1']) / (S * df_with_iv['iv'] * sqrt_tte)
        
        # Vega (call/put 相同，单位 %)
        df_with_iv['vega'] = S * np.exp(-q * df_with_iv['tte']) * norm.pdf(df_with_iv['d1']) * sqrt_tte / 100
        
        # Theta (年化 /365)
        theta_common = -(S * np.exp(-q * df_with_iv['tte']) * norm.pdf(df_with_iv['d1']) * df_with_iv['iv']) / (2 * sqrt_tte)
        
        theta_call = (theta_common 
                     - r * df_with_iv['strike'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(df_with_iv['d2'])
                     + q * S * np.exp(-q * df_with_iv['tte']) * norm.cdf(df_with_iv['d1']))
        
        theta_put = (theta_common 
                    + r * df_with_iv['strike'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(-df_with_iv['d2'])
                    - q * S * np.exp(-q * df_with_iv['tte']) * norm.cdf(-df_with_iv['d1']))
        
        df_with_iv['theta'] = np.where(mask_call, theta_call, theta_put) / 365
        
        # Rho
        df_with_iv['rho'] = np.where(mask_call,
                                    df_with_iv['strike'] * df_with_iv['tte'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(df_with_iv['d2']) / 100,
                                    -df_with_iv['strike'] * df_with_iv['tte'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(-df_with_iv['d2']) / 100)
        
        # 清理临时列
        df_with_iv.drop(columns=['d1', 'd2'], inplace=True, errors='ignore')
    
    # 处理IV=0合约的Greeks（虚值期权价格为0）
    if not df_zero_iv.empty:
        # Delta：虚值期权delta为0（或极接近0）
        df_zero_iv['delta'] = 0.0
        # Gamma：0
        df_zero_iv['gamma'] = 0.0
        # Vega：0
        df_zero_iv['vega'] = 0.0
        # Theta：0（无时间价值）
        df_zero_iv['theta'] = 0.0
        # Rho：0
        df_zero_iv['rho'] = 0.0
    
    # 合并结果
    df = pd.concat([df_with_iv, df_zero_iv], ignore_index=True) if not df_with_iv.empty and not df_zero_iv.empty else \
         (df_with_iv if not df_with_iv.empty else df_zero_iv)
    
    # 清理临时列
    df.drop(columns=['intrinsic_value', 'price_valid'], inplace=True, errors='ignore')
    
    return df

def calculate_black76_greeks(chain_df: pd.DataFrame, F: float, r: float) -> pd.DataFrame:
    """计算Black76模型希腊值（期货期权）
    chain_df 需列: market_price, strike, tte, option_type ('call'/'put')
    F: 期货价格
    返回新增: iv, theo_price, delta, gamma, theta, vega, rho
    """
    df = chain_df.copy()
    
    # 确保数值类型
    df['market_price'] = pd.to_numeric(df['market_price'], errors='coerce')
    df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
    df['tte'] = pd.to_numeric(df['tte'], errors='coerce')
    
    # 【关键修复】：过滤无效数据，但允许价格为0
    valid_mask = (
        df['market_price'].notna() & df['strike'].notna() & df['tte'].notna() &
        (df['market_price'] >= 0) &  # 改为 >= 0
        (df['strike'] > 0) & 
        (df['tte'] > 0)
    )
    df = df[valid_mask].copy()
    
    if df.empty:
        return df
    
    # 【新增】：计算内在价值（Black76模型）
    pv = np.exp(-r * df['tte'])
    df['intrinsic_value'] = df.apply(
        lambda row: max((F - row['strike']) * np.exp(-r * row['tte']), 0) if row['option_type'] == 'call' 
                   else max((row['strike'] - F) * np.exp(-r * row['tte']), 0),
        axis=1
    )
    
    # 【新增】：标记价格有效性
    df['price_valid'] = (df['market_price'] > 0) | ((df['market_price'] == 0) & (df['intrinsic_value'] == 0))
    df = df[df['price_valid']].copy()
    
    if df.empty:
        return df
    
    # IV 反推
    df['iv'] = df.apply(
        lambda row: implied_volatility(
            row['market_price'], S=0, K=row['strike'], T=row['tte'], 
            r=r, q=0, flag=row['option_type'], model='black76', F=F
        ),
        axis=1
    )
    
    # 【关键修复】：过滤IV无效的行，但保留IV=0
    df = df[df['iv'].notna()].copy()
    
    if df.empty:
        return df
    
    # 【新增】：处理IV=0的情况
    def calc_theo_price(row):
        if row['iv'] == 0:
            return row['intrinsic_value']
        else:
            return black76_price(F, row['strike'], row['tte'], r, row['iv'], row['option_type'])
    
    df['theo_price'] = df.apply(calc_theo_price, axis=1)
    
    # 分离IV>0和IV=0的合约
    df_with_iv = df[df['iv'] > 0].copy()
    df_zero_iv = df[df['iv'] == 0].copy()
    
    # 计算IV>0合约的Greeks
    if not df_with_iv.empty:
        sqrt_tte = np.sqrt(df_with_iv['tte'])
        pv_vec = np.exp(-r * df_with_iv['tte'])
        
        df_with_iv['d1'] = (np.log(F / df_with_iv['strike']) + 0.5 * df_with_iv['iv'] ** 2 * df_with_iv['tte']) / (df_with_iv['iv'] * sqrt_tte)
        df_with_iv['d2'] = df_with_iv['d1'] - df_with_iv['iv'] * sqrt_tte
        
        mask_call = df_with_iv['option_type'] == 'call'
        
        # Delta（Black76的Delta是贴现后的N(d1)）
        df_with_iv['delta'] = np.where(mask_call, 
                                      pv_vec * norm.cdf(df_with_iv['d1']),
                                      -pv_vec * norm.cdf(-df_with_iv['d1']))
        
        # Gamma
        df_with_iv['gamma'] = pv_vec * norm.pdf(df_with_iv['d1']) / (F * df_with_iv['iv'] * sqrt_tte)
        
        # Vega
        df_with_iv['vega'] = F * pv_vec * norm.pdf(df_with_iv['d1']) * sqrt_tte / 100
        
        # Theta
        theta_common = -F * pv_vec * norm.pdf(df_with_iv['d1']) * df_with_iv['iv'] / (2 * sqrt_tte)
        
        theta_call = theta_common - r * df_with_iv['strike'] * pv_vec * norm.cdf(df_with_iv['d2'])
        theta_put = theta_common + r * df_with_iv['strike'] * pv_vec * norm.cdf(-df_with_iv['d2'])
        
        df_with_iv['theta'] = np.where(mask_call, theta_call, theta_put) / 365
        
        # Rho
        rho_call = -df_with_iv['tte'] * pv_vec * (F * norm.cdf(df_with_iv['d1']) - df_with_iv['strike'] * norm.cdf(df_with_iv['d2']))
        rho_put = -df_with_iv['tte'] * pv_vec * (df_with_iv['strike'] * norm.cdf(-df_with_iv['d2']) - F * norm.cdf(-df_with_iv['d1']))
        df_with_iv['rho'] = np.where(mask_call, rho_call, rho_put) / 100
        
        # 清理临时列
        df_with_iv.drop(columns=['d1', 'd2'], inplace=True, errors='ignore')
    
    # 处理IV=0合约的Greeks
    if not df_zero_iv.empty:
        df_zero_iv['delta'] = 0.0
        df_zero_iv['gamma'] = 0.0
        df_zero_iv['vega'] = 0.0
        df_zero_iv['theta'] = 0.0
        df_zero_iv['rho'] = 0.0
    
    # 合并结果
    df = pd.concat([df_with_iv, df_zero_iv], ignore_index=True) if not df_with_iv.empty and not df_zero_iv.empty else \
         (df_with_iv if not df_with_iv.empty else df_zero_iv)
    
    # 清理临时列
    df.drop(columns=['intrinsic_value', 'price_valid'], inplace=True, errors='ignore')
    
    return df

def calculate_baw_greeks(chain_df: pd.DataFrame, S: float, r: float, q: float = 0.0) -> pd.DataFrame:
    """计算BAW模型希腊值（美式期权，简化实现）
    chain_df 需列: market_price, strike, tte, option_type ('call'/'put')
    返回新增: iv, theo_price, delta, gamma, theta, vega, rho
    """
    df = chain_df.copy()
    
    # 确保数值类型
    df['market_price'] = pd.to_numeric(df['market_price'], errors='coerce')
    df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
    df['tte'] = pd.to_numeric(df['tte'], errors='coerce')
    
    # 【关键修复】：过滤无效数据，但允许价格为0
    valid_mask = (
        df['market_price'].notna() & df['strike'].notna() & df['tte'].notna() &
        (df['market_price'] >= 0) &  # 改为 >= 0
        (df['strike'] > 0) & 
        (df['tte'] > 0)
    )
    df = df[valid_mask].copy()
    
    if df.empty:
        return df
    
    # 【新增】：计算内在价值
    df['intrinsic_value'] = df.apply(
        lambda row: max(S - row['strike'], 0) if row['option_type'] == 'call' 
                   else max(row['strike'] - S, 0),
        axis=1
    )
    
    # 【新增】：标记价格有效性
    df['price_valid'] = (df['market_price'] > 0) | ((df['market_price'] == 0) & (df['intrinsic_value'] == 0))
    df = df[df['price_valid']].copy()
    
    if df.empty:
        return df
    
    # IV 反推
    df['iv'] = df.apply(
        lambda row: implied_volatility(
            row['market_price'], S, row['strike'], row['tte'], r, q, 
            row['option_type'], model='baw'
        ),
        axis=1
    )
    
    # 【关键修复】：过滤IV无效的行，但保留IV=0
    df = df[df['iv'].notna()].copy()
    
    if df.empty:
        return df
    
    # 【新增】：处理IV=0的情况
    def calc_price(row):
        if row['iv'] == 0:
            return row['intrinsic_value']
        elif row['option_type'] == 'call':
            return baw_american_call_price(S, row['strike'], row['tte'], r, q, row['iv'])
        else:
            return baw_american_put_price(S, row['strike'], row['tte'], r, q, row['iv'])
    
    df['theo_price'] = df.apply(calc_price, axis=1)
    
    # 分离IV>0和IV=0的合约
    df_with_iv = df[df['iv'] > 0].copy()
    df_zero_iv = df[df['iv'] == 0].copy()
    
    # 计算IV>0合约的Greeks（使用BSM近似）
    if not df_with_iv.empty:
        sqrt_tte = np.sqrt(df_with_iv['tte'])
        df_with_iv['d1'] = (np.log(S / df_with_iv['strike']) + (r - q + 0.5 * df_with_iv['iv'] ** 2) * df_with_iv['tte']) / (df_with_iv['iv'] * sqrt_tte)
        df_with_iv['d2'] = df_with_iv['d1'] - df_with_iv['iv'] * sqrt_tte
        
        mask_call = df_with_iv['option_type'] == 'call'
        
        # Delta
        df_with_iv['delta'] = np.where(mask_call, 
                                      np.exp(-q * df_with_iv['tte']) * norm.cdf(df_with_iv['d1']),
                                      -np.exp(-q * df_with_iv['tte']) * norm.cdf(-df_with_iv['d1']))
        
        # Gamma
        df_with_iv['gamma'] = np.exp(-q * df_with_iv['tte']) * norm.pdf(df_with_iv['d1']) / (S * df_with_iv['iv'] * sqrt_tte)
        
        # Vega
        df_with_iv['vega'] = S * np.exp(-q * df_with_iv['tte']) * norm.pdf(df_with_iv['d1']) * sqrt_tte / 100
        
        # Theta
        theta_common = -(S * np.exp(-q * df_with_iv['tte']) * norm.pdf(df_with_iv['d1']) * df_with_iv['iv']) / (2 * sqrt_tte)
        
        theta_call = (theta_common 
                     - r * df_with_iv['strike'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(df_with_iv['d2'])
                     + q * S * np.exp(-q * df_with_iv['tte']) * norm.cdf(df_with_iv['d1']))
        
        theta_put = (theta_common 
                    + r * df_with_iv['strike'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(-df_with_iv['d2'])
                    - q * S * np.exp(-q * df_with_iv['tte']) * norm.cdf(-df_with_iv['d1']))
        
        df_with_iv['theta'] = np.where(mask_call, theta_call, theta_put) / 365
        
        # Rho
        df_with_iv['rho'] = np.where(mask_call,
                                    df_with_iv['strike'] * df_with_iv['tte'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(df_with_iv['d2']) / 100,
                                    -df_with_iv['strike'] * df_with_iv['tte'] * np.exp(-r * df_with_iv['tte']) * norm.cdf(-df_with_iv['d2']) / 100)
        
        # 清理临时列
        df_with_iv.drop(columns=['d1', 'd2'], inplace=True, errors='ignore')
    
    # 处理IV=0合约的Greeks
    if not df_zero_iv.empty:
        df_zero_iv['delta'] = 0.0
        df_zero_iv['gamma'] = 0.0
        df_zero_iv['vega'] = 0.0
        df_zero_iv['theta'] = 0.0
        df_zero_iv['rho'] = 0.0
    
    # 合并结果
    df = pd.concat([df_with_iv, df_zero_iv], ignore_index=True) if not df_with_iv.empty and not df_zero_iv.empty else \
         (df_with_iv if not df_with_iv.empty else df_zero_iv)
    
    # 清理临时列
    df.drop(columns=['intrinsic_value', 'price_valid'], inplace=True, errors='ignore')
    
    return df

def calculate_greeks(chain_df: pd.DataFrame, S: float, r: float, q: float = 0.02, 
                    model: str = 'bsm', is_american: bool = False, F: Optional[float] = None) -> pd.DataFrame:
    """根据指定模型计算希腊值
    model: 'bsm', 'black76', 'baw'
    is_american: 是否为美式期权
    F: 期货价格（仅black76模型需要）
    
    chain_df 需列: market_price, strike, tte, option_type ('call'/'put')
    返回新增: iv, theo_price, delta, gamma, theta, vega, rho
    """
    if model not in ['bsm', 'black76', 'baw']:
        raise ValueError(f"Unsupported model: {model}. Choose from 'bsm', 'black76', 'baw'.")
    
    if model == 'black76':
        if F is None or not np.isfinite(F):
            F = S  # 如果没有提供期货价格，使用标的资产价格
        return calculate_black76_greeks(chain_df, F, r)
    
    elif model == 'baw' or is_american:
        return calculate_baw_greeks(chain_df, S, r, q)
    
    else:
        return calculate_bsm_greeks(chain_df, S, r, q)
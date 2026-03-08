#src/calc/risker/base_risker.py
"""
风险计算基类 - 重构版
集成：品种级模型配置、增强HV计算、IVMR计算、详细报告
"""
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
import logging

from src.utils.logging_config import get_logger
from src.utils.database import db
from src.utils.utils import get_underlying_product, calculate_tte
from src.calc.hv_calculator import HVCalculator
from src.calc.ivmr_calculator import IVMRCalculator
from src.calc.calculation_report import (
    ContractStatus, 
    ExchangeReport, 
    ContractAvailabilityChecker
)

logger = get_logger("src.riskers.base_risker")


class RiskCalculatorBase(ABC):
    """增强版风险计算基类"""
    
    exchange_name: str = "BASE"
    
    def __init__(self, trade_date: str, config_dict: Dict[str, Any] = None):
        """
        初始化风险计算器
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD)
            config_dict: 配置字典（由 load_config() 返回）
        """
        self.trade_date = trade_date
        self.config = config_dict or {}
        self.db = db
        self.logger = logger.getChild(self.__class__.__name__)
        
        # 初始化组件
        self.hv_calculator = HVCalculator(self.db, self._get_trade_days())
        self.ivmr_calculator = IVMRCalculator(self.db, self._get_trade_days())
        self.availability_checker = ContractAvailabilityChecker(self.db, self.config)
        self.report = ExchangeReport(
            exchange=self.exchange_name.lower(), 
            trade_date=trade_date
        )
        
        self.logger.info(f"初始化完成 | 交易日期: {trade_date}")
    
    def _get_trade_days(self) -> List[str]:
        """获取交易日列表"""
        from src.utils.utils import get_trading_days
        days = get_trading_days()
        return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in days]
    
    @abstractmethod
    def _extract_product(self, contract_code: str) -> str:
        """从合约代码提取品种代码（小写）"""
        pass
    
    def calculate_risk(self, position_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        执行完整风险计算流程
        
        Returns:
            (结果DataFrame, 统计字典)
        """
        try:
            self.logger.info(f"▶️ 开始风险计算 | 输入合约数: {len(position_df)}")
            
            # 1. 预处理
            processed = self._preprocess(position_df)
            
            # 2. 检查合约可用性并分类
            available_contracts = self._check_contracts_availability(processed)
            
            # 3. 核心计算（仅对可用合约）
            results = self._core_calculation(available_contracts)
            
            # 4. 生成统计
            stats = self._generate_stats(results)
            
            self.logger.info(
                f"✅ 计算完成 | 成功:{stats['success_count']} | "
                f"跳过:{stats['expired_skipped_count']} | "
                f"缺失:{stats['missing_data_count']}"
            )
            
            return results, stats
            
        except Exception as e:
            self.logger.exception(f"❌ 风险计算异常: {str(e)}")
            raise
    
    def _check_contracts_availability(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        检查所有合约的可用性，分类处理
        """
        if df.empty:
            return df
        
        available_rows = []
        
        for _, row in df.iterrows():
            contract = row['contract']
            
            status = self.availability_checker.check_contract_status(
                contract, 
                self.exchange_name.lower(),
                self.trade_date
            )
            
            if status.status == 'available':
                available_rows.append(row)
            elif status.status == 'expired_skipped':
                self.report.expired_skipped.append(contract)
                self.logger.debug(f"⏭️ {contract}: 已到期跳过")
            elif status.status == 'not_yet_listed':
                self.report.not_yet_listed.append(contract)
                self.logger.debug(f"⏸️ {contract}: 尚未上市")
            elif status.status == 'missing_data':
                self.report.missing_data.append(contract)
                self.logger.warning(f"❓ {contract}: 数据缺失 - {status.message}")
            else:
                self.report.calculation_errors.append(contract)
                self.logger.error(f"❌ {contract}: {status.message}")
        
        return pd.DataFrame(available_rows)
    
    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """预处理：提取品种、生成标的映射"""
        if df.empty:
            return df
        
        df = df.copy()
        
        # 提取期权品种（子类实现）
        df['option_product'] = df['contract'].apply(self._extract_product)
        
        # 生成标的品种映射
        df['underlying_product'] = df['option_product'].apply(
            lambda p: get_underlying_product(p, self.exchange_name)
        )
        df['underlying_product_upper'] = df['underlying_product'].str.upper()
        df['fu_table'] = 'fu_' + df['underlying_product']
        
        # 记录配置合约列表（用于报告）
        self.report.config_contracts = df['contract'].tolist()
        
        return df
    
    def _core_calculation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        核心计算流程（各交易所可重写）
        """
        results = []
        
        for _, row in df.iterrows():
            try:
                result = self._calculate_single_contract(row)
                results.append(result)
                self.report.success_contracts.append(row['contract'])
                
            except Exception as e:
                self.logger.warning(f"合约 {row['contract']} 计算失败: {str(e)}")
                self.report.calculation_errors.append(row['contract'])
                results.append({
                    'contract': row['contract'],
                    'exchange': self.exchange_name,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return pd.DataFrame(results)
    
    def _calculate_single_contract(self, row: pd.Series) -> Dict:
        """
        计算单个合约的风险指标
        
        各交易所可以重写此方法实现特殊逻辑
        """
        contract = row['contract']
        product = row['option_product'].upper()
        
        # 获取品种级模型参数
        from src.calc.model.model_factory import get_model_params
        model_params = get_model_params(self.exchange_name.lower(), product)
        
        # 获取标的资产价格
        underlying_price = self._get_underlying_price(row)
        if underlying_price is None:
            raise ValueError(f"无法获取标的资产价格: {row['underlying_product_upper']}")
        
        # 计算HV（20日主力+次主力加权）
        hv = self.hv_calculator.calculate_hv(
            row['underlying_product'],
            self.trade_date
        )
        
        # 计算IVMR（单个合约IV时间序列斜率）
        ivmr_result = self.ivmr_calculator.calculate_ivmr(
            contract,
            self.trade_date,
            self.exchange_name.lower()
        )
        
        # 获取当前IV
        current_iv = self._get_current_iv(contract)
        
        # 计算希腊值
        greeks = self._calculate_contract_greeks(
            row, underlying_price, model_params
        )
        
        return {
            'contract': contract,
            'exchange': self.exchange_name,
            'product': product,
            'underlying': row['underlying_product_upper'],
            'underlying_price': underlying_price,
            'model_used': model_params['model'],
            'iv': current_iv,
            'hv20': hv,
            'ivmr3': ivmr_result.ivmr3,
            'ivmr7': ivmr_result.ivmr7,
            'ivmr15': ivmr_result.ivmr15,
            'ivmr30': ivmr_result.ivmr30,
            'ivmr60': ivmr_result.ivmr60,
            'ivmr90': ivmr_result.ivmr90,
            'ivmr': ivmr_result.ivmr,
            'ivmr_r2': ivmr_result.r_squared,
            'delta': greeks.get('delta'),
            'gamma': greeks.get('gamma'),
            'theta': greeks.get('theta'),
            'vega': greeks.get('vega'),
            'rho': greeks.get('rho'),
            'theo_price': greeks.get('theo_price'),
            'status': 'success'
        }
    
    def _get_underlying_price(self, row: pd.Series) -> Optional[float]:
        """获取标的资产价格（主力合约）"""
        table_name = row['fu_table']
        product_upper = row['underlying_product_upper']
        
        sql = f"""
            SELECT 收盘价 as close, 持仓量 as oi
            FROM {table_name}
            WHERE 交易日期 = ? AND 期货合约 LIKE ?
            ORDER BY 持仓量 DESC
            LIMIT 1
        """
        
        try:
            result = self.db.query_one(sql, [self.trade_date, f"{product_upper}%"])
            return float(result['close']) if result else None
        except Exception as e:
            self.logger.error(f"查询标的资产价格失败: {e}")
            return None
    
    def _get_current_iv(self, contract: str) -> Optional[float]:
        """获取合约当前IV"""
        op_table = f"op_{self.exchange_name.lower()}"
        sql = f"""
            SELECT 隐含波动率 as iv
            FROM {op_table}
            WHERE 期权合约代码 = ? AND 交易日期 = ?
        """
        
        try:
            result = self.db.query_one(sql, [contract, self.trade_date])
            return float(result['iv']) if result and result.get('iv') else None
        except Exception:
            return None
    
    def _calculate_contract_greeks(
        self, 
        row: pd.Series,
        underlying_price: float,
        model_params: Dict
    ) -> Dict:
        """计算单个合约的希腊值"""
        # 简化实现，实际应从数据库获取完整期权链数据
        from src.calc.model.greeks import calculate_greeks
        
        # 创建单合约DataFrame
        chain_df = pd.DataFrame([{
            'market_price': row.get('settle_price', 0),
            'strike': row.get('strike', 0),
            'tte': self._calculate_tte(row.get('expiry_date', '')),
            'option_type': 'call' if 'C' in row['contract'] or 'c' in row['contract'] else 'put'
        }])
        
        try:
            result = calculate_greeks(
                chain_df,
                S=underlying_price,
                r=model_params['r'],
                q=model_params.get('q', 0),
                model=model_params['model'],
                is_american=model_params.get('is_american', False)
            )
            
            if not result.empty:
                return {
                    'delta': result.iloc[0].get('delta'),
                    'gamma': result.iloc[0].get('gamma'),
                    'theta': result.iloc[0].get('theta'),
                    'vega': result.iloc[0].get('vega'),
                    'rho': result.iloc[0].get('rho'),
                    'theo_price': result.iloc[0].get('theo_price')
                }
        except Exception as e:
            self.logger.warning(f"希腊值计算失败: {e}")
        
        return {}
    
    def _calculate_tte(self, expiry_date: str) -> float:
        """计算到期时间（年化）"""
        if not expiry_date:
            return 30.0 / 365.0
        try:
            return calculate_tte(expiry_date, self.trade_date)
        except Exception:
            return 30.0 / 365.0
    
    def _generate_stats(self, results: pd.DataFrame) -> Dict:
        """生成统计信息"""
        return {
            'total_contracts': len(results),
            'success_count': len(self.report.success_contracts),
            'expired_skipped_count': len(self.report.expired_skipped),
            'missing_data_count': len(self.report.missing_data),
            'not_yet_listed_count': len(self.report.not_yet_listed),
            'calculation_error_count': len(self.report.calculation_errors),
            'exchange': self.exchange_name
        }
    
    def get_report(self) -> ExchangeReport:
        """获取计算报告"""
        return self.report
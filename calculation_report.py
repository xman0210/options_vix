# src/utils/calculation_report.py
"""
风险计算结果报告生成模块 - 增强版
支持到期合约识别、数据缺失分类、详细统计汇总
"""
import pandas as pd
import numpy as np
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class ContractStatus:
    """单个合约计算状态"""
    contract_code: str
    status: str  # 'success', 'expired_skipped', 'missing_data', 'not_yet_listed', 'calculation_error'
    message: str
    expiry_date: Optional[str] = None
    listing_date: Optional[str] = None
    calculated_at: Optional[str] = None
    greeks: Optional[Dict] = None


@dataclass
class ExchangeReport:
    """单个交易所计算报告"""
    exchange: str
    trade_date: str
    config_contracts: List[str] = field(default_factory=list)
    success_contracts: List[str] = field(default_factory=list)
    expired_skipped: List[str] = field(default_factory=list)
    missing_data: List[str] = field(default_factory=list)
    not_yet_listed: List[str] = field(default_factory=list)
    calculation_errors: List[str] = field(default_factory=list)
    
    def total_configured(self) -> int:
        return len(self.config_contracts)
    
    def total_success(self) -> int:
        return len(self.success_contracts)
    
    def total_failed(self) -> int:
        return (
            len(self.expired_skipped) + 
            len(self.missing_data) + 
            len(self.not_yet_listed) + 
            len(self.calculation_errors)
        )
    
    def success_rate(self) -> float:
        configured = self.total_configured()
        if configured == 0:
            return 0.0
        return len(self.success_contracts) / configured * 100
    
    def to_dict(self) -> Dict:
        return {
            'exchange': self.exchange,
            'trade_date': self.trade_date,
            'statistics': {
                'total_configured': self.total_configured(),
                'success_count': len(self.success_contracts),
                'expired_skipped_count': len(self.expired_skipped),
                'missing_data_count': len(self.missing_data),
                'not_yet_listed_count': len(self.not_yet_listed),
                'calculation_error_count': len(self.calculation_errors),
                'success_rate_percent': round(self.success_rate(), 2)
            },
            'details': {
                'success_contracts': self.success_contracts,
                'expired_skipped': self.expired_skipped,
                'missing_data': self.missing_data,
                'not_yet_listed': self.not_yet_listed,
                'calculation_errors': self.calculation_errors
            }
        }


class ContractAvailabilityChecker:
    """合约可用性检查器 - 区分到期/未上市/数据缺失"""
    
    # 各交易所期权上市提前期（大致天数）
    LISTING_ADVANCE_DAYS = {
        'cffex': 90,   # 中金所：到期前3个月上市
        'shfe': 120,   # 上期所：提前4个月
        'dce': 90,     # 大商所：提前3个月
        'czce': 60,    # 郑商所：提前2个月
        'gfex': 90     # 广期所：提前3个月
    }
    
    def __init__(self, db_manager, config_loader):
        self.db = db_manager
        self.config = config_loader
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def check_contract_status(
        self, 
        contract_code: str, 
        exchange: str, 
        trade_date: str
    ) -> ContractStatus:
        """
        检查合约状态，区分：
        1. 已到期（expired）- 不报错，归类为'到期期权，跳过计算'
        2. 未上市（not_yet_listed）- 提醒'期权合约尚未上市'
        3. 数据缺失（missing_data）- 提醒'期权表无数据，可能是需要补充数据'
        4. 可计算（available）- 正常处理
        
        Returns:
            ContractStatus对象
        """
        # 1. 检查配置中是否有到期日信息
        expiry_date = self._get_expiry_date_from_config(contract_code, exchange)
        
        if expiry_date:
            # 有配置到期日，比较是否已过期
            if trade_date > expiry_date:
                return ContractStatus(
                    contract_code=contract_code,
                    status='expired_skipped',
                    message=f'到期期权，跳过计算（已于{expiry_date}到期）',
                    expiry_date=expiry_date
                )
        
        # 2. 查询期权表检查数据存在性
        op_table = f"op_{exchange}"
        sql = f"""
            SELECT 交易日期, 结算价, 成交量, 隐含波动率 
            FROM {op_table} 
            WHERE 期权合约代码 = ? AND 交易日期 = ?
        """
        
        try:
            result = self.db.query_one(sql, [contract_code, trade_date])
            
            if result is None:
                # 无数据 - 判断是否已上市
                listing_date = self._estimate_listing_date(contract_code, exchange, expiry_date)
                
                if listing_date and trade_date < listing_date:
                    return ContractStatus(
                        contract_code=contract_code,
                        status='not_yet_listed',
                        message=f'期权合约尚未上市（预计上市日: {listing_date}）',
                        expiry_date=expiry_date,
                        listing_date=listing_date
                    )
                else:
                    return ContractStatus(
                        contract_code=contract_code,
                        status='missing_data',
                        message='期权表无数据，无法计算，可能是需要补充数据',
                        expiry_date=expiry_date
                    )
            
            # 有数据但关键字段为空
            if result.get('结算价') is None and result.get('隐含波动率') is None:
                return ContractStatus(
                    contract_code=contract_code,
                    status='missing_data',
                    message='期权表数据不完整（结算价和IV均为空），需要补充数据',
                    expiry_date=expiry_date
                )
            
            # 数据完整，可计算
            return ContractStatus(
                contract_code=contract_code,
                status='available',
                message='数据完整，可计算',
                expiry_date=expiry_date
            )
            
        except Exception as e:
            return ContractStatus(
                contract_code=contract_code,
                status='calculation_error',
                message=f'查询异常: {str(e)}',
                expiry_date=expiry_date
            )
    
    def _get_expiry_date_from_config(self, contract_code: str, exchange: str) -> Optional[str]:
        """从配置获取到期日"""
        expiry_config_key = f"{exchange}_op_expiry_dates"
        expiry_dates = self.config.get(expiry_config_key, {})
        return expiry_dates.get(contract_code)
    
    def _estimate_listing_date(
        self, 
        contract_code: str, 
        exchange: str,
        expiry_date: Optional[str]
    ) -> Optional[str]:
        """
        估算合约上市日期
        
        规则：到期日前N个月上市（各交易所不同）
        """
        if not expiry_date:
            return None
        
        advance_days = self.LISTING_ADVANCE_DAYS.get(exchange.lower(), 90)
        
        try:
            expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
            listing_dt = expiry_dt - pd.Timedelta(days=advance_days)
            return listing_dt.strftime('%Y-%m-%d')
        except Exception:
            return None


class CalculationReportGenerator:
    """计算结果报告生成器"""
    
    def __init__(self, trade_date: str, config_loader):
        self.trade_date = trade_date
        self.config = config_loader
        self.reports: Dict[str, ExchangeReport] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def add_exchange_report(self, report: ExchangeReport):
        """添加交易所报告"""
        self.reports[report.exchange] = report
        
    def generate_console_report(self) -> str:
        """生成控制台文本报告"""
        lines = []
        lines.append("=" * 80)
        lines.append(f"期权风险计算报告 - 交易日期: {self.trade_date}")
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 80)
        
        total_configured = 0
        total_success = 0
        total_expired = 0
        total_missing = 0
        total_not_listed = 0
        total_errors = 0
        
        for exchange, report in self.reports.items():
            stats = report.to_dict()['statistics']
            total_configured += stats['total_configured']
            total_success += stats['success_count']
            total_expired += stats['expired_skipped_count']
            total_missing += stats['missing_data_count']
            total_not_listed += stats['not_yet_listed_count']
            total_errors += stats['calculation_error_count']
            
            lines.append(f"\n【{exchange.upper()}】")
            lines.append(f"  配置合约总数: {stats['total_configured']}")
            lines.append(f"  ✅ 成功计算:   {stats['success_count']}")
            lines.append(f"  ⏭️  到期跳过:   {stats['expired_skipped_count']}")
            lines.append(f"  ⏸️  尚未上市:   {stats['not_yet_listed_count']}")
            lines.append(f"  ❓ 数据缺失:   {stats['missing_data_count']} (需补充数据)")
            lines.append(f"  ❌ 计算错误:   {stats['calculation_error_count']}")
            lines.append(f"  📊 成功率:     {stats['success_rate_percent']}%")
            
            # 显示详情（限制数量）
            if report.missing_data:
                lines.append(f"    数据缺失合约 ({len(report.missing_data)}):")
                for c in report.missing_data[:3]:
                    lines.append(f"      - {c}")
                if len(report.missing_data) > 3:
                    lines.append(f"      ... 等共{len(report.missing_data)}个")
            
            if report.not_yet_listed:
                lines.append(f"    尚未上市合约 ({len(report.not_yet_listed)}):")
                for c in report.not_yet_listed[:3]:
                    lines.append(f"      - {c}")
                if len(report.not_yet_listed) > 3:
                    lines.append(f"      ... 等共{len(report.not_yet_listed)}个")
        
        lines.append("\n" + "=" * 80)
        lines.append("【汇总统计】")
        lines.append(f"  总配置合约: {total_configured}")
        if total_configured > 0:
            lines.append(f"  总成功计算: {total_success} ({total_success/total_configured*100:.1f}%)")
        else:
            lines.append(f"  总成功计算: 0")
        lines.append(f"  总到期跳过: {total_expired}")
        lines.append(f"  总尚未上市: {total_not_listed}")
        lines.append(f"  总数据缺失: {total_missing}")
        lines.append(f"  总计算错误: {total_errors}")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def generate_html_report(self, output_path: Optional[str] = None) -> str:
        """生成HTML详细报告"""
        html_parts = []
        
        css = """
        <style>
            body { font-family: 'Microsoft YaHei', Arial, sans-serif; margin: 40px; background: #f5f7fa; }
            .container { max-width: 1400px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 15px; }
            h2 { color: #2980b9; margin-top: 30px; border-left: 4px solid #3498db; padding-left: 15px; }
            .summary-box { background: #e8f4f8; padding: 20px; border-radius: 8px; margin: 20px 0; }
            .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
            .stat-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); text-align: center; }
            .stat-number { font-size: 2em; font-weight: bold; color: #3498db; }
            .stat-label { color: #7f8c8d; margin-top: 5px; }
            table { width: 100%; border-collapse: collapse; margin: 20px 0; }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background-color: #3498db; color: white; }
            tr:hover { background-color: #f5f9ff; }
            .status-success { color: #27ae60; font-weight: bold; }
            .status-expired { color: #f39c12; }
            .status-missing { color: #e74c3c; }
            .status-notlisted { color: #9b59b6; }
            .status-error { color: #c0392b; font-weight: bold; }
            .contract-list { max-height: 200px; overflow-y: auto; background: #f8f9fa; padding: 10px; border-radius: 5px; font-family: monospace; font-size: 0.9em; }
            .badge { display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 0.85em; font-weight: bold; }
            .badge-success { background: #d4edda; color: #155724; }
            .badge-expired { background: #fff3cd; color: #856404; }
            .badge-notlisted { background: #e2d4f0; color: #6c3483; }
            .badge-missing { background: #f8d7da; color: #721c24; }
            .badge-error { background: #f5c6cb; color: #721c24; }
        </style>
        """
        
        html_parts.append(f"<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body>")
        html_parts.append("<div class='container'>")
        
        # 标题
        html_parts.append(f"<h1>📊 期权风险计算详细报告</h1>")
        html_parts.append(f"<p><strong>交易日期:</strong> {self.trade_date}</p>")
        html_parts.append(f"<p><strong>生成时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
        
        # 全局统计
        total_configured = sum(r.total_configured() for r in self.reports.values())
        total_success = sum(len(r.success_contracts) for r in self.reports.values())
        total_expired = sum(len(r.expired_skipped) for r in self.reports.values())
        total_not_listed = sum(len(r.not_yet_listed) for r in self.reports.values())
        total_missing = sum(len(r.missing_data) for r in self.reports.values())
        total_errors = sum(len(r.calculation_errors) for r in self.reports.values())
        
        html_parts.append("<div class='summary-box'>")
        html_parts.append("<h2>📈 全局汇总</h2>")
        html_parts.append("<div class='stat-grid'>")
        
        stats = [
            (total_configured, "配置合约总数", "#3498db"),
            (total_success, "成功计算", "#27ae60"),
            (total_expired, "到期跳过", "#f39c12"),
            (total_not_listed, "尚未上市", "#9b59b6"),
            (total_missing, "数据缺失", "#e74c3c"),
            (total_errors, "计算错误", "#c0392b")
        ]
        
        for value, label, color in stats:
            html_parts.append(f"""
                <div class='stat-card'>
                    <div class='stat-number' style='color: {color}'>{value:,}</div>
                    <div class='stat-label'>{label}</div>
                </div>
            """)
        
        html_parts.append("</div></div>")
        
        # 各交易所详情表格
        html_parts.append("<h2>📋 交易所详细统计</h2>")
        html_parts.append("<table>")
        html_parts.append("<tr><th>交易所</th><th>配置数</th><th>成功</th><th>到期跳过</th><th>尚未上市</th><th>数据缺失</th><th>计算错误</th><th>成功率</th></tr>")
        
        for exchange, report in self.reports.items():
            stats = report.to_dict()['statistics']
            html_parts.append(f"""
                <tr>
                    <td><strong>{exchange.upper()}</strong></td>
                    <td>{stats['total_configured']}</td>
                    <td class='status-success'>{stats['success_count']}</td>
                    <td class='status-expired'>{stats['expired_skipped_count']}</td>
                    <td class='status-notlisted'>{stats['not_yet_listed_count']}</td>
                    <td class='status-missing'>{stats['missing_data_count']}</td>
                    <td class='status-error'>{stats['calculation_error_count']}</td>
                    <td><strong>{stats['success_rate_percent']}%</strong></td>
                </tr>
            """)
        
        html_parts.append("</table>")
        
        # 各交易所详情
        for exchange, report in self.reports.items():
            html_parts.append(f"<h2>🔍 {exchange.upper()} 详情</h2>")
            
            details = report.to_dict()['details']
            
            # 各类状态合约列表
            status_configs = [
                ('success_contracts', '成功计算合约', 'badge-success'),
                ('expired_skipped', '到期跳过合约（已过期，无需计算）', 'badge-expired'),
                ('not_yet_listed', '尚未上市合约（等待上市）', 'badge-notlisted'),
                ('missing_data', '数据缺失合约（需补充数据）', 'badge-missing'),
                ('calculation_errors', '计算错误合约', 'badge-error')
            ]
            
            for field, label, badge_class in status_configs:
                contracts = details.get(field, [])
                if contracts:
                    html_parts.append(f"<h3><span class='badge {badge_class}'>{len(contracts)}</span> {label}</h3>")
                    html_parts.append("<div class='contract-list'>")
                    html_parts.append(", ".join(contracts))
                    html_parts.append("</div>")
        
        html_parts.append("</div></body></html>")
        
        html_content = "\n".join(html_parts)
        
        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(html_content, encoding='utf-8')
            self.logger.info(f"HTML报告已保存: {output_path}")
        
        return html_content
    
    def save_json_report(self, output_path: str):
        """保存JSON格式报告供程序解析"""
        report_data = {
            'trade_date': self.trade_date,
            'generated_at': datetime.now().isoformat(),
            'exchanges': {ex: r.to_dict() for ex, r in self.reports.items()}
        }
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"JSON报告已保存: {output_path}")
    
    def get_summary_dict(self) -> Dict[str, Any]:
        """获取汇总字典（供其他模块使用）"""
        total_configured = sum(r.total_configured() for r in self.reports.values())
        total_success = sum(len(r.success_contracts) for r in self.reports.values())
        total_expired = sum(len(r.expired_skipped) for r in self.reports.values())
        total_not_listed = sum(len(r.not_yet_listed) for r in self.reports.values())
        total_missing = sum(len(r.missing_data) for r in self.reports.values())
        total_errors = sum(len(r.calculation_errors) for r in self.reports.values())
        
        return {
            'trade_date': self.trade_date,
            'total_configured': total_configured,
            'total_success': total_success,
            'total_expired': total_expired,
            'total_not_listed': total_not_listed,
            'total_missing': total_missing,
            'total_errors': total_errors,
            'success_rate': total_success / total_configured if total_configured > 0 else 0,
            'exchange_reports': {ex: r.to_dict() for ex, r in self.reports.items()}
        }


def create_exchange_report(
    exchange: str,
    trade_date: str,
    config_contracts: List[str],
    checker: ContractAvailabilityChecker,
    db_manager,
    config_loader
) -> ExchangeReport:
    """
    创建单个交易所的完整报告（便捷函数）
    
    Args:
        exchange: 交易所代码
        trade_date: 交易日期
        config_contracts: 配置文件中定义的合约列表（*_ris_name）
        checker: 合约可用性检查器
        db_manager: 数据库管理器
        config_loader: 配置加载器
    
    Returns:
        ExchangeReport对象
    """
    report = ExchangeReport(
        exchange=exchange,
        trade_date=trade_date,
        config_contracts=config_contracts
    )
    
    # 检查每个合约的状态
    for contract_code in config_contracts:
        status = checker.check_contract_status(contract_code, exchange, trade_date)
        
        if status.status == 'success':
            report.success_contracts.append(contract_code)
        elif status.status == 'expired_skipped':
            report.expired_skipped.append(contract_code)
        elif status.status == 'not_yet_listed':
            report.not_yet_listed.append(contract_code)
        elif status.status == 'missing_data':
            report.missing_data.append(contract_code)
        else:  # calculation_error
            report.calculation_errors.append(contract_code)
    
    return report


def print_contract_status_summary(status_list: List[ContractStatus], max_display: int = 10):
    """
    打印合约状态汇总（调试使用）
    
    Args:
        status_list: ContractStatus对象列表
        max_display: 每类状态最多显示数量
    """
    from collections import Counter
    
    # 统计各类状态数量
    status_counts = Counter(s.status for s in status_list)
    
    print("-" * 60)
    print("合约状态汇总")
    print("-" * 60)
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    print("-" * 60)
    
    # 显示各类状态的合约示例
    status_groups = {}
    for s in status_list:
        status_groups.setdefault(s.status, []).append(s)
    
    for status, contracts in status_groups.items():
        print(f"\n【{status}】({len(contracts)}个)")
        for s in contracts[:max_display]:
            print(f"  - {s.contract_code}: {s.message}")
        if len(contracts) > max_display:
            print(f"  ... 等共{len(contracts)}个")
    
    print("-" * 60)


# 使用示例和测试代码
if __name__ == "__main__":
    # 示例：创建报告生成器
    report_gen = CalculationReportGenerator("2025-03-07", None)
    
    # 示例：添加交易所报告
    ex_report = ExchangeReport(
        exchange="cffex",
        trade_date="2025-03-07",
        config_contracts=["IO2503C4000", "IO2503P4000", "MO2503C6000"],
        success_contracts=["IO2503C4000", "IO2503P4000"],
        expired_skipped=["IO2502C3900"],
        missing_data=["MO2503C6000"],
        not_yet_listed=[],
        calculation_errors=[]
    )
    
    report_gen.add_exchange_report(ex_report)
    
    # 生成并打印控制台报告
    console_report = report_gen.generate_console_report()
    print(console_report)
    
    # 生成HTML报告
    # report_gen.generate_html_report("reports/test_report.html")
    
    # 保存JSON报告
    # report_gen.save_json_report("reports/test_report.json")
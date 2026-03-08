# src/calc/report_generator.py
"""
风险计算报告生成模块
职责：生成含统计摘要的HTML/Excel报告
"""
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def generate_risk_report(results: list, trade_date: str, exchange_stats: dict, output_dir: str = "reports"):
    """
    生成风险计算报告（HTML + Excel）
    
    Args:
        results: 计算结果列表
        trade_date: 交易日期
        exchange_stats: 按交易所的统计字典
        output_dir: 报告输出目录
    """
    if not results and not exchange_stats:
        logger.warning("无数据生成报告")
        return
    
    # 创建输出目录
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    # === 1. 生成详细结果Excel ===
    if results:
        results_df = pd.DataFrame(results)
        excel_path = os.path.join(output_dir, f"risk_results_{trade_date}.xlsx")
        results_df.to_excel(excel_path, index=False, sheet_name="详细结果")
        logger.info(f"✅ 详细结果已保存: {excel_path}")
    
    # === 2. 生成统计摘要HTML ===
    html_content = _generate_html_summary(trade_date, exchange_stats, results)
    html_path = os.path.join(output_dir, f"risk_summary_{trade_date}.html")
    
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"✅ 统计摘要报告已保存: {html_path}")
    logger.info(f"   👉 用浏览器打开: {os.path.abspath(html_path)}")

def _generate_html_summary(trade_date: str, exchange_stats: dict, results: list) -> str:
    """生成HTML统计摘要"""
    # 计算全局汇总
    total_final = sum(s['final_contract_count'] for s in exchange_stats.values())
    total_success = sum(s['success_count'] for s in exchange_stats.values())
    overall_rate = f"{(total_success / total_final * 100):.1f}%" if total_final > 0 else "N/A"
    
    # 生成交易所统计行
    exchange_rows = ""
    for ex, stats in exchange_stats.items():
        final = stats['final_contract_count']
        success = stats['success_count']
        rate = f"{(success / final * 100):.1f}%" if final > 0 else "N/A"
        
        # 状态标识
        status_icon = "✅" if final == success else "⚠️" if success > 0 else "❌"
        
        exchange_rows += f"""
        <tr>
            <td><strong>{ex.upper()}</strong> {status_icon}</td>
            <td class="num">{stats['config_contract_count']:,}</td>
            <td class="num">{stats['contracts_meet_expiry']:,}</td>
            <td class="num">{stats['op_table_contract_count']:,}</td>
            <td class="num"><strong>{final:,}</strong></td>
            <td class="num success">{success:,}</td>
            <td class="rate">{rate}</td>
        </tr>
        """
    
    # 失败合约摘要（仅显示前10个）
    failed_summary = ""
    if results:
        failed_items = [r for r in results if r.get('status') != 'success']
        if failed_items:
            failed_list = "<ul>" + "".join([
                f"<li><code>{r['contract_code']}</code>: {r.get('error_msg', 'Unknown error')[:60]}</li>"
                for r in failed_items[:10]
            ]) + "</ul>"
            if len(failed_items) > 10:
                failed_list += f"<p>... 共 {len(failed_items)} 个失败合约</p>"
            failed_summary = f"""
            <div class="section">
                <h3>❌ 失败合约摘要 ({len(failed_items)} 个)</h3>
                {failed_list}
            </div>
            """
    
    # 生成完整HTML
    html = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>风险计算报告 - {trade_date}</title>
        <style>
            body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; margin: 40px; background: #f5f7fa; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
            h2 {{ color: #2980b9; margin-top: 30px; }}
            .header {{ background: #e3f2fd; padding: 15px; border-radius: 8px; margin-bottom: 25px; }}
            .header div {{ margin: 5px 0; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ padding: 12px 15px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #3498db; color: white; font-weight: 600; }}
            tr:hover {{ background-color: #f5f9ff; }}
            .num {{ text-align: right; font-family: 'Courier New', monospace; }}
            .success {{ color: #27ae60; font-weight: bold; }}
            .rate {{ font-weight: bold; }}
            .section {{ margin: 25px 0; padding: 20px; background: #f8f9fa; border-left: 4px solid #3498db; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; color: #7f8c8d; font-size: 0.9em; }}
            @media print {{
                body {{ background: white; }}
                .container {{ box-shadow: none; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 风险计算报告</h1>
            
            <div class="header">
                <div><strong>交易日期:</strong> {trade_date}</div>
                <div><strong>生成时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
                <div><strong>总计待计算合约:</strong> <span style="font-size:1.3em; color:#e74c3c; font-weight:bold">{total_final:,}</span></div>
                <div><strong>总计成功计算:</strong> <span style="font-size:1.3em; color:#27ae60; font-weight:bold">{total_success:,}</span></div>
                <div><strong>整体成功率:</strong> <span style="font-size:1.4em; color:#2980b9; font-weight:bold">{overall_rate}</span></div>
            </div>
            
            <div class="section">
                <h2>📈 合约过滤与计算统计</h2>
                <table>
                    <thead>
                        <tr>
                            <th>交易所</th>
                            <th>配置ris_name合约数</th>
                            <th>满足配置到期日合约数</th>
                            <th>op_表存在合约数</th>
                            <th>待计算合约数</th>
                            <th>成功计算合约数</th>
                            <th>成功率</th>
                        </tr>
                    </thead>
                    <tbody>
                        {exchange_rows}
                        <tr style="font-weight:bold; background:#f8f9fa">
                            <td>总计</td>
                            <td class="num">-</td>
                            <td class="num">-</td>
                            <td class="num">-</td>
                            <td class="num">{total_final:,}</td>
                            <td class="num success">{total_success:,}</td>
                            <td class="rate">{overall_rate}</td>
                        </tr>
                    </tbody>
                </table>
                <p><small><strong>注:</strong> 满足配置到期日合约数 = ris_name中且到期日在expiry_date配置内的合约数量</small></p>
            </div>
            
            {failed_summary}
            
            <div class="footer">
                <p>报告生成系统 | 风险计算模块 v2.0 | 本报告仅用于内部风险管理</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html
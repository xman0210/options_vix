"""
使用streamlit构建web交互界面
支持公网访问（通过ngrok隧道）
运行命令：
streamlit run op_st.py

公网访问方式：
1. 自动模式：启动时自动创建ngrok隧道（需配置NGROK_AUTH_TOKEN环境变量）
2. 手动模式：侧边栏点击"启动公网访问"按钮
"""
import streamlit as st
import pandas as pd
import datetime
import re
from pathlib import Path
import sys
import traceback
from io import StringIO  # 用于 CSV 下载
import csv
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import linregress
import os
import socket
import subprocess
import time
import threading
import json

# 项目根路径设置
try:
    current_file = Path(__file__).resolve()
    project_root = None
    for parent in [current_file] + list(current_file.parents):
        if (parent / "src" / "utils").exists() and (parent / "config").exists():
            project_root = parent
            break
    if project_root is None:
        project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # 导入项目核心模块
    from src.utils.config_loader import get_db_path, get_log_dir, get_exchange_mapping
    from src.utils.logging_config import get_logger
    from src.utils.database import DatabaseManager
    
except Exception as e:
    st.error(f"❌ [致命] 项目模块导入失败: {e}")
    traceback.print_exc()
    sys.exit(1)

# =============== 全局配置 ===============
EXCHANGE_TO_OPTION_TABLE = {
    'cffex': 'op_cffex',
    'shfe': 'op_shfe',
    'dce': 'op_dce',
    'czce': 'op_czce',
    'gfex': 'op_gfex'
}

# IVMR 系列配置
IVMR_SERIES_OPTIONS = {
    'ivmr3': 'IVMR3 (3日动量)',
    'ivmr7': 'IVMR7 (7日动量)',
    'ivmr15': 'IVMR15 (15日动量)',
    'ivmr30': 'IVMR30 (30日动量)',
    'ivmr90': 'IVMR90 (90日动量)',
    'ivmr': 'IVMR (全周期动量)'
}

# IVMR系列对应的交易日数量映射
IVMR_DAYS_MAP = {
    'ivmr3': 3,
    'ivmr7': 7,
    'ivmr15': 15,
    'ivmr30': 30,
    'ivmr90': 90,
    'ivmr': 252  # 全周期默认取252个交易日（约一年）
}

# 图表类型配置（修改2：只保留一个选项）
CHART_TYPE_OPTIONS = {
    'iv_with_regression': 'IV + IVMR回归直线 + 结算价'
}

# 期权价值类型配置
OPTION_VALUE_TYPE_OPTIONS = {
    'all': '全部',
    'otm': '虚值 (OTM)',
    'atm': '平值 (ATM)',
    'itm': '实值 (ITM)'
}

# 字体修正：全局设置 matplotlib 使用 NotoSansSC 字体
font_path = str(project_root / "fonts" / "NotoSansSC-Regular.ttf")
if Path(font_path).exists():
    fm.fontManager.addfont(font_path)
    plt.rcParams['font.sans-serif'] = ['Noto Sans SC']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.family'] = 'sans-serif'
else:
    st.warning("字体文件 NotoSansSC-Regular.ttf 未找到，中文可能显示为方框")

# Streamlit 页面配置
st.set_page_config(page_title="📈 期权IV筛选系统 v2.5", layout="wide", initial_sidebar_state='expanded')

# =============== 公网访问功能 ===============
class PublicAccessManager:
    """管理公网访问功能（支持ngrok和本地网络）"""
    
    def __init__(self):
        self.ngrok_process = None
        self.public_url = None
        self.local_ip = self._get_local_ip()
        self.streamlit_port = 8501  # Streamlit默认端口
        
    def _get_local_ip(self):
        """获取本机IP地址"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"
    
    def get_local_url(self):
        """获取本地网络访问地址"""
        return f"http://{self.local_ip}:{self.streamlit_port}"
    
    def check_ngrok_installed(self):
        """检查是否安装了ngrok"""
        try:
            result = subprocess.run(['ngrok', 'version'], capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False
    
    def install_ngrok(self):
        """尝试安装ngrok（仅支持部分系统）"""
        try:
            import platform
            system = platform.system().lower()
            
            if system == "darwin":  # macOS
                try:
                    subprocess.run(['brew', 'install', 'ngrok'], check=True)
                    return True
                except:
                    pass
            elif system == "linux":
                # 尝试使用snap或下载二进制
                try:
                    subprocess.run(['snap', 'install', 'ngrok'], check=True)
                    return True
                except:
                    # 下载并安装
                    import urllib.request
                    ngrok_url = "https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-amd64.tgz"
                    urllib.request.urlretrieve(ngrok_url, "/tmp/ngrok.tgz")
                    subprocess.run(['tar', '-xzf', '/tmp/ngrok.tgz', '-C', '/usr/local/bin/'], check=True)
                    return True
            
            return False
        except Exception as e:
            st.error(f"安装ngrok失败: {e}")
            return False
    
    def start_ngrok(self, auth_token=None):
        """启动ngrok隧道"""
        if not self.check_ngrok_installed():
            st.info("正在尝试安装ngrok...")
            if not self.install_ngrok():
                return None, "ngrok未安装，请手动安装: https://ngrok.com/download"
        
        # 如果提供了auth token，先配置
        if auth_token:
            try:
                subprocess.run(['ngrok', 'config', 'add-authtoken', auth_token], 
                              capture_output=True, check=True)
            except Exception as e:
                return None, f"配置ngrok auth token失败: {e}"
        
        # 检查是否已有ngrok在运行
        try:
            result = subprocess.run(['curl', '-s', 'http://localhost:4040/api/tunnels'], 
                                  capture_output=True, text=True)
            if result.returncode == 0 and result.stdout:
                data = json.loads(result.stdout)
                for tunnel in data['tunnels']:
                    if tunnel['proto'] == 'http' and tunnel['config']['addr'].endswith(':8501'):
                        return tunnel['public_url'], None
        except Exception:
            pass
        
        # 启动新ngrok进程
        cmd = ['ngrok', 'http', '--log=stdout', str(self.streamlit_port)]
        self.ngrok_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # 等待隧道启动
        start_time = time.time()
        while time.time() - start_time < 30:  # 超时30秒
            line = self.ngrok_process.stdout.readline()
            if line:
                match = re.search(r'url[](https://.*\.ngrok-free\.app)', line)
                if match:
                    self.public_url = match.group(1)
                    return self.public_url, None
            time.sleep(0.1)
        
        return None, "ngrok隧道启动超时"
    
    def stop_ngrok(self):
        """停止ngrok隧道"""
        if self.ngrok_process:
            self.ngrok_process.terminate()
            self.ngrok_process.wait()
            self.ngrok_process = None
            self.public_url = None
    
    def get_ngrok_url(self):
        """获取ngrok公网URL"""
        return self.public_url

# =============== 期权筛选应用类 ===============
class OptionScreenerApp:
    """期权筛选应用主类"""
    
    def __init__(self):
        self.db_path = get_db_path()
        self.log_dir = get_log_dir()
        self.exchange_mapping = get_exchange_mapping()
        self.db_manager = DatabaseManager(self.db_path)
        self.logger = get_logger(self.log_dir)
        self.public_manager = PublicAccessManager()
        
        # 初始化 session_state
        if 'results_df' not in st.session_state:
            st.session_state.results_df = pd.DataFrame()
        if 'selected_code' not in st.session_state:
            st.session_state.selected_code = None
        if 'selected_ivmr_series' not in st.session_state:
            st.session_state.selected_ivmr_series = 'ivmr3'
        if 'selected_option_value_type' not in st.session_state:
            st.session_state.selected_option_value_type = 'all'
        if 'selected_chart_type' not in st.session_state:
            st.session_state.selected_chart_type = 'iv_with_regression'
        if 'last_exchange' not in st.session_state:
            st.session_state.last_exchange = None
        if 'last_trade_date' not in st.session_state:
            st.session_state.last_trade_date = None
        if 'public_access_enabled' not in st.session_state:
            st.session_state.public_access_enabled = False
        
        # 自动启动ngrok如果有环境变量
        auth_token = os.getenv('NGROK_AUTH_TOKEN')
        if auth_token and not st.session_state.public_access_enabled:
            url, error = self.public_manager.start_ngrok(auth_token)
            if url:
                st.session_state.public_access_enabled = True
                st.session_state.public_url = url
            else:
                st.warning(f"自动启动ngrok失败: {error}")
    
    def get_underlying_code(self, option_code: str, exchange: str):
        """从期权代码提取期货代码"""
        try:
            mapping = self.exchange_mapping.get(exchange, {})
            if not mapping:
                return None
            
            # 提取品种部分
            variety = re.match(r'([A-Za-z]+)', option_code).group(1).upper()
            underlying = mapping.get(variety)
            if not underlying:
                return None
            
            # 提取到期月份
            month_match = re.search(r'(\d{4})', option_code)
            if not month_match:
                return None
            month = month_match.group(1)
            
            return f"{underlying}{month}"
        except Exception as e:
            self.logger.warning(f"提取期货代码失败 {option_code}: {e}")
            return None
    
    def get_underlying_price(self, option_code: str, exchange: str, date: str, db_manager: DatabaseManager):
        """获取标的期货结算价"""
        underlying_code = self.get_underlying_code(option_code, exchange)
        if not underlying_code:
            self.logger.warning(f"无法提取期货代码 from {option_code}")
            return None
        
        fut_table = f"fut_{exchange}"
        query = f"""
            SELECT "结算价"
            FROM {fut_table}
            WHERE "合约代码" = ?
            AND "交易日期" = ?
        """
        params = (underlying_code, date)
        
        try:
            rows = db_manager.execute_query(query, params)
            if rows:
                return rows[0][0]
            return None
        except Exception as e:
            self.logger.error(f"查询期货价格失败 {underlying_code} {date}: {e}")
            return None
    
    def classify_option_value_type(self, row, underlying_price, atm_call=None, atm_put=None):
        """修改3：基于最接近strike分类期权价值类型"""
        try:
            strike = float(row.get('行权价', np.nan))
            option_type = row.get('期权类型', '').upper()
            underlying = float(underlying_price) if underlying_price else np.nan
            
            if np.isnan(underlying) or np.isnan(strike) or underlying == 0:
                return 'unknown'
            
            # Call
            if 'C' in option_type or 'CALL' in option_type:
                if atm_call is not None and strike == atm_call:
                    return 'atm'
                elif strike > underlying:
                    return 'otm'
                elif strike < underlying:
                    return 'itm'
            
            # Put
            elif 'P' in option_type or 'PUT' in option_type:
                if atm_put is not None and strike == atm_put:
                    return 'atm'
                elif strike < underlying:
                    return 'otm'
                elif strike > underlying:
                    return 'itm'
            return 'unknown'
        except (ValueError, TypeError) as e:
            self.logger.warning(f"分类期权价值类型失败: {e}")
            return 'unknown'


    def screen_options(self, prefix, exchanges, ivmr_series: str = 'ivmr3', option_value_type: str = 'all'):
        if len(exchanges) != 1:
            st.error("当前仅支持单交易所查询，请使用前缀自动识别或只选一个交易所")
            return
        exchange = exchanges[0]
        st.session_state.last_exchange = exchange
        op_table = EXCHANGE_TO_OPTION_TABLE.get(exchange)
        if not op_table:
            st.error(f"未知交易所: {exchange}")
            return

        # 检查表存在
        check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        if not self.db_manager.execute_query(check_query, (op_table,)):
            st.warning(f"表 {op_table} 不存在，跳过 {exchange}")
            return

        all_records = []

        # 循环最近7天
        date_query = """
            SELECT DISTINCT "交易日期"
            FROM ivmr
            ORDER BY "交易日期" DESC
            LIMIT 7
        """
        date_rows = self.db_manager.execute_query(date_query)
        recent_dates = [row[0] for row in date_rows] if date_rows else []

        for date in recent_dates:
            # 查询所有IVMR系列列，并包含行权价和期权类型用于价值类型判断 + vega
            query = f"""
                SELECT 
                    i."期权合约代码",
                    i."交易日期",
                    i."交易所",
                    o."持仓量",
                    o."持仓变化",
                    i.iv AS "iv",
                    i.hv AS "hv",
                    i.ivmr3 AS "ivmr3",
                    i.ivmr7 AS "ivmr7",
                    i.ivmr15 AS "ivmr15",
                    i.ivmr30 AS "ivmr30",
                    i.ivmr90 AS "ivmr90",
                    i.ivmr AS "ivmr",
                    i.vega AS "vega",
                    i."理论价格" AS "理论价格",
                    o."结算价" AS "结算价",
                    (o."结算价" - i."理论价格") AS "价差",
                    o."行权价" AS "行权价",
                    o."期权类型" AS "期权类型"
                FROM ivmr i
                INNER JOIN {op_table} o
                    ON i."期权合约代码" = o."期权合约代码"
                    AND i."交易日期" = o."交易日期"
                WHERE 
                    i."期权合约代码" LIKE ?
                    AND i."交易日期" = ?
                    AND i."交易所" = ?
                    AND i.iv IS NOT NULL
                    AND i."理论价格" IS NOT NULL
                    AND i.{ivmr_series} IS NOT NULL
                ORDER BY "价差" DESC
            """
            params = (f"{prefix}%", date, exchange)

            try:
                df_chunk = self.db_manager.execute_query_to_df(query, params)
                if not df_chunk.empty:
                    # 如果选择了特定的期权价值类型，进行筛选
                    if option_value_type != 'all':
                        try:
                            # 获取标的资产价格
                            sample_code = df_chunk.iloc[0]['期权合约代码']
                            underlying_price = self.get_underlying_price(
                                sample_code, exchange, date, self.db_manager
                            )
                            
                            # 添加 underlying_price 列到 df_chunk
                            df_chunk['underlying_price'] = underlying_price
                            
                            # 分离 Call 和 Put
                            call_mask = df_chunk['期权类型'].str.upper().str.contains('C|CALL')
                            put_mask = df_chunk['期权类型'].str.upper().str.contains('P|PUT')
                            
                            call_df = df_chunk[call_mask]
                            put_df = df_chunk[put_mask]
                            
                            # 找最近的 ATM strike
                            atm_call = None
                            if not call_df.empty:
                                call_strikes = call_df['行权价'].astype(float).unique()
                                call_distances = np.abs(call_strikes - underlying_price)
                                atm_call = call_strikes[np.argmin(call_distances)]
                            
                            atm_put = None
                            if not put_df.empty:
                                put_strikes = put_df['行权价'].astype(float).unique()
                                put_distances = np.abs(put_strikes - underlying_price)
                                atm_put = put_strikes[np.argmin(put_distances)]
                            
                            # 计算每个期权的价值类型
                            df_chunk['价值类型'] = df_chunk.apply(
                                lambda row: self.classify_option_value_type(row, underlying_price, atm_call, atm_put), 
                                axis=1
                            )
                            # 筛选指定的价值类型
                            df_chunk = df_chunk[df_chunk['价值类型'] == option_value_type]
                        except Exception as e:
                            self.logger.warning(f"价值类型判断失败: {e}")
                            # 如果判断失败，保留所有数据
                            df_chunk['价值类型'] = 'unknown'
                    
                    if not df_chunk.empty:
                        all_records.append(df_chunk)
                        self.logger.info(f"{exchange} {date} 查询到 {len(df_chunk)} 条记录")
                else:
                    self.logger.debug(f"{exchange} {date} 无匹配记录")
            except Exception as e:
                st.error(f"{exchange} {date} 查询失败: {str(e)}")
                self.logger.error(f"{exchange} {date} 查询失败: {e}", exc_info=True)

        if not all_records:
            st.warning("⚠️ 最近7天所有日期均无符合条件的记录")
            st.session_state.results_df = pd.DataFrame()
            return

        df = pd.concat(all_records, ignore_index=True)
        df['价差_abs'] = df['价差'].abs()

        # 根据选定的IVMR系列进行筛选（升波/降波逻辑）
        # 升波：IV < HV 且 IVMR > 0
        # 降波：IV > HV 且 IVMR < 0
        abnormal_mask = (
            ((df['iv'] > df['hv']) & (df[ivmr_series] < 0)) |  # 降波
            ((df['iv'] < df['hv']) & (df[ivmr_series] > 0))    # 升波
        )
        abnormal_df = df[abnormal_mask].copy()

        if not abnormal_df.empty:
            result_df = abnormal_df.sort_values('价差', ascending=False)
            self.logger.info(f"找到 {len(result_df)} 条异常记录（升波/降波）")
        else:
            result_df = df.sort_values('价差', ascending=False)
            self.logger.info(f"无异常记录，显示所有 {len(result_df)} 条（iv 非空）")

        st.session_state.results_df = result_df
        st.session_state.selected_code = None
        
        # 更新最近交易日
        if not result_df.empty:
            st.session_state.last_trade_date = result_df['交易日期'].max()

    def highlight_rows(self, row, ivmr_series: str = 'ivmr3'):
        """根据选定的IVMR系列高亮行"""
        try:
            val = float(row.get(ivmr_series, 0))
            iv = float(row.get('iv', 0))
            hv = float(row.get('hv', 0))
            
            # 升波：IV < HV 且 IVMR > 0 (绿色)
            if iv < hv and val > 0:
                return ['background-color: #44ff4420'] * len(row)
            # 降波：IV > HV 且 IVMR < 0 (红色)
            elif iv > hv and val < 0:
                return ['background-color: #ff444420'] * len(row)
        except (ValueError, TypeError):
            pass
        return [''] * len(row)

    def create_ivmr_line_chart(self, db_manager, option_code: str, ivmr_series: str, exchange: str = None, chart_type: str = 'iv_with_regression'):
        """生成IV + IVMR回归直线 + 结算价图表（修改2：删除ivmr_series分支）"""
        # 查询期权数据
        query = """
            SELECT "交易日期", "iv", "hv", "ivmr3", "ivmr7", "ivmr15", "ivmr30", "ivmr90", "ivmr"
            FROM ivmr
            WHERE "期权合约代码" = ?
            ORDER BY "交易日期" DESC
            LIMIT 252  # 最多一年交易日
        """
        params = (option_code,)
        
        df = db_manager.execute_query_to_df(query, params)
        if df.empty:
            return None
        
        # 日期转换
        df['交易日期'] = pd.to_datetime(df['交易日期'], format='%Y%m%d')
        
        # 查询结算价
        op_table = EXCHANGE_TO_OPTION_TABLE.get(exchange)
        if op_table:
            price_query = f"""
                SELECT "交易日期", "结算价"
                FROM {op_table}
                WHERE "期权合约代码" = ?
                ORDER BY "交易日期" DESC
                LIMIT 252
            """
            price_df = db_manager.execute_query_to_df(price_query, params)
            price_df['交易日期'] = pd.to_datetime(price_df['交易日期'], format='%Y%m%d')
        else:
            price_df = None
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        series_display = IVMR_SERIES_OPTIONS.get(ivmr_series, ivmr_series)
        
        # 计算IVMR回归直线
        iv_values = df['iv'].values
        slope, intercept, r_value, p_value, std_err = linregress(range(len(iv_values)), iv_values)
        regression_line = slope * np.arange(len(iv_values)) + intercept
        
        # 绘制IV线
        ax1.plot(df['交易日期'], iv_values, marker='o', label='IV', color='blue', linewidth=1.5)
        
        # 绘制回归直线
        ax1.plot(df['交易日期'], regression_line, linestyle='--', color='red', label='IVMR回归直线')
        
        # 双轴：结算价
        if price_df is not None and not price_df.empty:
            ax2 = ax1.twinx()
            ax2.plot(price_df['交易日期'], price_df['结算价'], marker='s', label='结算价', color='green', linewidth=1.5)
            ax2.set_ylabel('结算价', color='green')
            ax2.tick_params(axis='y', labelcolor='green')
            ax2.legend(loc='upper right')
        
        ax1.set_xlabel('交易日期')
        ax1.set_ylabel('IV 值', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.legend(loc='upper left')
        
        ax1.set_title(f"{option_code} - IV + IVMR回归直线 ({series_display})")
        ax1.grid(True, linestyle='--', alpha=0.7)
        
        # 日期格式化
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        return fig


    def render_public_access_section(self):
        """渲染公网访问配置部分"""
        st.subheader("🌐 公网访问配置")
        
        auth_token = st.text_input(
            "NGROK_AUTH_TOKEN (可选)",
            type="password",
            help="从 https://ngrok.com 获取免费Auth Token"
        )
        
        if st.button("启动公网访问"):
            if not auth_token:
                st.warning("请输入NGROK_AUTH_TOKEN")
            else:
                url, error = self.public_manager.start_ngrok(auth_token)
                if url:
                    st.session_state.public_access_enabled = True
                    st.session_state.public_url = url
                    st.success(f"公网访问地址: {url}")
                    st.info(f"本地网络访问: {self.public_manager.get_local_url()}")
                else:
                    st.error(error)
        
        if st.session_state.public_access_enabled:
            st.success(f"当前公网URL: {st.session_state.public_url}")
            if st.button("停止公网访问"):
                self.public_manager.stop_ngrok()
                st.session_state.public_access_enabled = False
                st.session_state.public_url = None
                st.rerun()

    def run(self):
        st.title("📈 期权IV筛选系统 v2.5")
        
        # 侧边栏配置
        with st.sidebar:
            st.header("⚙️ 筛选条件")
            
            # ========== 合约前缀输入 ==========
            st.subheader("🔤 合约筛选")
            
            if 'prefix_raw' not in st.session_state:
                st.session_state.prefix_raw = ""
            if 'prefix_clean' not in st.session_state:
                st.session_state.prefix_clean = ""
            if 'prefix_valid' not in st.session_state:
                st.session_state.prefix_valid = False
            
            def clean_and_validate_prefix():
                raw = st.session_state.prefix_raw.strip()
                cleaned = re.sub(r'[^A-Za-z0-9]', '', raw).upper()
                st.session_state.prefix_clean = cleaned
                
                length = len(cleaned)
                st.session_state.prefix_valid = 5 <= length <= 6
                
                if length == 0:
                    st.session_state.prefix_message = "请输入合约前缀"
                    st.session_state.prefix_message_type = "info"
                elif length < 5:
                    st.session_state.prefix_message = f"前缀过短 (当前 {length} 位，必须 ≥5 位)"
                    st.session_state.prefix_message_type = "error"
                elif length > 6:
                    st.session_state.prefix_message = f"前缀过长 (当前 {length} 位，必须 ≤6 位)"
                    st.session_state.prefix_message_type = "error"
                else:
                    st.session_state.prefix_message = f"有效前缀：{cleaned} ({length} 位)"
                    st.session_state.prefix_message_type = "success"
            
            st.text_input(
                "合约前缀",
                value=st.session_state.prefix_clean,
                key="prefix_raw",
                placeholder="例如: IO250 或 M2505",
                help="仅允许字母和数字，长度必须为5~6位。输入时自动过滤非法字符，转为大写",
                on_change=clean_and_validate_prefix
            )
            
            # 显示校验结果
            msg = st.session_state.get('prefix_message', "")
            msg_type = st.session_state.get('prefix_message_type', "info")
            if msg_type == "success":
                st.success(msg)
            elif msg_type == "error":
                st.error(msg)
            else:
                st.info(msg)
            
            prefix = st.session_state.prefix_clean
            
            # 自动识别交易所
            inferred_exchange = None
            if prefix:
                for key in sorted(self.exchange_mapping.keys(), key=len, reverse=True):
                    if prefix.startswith(key):
                        inferred_exchange = self.exchange_mapping[key]
                        break
            
            if inferred_exchange:
                st.success(f"自动识别交易所：{inferred_exchange.upper()}")
                selected_exchanges = [inferred_exchange]
                st.session_state.last_exchange = inferred_exchange
            else:
                st.warning("无法自动识别交易所，请手动选择")
                selected_exchanges = st.multiselect(
                    "交易所",
                    options=list(EXCHANGE_TO_OPTION_TABLE.keys()),
                    default=[],
                    key="exchange_select"
                )
                if selected_exchanges:
                    st.session_state.last_exchange = selected_exchanges[0]
            
            st.divider()
            
            # ========== 期权价值类型选择 ==========
            st.subheader("💰 期权价值类型")
            
            selected_value_type = st.radio(
                "选择期权价值类型",
                options=list(OPTION_VALUE_TYPE_OPTIONS.keys()),
                format_func=lambda x: OPTION_VALUE_TYPE_OPTIONS[x],
                index=list(OPTION_VALUE_TYPE_OPTIONS.keys()).index(st.session_state.selected_option_value_type),
                key="option_value_type_select",
                help="根据行权价与标的资产价格的关系筛选期权类型"
            )
            
            st.session_state.selected_option_value_type = selected_value_type
            
            # 显示说明
            with st.expander("价值类型说明"):
                st.markdown("""
                **看涨期权 (CALL)：**
                - 虚值 (OTM)：行权价 > 标的价格
                - 平值 (ATM)：行权价最接近标的价格（各一个）
                - 实值 (ITM)：行权价 < 标的价格
                
                **看跌期权 (PUT)：**
                - 虚值 (OTM)：行权价 < 标的价格
                - 平值 (ATM)：行权价最接近标的价格（各一个）
                - 实值 (ITM)：行权价 > 标的价格
                """)
            
            st.divider()
            
            # ========== IVMR 系列选择 ==========
            st.subheader("📊 IVMR 系列选择")
            
            selected_series = st.radio(
                "选择筛选系列",
                options=list(IVMR_SERIES_OPTIONS.keys()),
                format_func=lambda x: IVMR_SERIES_OPTIONS[x],
                index=list(IVMR_SERIES_OPTIONS.keys()).index(st.session_state.selected_ivmr_series),
                key="ivmr_series_select",
                help="选择用于筛选和图表显示的IVMR动量系列"
            )
            
            st.session_state.selected_ivmr_series = selected_series
            
            st.divider()
            
            # ========== 图表类型选择 ==========
            st.subheader("📈 图表类型选择")
            
            selected_chart = st.radio(
                "选择图表显示模式",
                options=list(CHART_TYPE_OPTIONS.keys()),
                format_func=lambda x: CHART_TYPE_OPTIONS[x],
                index=0,
                key="chart_type_select",
                help="选择图表显示内容：IV + IVMR回归直线 + 结算价"
            )
            
            st.session_state.selected_chart_type = selected_chart
            
            st.divider()
            
            # ========== 操作按钮 ==========
            st.subheader("🚀 执行操作")
            
            execute_disabled = not st.session_state.prefix_valid or not selected_exchanges
            
            if st.button(
                "🔍 执行筛选",
                key="execute_screening",
                disabled=execute_disabled,
                use_container_width=True,
                help="前缀必须为5~6位字母数字组合" if execute_disabled else ""
            ):
                self.screen_options(
                    prefix=prefix,
                    exchanges=selected_exchanges,
                    ivmr_series=st.session_state.selected_ivmr_series,
                    option_value_type=st.session_state.selected_option_value_type
                )
            
            if st.button("🧹 清空结果", key="clear_results", use_container_width=True):
                st.session_state.results_df = pd.DataFrame()
                st.session_state.selected_code = None
                st.rerun()
            
            st.divider()
            
            # ========== 公网访问配置 ==========
            self.render_public_access_section()
        
        # 自动触发筛选逻辑
        if 'last_screen_hash' not in st.session_state:
            st.session_state.last_screen_hash = ""
        
        current_hash = f"{prefix}|{','.join(selected_exchanges)}|{st.session_state.selected_ivmr_series}|{st.session_state.selected_option_value_type}"
        
        should_screen = (
            st.session_state.prefix_valid and
            len(selected_exchanges) == 1 and
            current_hash != st.session_state.last_screen_hash
        )
        
        if should_screen:
            with st.spinner("⚡ 自动筛选中..."):
                self.screen_options(prefix, selected_exchanges, st.session_state.selected_ivmr_series, st.session_state.selected_option_value_type)
            st.session_state.last_screen_hash = current_hash
        
        # 主区域：结果显示
        current_series = st.session_state.selected_ivmr_series
        series_display = IVMR_SERIES_OPTIONS[current_series]
        current_chart_type = st.session_state.selected_chart_type
        chart_display = CHART_TYPE_OPTIONS[current_chart_type]
        current_value_type = st.session_state.selected_option_value_type
        value_type_display = OPTION_VALUE_TYPE_OPTIONS[current_value_type]
        last_date = st.session_state.last_trade_date
        
        # 修改1：标题缩小一号
        st.subheader(f"📊 筛选结果 - {series_display} ({last_date} 最新交易日)")
        
        if current_value_type != 'all':
            st.caption(f"💰 期权价值类型筛选: {value_type_display}")
        
        if not st.session_state.results_df.empty:
            # 只保留最近交易日的数据
            if last_date:
                latest_df = st.session_state.results_df[st.session_state.results_df['交易日期'] == last_date].copy()
            else:
                latest_df = st.session_state.results_df.copy()
            
            if latest_df.empty:
                st.warning(f"⚠️ 最近交易日 ({last_date}) 无符合条件的记录")
            else:
                # 准备显示的数据（去掉辅助列）
                display_df = latest_df.drop(columns=['价差_abs'], errors='ignore').copy()
                
                # 调整列顺序，让价格相关列更显眼（修改4：添加vega）
                priority_cols = ['期权合约代码', '交易日期', '结算价', '理论价格', '价差', 'iv', 'vega', 'hv', current_series, '持仓量', '持仓变化', '交易所']
                if '价值类型' in display_df.columns:
                    priority_cols.insert(1, '价值类型')
                priority_cols = [c for c in priority_cols if c in display_df.columns]
                other_cols = [c for c in display_df.columns if c not in priority_cols]
                display_df = display_df[priority_cols + other_cols]
                
                # 格式化数值显示（修改4：添加vega格式化）
                display_formatted = display_df.copy()
                
                # 格式化价格相关列（4位小数）
                price_cols = ['结算价', '理论价格', '价差']
                for col in price_cols:
                    if col in display_formatted.columns:
                        display_formatted[col] = display_formatted[col].apply(
                            lambda x: f"{x:.4f}" if pd.notna(x) else ""
                        )
                
                # 格式化 IV/HV/vega（2位小数）
                for col in ['iv', 'hv', 'vega']:
                    if col in display_formatted.columns:
                        display_formatted[col] = display_formatted[col].apply(
                            lambda x: f"{x:.2f}" if pd.notna(x) else ""
                        )
                
                # 格式化 IVMR 系列（4位小数）
                if current_series in display_formatted.columns:
                    display_formatted[current_series] = display_formatted[current_series].apply(
                        lambda x: f"{x:.4f}" if pd.notna(x) else ""
                    )
                
                # 创建行选择器
                event = st.dataframe(
                    display_formatted.style.apply(lambda row: self.highlight_rows(row, current_series), axis=1),
                    use_container_width=True,
                    height=400,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="option_selector"
                )
                
                # 处理行选择事件
                selected_rows = event.selection.rows if event.selection else []
                
                if selected_rows:
                    selected_idx = selected_rows[0]
                    selected_code = display_df.iloc[selected_idx]['期权合约代码']
                    st.session_state.selected_code = selected_code
                
                # 统计信息 - 升波/降波/正常
                total = len(latest_df)
                if current_series in latest_df.columns and 'iv' in latest_df.columns and 'hv' in latest_df.columns:
                    rising_wave_mask = (latest_df['iv'] < latest_df['hv']) & (latest_df[current_series] > 0)
                    rising_wave = len(latest_df[rising_wave_mask])
                    
                    falling_wave_mask = (latest_df['iv'] > latest_df['hv']) & (latest_df[current_series] < 0)
                    falling_wave = len(latest_df[falling_wave_mask])
                    
                    normal = total - rising_wave - falling_wave
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("总计", total)
                    with col2:
                        st.metric("📈 升波", rising_wave, delta=f"{rising_wave/total*100:.1f}%" if total > 0 else "0%")
                    with col3:
                        st.metric("📉 降波", falling_wave, delta=f"{falling_wave/total*100:.1f}%" if total > 0 else "0%")
                    with col4:
                        st.metric("➖ 正常", normal, delta=f"{normal/total*100:.1f}%" if total > 0 else "0%")
                    
                    # 详细说明
                    with st.expander("统计说明"):
                        st.markdown(f"""
                        **升波 (IV < HV 且 {current_series} > 0)：** {rising_wave} 条
                        - 隐含波动率低于历史波动率，且IVMR趋势向上
                        - 可能预示波动率即将上升
                        
                        **降波 (IV > HV 且 {current_series} < 0)：** {falling_wave} 条
                        - 隐含波动率高于历史波动率，且IVMR趋势向下
                        - 可能预示波动率即将下降
                        
                        **正常 (其他情况)：** {normal} 条
                        - 不符合上述两种极端情况
                        """)
                else:
                    st.info(f"📊 统计: 总计 {total} 条")
                
                # 提示用户如何操作
                st.caption(f"💡 提示：点击表格中的任意一行，即可查看该合约的 {chart_display} 图表（含结算价）")
                
                # 导出按钮
                csv_data = display_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 导出 CSV",
                    data=csv_data,
                    file_name=f"option_screener_{current_series}_{last_date}_{datetime.now().strftime('%H%M%S')}.csv",
                    mime="text/csv",
                    key="export_csv"
                )
        else:
            st.info(f"🕒 等待筛选... | 提示: IV>HV且{series_display}<0=降波 | IV<HV且{series_display}>0=升波")
        
        # 图表显示部分
        selected_code = st.session_state.selected_code
        if selected_code:
            st.header(f"📈 {selected_code} 图表 - {chart_display}")
            
            fig = self.create_ivmr_line_chart(
                self.db_manager,
                selected_code,
                st.session_state.selected_ivmr_series,
                st.session_state.last_exchange,
                st.session_state.selected_chart_type
            )
            
            if fig:
                st.pyplot(fig)
            else:
                st.warning("⚠️ 无可用图表数据")

# 运行应用
if __name__ == "__main__":
    app = OptionScreenerApp()
    app.run()
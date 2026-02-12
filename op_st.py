"""使用streamlit构建web交互界面支持公网访问（通过ngrok隧道）
运行命令：streamlit run op_st.py

公网访问方式：
1. 自动模式：启动时自动创建ngrok隧道（需配置NGROK_AUTH_TOKEN环境变量）
2. 手动模式：侧边栏点击"启动公网访问"按钮
"""

import csv
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats
import streamlit as st

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
    from src.utils.config_loader import get_db_path, get_exchange_mapping, get_log_dir
    from src.utils.database import DatabaseManager
    from src.utils.logging_config import get_logger

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

# 图表类型配置
CHART_TYPE_OPTIONS = {
    'ivmr_series': 'IVMR系列值 + 结算价',
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
st.set_page_config(
    page_title="📈 期权IV筛选系统 v2.5",
    layout="wide",
    initial_sidebar_state='expanded'
)


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
            result = subprocess.run(
                ['ngrok', 'version'],
                capture_output=True,
                text=True
            )
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
            elif system == "linux":  # 尝试使用snap或下载二进制
                try:
                    subprocess.run(['snap', 'install', 'ngrok'], check=True)
                    return True
                except:
                    # 下载并安装
                    import urllib.request
                    ngrok_url = "https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-amd64.tgz"
                    urllib.request.urlretrieve(ngrok_url, "/tmp/ngrok.tgz")
                    subprocess.run(
                        ['tar', '-xzf', '/tmp/ngrok.tgz', '-C', '/usr/local/bin/'],
                        check=True
                    )
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
                subprocess.run(
                    ['ngrok', 'config', 'add-authtoken', auth_token],
                    capture_output=True,
                    check=True
                )
            except Exception as e:
                return None, f"配置ngrok auth token失败: {e}"

        # 检查是否已有ngrok在运行
        try:
            result = subprocess.run(
                ['curl', '-s', 'http://localhost:4040/api/tunnels'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout:
                data = json.loads(result.stdout)
                if data.get('tunnels'):
                    self.public_url = data['tunnels'][0]['public_url']
                    return self.public_url, "已连接到现有ngrok隧道"
        except:
            pass

        # 启动新的ngrok进程
        try:
            self.ngrok_process = subprocess.Popen(
                ['ngrok', 'http', str(self.streamlit_port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # 等待ngrok启动并获取URL
            time.sleep(3)

            # 获取公网URL
            for _ in range(10):  # 最多尝试10次
                try:
                    result = subprocess.run(
                        ['curl', '-s', 'http://localhost:4040/api/tunnels'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0 and result.stdout:
                        data = json.loads(result.stdout)
                        if data.get('tunnels'):
                            self.public_url = data['tunnels'][0]['public_url']
                            return self.public_url, "ngrok隧道启动成功"
                except:
                    pass
                time.sleep(1)

            return None, "ngrok启动超时，请检查配置"
        except Exception as e:
            return None, f"启动ngrok失败: {e}"

    def stop_ngrok(self):
        """停止ngrok隧道"""
        if self.ngrok_process:
            self.ngrok_process.terminate()
            self.ngrok_process = None
            self.public_url = None
            return True
        return False

    def get_network_info(self):
        """获取网络信息摘要"""
        info = {
            'local_ip': self.local_ip,
            'local_url': self.get_local_url(),
            'port': self.streamlit_port,
            'ngrok_installed': self.check_ngrok_installed(),
            'public_url': self.public_url
        }
        return info


# 初始化公网访问管理器
if 'public_access' not in st.session_state:
    st.session_state.public_access = PublicAccessManager()


def calculate_ivmr_regression_line(iv_series: np.ndarray) -> tuple:
    """
    计算IV序列的IVMR回归直线
    
    Returns:
        tuple: (slope, intercept, regression_values)
    """
    if len(iv_series) < 2:
        return 0, iv_series[0] if len(iv_series) > 0 else 0, iv_series

    t = np.arange(len(iv_series))
    slope, intercept, _, _, _ = stats.linregress(t, iv_series)
    regression_line = slope * t + intercept
    return slope, intercept, regression_line


def create_ivmr_line_chart(
    db_manager,
    option_code: str,
    ivmr_series: str,
    exchange: str = None,
    chart_type: str = 'ivmr_series'
):
    """
    查询指定期权合约的IVMR数据，生成双轴折线图
    
    Args:
        db_manager: 数据库管理器实例
        option_code: 期权合约代码
        ivmr_series: IVMR系列名称 (ivmr3, ivmr7, ivmr15, ivmr30, ivmr90, ivmr)
        exchange: 交易所代码（可选，用于查询结算价）
        chart_type: 图表类型 ('ivmr_series' 或 'iv_with_regression')
    
    Returns:
        matplotlib.figure.Figure: 折线图对象
    """
    # 获取该系列需要的交易日数量
    days_needed = IVMR_DAYS_MAP.get(ivmr_series, 30)

    # 查询该合约最近的交易日（从ivmr表中获取）
    date_query = """
    SELECT DISTINCT "交易日期" 
    FROM ivmr 
    WHERE "期权合约代码" = ? 
    ORDER BY "交易日期" DESC 
    LIMIT ?
    """
    date_rows = db_manager.execute_query(date_query, (option_code, days_needed))

    if not date_rows:
        return None

    # 转换为日期列表（升序排列用于图表）
    recent_dates = [row[0] for row in date_rows][::-1]
    if len(recent_dates) < 2:
        return None

    # 查询该合约最近日期的IVMR数据 - 查询所有IVMR系列列
    placeholders = ','.join(['?' for _ in recent_dates])
    query = f"""
    SELECT "交易日期", "期权合约代码", ivmr3, ivmr7, ivmr15, ivmr30, ivmr90, ivmr, iv, hv
    FROM ivmr 
    WHERE "期权合约代码" = ? AND "交易日期" IN ({placeholders}) 
        AND {ivmr_series} IS NOT NULL 
    ORDER BY "交易日期" ASC
    """
    params = (option_code,) + tuple(recent_dates)
    df = db_manager.execute_query_to_df(query, params)

    if df.empty:
        return None

    # 转换日期格式用于matplotlib
    df['交易日期'] = pd.to_datetime(df['交易日期'])

    # 尝试查询结算价（如果提供了交易所）
    price_df = None
    if exchange:
        op_table = EXCHANGE_TO_OPTION_TABLE.get(exchange)
        if op_table:
            try:
                price_query = f"""
                SELECT o."交易日期", o."结算价" 
                FROM {op_table} o 
                WHERE o."期权合约代码" = ? AND o."交易日期" IN ({placeholders}) 
                    AND o."结算价" IS NOT NULL 
                ORDER BY o."交易日期" ASC
                """
                price_params = (option_code,) + tuple(recent_dates)
                price_df = db_manager.execute_query_to_df(price_query, price_params)

                if not price_df.empty:
                    price_df['交易日期'] = pd.to_datetime(price_df['交易日期'])
            except Exception:
                pass  # 结算价查询失败不影响主图表

    # 获取系列显示名称
    series_display = IVMR_SERIES_OPTIONS.get(ivmr_series, ivmr_series)

    # 根据数据量动态调整图表大小
    n_points = len(df)
    if n_points <= 7:
        fig_width = 10
    elif n_points <= 30:
        fig_width = 12
    else:
        fig_width = min(16, 8 + n_points * 0.15)  # 随点数增加宽度，但最大16

    # 创建双轴图表
    fig, ax1 = plt.subplots(figsize=(fig_width, 6), dpi=100)

    if chart_type == 'iv_with_regression':
        # ========== 新模式：IV + IVMR回归直线 + 结算价 ==========
        # 计算IVMR回归直线
        iv_values = df['iv'].values
        slope, intercept, regression_line = calculate_ivmr_regression_line(iv_values)

        # 左轴：IV实际值（散点+连线）
        color_iv = '#2E86AB'
        line1 = ax1.plot(
            df['交易日期'],
            df['iv'],
            marker='o',
            linewidth=2,
            markersize=5 if n_points > 30 else 7,
            color=color_iv,
            alpha=0.7,
            label='IV (隐含波动率)',
            zorder=3
        )

        # 左轴：IVMR回归直线（粗线）
        color_reg = '#FF6B35'
        line2 = ax1.plot(
            df['交易日期'],
            regression_line,
            linewidth=3,
            color=color_reg,
            linestyle='--',
            label=f'IVMR回归线 (slope={slope:.6f})',
            zorder=4
        )

        # 填充IV与回归线之间的区域（显示偏离程度）
        ax1.fill_between(
            df['交易日期'],
            df['iv'],
            regression_line,
            alpha=0.2,
            color='purple',
            label='IV偏离度',
            zorder=1
        )

        # 添加IV数值标签（数据点少时显示）
        if n_points <= 10:
            for i, row in df.iterrows():
                ax1.annotate(
                    f'{row["iv"]:.2f}',
                    (row['交易日期'], row['iv']),
                    textcoords="offset points",
                    xytext=(0, 8),
                    ha='center',
                    fontsize=8,
                    color=color_iv,
                    fontweight='bold'
                )

        # 设置左轴标签
        ax1.set_ylabel('隐含波动率 (IV)', color=color_iv, fontsize=11, fontweight='bold')
        ax1.tick_params(axis='y', labelcolor=color_iv)

        # 设置标题
        actual_days = len(df)
        ax1.set_title(
            f'{option_code} - IV与IVMR回归分析 ({actual_days}个交易日)\n回归斜率: {slope:.6f}',
            fontsize=13,
            fontweight='bold',
            pad=15
        )

        # 合并图例项（IV + 回归线）
        lines = line1 + line2
        labels = ['IV (隐含波动率)', f'IVMR回归线 (slope={slope:.6f})', 'IV偏离度']
    else:
        # ========== 原模式：IVMR系列值 + 结算价 ==========
        # 左轴：IVMR 数据
        color_ivmr = '#2E86AB'
        line1 = ax1.plot(
            df['交易日期'],
            df[ivmr_series],
            marker='o',
            linewidth=2.5,
            markersize=6 if n_points > 30 else 8,
            color=color_ivmr,
            label=series_display,
            zorder=3
        )

        # 填充区域（IVMR与零线之间）
        ax1.fill_between(
            df['交易日期'],
            0,
            df[ivmr_series],
            where=(df[ivmr_series] >= 0),
            alpha=0.2,
            color='green',
            label='IVMR>0',
            zorder=1
        )
        ax1.fill_between(
            df['交易日期'],
            0,
            df[ivmr_series],
            where=(df[ivmr_series] < 0),
            alpha=0.2,
            color='red',
            label='IVMR<0',
            zorder=1
        )

        # 添加 IVMR 数值标签（数据点少时显示）
        if n_points <= 15:
            for i, row in df.iterrows():
                ax1.annotate(
                    f'{row[ivmr_series]:.4f}',
                    (row['交易日期'], row[ivmr_series]),
                    textcoords="offset points",
                    xytext=(0, 10),
                    ha='center',
                    fontsize=8,
                    color=color_ivmr,
                    fontweight='bold'
                )

        # 设置左轴标签
        ax1.set_ylabel(series_display, color=color_ivmr, fontsize=11, fontweight='bold')
        ax1.tick_params(axis='y', labelcolor=color_ivmr)

        # 设置标题
        actual_days = len(df)
        ax1.set_title(
            f'{option_code} - {series_display} 与结算价趋势 ({actual_days}个交易日)',
            fontsize=13,
            fontweight='bold',
            pad=15
        )

        # 添加零线参考
        ax1.axhline(
            y=0,
            color='red',
            linestyle='--',
            linewidth=1.5,
            alpha=0.7,
            label='零线',
            zorder=2
        )

        # 合并图例项
        lines = line1
        labels = [series_display, 'IVMR>0', 'IVMR<0', '零线']

    ax1.set_xlabel('交易日期', fontsize=11)

    # 右轴：结算价（如果查询成功）
    if price_df is not None and not price_df.empty:
        ax2 = ax1.twinx()
        color_price = '#E94F37'
        line_price = ax2.plot(
            price_df['交易日期'],
            price_df['结算价'],
            marker='s',
            linewidth=2,
            markersize=5 if n_points > 30 else 7,
            color=color_price,
            alpha=0.85,
            label='结算价',
            linestyle='-.',  # 点划线
            zorder=5
        )

        # 添加结算价数值标签（数据点少时显示）
        if len(price_df) <= 10:
            for i, row in price_df.iterrows():
                ax2.annotate(
                    f'{row["结算价"]:.2f}',
                    (row['交易日期'], row['结算价']),
                    textcoords="offset points",
                    xytext=(0, -15),
                    ha='center',
                    fontsize=8,
                    color=color_price
                )

        ax2.set_ylabel('结算价', color=color_price, fontsize=11, fontweight='bold')
        ax2.tick_params(axis='y', labelcolor=color_price)

        # 添加到图例
        lines = lines + line_price
        labels = labels + ['结算价']

    # 设置图例
    ax1.legend(lines, labels, loc='upper left', framealpha=0.9, fontsize=9)

    # 设置网格
    ax1.grid(True, linestyle='--', alpha=0.3, zorder=0)

    # ========== 横轴日期美化 ==========
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

    if n_points <= 7:  # 少数据：显示所有日期，旋转30度
        ax1.xaxis.set_major_locator(mdates.DayLocator())
        plt.xticks(rotation=30, ha='right')
    elif n_points <= 15:  # 中数据：每隔1-2天显示
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        plt.xticks(rotation=45, ha='right')
    elif n_points <= 30:  # 月数据：每周一显示
        ax1.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MONDAY))
        plt.xticks(rotation=45, ha='right')
    elif n_points <= 90:  # 季度数据：每周一显示，格式简化
        ax1.xaxis.set_major_locator(mdates.WeekdayLocator(
            byweekday=mdates.MONDAY,
            interval=2
        ))
        plt.xticks(rotation=45, ha='right')
    else:  # 年数据：每月1日显示
        ax1.xaxis.set_major_locator(mdates.MonthLocator())
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.xticks(rotation=45, ha='right')

    # 自动调整布局
    plt.tight_layout()
    return fig


# 主应用类
class OptionScreenerApp:
    def __init__(self):
        self.exchange_mapping = get_exchange_mapping()
        self.logger = get_logger("option_screener")
        self.logger.info("=" * 50)
        self.logger.info("期权波动率筛选系统启动")
        self.logger.info(f"项目根目录: {project_root}")
        self.logger.info(f"数据库路径: {get_db_path()}")

        # 初始化数据库
        try:
            self.db_path = get_db_path()
            self.db_manager = DatabaseManager(self.db_path, self.logger)
            self.logger.info("数据库连接成功")
        except Exception as e:
            error_msg = f"数据库初始化失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            st.error(f"{error_msg}\n\n请检查数据库配置")
            sys.exit(1)

        # 初始化配置
        self.config = {
            'ivmr3_upper': 10.0,
            'ivmr3_lower': -10.0
        }

        # Streamlit 状态初始化
        if 'results_df' not in st.session_state:
            st.session_state.results_df = pd.DataFrame()
        if 'selected_code' not in st.session_state:
            st.session_state.selected_code = None
        if 'selected_ivmr_series' not in st.session_state:
            st.session_state.selected_ivmr_series = 'ivmr3'
        if 'selected_chart_type' not in st.session_state:
            st.session_state.selected_chart_type = 'ivmr_series'
        if 'selected_option_value_type' not in st.session_state:
            st.session_state.selected_option_value_type = 'all'
        if 'last_exchange' not in st.session_state:
            st.session_state.last_exchange = None
        if 'last_trade_date' not in st.session_state:
            st.session_state.last_trade_date = None
        if 'public_access' not in st.session_state:
            st.session_state.public_access = PublicAccessManager()
        if 'ngrok_started' not in st.session_state:
            st.session_state.ngrok_started = False

        self.load_last_trade_date()

        # 尝试自动启动ngrok（如果配置了token）
        self._try_auto_start_ngrok()

    def _try_auto_start_ngrok(self):
        """尝试自动启动ngrok（如果配置了环境变量）"""
        if st.session_state.ngrok_started:
            return

        auth_token = os.getenv('NGROK_AUTH_TOKEN')
        if auth_token:
            try:
                public_url, message = st.session_state.public_access.start_ngrok(auth_token)
                if public_url:
                    st.session_state.ngrok_started = True
                    st.session_state.public_url = public_url
                    self.logger.info(f"自动启动ngrok成功: {public_url}")
            except Exception as e:
                self.logger.warning(f"自动启动ngrok失败: {e}")

    def load_last_trade_date(self):
        """加载最近交易日"""
        @st.cache_data(ttl=3600)
        def _fetch():
            query = """
            SELECT DISTINCT "交易日期" 
            FROM ivmr 
            ORDER BY "交易日期" DESC 
            LIMIT 1
            """
            rows = self.db_manager.execute_query(query)
            return rows[0][0] if rows else None

        last_date = _fetch()
        st.session_state.last_trade_date = last_date
        return last_date

    def render_public_access_section(self):
        """渲染公网访问配置区域"""
        st.subheader("🌐 公网访问")

        # 显示本地网络信息
        network_info = st.session_state.public_access.get_network_info()
        st.info(f"""
        **本地访问地址：**
        - 本机：http://localhost:{network_info['port']}
        - 局域网：{network_info['local_url']}
        """)

        # 显示公网访问状态
        if st.session_state.ngrok_started and st.session_state.get('public_url'):
            st.success(f"""
            **✅ 公网访问已启用**
            公网地址：{st.session_state.public_url}
            """)
            if st.button("🛑 停止公网访问", key="stop_ngrok", use_container_width=True):
                st.session_state.public_access.stop_ngrok()
                st.session_state.ngrok_started = False
                st.session_state.pop('public_url', None)
                st.rerun()
        else:
            # 手动启动ngrok
            with st.expander("配置公网访问 (ngrok)"):
                st.markdown("""
                **使用说明：**
                1. 访问 https://ngrok.com 注册账号
                2. 获取 Authtoken（免费版即可）
                3. 输入token并点击启动
                """)

                auth_token = st.text_input(
                    "ngrok Authtoken",
                    type="password",
                    placeholder="输入你的ngrok authtoken",
                    help="从 https://dashboard.ngrok.com/get-started/your-authtoken 获取"
                )

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🚀 启动公网访问", key="start_ngrok", use_container_width=True):
                        if not auth_token:
                            st.error("请输入ngrok authtoken")
                        else:
                            with st.spinner("正在启动ngrok隧道..."):
                                public_url, message = st.session_state.public_access.start_ngrok(auth_token)
                                if public_url:
                                    st.session_state.ngrok_started = True
                                    st.session_state.public_url = public_url
                                    st.success(f"启动成功！公网地址：{public_url}")
                                    time.sleep(2)
                                    st.rerun()
                                else:
                                    st.error(f"启动失败：{message}")
                with col2:
                    if st.button("📋 复制本地地址", key="copy_local", use_container_width=True):
                        st.code(network_info['local_url'], language=None)
                        st.success("地址已显示，请手动复制")

    def get_underlying_price(self, option_code: str, exchange: str, date_str: str, db_manager) -> float:
        """
        获取标的资产价格
        
        商品期货：从 fu_* 表的'结算价'字段获取
        股指期权：从 stock 表的'收盘价'字段获取
        """
        # 股指期权判断（IO, HO, MO 开头）
        index_mapping = {'HO': '沪深300', 'IO': '上证50', 'MO': '中证1000'}
        prefix = option_code[:2] if len(option_code) >= 2 else ''

        if prefix in index_mapping:  # 股指期权
            index_name = index_mapping[prefix]
            query = 'SELECT "收盘价" FROM stock WHERE "指数名称" = ? AND "交易日期" = ?'
            result = db_manager.execute_query(query, (index_name, date_str))
            if not result:
                raise ValueError(f"{date_str} 无 {index_name} 数据")
            return float(result[0][0])
        else:  # 商品期货期权
            # 需要从合约代码提取期货合约代码
            # 假设期权合约代码格式如：m2505C3000 -> 期货合约 m2505
            match = re.match(r'^([a-zA-Z]+\d+)', option_code)
            if not match:
                raise ValueError(f"无法从期权代码 {option_code} 提取期货合约代码")
            
            future_contract = match.group(1).upper()
            fu_table = f"fu_{exchange}"
            
            query = f'SELECT "结算价" FROM {fu_table} WHERE "期货合约" = ? AND "交易日期" = ?'
            result = db_manager.execute_query(query, (future_contract, date_str))
            if not result:
                raise ValueError(f"{date_str} 无 {future_contract} 期货价格")
            return float(result[0][0])

    def classify_option_value_type(self, row, underlying_price: float, atm_threshold_pct: float = 0.01):
        """
        根据行权价判断期权价值类型
        
        规则（行业标准）：
        - 平值(ATM)：行权价与标的价格的差距在 ±1% 以内
        - 看涨期权(CALL)：行权价 > 标的价格(1%以上) = 虚值(OTM)，行权价 < 标的价格(1%以上) = 实值(ITM)
        - 看跌期权(PUT)：行权价 < 标的价格(1%以上) = 虚值(OTM)，行权价 > 标的价格(1%以上) = 实值(ITM)
        
        Args:
            row: DataFrame 行数据
            underlying_price: 标的资产价格
            atm_threshold_pct: 平值期权阈值百分比（默认1%）
        
        Returns:
            'otm', 'atm', 'itm'
        """
        strike = row.get('行权价')
        option_type = row.get('期权类型', '').upper()

        if pd.isna(strike) or not option_type:
            return 'unknown'

        # 确保 strike 是数值
        try:
            strike_float = float(strike)
        except (ValueError, TypeError):
            return 'unknown'

        if pd.isna(underlying_price):
            return 'unknown'

        # 确保 underlying_price 是数值
        try:
            underlying_float = float(underlying_price)
        except (ValueError, TypeError):
            return 'unknown'

        # 计算行权价与标的价格的差距百分比
        price_diff_pct = abs(strike_float - underlying_float) / underlying_float

        # 平值判断：差距在1%以内
        if price_diff_pct <= atm_threshold_pct:
            return 'atm'

        # 判断是否是看涨期权
        is_call = 'C' in option_type or 'CALL' in option_type
        is_put = 'P' in option_type or 'PUT' in option_type

        if is_call:
            if strike_float > underlying_float:
                return 'otm'  # 虚值：行权价 > 标的价格
            else:
                return 'itm'  # 实值：行权价 < 标的价格
        elif is_put:
            if strike_float < underlying_float:
                return 'otm'  # 虚值：行权价 < 标的价格
            else:
                return 'itm'  # 实值：行权价 > 标的价格

        # 如果无法判断类型，根据行权价和标的价格的关系判断（默认按看涨期权逻辑）
        if strike_float > underlying_float:
            return 'otm'
        else:
            return 'itm'

    def run(self):
        st.title("期权IV筛选系统 v2.5")

        # 侧边栏配置
        with st.sidebar:
            st.header("⚙️ 筛选条件")

            # ========== 合约前缀输入（移到第一位）==========
            st.subheader("🔤 合约筛选")

            # 前缀输入 + 实时清洗 + 严格校验
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
                index=list(OPTION_VALUE_TYPE_OPTIONS.keys()).index(
                    st.session_state.get('selected_option_value_type', 'all')
                ),
                key="option_value_type_select",
                help="根据行权价与标的资产价格的关系筛选期权类型"
            )
            st.session_state.selected_option_value_type = selected_value_type  # 统一存入 session_state

            st.divider()

            # ========== IVMR 系列选择 ==========
            st.subheader("📊 IVMR 系列选择")
            selected_series = st.radio(
                "选择筛选系列",
                options=list(IVMR_SERIES_OPTIONS.keys()),
                format_func=lambda x: IVMR_SERIES_OPTIONS[x],
                index=list(IVMR_SERIES_OPTIONS.keys()).index(
                    st.session_state.get('selected_ivmr_series', 'ivmr3')
                ),
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
                index=list(CHART_TYPE_OPTIONS.keys()).index(
                    st.session_state.get('selected_chart_type', 'ivmr_series')
                ),
                key="chart_type_select",
                help="选择图表显示内容：IVMR系列值 或 IV+回归直线"
            )
            st.session_state.selected_chart_type = selected_chart

            st.divider()

            # ========== 操作按钮（保留清空功能，执行筛选可以去掉或保留作为手动触发） ==========
            st.subheader("🚀 执行操作")
            execute_disabled = not st.session_state.prefix_valid or not selected_exchanges
            
            # 可以选择保留一个手动"刷新"按钮，作为兜底
            if st.button(
                "🔄 手动刷新结果",
                key="manual_refresh",
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

        # ────────────────────────────────────────────────────────────────
        # 【核心改动】自动触发筛选逻辑 —— 放在侧边栏之后、主区域之前
        # ────────────────────────────────────────────────────────────────

        # 初始化上次筛选参数（第一次运行时为空）
        if 'last_screen_hash' not in st.session_state:
            st.session_state.last_screen_hash = ""

        # 当前所有关键筛选条件的"指纹"
        current_hash = (
            f"{st.session_state.prefix_clean}|"
            f"{tuple(st.session_state.get('exchange_select', []))}|"
            f"{st.session_state.get('selected_ivmr_series', 'ivmr3')}|"
            f"{st.session_state.get('selected_option_value_type', 'all')}"
        )

        # 判断是否需要重新执行筛选
        should_screen = (
            st.session_state.prefix_valid and  # 前缀有效
            len(st.session_state.get('exchange_select', [])) == 1 and  # 单交易所
            current_hash != st.session_state.last_screen_hash  # 参数有变化
        )

        if should_screen:
            with st.spinner("正在自动筛选最新条件..."):
                self.screen_options(
                    prefix=st.session_state.prefix_clean,
                    exchanges=st.session_state.get(
                        'exchange_select',
                        [st.session_state.last_exchange]
                    ),
                    ivmr_series=st.session_state.selected_ivmr_series,
                    option_value_type=st.session_state.selected_option_value_type
                )
            # 更新指纹，避免重复执行
            st.session_state.last_screen_hash = current_hash

        # 主区域：结果显示
        current_series = st.session_state.selected_ivmr_series
        series_display = IVMR_SERIES_OPTIONS[current_series]
        current_chart_type = st.session_state.selected_chart_type
        chart_display = CHART_TYPE_OPTIONS[current_chart_type]
        current_value_type = st.session_state.selected_option_value_type
        value_type_display = OPTION_VALUE_TYPE_OPTIONS[current_value_type]
        last_date = st.session_state.last_trade_date

        # 主区域：只显示最近交易日的记录
        st.header(f"📊 筛选结果 - {series_display} ({last_date} 最新交易日)")
        if current_value_type != 'all':
            st.caption(f"💰 期权价值类型筛选: {value_type_display}")

        if not st.session_state.results_df.empty:
            # 只保留最近交易日的数据
            if last_date:
                latest_df = st.session_state.results_df[
                    st.session_state.results_df['交易日期'] == last_date
                ].copy()
            else:
                latest_df = st.session_state.results_df.copy()

            if latest_df.empty:
                st.warning(f"⚠️ 最近交易日 ({last_date}) 无符合条件的记录")
            else:
                # 准备显示的数据（去掉辅助列）
                display_df = latest_df.drop(columns=['价差_abs'], errors='ignore').copy()

                # 调整列顺序，让价格相关列更显眼
                priority_cols = [
                    '期权合约代码',
                    '交易日期',
                    '结算价',
                    '理论价格',
                    '价差',
                    'iv',
                    'hv',
                    current_series,
                    '持仓量',
                    '持仓变化',
                    '交易所'
                ]
                
                # 如果有价值类型列，也加入优先级
                if '价值类型' in display_df.columns:
                    priority_cols.insert(1, '价值类型')
                
                priority_cols = [c for c in priority_cols if c in display_df.columns]
                other_cols = [c for c in display_df.columns if c not in priority_cols]
                display_df = display_df[priority_cols + other_cols]

                # 格式化数值显示
                display_formatted = display_df.copy()

                # 格式化价格相关列（4位小数）
                price_cols = ['结算价', '理论价格', '价差']
                for col in price_cols:
                    if col in display_formatted.columns:
                        display_formatted[col] = display_formatted[col].apply(
                            lambda x: f"{x:.4f}" if pd.notna(x) else ""
                        )

                # 格式化 IV/HV（2位小数）
                for col in ['iv', 'hv']:
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
                    display_formatted.style.apply(
                        lambda row: self.highlight_rows(row, current_series),
                        axis=1
                    ),
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

                # ========== 统计信息 - 升波/降波/正常 ==========
                total = len(latest_df)
                if current_series in latest_df.columns and 'iv' in latest_df.columns and 'hv' in latest_df.columns:
                    # 升波：IV < HV 且 IVMR > 0
                    rising_wave_mask = (latest_df['iv'] < latest_df['hv']) & (latest_df[current_series] > 0)
                    rising_wave = len(latest_df[rising_wave_mask])

                    # 降波：IV > HV 且 IVMR < 0
                    falling_wave_mask = (latest_df['iv'] > latest_df['hv']) & (latest_df[current_series] < 0)
                    falling_wave = len(latest_df[falling_wave_mask])

                    # 正常：其他情况
                    normal = total - rising_wave - falling_wave

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("总计", total)
                    with col2:
                        st.metric(
                            "📈 升波",
                            rising_wave,
                            delta=f"{rising_wave/total*100:.1f}%" if total > 0 else "0%"
                        )
                    with col3:
                        st.metric(
                            "📉 降波",
                            falling_wave,
                            delta=f"{falling_wave/total*100:.1f}%" if total > 0 else "0%"
                        )
                    with col4:
                        st.metric(
                            "➖ 正常",
                            normal,
                            delta=f"{normal/total*100:.1f}%" if total > 0 else "0%"
                        )

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
                st.caption(
                    f"💡 提示：点击表格中的任意一行，即可查看该合约的 {chart_display} 图表（含结算价）"
                )

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

        # IV 图显示区域
        selected_code = st.session_state.get('selected_code')
        if selected_code:
            st.divider()
            st.subheader(f"📈 {chart_display} - {selected_code}")

            with st.spinner("生成图表中..."):
                try:
                    # 获取交易所信息用于查询结算价
                    exchange = st.session_state.get('last_exchange')
                    fig = create_ivmr_line_chart(
                        db_manager=self.db_manager,
                        option_code=selected_code,
                        ivmr_series=current_series,
                        exchange=exchange,
                        chart_type=current_chart_type
                    )

                    if fig:
                        st.pyplot(fig)

                        # 显示数据表格（展开式）
                        with st.expander("查看原始数据"):
                            # 查询详细数据（根据系列确定天数）
                            days_needed = IVMR_DAYS_MAP.get(current_series, 30)
                            date_query = """
                            SELECT DISTINCT "交易日期" 
                            FROM ivmr 
                            WHERE "期权合约代码" = ? 
                            ORDER BY "交易日期" DESC 
                            LIMIT ?
                            """
                            date_rows = self.db_manager.execute_query(date_query, (selected_code, days_needed))
                            recent_dates = [row[0] for row in date_rows] if date_rows else []

                            if recent_dates:
                                placeholders = ','.join(['?' for _ in recent_dates])

                                # IVMR 数据 - 查询所有列
                                ivmr_query = f"""
                                SELECT "交易日期", "期权合约代码", 
                                    ivmr3, ivmr7, ivmr15, ivmr30, ivmr90, ivmr, 
                                    iv AS "IV", hv AS "HV"
                                FROM ivmr 
                                WHERE "期权合约代码" = ? AND "交易日期" IN ({placeholders}) 
                                    AND {current_series} IS NOT NULL 
                                ORDER BY "交易日期" ASC
                                """
                                params = (selected_code,) + tuple(recent_dates)
                                data_df = self.db_manager.execute_query_to_df(ivmr_query, params)

                                # 结算价数据
                                if exchange:
                                    op_table = EXCHANGE_TO_OPTION_TABLE.get(exchange)
                                    if op_table:
                                        price_query = f"""
                                        SELECT "交易日期", "结算价", "理论价格", 
                                            ("结算价" - "理论价格") AS "价差"
                                        FROM {op_table} 
                                        WHERE "期权合约代码" = ? AND "交易日期" IN ({placeholders})
                                        """
                                        price_df = self.db_manager.execute_query_to_df(price_query, params)
                                        if not price_df.empty:
                                            data_df = data_df.merge(price_df, on='交易日期', how='left')

                                # 如果是回归模式，添加回归线数据
                                if current_chart_type == 'iv_with_regression' and not data_df.empty:
                                    iv_values = data_df['IV'].values
                                    _, _, regression_line = calculate_ivmr_regression_line(iv_values)
                                    data_df['IV回归线'] = regression_line

                                # 格式化显示
                                if not data_df.empty:
                                    col_order = [
                                        '交易日期',
                                        '期权合约代码',
                                        '结算价',
                                        '理论价格',
                                        '价差',
                                        'IV',
                                        'HV',
                                        current_series
                                    ]
                                    
                                    if 'IV回归线' in data_df.columns:
                                        col_order.insert(6, 'IV回归线')
                                    
                                    col_order = [c for c in col_order if c in data_df.columns]
                                    data_df = data_df[col_order]

                                    format_dict = {}
                                    if '结算价' in data_df.columns:
                                        format_dict['结算价'] = '{:.4f}'
                                    if '理论价格' in data_df.columns:
                                        format_dict['理论价格'] = '{:.4f}'
                                    if '价差' in data_df.columns:
                                        format_dict['价差'] = '{:.4f}'
                                    if 'IV' in data_df.columns:
                                        format_dict['IV'] = '{:.2f}'
                                    if 'IV回归线' in data_df.columns:
                                        format_dict['IV回归线'] = '{:.2f}'
                                    if 'HV' in data_df.columns:
                                        format_dict['HV'] = '{:.2f}'
                                    if current_series in data_df.columns:
                                        format_dict[current_series] = '{:.4f}'

                                    st.dataframe(
                                        data_df.style.format(format_dict),
                                        use_container_width=True
                                    )
                                else:
                                    st.warning("未查询到详细数据")
                            else:
                                st.warning(f"未找到 {selected_code} 的 {series_display} 数据")
                except Exception as e:
                    st.error(f"生成图表失败: {e}")
                    self.logger.error(f"图表生成错误: {e}", exc_info=True)

    def screen_options(self, prefix, exchanges, ivmr_series: str = 'ivmr3', option_value_type: str = 'all'):
        """筛选期权数据"""
        if len(exchanges) != 1:
            st.error("当前仅支持单交易所查询，请使用前缀自动识别或只选一个交易所")
            return

        exchange = exchanges[0]
        st.session_state.last_exchange = exchange
        op_table = EXCHANGE_TO_OPTION_TABLE.get(exchange)
        
        if not op_table:
            st.error(f"未知交易所: {exchange}")
            return

        # 检查表是否存在
        check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        if not self.db_manager.execute_query(check_query, (op_table,)):
            st.warning(f"表 {op_table} 不存在，跳过 {exchange}")
            return

        all_records = []

        # 循环最近7天（用于筛选异常记录）
        date_query = """
        SELECT DISTINCT "交易日期" 
        FROM ivmr 
        ORDER BY "交易日期" DESC 
        LIMIT 7
        """
        date_rows = self.db_manager.execute_query(date_query)
        recent_dates = [row[0] for row in date_rows] if date_rows else []

        for date in recent_dates:
            # 查询所有IVMR系列列，并包含行权价和期权类型用于价值类型判断
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
                            # 从第一个合约代码获取标的资产价格（同一日期同一品种标的价格相同）
                            sample_code = df_chunk.iloc[0]['期权合约代码']
                            underlying_price = self.get_underlying_price(
                                sample_code, exchange, date, self.db_manager
                            )

                            # 添加 underlying_price 列到 df_chunk（用于后续显示或调试）
                            df_chunk['underlying_price'] = underlying_price

                            # 计算每个期权的价值类型（使用1%规则）
                            df_chunk['价值类型'] = df_chunk.apply(
                                lambda row: self.classify_option_value_type(row, underlying_price),
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


# 运行应用
if __name__ == "__main__":
    app = OptionScreenerApp()
    app.run()
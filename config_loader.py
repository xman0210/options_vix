# ==================== src/utils/config_loader.py ====================
import json
import os
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

# 模块级缓存
_CONFIG: Dict[str, Any] = None
_PROJECT_ROOT: Path = None
# 新增：交易配置缓存
_TRADE_CONFIG: Dict[str, Any] = None


def get_project_root() -> Path:
    """
    可靠定位项目根目录：从调用者位置向上查找同时包含 src 和 config 的目录
    这样可以确保不同版本的系统（Vix, Vix_kimi等）各自操作自己的数据库
    """
    global _PROJECT_ROOT
    if _PROJECT_ROOT is not None:
        return _PROJECT_ROOT

    # 策略1：从调用者位置查找（最优先）
    try:
        import inspect
        # 遍历调用栈，找到第一个不在 config_loader.py 中的调用者
        for frame_info in inspect.stack():
            caller_file = Path(frame_info.filename).resolve()
            
            # 跳过 config_loader.py 自身
            if caller_file.name == 'config_loader.py':
                continue
            # 跳过 Python 标准库和第三方库
            if any(skip in str(caller_file) for skip in ['site-packages', 'lib/python', 'importlib']):
                continue
            
            # 从调用者目录向上查找 src + config
            caller_dir = caller_file.parent
            for parent in [caller_dir] + list(caller_dir.parents):
                if (parent / "src").is_dir() and (parent / "config").is_dir():
                    _PROJECT_ROOT = parent
                    logger.debug(f"项目根目录从调用者定位成功: {_PROJECT_ROOT}")
                    return _PROJECT_ROOT
                    
    except Exception as e:
        logger.debug(f"从调用者定位项目根失败: {e}")

    # 策略2：从当前工作目录查找
    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents):
        if (parent / "src").is_dir() and (parent / "config").is_dir():
            _PROJECT_ROOT = parent
            logger.debug(f"项目根目录从工作目录定位成功: {_PROJECT_ROOT}")
            return _PROJECT_ROOT

    # 策略3：从 config_loader.py 自身位置查找（最后回退）
    # 注意：这会导致所有版本都指向同一个数据库，仅作为最后的备用
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "src").is_dir() and (parent / "config").is_dir():
            _PROJECT_ROOT = parent
            logger.warning(f"项目根目录从 config_loader 自身定位成功（可能导致多版本系统冲突）: {_PROJECT_ROOT}")
            return _PROJECT_ROOT
    
    logger.warning("无法自动定位项目根，使用当前工作目录作为根")
    _PROJECT_ROOT = Path.cwd()
    return _PROJECT_ROOT

def load_config() -> Dict[str, Any]:
    """
    加载 config.json，支持缓存
    """
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG

    config_path = get_project_root() / "config" / "config.json"
    
    default_config = {
        "risk_free_rate": 0.02,
        "target_dates": [],
        "shfe_ris_name": ["AU", "CU"],
        "dce_ris_name": ["M", "B"],
        "gfex_ris_name": ["LC"],
        "czce_ris_name": ["FG", "SA"],
        "cffex_ris_name": ["MO", "IO", "HO"],
        "cffex_op_expiry_dates": {},
        "paths": {
            "data_dir": "data",
            "log_dir": "logs",
            "sound_file": "sound/warring.wav",
            "macos_sound_player": "afplay",
            "exchange_mapping": {}
        }
    }

    if not config_path.is_file():
        logger.warning(f"配置文件不存在: {config_path}，使用默认配置")
        _CONFIG = default_config
        return _CONFIG

    try:
        with config_path.open('r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"成功加载配置文件: {config_path}")
        
        merged = default_config.copy()
        merged.update(config)
        if "paths" in config:
            merged["paths"].update(config["paths"])
        
        _CONFIG = merged
        return _CONFIG
    
    except json.JSONDecodeError as e:
        logger.error(f"config.json 格式错误: {e}", exc_info=True)
        logger.warning("使用默认配置")
        _CONFIG = default_config
        return _CONFIG
    except PermissionError:
        logger.error(f"无权限读取配置文件: {config_path}")
        raise
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}", exc_info=True)
        raise


def load_trade_settings() -> Dict[str, Any]:
    """
    加载交易参数配置 trade_set.json
    支持全局默认、交易所级别、品种级别三级覆盖
    """
    global _TRADE_CONFIG
    if _TRADE_CONFIG is not None:
        return _TRADE_CONFIG

    trade_config_path = get_project_root() / "config" / "trade_set.json"
    
    default_settings = {
        "target_profit": 1.20,
        "stop_loss": 0.90,
        "max_hold_days": 7,
        "close_price_type": {
            "profit": "high",
            "loss": "open"
        },
        "open_conditions": {
            "volume_min": 20,
            "volume_field": "成交量",
            "use_volume": True
        }
    }
    
    if not trade_config_path.is_file():
        logger.warning(f"交易配置文件不存在: {trade_config_path}，使用默认配置")
        _TRADE_CONFIG = {
            "default": default_settings,
            "by_exchange": {},
            "by_product": {}
        }
        return _TRADE_CONFIG
    
    try:
        with trade_config_path.open('r', encoding='utf-8') as f:
            trade_config = json.load(f)
        logger.info(f"成功加载交易配置: {trade_config_path}")
        
        settings = trade_config.get("trade_settings", {})
        
        # 转换默认配置值为合适类型
        default = settings.get("default", {})
        default_settings = {
            "target_profit": float(default.get("target_profit", 1.20)),
            "stop_loss": float(default.get("stop_loss", 0.90)),
            "max_hold_days": int(default.get("max_hold_days", 7)),
            "close_price_type": default.get("close_price_type", {
                "profit": "high",
                "loss": "open"
            }),
            "open_conditions": {
                "volume_min": int(default.get("open_conditions", {}).get("volume_min", 20)),
                "volume_field": default.get("open_conditions", {}).get("volume_field", "成交量"),
                "use_volume": True
            }
        }
        
        # 处理交易所级别配置
        by_exchange = {}
        for exch, exch_cfg in settings.get("by_exchange", {}).items():
            open_conds = exch_cfg.get("open_conditions", {})
            by_exchange[exch.lower()] = {
                "target_profit": float(exch_cfg.get("target_profit", default_settings["target_profit"])),
                "stop_loss": float(exch_cfg.get("stop_loss", default_settings["stop_loss"])),
                "max_hold_days": int(exch_cfg.get("max_hold_days", default_settings["max_hold_days"])),
                "close_price_type": exch_cfg.get("close_price_type", default_settings["close_price_type"]),
                "open_conditions": {
                    "volume_min": int(open_conds.get("volume_min", default_settings["open_conditions"]["volume_min"])),
                    "volume_field": open_conds.get("volume_field", default_settings["open_conditions"]["volume_field"]),
                    "use_volume": True
                },
                "comment": exch_cfg.get("comment", "")
            }
        
        # 处理品种级别配置 - 键转为大写
        by_product = {}
        for prod, prod_cfg in settings.get("by_product", {}).items():
            open_conds = prod_cfg.get("open_conditions", {})
            by_product[prod.upper()] = {
                "target_profit": float(prod_cfg.get("target_profit", default_settings["target_profit"])),
                "stop_loss": float(prod_cfg.get("stop_loss", default_settings["stop_loss"])),
                "max_hold_days": int(prod_cfg.get("max_hold_days", default_settings["max_hold_days"])),
                "close_price_type": prod_cfg.get("close_price_type", default_settings["close_price_type"]),
                "open_conditions": {
                    "volume_min": int(open_conds.get("volume_min", default_settings["open_conditions"]["volume_min"])),
                    "volume_field": open_conds.get("volume_field", default_settings["open_conditions"]["volume_field"]),
                    "use_volume": True
                },
                "early_close": prod_cfg.get("early_close", {}),
                "comment": prod_cfg.get("comment", "")
            }
        
        _TRADE_CONFIG = {
            "default": default_settings,
            "by_exchange": by_exchange,
            "by_product": by_product
        }
        
        logger.info(f"交易配置加载完成: 默认{default_settings}, 交易所{list(by_exchange.keys())}, 品种{list(by_product.keys())}")
        return _TRADE_CONFIG
        
    except json.JSONDecodeError as e:
        logger.error(f"trade_set.json 格式错误: {e}", exc_info=True)
        _TRADE_CONFIG = {
            "default": default_settings,
            "by_exchange": {},
            "by_product": {}
        }
        return _TRADE_CONFIG
    except Exception as e:
        logger.error(f"加载交易配置失败: {e}", exc_info=True)
        _TRADE_CONFIG = {
            "default": default_settings,
            "by_exchange": {},
            "by_product": {}
        }
        return _TRADE_CONFIG


def get_product_trade_settings(option_code: str, exchange: str = "") -> Dict[str, Any]:
    """
    获取指定品种的交易参数，按优先级合并配置
    
    优先级: 品种级别 > 交易所级别 > 全局默认
    
    Args:
        option_code: 期权合约代码，如 "IO2503C4000"
        exchange: 交易所代码，如 "cffex"
    
    Returns:
        合并后的交易参数字典，包含中文数据库字段映射
    """
    all_settings = load_trade_settings()
    default = all_settings["default"]
    
    # 提取品种代码 - 优先尝试2字母，但只在配置存在时才使用，否则尝试1字母
    product = None
    if len(option_code) >= 2:
        prefix_2 = option_code[:2].upper()
        # 检查2字母是否在配置中
        if prefix_2 in all_settings["by_product"]:
            product = prefix_2
            logger.debug(f"品种识别(2字母): {option_code} -> {product}")
    
    # 如果2字母不在配置中，尝试1字母
    if product is None and len(option_code) >= 1:
        prefix_1 = option_code[0].upper()
        if prefix_1 in all_settings["by_product"]:
            product = prefix_1
            logger.debug(f"品种识别(1字母): {option_code} -> {product}")
    
    # 如果都不在配置中，默认使用2字母（用于后续可能的动态配置）
    if product is None:
        product = option_code[:2].upper() if len(option_code) >= 2 else option_code.upper()
        logger.debug(f"品种识别(默认): {option_code} -> {product} (未在配置中找到)")
    
    # 提取交易所
    exch = exchange.lower() if exchange else ""
    
    # 从默认开始合并
    settings = default.copy()
    logger.debug(f"开始合并配置: 默认设置 target_profit={settings['target_profit']}, stop_loss={settings['stop_loss']}")
    
    # 合并交易所级别（如果存在）
    if exch and exch in all_settings["by_exchange"]:
        exch_settings = all_settings["by_exchange"][exch]
        settings.update({
            k: v for k, v in exch_settings.items() 
            if k not in ["comment"] and v is not None
        })
        logger.debug(f"应用交易所配置({exch}): target_profit={settings['target_profit']}, stop_loss={settings['stop_loss']}")
    
    # 合并品种级别（优先级最高，如果存在）
    if product and product in all_settings["by_product"]:
        prod_settings = all_settings["by_product"][product]
        settings.update({
            k: v for k, v in prod_settings.items() 
            if k not in ["comment"] and v is not None
        })
        logger.info(f"应用品种配置({product}): target_profit={settings['target_profit']}, stop_loss={settings['stop_loss']}, max_hold_days={settings['max_hold_days']}")
    else:
        logger.info(f"未找到品种配置({product})，使用交易所/默认配置: target_profit={settings['target_profit']}, stop_loss={settings['stop_loss']}")
    
    # 添加中文数据库字段映射
    price_type = settings.get("close_price_type", {})
    field_map = {
        "open": "开盘价",
        "close": "收盘价",
        "settlement": "结算价", 
        "high": "最高价",
        "low": "最低价"
    }
    
    settings["close_price_field"] = {
        "profit": field_map.get(price_type.get("profit", "high"), "最高价"),
        "loss": field_map.get(price_type.get("loss", "open"), "开盘价")
    }
    
    return settings


def get_db_path():
    config = load_config()
    data_dir_str = config["paths"].get("data_dir", "data")
    data_dir = get_project_root() / data_dir_str
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "data.db"


def get_log_dir():
    config = load_config()
    log_dir = get_project_root() / config["paths"].get("log_dir", "logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def get_data_dir():
    config = load_config()
    data_dir = get_project_root() / config["paths"].get("data_dir", "data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_sound_file():
    config = load_config()
    sound_file_path = config["paths"].get("sound_file", "sound/warring.wav")
    return os.path.expanduser(sound_file_path)


def get_macos_sound_player():
    """获取 macOS 音频播放器策略 (afplay 为系统标准方案)"""
    config = load_config()
    return config["paths"].get("macos_sound_player", "afplay")


def get_exchange_mapping() -> Dict[str, str]:
    try:
        config = load_config()
        raw_mapping = config.get("exchange_mapping", {})
        
        if not raw_mapping:
            logger.warning("config.json 中缺少或为空的 'exchange_mapping'，返回空映射")
            return {}
        
        mapping = {k.upper(): v.lower() for k, v in raw_mapping.items()}
        logger.debug(f"交易所映射加载完成，共 {len(mapping)} 条规则")
        return mapping
    
    except Exception as e:
        logger.error(f"获取交易所映射失败，使用空映射: {e}", exc_info=True)
        return {}
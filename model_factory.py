"""
模型工厂 - 品种级模型配置
从交易所级配置升级到品种级配置
"""
from src.utils.config_loader import load_config
from typing import Dict, Any, Optional, List

# 全局配置缓存
_CONFIG = None


def _get_config() -> Dict[str, Any]:
    """获取配置（带缓存）"""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = load_config()
    return _CONFIG


def get_model_params(exchange: str, product: str) -> Dict[str, Any]:
    """
    获取品种级的模型参数
    
    Args:
        exchange: 交易所代码（小写，如'cffex', 'shfe'）
        product: 品种代码（大写，如'IO', 'CU', 'AU'）
    
    Returns:
        包含model, r, q, is_american等参数的字典
    """
    config = _get_config()
    models_config = config.get("models", {})
    global_cfg = models_config.get("global", {})
    
    # 基础参数
    r = global_cfg.get("r", 0.019)
    default_model = global_cfg.get("default_model", "black76")
    default_q = global_cfg.get("q", 0.0)
    default_is_american = False
    
    # 品种级配置（优先级最高）
    by_product = models_config.get("by_product", {})
    product_cfg = by_product.get(product, {})
    
    # 如果品种有独立配置
    if product_cfg:
        model = product_cfg.get("model", default_model)
        q = product_cfg.get("q", default_q)
        is_american = product_cfg.get("is_american", default_is_american)
        
        # 验证exchange一致性（用于数据校验）
        cfg_exchange = product_cfg.get("exchange", "").lower()
        if cfg_exchange and cfg_exchange != exchange.lower():
            import logging
            logging.getLogger("model_factory").warning(
                f"品种{product}配置交易所({cfg_exchange})与调用({exchange})不一致"
            )
    else:
        # 回退到交易所默认配置（兼容旧配置）
        exchange_cfg = models_config.get(exchange, {})
        default_ex_cfg = exchange_cfg.get("default", {})
        
        # 尝试从交易所配置中查找品种
        variety_cfg = exchange_cfg.get(product, default_ex_cfg)
        
        if isinstance(variety_cfg, dict):
            model = variety_cfg.get("model", default_model)
            q = variety_cfg.get("q", default_q)
            is_american = variety_cfg.get("is_american", default_is_american)
        else:
            # 兼容旧配置中可能是字符串的情况
            model = default_model
            q = default_q
            is_american = default_is_american
    
    return {
        "model": model,
        "r": r,
        "q": q,
        "is_american": is_american,
        "exchange": exchange,
        "product": product
    }


def get_model_for_contract(contract_code: str, exchange: str) -> Dict[str, Any]:
    """
    根据合约代码自动提取品种并获取模型参数
    
    Args:
        contract_code: 合约代码，如"IO2503C4000", "CU2505C68000"
        exchange: 交易所代码
    
    Returns:
        模型参数字典
    """
    # 提取品种代码（字母部分）
    product = ''.join(filter(str.isalpha, contract_code)).upper()
    
    # 特殊处理：CFFEX的品种代码映射
    cffex_mapping = {
        'IO': 'IO',   # 沪深300股指期权
        'MO': 'MO',   # 中证1000股指期权  
        'HO': 'HO',   # 上证50股指期权
        'EO': 'EO',   # 中证500股指期权（如有）
    }
    
    if exchange.lower() == 'cffex' and product in cffex_mapping:
        product = cffex_mapping[product]
    
    return get_model_params(exchange, product)


def validate_model_configs() -> Dict[str, Any]:
    """
    验证所有配置的品种模型参数是否完整
    
    检查所有ris_name中的品种是否在models.by_product中有配置
    
    Returns:
        {
            'total_varieties': 总品种数,
            'configured_varieties': 已配置品种数,
            'missing_configs': 缺失配置列表,
            'is_valid': 是否全部有效
        }
    """
    import logging
    logger = logging.getLogger("model_factory")
    
    config = _get_config()
    ris_names = {}
    
    # 收集所有需要计算的品种
    for exchange in ['cffex', 'shfe', 'dce', 'czce', 'gfex']:
        key = f"{exchange}_ris_name"
        names = config.get(key, [])
        ris_names[exchange] = set()
        for name in names:
            # 提取品种代码（字母部分）
            product = ''.join(filter(str.isalpha, name)).upper()
            ris_names[exchange].add(product)
    
    # 检查每个品种是否有模型配置
    by_product = config.get("models", {}).get("by_product", {})
    missing_configs = []
    
    for exchange, products in ris_names.items():
        for product in products:
            if product not in by_product:
                missing_configs.append({
                    'exchange': exchange,
                    'product': product,
                    'suggestion': f'请在config.json的models.by_product中添加{product}的配置'
                })
    
    total_varieties = sum(len(p) for p in ris_names.values())
    configured_varieties = len(by_product)
    
    result = {
        'total_varieties': total_varieties,
        'configured_varieties': configured_varieties,
        'missing_configs': missing_configs,
        'is_valid': len(missing_configs) == 0
    }
    
    logger.debug(
        f"模型配置验证: 总品种{total_varieties}, "
        f"已配置{configured_varieties}, "
        f"缺失{len(missing_configs)}"
    )
    
    return result


def get_all_configured_products() -> List[str]:
    """获取所有已配置的品种列表"""
    config = _get_config()
    by_product = config.get("models", {}).get("by_product", {})
    return list(by_product.keys())


def get_product_config(product: str) -> Optional[Dict[str, Any]]:
    """获取指定品种的完整配置"""
    config = _get_config()
    by_product = config.get("models", {}).get("by_product", {})
    return by_product.get(product)


def clear_config_cache():
    """清除配置缓存（用于热更新）"""
    global _CONFIG
    _CONFIG = None
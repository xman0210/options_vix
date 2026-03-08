# src/utils/logging_config.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .config_loader import get_log_dir

def get_logger(module_name: str, log_filename: str | None = None):
    logger = logging.getLogger(module_name)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.setLevel(logging.DEBUG) #调试阶段，设置为debug
    
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    try:
        log_dir = get_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if log_filename is None:
            log_filename = module_name.split(".")[-1] + ".log"
        log_file = log_dir / log_filename
        
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10_485_760, 
            backupCount=5, 
            encoding='utf-8'
        )
        file_handler.setFormatter(console_formatter)
        logger.addHandler(file_handler)
        logger.info(f"日志文件初始化成功: {log_file}")
    except Exception as e:
        logger.error(f"日志文件初始化失败（回退到控制台输出）: {e}")
    
    logger.propagate = False
    return logger
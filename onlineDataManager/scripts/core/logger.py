"""
core/logger.py
===============

统一日志配置 — service / scheduler 都用这个。

设计:
- 控制台(StreamHandler)+ 文件(FileHandler)同时输出
- 文件路径 = ~/TradingAgent/onlineDataManager/logs/<name>.log
- launchd 启动后 stdout 不是 tty → 文件日志保证可见
- 日志级别 INFO,带时间戳 + 模块名

用法:
    from core.logger import setup_logger
    log = setup_logger("service_writeredis_auction")  # 自动写 logs/service_writeredis_auction.log
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


from coreClient._paths import manager_root

ROOT = manager_root(__file__, 3)
ONLINE_DATA_ROOT = ROOT / "onlineDataManager"
LOG_DIR = ONLINE_DATA_ROOT / "logs"

# 日期前缀(每天一个文件)
def _today_str() -> str:
    return datetime.now().strftime("%Y%m%d")


def setup_logger(name: str, *, level: int = logging.INFO) -> logging.Logger:
    """统一的 logger: StreamHandler + FileHandler 都装好

    Args:
        name: logger 名(同时作为日志文件名,不含日期后缀)
        level: 日志级别

    Returns:
        配好 handler 的 Logger
    """
    log = logging.getLogger(name)
    log.setLevel(level)

    # 防止重复添加(handler 已经在,跳过)
    if log.handlers:
        return log

    fmt = logging.Formatter(
        "[%(asctime)s] %(name)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1. 控制台(StreamHandler)— 走 stdout,flush 立即
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(level)
    log.addHandler(sh)

    # 2. 文件(FileHandler)— 每天一个文件,launchd 后台跑也能看见
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / f"{name}_{_today_str()}.log"
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(level)
        log.addHandler(fh)
    except Exception as e:
        # 文件 handler 失败不应阻塞主流程,只 stderr 警告
        sys.stderr.write(f"[{name}] 无法创建 FileHandler: {e}\n")

    # 不让日志往 root logger 冒泡
    log.propagate = False
    return log

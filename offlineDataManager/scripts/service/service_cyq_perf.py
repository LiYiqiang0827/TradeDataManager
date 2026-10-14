"""
service_cyq_perf.py - 每日筹码及胜率更新

数据源:tushare pro.cyq_perf
文档:https://tushare.pro/document/2?doc_id=293

依赖:settings.py / offline_downloader.py / offline_db_client.py
"""

import sys, time, argparse
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from config.settings import LOG_FORMAT, LOG_LEVEL, LOG_DIR, PROJECT_ROOT
from core.offline_downloader import CNDataDown
from service.common import resolve_date_range  # 复用


# ====== 业务常量 ======
CTRL_KEY = "cn_cyq_perf"  # tbl_basic_ctrl key
SERVICE_NAME = "service_cyq_perf"


def init_logger(name: str):
    """初始化 loguru logger(简化版,每 service 一份 log 文件)"""
    log_path = LOG_DIR / f"{name}.log"
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(str(log_path), format=LOG_FORMAT, level=LOG_LEVEL, rotation="20 MB", encoding="utf-8")
    logger.add(lambda msg: print(msg, end=""), format=LOG_FORMAT, level=LOG_LEVEL)


def main():
    init_logger(SERVICE_NAME)
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--start-date", help="起始日期 YYYYMMDD")
    parser.add_argument("--end-date", help="结束日期 YYYYMMDD")
    parser.add_argument("--trade-date", help="单日模式 YYYYMMDD")
    args = parser.parse_args()

    from core.offline_db_client import init_db
    init_db("basic")

    down = CNDataDown()
    t0 = time.time()

    if args.trade_date:
        # 单日模式
        n = down.update_cyq_perf(start_date=args.trade_date, end_date=args.trade_date)
    else:
        # 增量模式(默认从 ctrl 取)
        from core.offline_db_client import get_conn, get_ctrl
        conn = get_conn("basic")
        sd, ed, desc = resolve_date_range(conn, CTRL_KEY, args, default_start="20200102")
        n = down.update_cyq_perf(start_date=sd, end_date=ed)

    elapsed = time.time() - t0
    logger.info(f"[{SERVICE_NAME}] +{n:,} 行, 耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    main()
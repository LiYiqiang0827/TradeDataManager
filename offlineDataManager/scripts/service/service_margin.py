"""
service_margin.py - 融资融券交易汇总更新

数据源:tushare pro.margin
文档:https://tushare.pro/document/2?doc_id=58

依赖:settings.py / offline_downloader.py / offline_db_client.py
"""

import sys, time, argparse
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.offline_downloader import CNDataDown
from core.offline_db_client import get_ctrl

CTRL_KEY = "cn_margin"
INCREMENTAL_LOOKBACK_DAYS = 10


def _resolve_margin_date_range(conn, ctrl_key, args, *, today=None):
    """Explicit history stays explicit; incremental runs retry late market data."""
    def normalize(value):
        return datetime.strptime(str(value).replace("-", ""), "%Y%m%d").strftime("%Y%m%d")

    today = normalize(today or datetime.now().strftime("%Y%m%d"))
    trade_date = getattr(args, "trade_date", None)
    start_date = getattr(args, "start_date", None)
    end_date = getattr(args, "end_date", None)
    if trade_date:
        if start_date or end_date:
            raise ValueError("--trade-date 不能与 --start-date/--end-date 同时使用")
        sd = ed = normalize(trade_date)
        desc = f"单日 {sd}"
    elif start_date or end_date:
        # Do not clamp a user-requested repair to a later ctrl watermark.
        sd = normalize(start_date or "20150101")
        ed = normalize(end_date or today)
        desc = f"指定日期段 {sd} ~ {ed}"
    else:
        last = get_ctrl(conn, ctrl_key)
        if last:
            anchor = min(normalize(last), today)
            sd = max("20150101", (
                datetime.strptime(anchor, "%Y%m%d")
                - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
            ).strftime("%Y%m%d"))
        else:
            sd = "20150101"
        ed = today
        desc = f"增量回看 {INCREMENTAL_LOOKBACK_DAYS} 自然日 {sd} ~ {ed}"
    if sd > ed:
        raise ValueError(f"起始日期不能晚于结束日期: {sd} > {ed}")
    return sd, ed, desc


def main():
    parser = argparse.ArgumentParser(description="Update 融资融券交易汇总(tushare pro.margin)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--trade-date", type=str, default=None, help="单日 YYYYMMDD")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_margin.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = _resolve_margin_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_margin(start_date=sd, end_date=ed)
        logger.info(f"[service_margin] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_margin] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

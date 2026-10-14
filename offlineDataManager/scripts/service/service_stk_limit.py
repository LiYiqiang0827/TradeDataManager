"""
service_stk_limit.py - 每日涨跌停价格更新

数据源:tushare pro.stk_limit
文档:https://tushare.pro/document/2?doc_id=183

用法:
  python3 service_stk_limit.py --trade-date 20241008
  python3 service_stk_limit.py --start-date 20240101 --end-date 20241008
  python3 service_stk_limit.py  # 增量(ctrl)
"""
import sys
import argparse
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from service.common import resolve_date_range
from loguru import logger

CTRL_KEY = "cn_stk_limit"


def main():
    parser = argparse.ArgumentParser(description="Update 每日涨跌停价格")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_stk_limit.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_stk_limit(start_date=sd, end_date=ed)
        logger.info(f"[service_stk_limit] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_stk_limit] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

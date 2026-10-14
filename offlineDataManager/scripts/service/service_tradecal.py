"""
service_tradecal.py - 交易日历更新

用法:
  python3 service_tradecal.py --trade-date 20240909
  python3 service_tradecal.py --start-date 20180101 --end-date 20261231
  python3 service_tradecal.py  # 增量
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


def main():
    parser = argparse.ArgumentParser(description="Update 交易日历")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_tradecal.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        # tradecal 有 3 个交易所,ctrl 分开,这里用 SSE 断点作为统一起点
        # update_tradecal 内部每个交易所仍会用各自的 ctrl 增量
        sd, ed, desc = resolve_date_range(down.conn_basic, "cn_tradecal_SSE", args)
        n = down.update_tradecal(start_date=sd, end_date=ed)
        logger.info(f"[service_tradecal] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_tradecal] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

"""
service_adj_factor.py - A 股复权因子更新

用法:
  # 单日(优先)
  python3 service_adj_factor.py --trade-date 20240105

  # 日期段(start_date/end_date)
  python3 service_adj_factor.py --start-date 20240101 --end-date 20241231

  # 增量(默认从 ctrl + 1 到今天)
  python3 service_adj_factor.py

逻辑(和 MyATM 一致):
  - 如果输入 --trade-date:start_date = end_date = trade_date,忽略 start-date/end-date
  - 如果没有 --trade-date:
    - start_date 默认 2015-01-01
    - end_date 默认 今天
    - start_date 必须 ≥ tbl_ctrl 里的日期 + 1(避免重拉已有数据)
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

CTRL_KEY = "cn_adj_factor"


def main():
    parser = argparse.ArgumentParser(description="Update 复权因子")
    parser.add_argument("--trade-date", type=str, default=None,
                        help="单日(YYYYMMDD),与 --start-date/--end-date 互斥,优先级最高")
    parser.add_argument("--start-date", type=str, default=None,
                        help="起始日期(YYYYMMDD),默认 2015-01-01")
    parser.add_argument("--end-date", type=str, default=None,
                        help="结束日期(YYYYMMDD),默认今天")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_adj_factor.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)

        n = down.update_adj_factor(start_date=sd, end_date=ed)
        logger.info(f"[service_adj_factor] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_adj_factor] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

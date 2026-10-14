"""
service_kpl_concept_cons.py - 开盘啦题材成分更新

用法:
  python3 service_kpl_concept_cons.py --trade-date 20240909
  python3 service_kpl_concept_cons.py --start-date 20240101 --end-date 20241231
  python3 service_kpl_concept_cons.py  # 增量
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

CTRL_KEY = "cn_kpl_concept_cons"


def main():
    parser = argparse.ArgumentParser(description="Update 题材成分")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_kpl_concept_cons.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(
            down.conn_kpl, CTRL_KEY, args, ctrl_store="kpl"
        )
        n = down.update_kpl_concept_cons(start_date=sd, end_date=ed)
        logger.info(f"[service_kpl_concept_cons] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_kpl_concept_cons] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

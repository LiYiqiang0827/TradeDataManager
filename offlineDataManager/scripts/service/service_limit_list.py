"""
service_limit_list.py - 每日涨跌停列表更新

数据源:tushare pro.limit_list_d
文档:https://tushare.pro/document/2?doc_id=298
"""

import sys, time, argparse
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.offline_downloader import CNDataDown
from service.common import resolve_date_range

CTRL_KEY = "cn_limit_list"


def main():
    parser = argparse.ArgumentParser(description="Update 每日涨跌停列表(tushare pro.limit_list_d)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期 YYYYMMDD")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_limit_list.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(
            down.conn_kpl, CTRL_KEY, args, ctrl_store="kpl"
        )
        n = down.update_limit_list(start_date=sd, end_date=ed)
        logger.info(f"[service_limit_list] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_limit_list] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

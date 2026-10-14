"""
service_top_list.py - 龙虎榜每日活跃更新

数据源:tushare pro.top_list
文档:https://tushare.pro/document/2?doc_id=106

用法:
  python3 service_top_list.py  # 增量(从 ctrl 到今天)
  python3 service_top_list.py --start-date 20201201 --end-date 20240930

依赖:settings.py / offline_downloader.py / offline_db_client.py
"""

import sys
import time
import argparse
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.offline_downloader import CNDataDown
from service.common import resolve_date_range

CTRL_KEY = "cn_top_list"


def main():
    parser = argparse.ArgumentParser(description="Update 龙虎榜每日活跃(tushare pro.top_list)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期 YYYYMMDD")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_top_list.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(
            down.conn_kpl, CTRL_KEY, args, ctrl_store="kpl"
        )
        n = down.update_top_list(start_date=sd, end_date=ed)
        logger.info(f"[service_top_list] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_top_list] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

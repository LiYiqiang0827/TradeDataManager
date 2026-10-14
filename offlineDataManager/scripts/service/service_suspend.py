"""
service_suspend.py - 每日停复牌信息更新

数据源:tushare pro.suspend_d
文档:https://tushare.pro/document/2?doc_id=214

用法:
  python3 service_suspend.py --start-date 20150101 --end-date 20240930
  python3 service_suspend.py  # 增量(从 ctrl.max_date 到今天)

依赖:settings.py / offline_downloader.py / offline_db_client.py
"""

import sys
import time
import argparse
from pathlib import Path

from loguru import logger

# 让 service 能 import core/
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.offline_downloader import CNDataDown
from service.common import resolve_date_range

CTRL_KEY = "cn_suspend"


def main():
    parser = argparse.ArgumentParser(description="Update 每日停复牌信息(tushare pro.suspend_d)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期 YYYYMMDD")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_suspend.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_suspend(start_date=sd, end_date=ed)
        logger.info(f"[service_suspend] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_suspend] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

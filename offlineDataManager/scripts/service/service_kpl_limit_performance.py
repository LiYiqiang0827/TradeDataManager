"""
service_kpl_limit_performance.py - 开盘啦涨停表现详情更新

拉取每只涨停股的完整字段(封单/净额/振幅/炸板标记/收盘价等),
写入 db_cn_kpl.db:tbl_cn_kpl_limit_performance。

kpl_list (tushare) 第二天早上才更新,我们每天收盘后用这个 service 实时补当天数据。

用法:
  python3 service_kpl_limit_performance.py --trade-date 20260911
  python3 service_kpl_limit_performance.py --start-date 20260907 --end-date 20260911
  python3 service_kpl_limit_performance.py  # 增量(从本周周一 + ctrl.max_date 较大值到今天)
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

CTRL_KEY = "cn_kpl_limit_performance"


def main():
    parser = argparse.ArgumentParser(description="Update 涨停表现详情(实时补 kpl_list 滞后)")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_kpl_limit_performance.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        # 断点表在 db_cn_basic.db:tbl_basic_ctrl(与 kpl_list / kpl_concept_cons 统一管理)
        sd, ed, desc = resolve_date_range(
            down.conn_kpl, CTRL_KEY, args, ctrl_store="kpl"
        )
        n = down.update_kpl_limit_performance(start_date=sd, end_date=ed)
        logger.info(f"[service_kpl_limit_performance] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_kpl_limit_performance] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

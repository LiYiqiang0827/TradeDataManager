"""
service_month.py - A 股月 K 线更新(同时生成前复权与原始表)

用法:
  python3 service_month.py  # 全量生成 tbl_cn_month + tbl_cn_month_origin

注意:
  - 周月 K 是派生数据,不需要日期参数
  - 每次运行会原子覆盖前复权表和原始不复权表
  - **强依赖**:tbl_cn_day 和 tbl_cn_adj_factor 必须已更新完毕
  - 启动时会校验 tbl_ctrl 里 cn_daily 和 cn_adj_factor 的日期是否一致
    且都是最近已收盘交易日,否则中止(避免用过期的 day/adj_factor 算前复权)
"""
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from core.offline_db_client import get_ctrl
from service.service_week import check_daily_adj_consistency  # 复用校验逻辑
from loguru import logger


def main():
    log_file = PROJECT_ROOT / "logs" / "service_month.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()

        # 强校验:cn_daily 和 cn_adj_factor 必须一致且 ≥ 今天(复用 service_week 的校验)
        if not check_daily_adj_consistency(down):
            logger.error("[service_month] 中止:cn_daily / cn_adj_factor 未就绪")
            return 2  # 用 return 2 区分"业务中止"(return 1 是异常)

        # 月 K 不传日期参数,一次性全量生成(覆盖更新)
        n = down.update_month()
        logger.info(f"[service_month] 完成(前复权+原始全量): 各 +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_month] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

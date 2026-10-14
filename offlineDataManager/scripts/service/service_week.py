"""
service_week.py - A 股周 K 线更新(同时生成前复权与原始表)

用法:
  python3 service_week.py  # 全量生成 tbl_cn_week + tbl_cn_week_origin

注意:
  - 周月 K 是派生数据,不需要日期参数
  - 每次运行会原子覆盖前复权表和原始不复权表
  - **强依赖**:tbl_cn_day 和 tbl_cn_adj_factor 必须已更新完毕
  - 启动时会校验 tbl_ctrl 里 cn_daily 和 cn_adj_factor 的日期是否一致
    且都是最近已收盘交易日,否则中止(避免用过期的 day/adj_factor 算前复权)
"""
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from core.offline_db_client import get_ctrl
from service.common import latest_completed_trade_date
from loguru import logger


def check_daily_adj_consistency(down: CNDataDown) -> bool:
    """校验 tbl_ctrl 中 cn_daily 和 cn_adj_factor 的日期是否一致

    规则:
      - 两者都必须存在
      - 两者的日期字符串必须完全相等(YYYYMMDD 格式)
      - 必须 ≥ 最近已收盘交易日

    Returns:
        True=一致且最新,可以安全算周月 K
        False=不一致或过期,中止周月 K 计算
    """
    today = latest_completed_trade_date(down.conn_basic)
    ctrl_daily = get_ctrl(down.conn_basic, "cn_daily")
    ctrl_adj = get_ctrl(down.conn_basic, "cn_adj_factor")

    logger.info(f"[校验] ctrl.cn_daily={ctrl_daily}, ctrl.cn_adj_factor={ctrl_adj}, 今天={today}")

    if not ctrl_daily or not ctrl_adj:
        logger.error(
            f"[校验] ✗ ctrl 缺失:cn_daily={ctrl_daily}, cn_adj_factor={ctrl_adj}"
        )
        return False

    if ctrl_daily != ctrl_adj:
        logger.error(
            f"[校验] ✗ ctrl.cn_daily ({ctrl_daily}) ≠ ctrl.cn_adj_factor ({ctrl_adj})\n"
            f"  必须两者一致,才能算周月 K"
        )
        return False

    if ctrl_daily < today:
        logger.error(
            f"[校验] ✗ ctrl.cn_daily={ctrl_daily} < 今天={today},数据过期\n"
            f"  必须先跑 service_daily 和 service_adj_factor"
        )
        return False

    logger.info(f"[校验] ✓ ctrl 一致且最新 ({ctrl_daily}),可以算周月 K")
    return True


def main():
    log_file = PROJECT_ROOT / "logs" / "service_week.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()

        # 强校验:cn_daily 和 cn_adj_factor 必须一致且 ≥ 今天
        if not check_daily_adj_consistency(down):
            logger.error("[service_week] 中止:cn_daily / cn_adj_factor 未就绪")
            return 2  # 用 return 2 区分"业务中止"(return 1 是异常)

        # 周 K 不传日期参数,一次性全量生成(覆盖更新)
        n = down.update_week()
        logger.info(f"[service_week] 完成(前复权+原始全量): 各 +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_week] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

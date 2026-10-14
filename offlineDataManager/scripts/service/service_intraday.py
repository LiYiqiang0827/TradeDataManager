"""
service_intraday.py — 个股分钟 K 更新(2026-09-17 新增)

数据源:tdx get_history_minute_time_data
落库:policy_minute.db / tbl_minute + tbl_minute_ctrl

调用模式(3 种,优先级从高到低):
  1. --watchlist csv 路径(必须有 ts_code + trade_date 列)
  2. --ts-codes 列表 + --trade-date 单日 / --start-date~--end-date 日期段
  3. 都没传 → 报错退出

示例:
  # 用 policyStudy 的 watchlist(13738 对)
  python3 service_intraday.py --watchlist watchlist_tczt_20260101_20260831.csv

  # 拉单只单日
  python3 service_intraday.py --ts-codes 000006.SZ --trade-date 2026-08-28

  # 拉多只 + 日期段
  python3 service_intraday.py --ts-codes "000006.SZ,000636.SZ" --start-date 2026-08-26 --end-date 2026-08-28

  # 强制重拉
  python3 service_intraday.py --watchlist x.csv --force
"""
import sys, time, argparse
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT.parent / "coreClient"))

from core.offline_downloader import CNDataDown
from core.offline_db_client import ensure_schema
from tdx_client import TdxClient
from service.common import (
    resolve_market_pairs, run_market_loop, add_market_args,
)


def main():
    parser = argparse.ArgumentParser(description="Update 个股分钟 K(tdx → policy_minute.db)")
    add_market_args(parser)
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_intraday.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    # 解析参数
    pairs, source = resolve_market_pairs(args)
    if source == "no_pairs" or not pairs:
        logger.error("[service_intraday] 必须传 --watchlist 或 (--ts-codes + 日期参数)")
        return 1
    logger.info(f"[service_intraday] 数据源: {source}, 共 {len(pairs)} 对")

    # 确保 schema
    ensure_schema(db_kind="minute", verbose=False)

    t0 = time.time()
    client = TdxClient()
    try:
        down = CNDataDown()
        stats = run_market_loop(
            down, pairs,
            update_method="update_minute",
            client=client,
            rate=args.rate,
            force=args.force,
            logger=logger,
        )
        logger.info(
            f"[service_intraday] 完成: total={stats['total']} "
            f"inserted={stats['inserted']}({stats['inserted_rows']} 行) "
            f"skipped={stats['skipped']} failed={stats['failed']} "
            f"用时 {stats['elapsed']:.1f}s"
        )
        return 0
    except Exception as e:
        logger.error(f"[service_intraday] 失败: {e}", exc_info=True)
        return 1
    finally:
        try:
            client.api.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

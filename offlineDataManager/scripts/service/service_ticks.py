"""
service_ticks.py — 个股分笔成交更新(2026-09-17 新增)

数据源:tdx get_history_ticks
落库:policy_ticks.db / tbl_tick + tbl_tick_ctrl

调用模式同 service_intraday:
  1. --watchlist csv
  2. --ts-codes + --trade-date / --start-date~--end-date
  3. 都没传 → 报错退出

示例:
  python3 service_ticks.py --watchlist watchlist_tczt_20260101_20260831.csv
  python3 service_ticks.py --ts-codes 000006.SZ --trade-date 2026-08-28
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
    parser = argparse.ArgumentParser(description="Update 个股分笔成交(tdx → policy_ticks.db)")
    add_market_args(parser)
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_ticks.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    pairs, source = resolve_market_pairs(args)
    if source == "no_pairs" or not pairs:
        logger.error("[service_ticks] 必须传 --watchlist 或 (--ts-codes + 日期参数)")
        return 1
    logger.info(f"[service_ticks] 数据源: {source}, 共 {len(pairs)} 对")

    ensure_schema(db_kind="ticks", verbose=False)

    t0 = time.time()
    client = TdxClient()
    try:
        down = CNDataDown()
        stats = run_market_loop(
            down, pairs,
            update_method="update_ticks",
            client=client,
            rate=args.rate,
            force=args.force,
            logger=logger,
        )
        logger.info(
            f"[service_ticks] 完成: total={stats['total']} "
            f"inserted={stats['inserted']}({stats['inserted_rows']} 行) "
            f"skipped={stats['skipped']} failed={stats['failed']} "
            f"用时 {stats['elapsed']:.1f}s"
        )
        return 0
    except Exception as e:
        logger.error(f"[service_ticks] 失败: {e}", exc_info=True)
        return 1
    finally:
        try:
            client.api.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

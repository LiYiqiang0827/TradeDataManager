"""
service_intraday_index.py — 大盘指数分钟 K 更新(2026-09-17 新增)

数据源:tdx get_history_minute_time_data(对指数代码也直接支持)
落库:policy_minute.db / tbl_minute_index + tbl_minute_index_ctrl

特殊:
  - 不用 watchlist(指数是固定 5 个,跟具体股票无关)
  - 指数列表固定 INDEX_CODES_HERE(000001.SH/399001.SZ/399006.SZ/000688.SH/000016.SH)
  - 日期参数只有两种模式: --trade-date 单日 / --start-date~--end-date 日期段

示例:
  # 单日 5 指数
  python3 service_intraday_index.py --trade-date 2026-08-28

  # 日期段 5 指数 × N 个交易日
  python3 service_intraday_index.py --start-date 2026-08-26 --end-date 2026-08-28
"""
import sys, time, argparse
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT.parent / "coreClient"))

from core.offline_downloader import CNDataDown, INDEX_CODES_HERE
from core.offline_db_client import ensure_schema
from tdx_client import TdxClient
from service.common import (
    build_index_pairs, run_market_loop, add_market_args,
)


def main():
    parser = argparse.ArgumentParser(description="Update 大盘指数分钟 K(tdx → policy_minute.db tbl_minute_index)")
    # 指数 service 不需要 --watchlist / --ts-codes
    add_market_args(parser)
    # 把 --watchlist / --ts-codes 从 args 里删除(指数 service 不接受)
    args = parser.parse_args()
    if args.watchlist or args.ts_codes:
        logger.error("[service_intraday_index] 指数 service 不接受 --watchlist / --ts-codes,只需日期参数")
        return 1

    log_file = PROJECT_ROOT / "logs" / "service_intraday_index.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    pairs = build_index_pairs(args)
    if not pairs:
        logger.error("[service_intraday_index] 必须传 --trade-date 或 (--start-date + --end-date)")
        return 1
    logger.info(f"[service_intraday_index] 指数列表: {INDEX_CODES_HERE}, 共 {len(pairs)} 对 (5 指数 × {len(pairs)//len(INDEX_CODES_HERE)} 交易日)")

    ensure_schema(db_kind="minute_index", verbose=False)

    t0 = time.time()
    client = TdxClient()
    try:
        down = CNDataDown()
        stats = run_market_loop(
            down, pairs,
            update_method="update_minute_index",
            client=client,
            rate=args.rate,
            force=args.force,
            logger=logger,
        )
        logger.info(
            f"[service_intraday_index] 完成: total={stats['total']} "
            f"inserted={stats['inserted']}({stats['inserted_rows']} 行) "
            f"skipped={stats['skipped']} failed={stats['failed']} "
            f"用时 {stats['elapsed']:.1f}s"
        )
        return 0
    except Exception as e:
        logger.error(f"[service_intraday_index] 失败: {e}", exc_info=True)
        return 1
    finally:
        try:
            client.api.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

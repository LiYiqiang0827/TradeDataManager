"""TDX全市场1分钟OHLC历史替换与每日增量服务。

正式口径为09:31—11:30、13:01—15:00，每个完整交易日240根。从
2026-05-07起以TDX原生1分钟OHLC为主；更早的既有Parquet通过迁移命令删除
09:30并重排time_idx。TDX服务器历史覆盖不足的早期个股仅从已标准化CSV补齐，
并在清单中明确标记。下载先写staging，全部成功后才原子替换逐日正式分区。
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
import math
from pathlib import Path
import shutil
import sys
import threading
import time

import pandas as pd
from loguru import logger

OFFLINE_ROOT = Path(__file__).resolve().parents[2]
TRADING_AGENT_ROOT = OFFLINE_ROOT.parent
sys.path.insert(0, str(OFFLINE_ROOT / "scripts"))
sys.path.insert(0, str(TRADING_AGENT_ROOT))

from config.settings import ONE_MIN_CATALOG_PATH, ONE_MIN_DATA_DIR, ONE_MIN_PARQUET_ROOT
from core.offline_db_client import get_basic, get_conn, get_tradecal
from core.one_min_store import list_one_min_dates
from core.tdx_one_min_store import (
    TDX_ONE_MIN_START,
    ensure_tdx_one_min_catalog,
    finalize_tdx_one_min_day,
    get_tdx_one_min_sync,
    mark_tdx_one_min_sync,
    normalize_existing_one_min_partitions,
    remove_one_min_partition,
    stage_tdx_one_min_batch,
)
from coreClient.tdx_client import TdxClient
from service.common import latest_completed_trade_date


_THREAD_LOCAL = threading.local()
_CLIENTS: list[TdxClient] = []
_CLIENTS_LOCK = threading.Lock()


def _compact(value: str) -> str:
    text = str(value).strip().replace("-", "").replace("/", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"日期必须是YYYYMMDD或YYYY-MM-DD: {value}")
    datetime.strptime(text, "%Y%m%d")
    return text


def _latest_trade_date() -> str:
    conn = get_conn("basic")
    try:
        return latest_completed_trade_date(conn)
    finally:
        conn.close()


def _next_day(value: int | str) -> str:
    return (datetime.strptime(str(value), "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


def _load_universe(ts_codes: str | None) -> pd.DataFrame:
    basic = get_basic(
        list_status=["L", "P"], columns=["ts_code", "name", "list_date", "exchange"],
    )
    if ts_codes:
        wanted = {item.strip().upper() for item in ts_codes.split(",") if item.strip()}
        missing = sorted(wanted - set(basic["ts_code"].astype(str)))
        if missing:
            raise ValueError(f"股票代码不在本地上市股票表: {missing}")
        basic = basic[basic["ts_code"].isin(wanted)]
    return basic.sort_values("ts_code").reset_index(drop=True)


def _trade_dates(start: str, end: str) -> list[str]:
    calendar = get_tradecal(
        start_date=start, end_date=end, market="SSE", is_open=True, columns=["cal_date"],
    )
    return sorted({str(value) for value in calendar["cal_date"].tolist()})


def _max_pages(start_date: str, end_date: str, configured: int | None) -> int:
    if configured is not None:
        return configured
    natural_days = (
        datetime.strptime(end_date, "%Y%m%d") - datetime.strptime(start_date, "%Y%m%d")
    ).days + 1
    estimated_bars = math.ceil(natural_days / 365 * 250 * 240)
    return max(2, math.ceil(estimated_bars / 800) + 2)


def _thread_client() -> TdxClient:
    client = getattr(_THREAD_LOCAL, "tdx_client", None)
    if client is None:
        client = TdxClient()
        _THREAD_LOCAL.tdx_client = client
        with _CLIENTS_LOCK:
            _CLIENTS.append(client)
    return client


def _download_job(job: dict, rate: float) -> dict:
    client = _thread_client()
    try:
        rows = client.download_one_minute(
            job["code"], start_date=job["request_start"],
            end_date=job["desired_end"], max_pages=job["pages"],
        )
        if rate > 0:
            time.sleep(rate)
        return {**job, "rows": rows, "host": f"{client.ip}:{client.port}", "error": None}
    except Exception as exc:
        return {**job, "rows": [], "host": f"{client.ip}:{client.port}", "error": exc}


def _bounded_downloads(executor, jobs: list[dict], rate: float, limit: int):
    iterator = iter(jobs)
    pending = set()
    for _ in range(min(limit, len(jobs))):
        pending.add(executor.submit(_download_job, next(iterator), rate))
    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            yield future.result()
            try:
                pending.add(executor.submit(_download_job, next(iterator), rate))
            except StopIteration:
                pass


def _flush_stage(batch: list[dict], stage_root: Path, catalog: Path, stats: dict) -> None:
    if not batch:
        return
    result = stage_tdx_one_min_batch(batch, stage_root)
    stats["rows"] += int(result["rows"])
    for item in batch:
        mark_tdx_one_min_sync(
            item["code"], item["request_start"], item["desired_end"],
            rows=item["rows"], status="downloaded", source_host=item["host"],
            catalog_path=catalog,
        )
        stats["downloaded"] += 1
    batch.clear()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TDX → 全市场1分钟OHLC Parquet")
    parser.add_argument("--output-root", default=str(ONE_MIN_PARQUET_ROOT))
    parser.add_argument("--catalog", default=str(ONE_MIN_CATALOG_PATH))
    parser.add_argument("--stage-root", default=str(ONE_MIN_DATA_DIR / "tdx_stage"))
    parser.add_argument("--ts-codes", help="逗号分隔；不传则全部沪深北上市股票")
    parser.add_argument("--trade-date", help="单日模式")
    parser.add_argument("--start-date", default=TDX_ONE_MIN_START)
    parser.add_argument("--end-date", help="默认最近已完成交易日")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--write-batch-size", type=int, default=20)
    parser.add_argument("--rate", type=float, default=0.02)
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--force", action="store_true", help="忽略逐股票TDX断点并重新下载")
    parser.add_argument("--limit", type=int, help="仅供冒烟测试；不会写正式分区")
    parser.add_argument(
        "--normalize-history-before-cutoff", action="store_true",
        help="先把2026-05-07以前的既有Parquet删除09:30并重排为240根口径",
    )
    parser.add_argument("--keep-stage", action="store_true")
    args = parser.parse_args(argv)

    output_root = Path(args.output_root).expanduser()
    catalog = ensure_tdx_one_min_catalog(Path(args.catalog).expanduser())
    if args.normalize_history_before_cutoff:
        cutoff_end = (
            datetime.strptime(TDX_ONE_MIN_START, "%Y%m%d") - timedelta(days=1)
        ).strftime("%Y%m%d")
        logger.info(f"开始统一历史oneMin口径，截止{cutoff_end}")
        normalized = normalize_existing_one_min_partitions(
            end_date=cutoff_end, root=output_root, catalog_path=catalog,
        )
        logger.info(f"历史oneMin口径迁移完成: dates={len(normalized)}")

    if args.trade_date:
        start_date = end_date = _compact(args.trade_date)
    else:
        start_date = max(_compact(args.start_date), TDX_ONE_MIN_START)
        end_date = _compact(args.end_date) if args.end_date else _latest_trade_date()
    if start_date > end_date:
        raise ValueError(f"start_date不能晚于end_date: {start_date}>{end_date}")
    if not 1 <= args.workers <= 16:
        raise ValueError("workers必须在1到16之间")
    if not 1 <= args.write_batch_size <= 100:
        raise ValueError("write-batch-size必须在1到100之间")

    target_dates = _trade_dates(start_date, end_date)
    if not target_dates:
        logger.info(f"{start_date}..{end_date}没有交易日")
        return 0
    universe = _load_universe(args.ts_codes)
    universe = universe[universe["list_date"].fillna("99999999").astype(str) <= end_date]
    if args.limit:
        universe = universe.head(args.limit)
    if universe.empty:
        logger.warning("没有符合条件的股票")
        return 0

    if not args.limit and not args.ts_codes:
        existing_dates = {
            day for day in list_one_min_dates(output_root)
            if start_date <= day <= end_date
        }
        stale_dates = sorted(
            existing_dates - set(target_dates)
        )
        for day in stale_dates:
            remove_one_min_partition(
                day, root=output_root, catalog_path=catalog,
            )
        if stale_dates:
            logger.info(f"已删除{len(stale_dates)}个非交易日旧分区: {','.join(stale_dates)}")

    range_key = f"{start_date}_{end_date}"
    stage_root = Path(args.stage_root).expanduser() / range_key
    stage_root.mkdir(parents=True, exist_ok=True)
    sync = get_tdx_one_min_sync(catalog)
    sync_map = {str(row.ts_code): row for row in sync.itertuples(index=False)}
    jobs = []
    skipped = 0
    for item in universe.itertuples(index=False):
        code = str(item.ts_code)
        desired_start = max(start_date, str(item.list_date) if pd.notna(item.list_date) else start_date)
        desired_end = end_date
        request_start = desired_start
        current = sync_map.get(code)
        if current is not None and not args.force:
            covered_start = int(current.covered_start) if pd.notna(current.covered_start) else None
            covered_end = int(current.covered_end) if pd.notna(current.covered_end) else None
            if (
                covered_start is not None and covered_end is not None
                and covered_start <= int(desired_start) and covered_end >= int(desired_end)
                and str(current.status) == "downloaded"
            ):
                skipped += 1
                continue
            if (
                covered_start is not None and covered_end is not None
                and covered_start <= int(desired_start) and covered_end < int(desired_end)
            ):
                request_start = _next_day(covered_end)
        jobs.append({
            "code": code, "name": item.name,
            "request_start": request_start, "desired_end": desired_end,
            "pages": _max_pages(request_start, desired_end, args.max_pages),
        })

    logger.info(
        f"[service_oneMinute] TDX开始 symbols={len(universe)} jobs={len(jobs)} "
        f"skipped={skipped} range={start_date}..{end_date} stage={stage_root}"
    )
    staged_before = sorted(
        path.name.split("=", 1)[1]
        for path in stage_root.glob("trade_date=*") if path.is_dir()
    )
    if not jobs and not staged_before:
        try:
            stage_root.rmdir()
        except OSError:
            pass
        logger.info("TDX oneMin已经覆盖到目标日期，没有新增数据")
        return 0
    stats = {"downloaded": 0, "empty": 0, "failed": 0, "rows": 0}
    started = time.time()
    batch: list[dict] = []
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for finished, outcome in enumerate(
                _bounded_downloads(executor, jobs, args.rate, max(args.workers * 2, 1)), start=1,
            ):
                if outcome["error"] is not None:
                    stats["failed"] += 1
                    mark_tdx_one_min_sync(
                        outcome["code"], outcome["request_start"], outcome["desired_end"],
                        status="failed", source_host=outcome["host"],
                        error=f"{type(outcome['error']).__name__}: {outcome['error']}",
                        catalog_path=catalog,
                    )
                elif not outcome["rows"]:
                    single_day = outcome["request_start"] == outcome["desired_end"]
                    status = "empty" if single_day else "tdx_empty"
                    stats["empty" if single_day else "failed"] += 1
                    mark_tdx_one_min_sync(
                        outcome["code"], outcome["request_start"], outcome["desired_end"],
                        status=status, source_host=outcome["host"],
                        error=None if single_day else "TDX长区间返回空数据",
                        catalog_path=catalog,
                    )
                else:
                    batch.append(outcome)
                    if len(batch) >= args.write_batch_size:
                        _flush_stage(batch, stage_root, catalog, stats)
                if finished % 100 == 0 or finished == len(jobs):
                    logger.info(
                        f"progress={finished}/{len(jobs)} downloaded={stats['downloaded']} "
                        f"empty={stats['empty']} failed={stats['failed']} rows={stats['rows']}"
                    )
        _flush_stage(batch, stage_root, catalog, stats)
    finally:
        for client in _CLIENTS:
            client.disconnect()

    if stats["failed"]:
        logger.error(f"存在{stats['failed']}只下载失败；保留staging供下次断点续传，不替换正式分区")
        return 1
    if args.limit or args.ts_codes:
        logger.info("部分股票模式仅完成下载与staging，不替换全市场正式分区")
        return 0

    hosts = sorted({
        str(value) for value in get_tdx_one_min_sync(catalog)["source_host"].dropna().tolist()
    })
    final_dates = [
        day for day in target_dates if (stage_root / f"trade_date={day}").is_dir()
    ]
    for index, day in enumerate(final_dates, start=1):
        result = finalize_tdx_one_min_day(
            day, stage_root, root=output_root, catalog_path=catalog,
            source_host=",".join(hosts),
        )
        logger.info(
            f"finalize {index}/{len(final_dates)} {day}: rows={result['rows']} "
            f"stocks={result['stock_count']} bars={result['bars_per_stock_min']}.."
            f"{result['bars_per_stock_max']}"
        )
    if not args.keep_stage:
        shutil.rmtree(stage_root)
    logger.info(
        f"[service_oneMinute] 完成 elapsed={time.time()-started:.1f}s stats={stats} "
        f"dates={len(final_dates)} output={output_root}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

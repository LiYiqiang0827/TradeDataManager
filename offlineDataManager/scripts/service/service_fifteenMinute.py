"""TDX原生15分钟K线全量回补与每日增量服务。

默认从2024-09-01回补全部沪深A股；已完成股票通过sync表跳过，之后无参数运行
只会补到最近一个已收盘交易日。数据写入DuckDB ``db_fifteenMinute.db``。
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
import math
from pathlib import Path
import sys
import threading
import time

import pandas as pd
from loguru import logger

OFFLINE_ROOT = Path(__file__).resolve().parents[2]
TRADING_AGENT_ROOT = OFFLINE_ROOT.parent
sys.path.insert(0, str(OFFLINE_ROOT / "scripts"))
sys.path.insert(0, str(TRADING_AGENT_ROOT))

from config.settings import FIFTEEN_MIN_DB_PATH
from core.offline_db_client import (
    FifteenMinuteWriter,
    get_adj_factor,
    get_basic,
    get_conn,
    get_fifteen_min_stats,
    get_fifteen_min_status,
    init_fifteen_min_db,
    mark_fifteen_min_status,
    optimize_fifteen_min_db,
)
from coreClient.tdx_client import TdxClient
from service.common import latest_completed_trade_date


DEFAULT_START_DATE = "20240901"
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


def _load_universe(ts_codes: str | None, include_bj: bool) -> pd.DataFrame:
    basic = get_basic(
        list_status=["L", "P"],
        columns=["ts_code", "name", "list_date", "exchange"],
    )
    if not include_bj:
        basic = basic[basic["exchange"].isin(["SSE", "SZSE"])]
    if ts_codes:
        wanted = {item.strip().upper() for item in ts_codes.split(",") if item.strip()}
        missing = sorted(wanted - set(basic["ts_code"].astype(str)))
        if missing:
            raise ValueError(f"股票代码不在本地上市股票表: {missing}")
        basic = basic[basic["ts_code"].isin(wanted)]
    return basic.sort_values("ts_code").reset_index(drop=True)


def _factor_map(ts_code: str, start_date: str, end_date: str) -> dict[str, float]:
    factors = get_adj_factor(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        columns=["trade_date", "adj_factor"],
    )
    if factors is None or factors.empty:
        return {}
    return {
        str(row.trade_date).replace("-", ""): float(row.adj_factor)
        for row in factors.itertuples(index=False)
        if pd.notna(row.adj_factor)
    }


def _repair_local_adj_factors(
    writer: FifteenMinuteWriter,
    start_date: str,
    end_date: str,
    ts_codes: list[str] | None = None,
    *,
    batch_days: int = 60,
) -> dict:
    """核验请求区间内已有分钟日期，分批同步同日因子，包含无下载任务时。

    每批最多60个已有交易日，避免一次读取整个本地因子表或逐日反复扫描
    分钟大表。使用writer现有连接；读取SQLite因子与DuckDB更新严格串行。
    """
    if batch_days < 1:
        raise ValueError("batch_days必须为正数")
    result = {"dates_checked": 0, "batches": 0, "source_rows": 0,
              "valid_factor_keys": 0, "ignored_invalid_factor_rows": 0,
              "updated_rows": 0, "remaining_null_rows": 0}
    if ts_codes == []:
        return result
    where_sql = "trade_date BETWEEN ? AND ?"
    params: list = [int(start_date), int(end_date)]
    if ts_codes is not None:
        where_sql += " AND ts_code IN (" + ",".join("?" for _ in ts_codes) + ")"
        params.extend(ts_codes)
    dates = [int(row[0]) for row in writer.connection.execute(
        f"SELECT DISTINCT trade_date FROM fifteen_min_days WHERE {where_sql} ORDER BY trade_date",
        params,
    ).fetchall()]
    if not dates:
        return result
    factor_conn = get_conn("basic")
    try:
        for offset in range(0, len(dates), batch_days):
            batch = dates[offset:offset + batch_days]
            factors = get_adj_factor(
                start_date=str(batch[0]), end_date=str(batch[-1]),
                ts_codes=ts_codes, columns=["ts_code", "trade_date", "adj_factor"],
                conn=factor_conn,
            )
            # A calendar interval may contain dates absent from the minute store.
            if not factors.empty:
                factor_dates = factors["trade_date"].astype(str).str.replace("-", "", regex=False)
                factors = factors[factor_dates.isin({str(day) for day in batch})]
            repaired = writer.repair_adj_factors(factors)
            for key in ("source_rows", "valid_factor_keys", "ignored_invalid_factor_rows", "updated_rows"):
                result[key] += int(repaired[key])
            result["dates_checked"] += len(batch)
            result["batches"] += 1
    finally:
        factor_conn.close()
    result["remaining_null_rows"] = int(writer.connection.execute(
        f"SELECT count(*) FROM fifteen_min_bars WHERE {where_sql} AND adj_factor IS NULL",
        params,
    ).fetchone()[0])
    return result


def _max_pages(start_date: str, end_date: str, configured: int | None) -> int:
    if configured is not None:
        return configured
    natural_days = (datetime.strptime(end_date, "%Y%m%d") -
                    datetime.strptime(start_date, "%Y%m%d")).days + 1
    estimated_bars = math.ceil(natural_days / 365 * 250 * 16)
    return max(3, math.ceil(estimated_bars / 800) + 3)


def _next_day(value: int | str) -> str:
    return (datetime.strptime(str(value), "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


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
        rows = client.download_fifteen_minute(
            job["code"],
            start_date=job["request_start"],
            end_date=job["desired_end"],
            max_pages=job["pages"],
        )
        if rate > 0:
            time.sleep(rate)
        return {
            **job,
            "rows": rows,
            "host": f"{client.ip}:{client.port}",
            "error": None,
        }
    except Exception as exc:
        return {
            **job,
            "rows": [],
            "host": f"{client.ip}:{client.port}",
            "error": exc,
        }


def _bounded_downloads(executor, jobs: list[dict], rate: float, limit: int):
    """限制在途Future数量，避免数千只股票结果同时驻留内存。"""
    iterator = iter(jobs)
    pending = set()
    for _ in range(min(limit, len(jobs))):
        pending.add(executor.submit(_download_job, next(iterator), rate))
    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            yield future.result()
            try:
                job = next(iterator)
            except StopIteration:
                continue
            pending.add(executor.submit(_download_job, job, rate))


def _flush_write_batch(
    writer, batch: list[dict], database: Path, stats: dict, *, append_only: bool,
) -> None:
    if not batch:
        return
    try:
        result = writer.upsert_batch(batch, append_only=append_only)
        stats["downloaded"] += len(batch)
        stats["rows"] += int(result["rows"])
    except Exception:
        logger.exception(f"批量落库失败，回退逐股写入: symbols={len(batch)}")
        for item in batch:
            try:
                result = writer.upsert(
                    item["rows"],
                    requested_start=item["requested_start"],
                    requested_end=item["requested_end"],
                    name=item.get("name"),
                    adj_factors=item.get("adj_factors"),
                    source_host=item.get("source_host"),
                    replace=True,
                    append_only=append_only,
                )
                stats["downloaded"] += 1
                stats["rows"] += int(result["rows"])
            except Exception as exc:
                stats["failed"] += 1
                mark_fifteen_min_status(
                    item["code"], item["requested_start"], item["requested_end"],
                    status="failed", source_host=item.get("source_host"),
                    error=f"{type(exc).__name__}: {exc}", data_root=database,
                )
                logger.exception(f"{item['code']} 逐股回退落库仍失败")
    batch.clear()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TDX → DuckDB 全市场15分钟K线")
    parser.add_argument("--database", default=str(FIFTEEN_MIN_DB_PATH))
    parser.add_argument("--ts-codes", help="逗号分隔；不传则全部上市股票")
    parser.add_argument("--trade-date", help="单日模式")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", help="默认最近已完成交易日")
    parser.add_argument("--include-bj", action="store_true", help="包含北交所股票")
    parser.add_argument("--force", action="store_true", help="忽略sync覆盖区间并重拉")
    parser.add_argument("--max-pages", type=int, help="TDX单股最大分页数；默认按日期跨度计算")
    parser.add_argument("--workers", type=int, default=4, help="并行TDX连接数，默认4")
    parser.add_argument("--write-batch-size", type=int, default=10, help="每个DuckDB事务写入的股票数")
    parser.add_argument("--rate", type=float, default=0.02, help="每只股票完成后的间隔秒数")
    parser.add_argument("--limit", type=int, help="只处理前N只，供冒烟验证")
    parser.add_argument("--stats-only", action="store_true")
    parser.add_argument("--optimize", action="store_true", help="完成后物理排序并创建查询索引")
    args = parser.parse_args(argv)

    database = Path(args.database).expanduser()
    init_fifteen_min_db(database)
    if args.stats_only:
        logger.info(get_fifteen_min_stats(database))
        return 0

    if args.trade_date:
        start_date = end_date = _compact(args.trade_date)
    else:
        start_date = _compact(args.start_date)
        end_date = _compact(args.end_date) if args.end_date else _latest_trade_date()
    if start_date > end_date:
        raise ValueError(f"start_date不能晚于end_date: {start_date}>{end_date}")
    if args.workers < 1 or args.workers > 16:
        raise ValueError("workers必须在1到16之间")
    if args.write_batch_size < 1 or args.write_batch_size > 100:
        raise ValueError("write-batch-size必须在1到100之间")

    universe = _load_universe(args.ts_codes, args.include_bj)
    universe = universe[universe["list_date"].fillna("99999999").astype(str) <= end_date]
    if args.limit:
        universe = universe.head(args.limit)
    if universe.empty:
        logger.warning("没有符合条件的股票")
        return 0

    sync = get_fifteen_min_status(database)
    sync_map = {str(row.ts_code): row for row in sync.itertuples(index=False)}
    log_path = OFFLINE_ROOT / "logs" / "service_fifteenMinute.log"
    logger.add(str(log_path), rotation="50 MB", level="INFO", enqueue=True)
    logger.info(
        f"[service_fifteenMinute] TDX开始: symbols={len(universe)} "
        f"range={start_date}..{end_date} database={database}"
    )

    stats = {"downloaded": 0, "skipped": 0, "empty": 0, "failed": 0, "rows": 0}
    started = time.time()
    jobs = []
    for item in universe.itertuples(index=False):
        code = str(item.ts_code)
        name = str(item.name) if pd.notna(item.name) else None
        list_date = str(item.list_date) if pd.notna(item.list_date) else start_date
        desired_start = max(start_date, list_date)
        desired_end = end_date
        request_start = desired_start
        current = sync_map.get(code)
        append_only = current is None and not args.force
        if current is not None and not args.force:
            covered_start = int(current.covered_start) if pd.notna(current.covered_start) else None
            covered_end = int(current.covered_end) if pd.notna(current.covered_end) else None
            if (covered_start is not None and covered_end is not None and
                    covered_start <= int(desired_start) and covered_end >= int(desired_end) and
                    str(current.status) in {"downloaded", "empty"}):
                stats["skipped"] += 1
                continue
            if (covered_start is not None and covered_end is not None and
                    covered_start <= int(desired_start) and covered_end < int(desired_end)):
                request_start = _next_day(covered_end)
        jobs.append({
            "code": code,
            "name": name,
            "request_start": request_start,
            "desired_end": desired_end,
            "pages": _max_pages(request_start, desired_end, args.max_pages),
            "append_only": append_only,
        })

    try:
        with FifteenMinuteWriter(database) as writer, ThreadPoolExecutor(
            max_workers=args.workers
        ) as executor:
            write_batches: dict[bool, list[dict]] = {True: [], False: []}
            outcomes = _bounded_downloads(
                executor, jobs, args.rate, limit=max(args.workers * 2, 1),
            )
            for finished, outcome in enumerate(outcomes, start=1):
                code = outcome["code"]
                name = outcome["name"]
                request_start = outcome["request_start"]
                desired_end = outcome["desired_end"]
                append_only = bool(outcome["append_only"])
                host = outcome["host"]
                rows = outcome["rows"]
                exc = outcome["error"]
                processed = stats["skipped"] + finished
                if exc is not None:
                    stats["failed"] += 1
                    mark_fifteen_min_status(
                        code, request_start, desired_end,
                        status="failed", source_host=host,
                        error=f"{type(exc).__name__}: {exc}", data_root=database,
                    )
                    logger.error(f"{code} {request_start}..{desired_end} 下载失败: {exc}")
                    continue

                if not rows:
                    # 单个交易日无K线通常表示停牌；把该日记为已覆盖，避免每日
                    # 调度永久重试。较长区间整体为空仍视为上游异常并保留重试。
                    empty_status = "empty" if request_start == desired_end else "tdx_empty"
                    mark_fifteen_min_status(
                        code, request_start, desired_end,
                        status=empty_status, source_host=host,
                        error="TDX返回空", data_root=database,
                    )
                    stats["empty"] += 1
                else:
                    write_batch = write_batches[append_only]
                    write_batch.append({
                        "code": code,
                        "rows": rows,
                        "requested_start": request_start,
                        "requested_end": desired_end,
                        "name": name,
                        "adj_factors": _factor_map(code, request_start, desired_end),
                        "source_host": host,
                    })
                    if len(write_batch) >= args.write_batch_size:
                        _flush_write_batch(
                            writer, write_batch, database, stats,
                            append_only=append_only,
                        )

                if processed % 50 == 0 or finished == len(jobs):
                    logger.info(
                        f"进度 {processed}/{len(universe)} downloaded={stats['downloaded']} "
                        f"skipped={stats['skipped']} empty={stats['empty']} "
                        f"failed={stats['failed']} rows={stats['rows']}"
                    )
            for append_only, write_batch in write_batches.items():
                _flush_write_batch(
                    writer, write_batch, database, stats,
                    append_only=append_only,
                )
            # sync已覆盖也必须执行：上次可能在日因子就绪前下载，或本地因子
            # 随后被修正。原始OHLCVA及TDX断点不变，不用未知因子填1。
            factor_repair = _repair_local_adj_factors(
                writer, start_date, end_date,
                ts_codes=universe["ts_code"].astype(str).tolist()
                if args.ts_codes or args.limit else None,
            )
            logger.info(f"[service_fifteenMinute] 同日复权因子核验: {factor_repair}")
    finally:
        with _CLIENTS_LOCK:
            clients = list(_CLIENTS)
            _CLIENTS.clear()
        for client in clients:
            try:
                client.api.disconnect()
            except Exception:
                pass

    elapsed = time.time() - started
    final_stats = get_fifteen_min_stats(database)
    optimized = None
    if args.optimize and stats["failed"] == 0:
        optimized = optimize_fifteen_min_db(database)
    logger.info(
        f"[service_fifteenMinute] 完成 elapsed={elapsed:.1f}s run={stats} "
        f"db={final_stats} factor_repair={factor_repair} optimized={optimized}"
    )
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

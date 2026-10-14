"""Check completeness of the core daily stock tables.

The public entry point is :func:`check_database`.  It checks only Shanghai
Stock Exchange open days from ``tbl_cn_tradecal`` and reports dates that should
be downloaded again.  Three table-specific helpers are also public:

* :func:`check_daily_data`
* :func:`check_daily_basic_data`
* :func:`check_adj_factor_data`

Examples::

    from core.check_database import check_database

    report = check_database(
        ["daily", "daily_basic", "adj_factor"],
        start_date="20260901",
        end_date="20260915",
    )

    one_stock = check_database(
        "daily",
        trade_date="20260910",
        ts_code="000001.SZ",
    )

The unified API repairs detected dates by default, then checks them again.  A
successful daily/adjustment-factor repair also rebuilds adjusted and original
weekly/monthly tables.  Pass ``auto_repair=False`` (or CLI ``--no-repair``) for
a read-only audit.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sqlite3
import sys
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any


SCRIPTS_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = SCRIPTS_ROOT.parent.parent
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from config.settings import DB_PATH_BASIC  # noqa: E402


DEFAULT_NEIGHBOR_RATIO_THRESHOLD = 0.90

TABLE_CONFIGS = {
    "daily": {
        "table_name": "tbl_cn_day",
        "service": "service_daily.py",
        "update_method": "update_daily",
    },
    "daily_basic": {
        "table_name": "tbl_cn_daily_basic",
        "service": "service_daily_basic.py",
        "update_method": "update_daily_basic",
    },
    "adj_factor": {
        "table_name": "tbl_cn_adj_factor",
        "service": "service_adj_factor.py",
        "update_method": "update_adj_factor",
    },
}

TABLE_ALIASES = {
    "daily": "daily",
    "day": "daily",
    "cn_daily": "daily",
    "tbl_cn_day": "daily",
    "日线": "daily",
    "dailybasic": "daily_basic",
    "daily_basic": "daily_basic",
    "cn_daily_basic": "daily_basic",
    "tbl_cn_daily_basic": "daily_basic",
    "每日指标": "daily_basic",
    "adjfactor": "adj_factor",
    "adj_factor": "adj_factor",
    "cn_adj_factor": "adj_factor",
    "tbl_cn_adj_factor": "adj_factor",
    "复权因子": "adj_factor",
}


def _normalize_date(value: str, field_name: str) -> str:
    text = str(value).strip().replace("-", "")
    try:
        parsed = datetime.strptime(text, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} 必须是 YYYYMMDD 或 YYYY-MM-DD: {value}") from exc
    return parsed.strftime("%Y%m%d")


def _resolve_date_range(
    *,
    start_date: str | None,
    end_date: str | None,
    trade_date: str | None,
) -> tuple[str, str]:
    if trade_date is not None:
        if start_date is not None or end_date is not None:
            raise ValueError("trade_date 不能与 start_date/end_date 同时使用")
        normalized = _normalize_date(trade_date, "trade_date")
        return normalized, normalized

    if start_date is None or end_date is None:
        raise ValueError("必须提供 trade_date，或者同时提供 start_date 和 end_date")

    start = _normalize_date(start_date, "start_date")
    end = _normalize_date(end_date, "end_date")
    if start > end:
        raise ValueError(f"start_date 不能晚于 end_date: {start} > {end}")
    return start, end


def _resolve_date_aliases(
    *,
    start_date: str | None,
    end_date: str | None,
    trade_date: str | None,
    startdate: str | None,
    enddate: str | None,
    tradedate: str | None,
) -> tuple[str, str]:
    pairs = (
        ("start_date", start_date, "startdate", startdate),
        ("end_date", end_date, "enddate", enddate),
        ("trade_date", trade_date, "tradedate", tradedate),
    )
    for primary_name, primary, alias_name, alias in pairs:
        if primary is not None and alias is not None:
            raise ValueError(f"{primary_name} 和 {alias_name} 不能同时使用")
    return _resolve_date_range(
        start_date=start_date if start_date is not None else startdate,
        end_date=end_date if end_date is not None else enddate,
        trade_date=trade_date if trade_date is not None else tradedate,
    )


def _normalize_table_names(table_names: str | Iterable[str]) -> list[str]:
    if isinstance(table_names, str):
        raw_names = [part.strip() for part in table_names.split(",") if part.strip()]
    else:
        raw_names = [str(name).strip() for name in table_names if str(name).strip()]
    if not raw_names:
        raise ValueError("table_names 不能为空")

    normalized: list[str] = []
    for raw_name in raw_names:
        alias = raw_name.lower() if raw_name.isascii() else raw_name
        table_key = TABLE_ALIASES.get(alias)
        if table_key is None:
            supported = ", ".join(TABLE_CONFIGS)
            raise ValueError(f"暂不支持表 {raw_name!r}；当前支持: {supported}")
        if table_key not in normalized:
            normalized.append(table_key)
    return normalized


def _normalize_ts_codes(
    ts_code: str | Sequence[str] | None,
) -> list[str] | None:
    if ts_code is None:
        return None
    if isinstance(ts_code, str):
        values = ts_code.split(",")
    else:
        values = ts_code
    normalized = sorted({str(value).strip().upper() for value in values if str(value).strip()})
    return normalized or None


def _resolve_ts_code_alias(
    ts_code: str | Sequence[str] | None,
    tscode: str | Sequence[str] | None,
) -> list[str] | None:
    if ts_code is not None and tscode is not None:
        raise ValueError("ts_code 和 tscode 是同一参数的别名，不能同时使用")
    return _normalize_ts_codes(ts_code if ts_code is not None else tscode)


def _open_trade_dates(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> tuple[list[str], str | None, str | None]:
    coverage = conn.execute(
        "SELECT MIN(cal_date), MAX(cal_date) FROM tbl_cn_tradecal WHERE exchange = 'SSE'"
    ).fetchone()
    if not coverage or not coverage[0] or not coverage[1]:
        raise RuntimeError("tbl_cn_tradecal 没有 SSE 交易日历数据")
    if start_date < str(coverage[0]) or end_date > str(coverage[1]):
        raise RuntimeError(
            "检查日期超出本地 SSE 交易日历范围: "
            f"请求 {start_date}~{end_date}，本地 {coverage[0]}~{coverage[1]}"
        )

    rows = conn.execute(
        "SELECT cal_date FROM tbl_cn_tradecal "
        "WHERE exchange = 'SSE' AND is_open = 1 AND cal_date BETWEEN ? AND ? "
        "ORDER BY cal_date",
        (start_date, end_date),
    ).fetchall()
    dates = [str(row[0]) for row in rows]
    if not dates:
        return [], None, None

    previous_row = conn.execute(
        "SELECT MAX(cal_date) FROM tbl_cn_tradecal "
        "WHERE exchange = 'SSE' AND is_open = 1 AND cal_date < ?",
        (dates[0],),
    ).fetchone()
    next_row = conn.execute(
        "SELECT MIN(cal_date) FROM tbl_cn_tradecal "
        "WHERE exchange = 'SSE' AND is_open = 1 AND cal_date > ?",
        (dates[-1],),
    ).fetchone()
    previous_date = str(previous_row[0]) if previous_row and previous_row[0] else None
    next_date = str(next_row[0]) if next_row and next_row[0] else None
    return dates, previous_date, next_date


def _count_rows_by_date(
    conn: sqlite3.Connection,
    table_name: str,
    dates: Sequence[str],
    ts_codes: Sequence[str] | None,
) -> dict[str, int]:
    if not dates:
        return {}
    counts = {date: 0 for date in dates}
    requested_dates = set(dates)
    code_batches: list[Sequence[str] | None]
    if ts_codes:
        # Keep well below SQLite builds that use a 999-variable limit.
        code_batches = [ts_codes[index : index + 800] for index in range(0, len(ts_codes), 800)]
    else:
        code_batches = [None]

    for code_batch in code_batches:
        params: list[Any] = [min(dates), max(dates)]
        where = "trade_date BETWEEN ? AND ?"
        if code_batch:
            code_placeholders = ",".join("?" for _ in code_batch)
            where += f" AND ts_code IN ({code_placeholders})"
            params.extend(code_batch)
        rows = conn.execute(
            f"SELECT trade_date, COUNT(*) FROM {table_name} "
            f"WHERE {where} GROUP BY trade_date",
            params,
        ).fetchall()
        for date, count in rows:
            date = str(date)
            if date in requested_dates:
                counts[date] += int(count)
    return counts


def _missing_codes_by_date(
    conn: sqlite3.Connection,
    table_name: str,
    dates: Sequence[str],
    ts_codes: Sequence[str],
) -> dict[str, list[str]]:
    if not dates:
        return {}
    present: dict[str, set[str]] = {date: set() for date in dates}
    requested_dates = set(dates)
    for index in range(0, len(ts_codes), 800):
        code_batch = ts_codes[index : index + 800]
        code_placeholders = ",".join("?" for _ in code_batch)
        rows = conn.execute(
            f"SELECT trade_date, ts_code FROM {table_name} "
            "WHERE trade_date BETWEEN ? AND ? "
            f"AND ts_code IN ({code_placeholders})",
            [min(dates), max(dates), *code_batch],
        ).fetchall()
        for date, code in rows:
            date = str(date)
            if date in requested_dates:
                present[date].add(str(code).upper())
    expected = set(ts_codes)
    return {date: sorted(expected - present[date]) for date in dates}


def _check_table(
    table_key: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    trade_date: str | None = None,
    startdate: str | None = None,
    enddate: str | None = None,
    tradedate: str | None = None,
    ts_code: str | Sequence[str] | None = None,
    tscode: str | Sequence[str] | None = None,
    neighbor_ratio_threshold: float = DEFAULT_NEIGHBOR_RATIO_THRESHOLD,
    db_path: str | Path = DB_PATH_BASIC,
    connection: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    if not 0 < neighbor_ratio_threshold <= 1:
        raise ValueError("neighbor_ratio_threshold 必须在 (0, 1] 范围内")

    start, end = _resolve_date_aliases(
        start_date=start_date,
        end_date=end_date,
        trade_date=trade_date,
        startdate=startdate,
        enddate=enddate,
        tradedate=tradedate,
    )
    ts_codes = _resolve_ts_code_alias(ts_code, tscode)
    config = TABLE_CONFIGS[table_key]
    conn = connection or sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    close_connection = connection is None

    try:
        open_dates, previous_date, next_date = _open_trade_dates(conn, start, end)
        context_dates = [date for date in [previous_date, *open_dates, next_date] if date]
        counts = _count_rows_by_date(
            conn,
            config["table_name"],
            context_dates,
            ts_codes,
        )
        missing_codes = (
            _missing_codes_by_date(conn, config["table_name"], open_dates, ts_codes)
            if ts_codes
            else {}
        )

        day_results: list[dict[str, Any]] = []
        redownload_dates: list[str] = []
        for index, date in enumerate(open_dates):
            prior = previous_date if index == 0 else open_dates[index - 1]
            following = next_date if index == len(open_dates) - 1 else open_dates[index + 1]
            prior_count = counts.get(prior, 0) if prior else None
            following_count = counts.get(following, 0) if following else None
            neighbor_counts = [
                count for count in (prior_count, following_count) if count is not None
            ]
            neighbor_baseline = max(neighbor_counts, default=0)
            count = counts.get(date, 0)
            ratio = count / neighbor_baseline if neighbor_baseline > 0 else None

            reasons: list[str] = []
            if count == 0:
                reasons.append("missing_open_day")
            if ts_codes and missing_codes[date]:
                reasons.append("missing_ts_codes")
            if (
                count > 0
                and neighbor_baseline > 0
                and ratio is not None
                and ratio < neighbor_ratio_threshold
            ):
                reasons.append("low_count_vs_neighbors")

            needs_redownload = bool(reasons)
            if needs_redownload:
                redownload_dates.append(date)
            day_results.append(
                {
                    "trade_date": date,
                    "row_count": count,
                    "previous_trade_date": prior,
                    "previous_row_count": prior_count,
                    "next_trade_date": following,
                    "next_row_count": following_count,
                    "neighbor_baseline": neighbor_baseline or None,
                    "neighbor_ratio": round(ratio, 6) if ratio is not None else None,
                    "missing_ts_codes": missing_codes.get(date, []),
                    "needs_redownload": needs_redownload,
                    "reasons": reasons,
                }
            )

        return {
            "table": table_key,
            "table_name": config["table_name"],
            "repair_service": config["service"],
            "start_date": start,
            "end_date": end,
            "ts_codes": ts_codes,
            "neighbor_ratio_threshold": neighbor_ratio_threshold,
            "checked_trade_dates": len(open_dates),
            "redownload_dates": redownload_dates,
            "ok": not redownload_dates,
            "days": day_results,
        }
    finally:
        if close_connection:
            conn.close()


def check_daily_data(**kwargs: Any) -> dict[str, Any]:
    """Check ``tbl_cn_day`` and return dates that need another download."""
    return _check_table("daily", **kwargs)


def check_daily_basic_data(**kwargs: Any) -> dict[str, Any]:
    """Check ``tbl_cn_daily_basic`` and return repair candidates."""
    return _check_table("daily_basic", **kwargs)


def check_adj_factor_data(**kwargs: Any) -> dict[str, Any]:
    """Check ``tbl_cn_adj_factor`` and return repair candidates."""
    return _check_table("adj_factor", **kwargs)


CHECK_FUNCTIONS = {
    "daily": check_daily_data,
    "daily_basic": check_daily_basic_data,
    "adj_factor": check_adj_factor_data,
}


def check_database(
    table_names: str | Iterable[str],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    trade_date: str | None = None,
    startdate: str | None = None,
    enddate: str | None = None,
    tradedate: str | None = None,
    ts_code: str | Sequence[str] | None = None,
    tscode: str | Sequence[str] | None = None,
    neighbor_ratio_threshold: float = DEFAULT_NEIGHBOR_RATIO_THRESHOLD,
    auto_repair: bool = True,
    regenerate_derived: bool = True,
    db_path: str | Path = DB_PATH_BASIC,
    connection: sqlite3.Connection | None = None,
    downloader: Any | None = None,
) -> dict[str, Any]:
    """Run one or more supported database checks through a unified API.

    Args:
        table_names: One name or an iterable.  Supported canonical names are
            ``daily``, ``daily_basic`` and ``adj_factor``; physical table names
            and common aliases are accepted too.
        start_date/end_date: Inclusive date range.  Both are required unless
            ``trade_date`` is used.  ``startdate``/``enddate`` are aliases.
        trade_date: Single-date mode; mutually exclusive with the range.
            ``tradedate`` is an alias.
        ts_code/tscode: One code, a comma-separated string, a sequence, or ``None``
            for all stocks.  In a code-scoped check, any requested code absent
            on an SSE open day is reported explicitly.
        neighbor_ratio_threshold: A positive row count below this fraction of
            the larger adjacent-trading-day count is considered incomplete.
        auto_repair: Download every detected table/date again and verify the
            result.  Enabled by default.
        regenerate_derived: After a successful daily/adj-factor repair and
            verification, rebuild weekly and monthly adjusted/original tables.
            Enabled by default.
        db_path: Basic SQLite database path.
        connection: Optional existing connection, mainly for callers that
            manage their own transaction or tests.  It is never closed here.
        downloader: Optional downloader object implementing ``update_daily``,
            ``update_daily_basic`` and/or ``update_adj_factor``.  When omitted,
            a ``CNDataDown`` instance is created lazily only if repair is needed.
    """
    tables = _normalize_table_names(table_names)
    # Validate shared inputs once, before opening the same DB for each table.
    start, end = _resolve_date_aliases(
        start_date=start_date,
        end_date=end_date,
        trade_date=trade_date,
        startdate=startdate,
        enddate=enddate,
        tradedate=tradedate,
    )
    ts_codes = _resolve_ts_code_alias(ts_code, tscode)

    initial_reports: dict[str, dict[str, Any]] = {}
    own_connection = connection is None
    own_downloader = False
    derived_regeneration = {
        "requested": False,
        "status": "not_needed",
        "trigger_tables": [],
        "reason": None,
        "week": None,
        "month": None,
    }
    conn = connection or sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        for table_key in tables:
            initial_reports[table_key] = CHECK_FUNCTIONS[table_key](
                start_date=start,
                end_date=end,
                ts_code=ts_codes,
                neighbor_ratio_threshold=neighbor_ratio_threshold,
                db_path=db_path,
                connection=conn,
            )

        initial_redownload_dates = {
            table_key: report["redownload_dates"]
            for table_key, report in initial_reports.items()
        }
        has_issues = any(initial_redownload_dates.values())
        repair_attempts: dict[str, list[dict[str, Any]]] = {
            table_key: [] for table_key in tables
        }
        if auto_repair and has_issues:
            if downloader is None:
                if Path(db_path).resolve() != Path(DB_PATH_BASIC).resolve():
                    raise ValueError(
                        "自定义 db_path 自动修复时必须传入 downloader，避免写入默认数据库"
                    )
                _load_project_env()
                from core.offline_downloader import CNDataDown

                downloader = CNDataDown()
                own_downloader = True

            for table_key in tables:
                if not initial_redownload_dates[table_key]:
                    continue
                update_method_name = TABLE_CONFIGS[table_key]["update_method"]
                update_method = getattr(downloader, update_method_name)
                for date in initial_redownload_dates[table_key]:
                    attempt = {
                        "trade_date": date,
                        "update_method": update_method_name,
                        "success": False,
                        "inserted_rows": None,
                        "error": None,
                    }
                    try:
                        inserted = update_method(start_date=date, end_date=date)
                        attempt["inserted_rows"] = int(inserted or 0)
                        attempt["success"] = True
                    except Exception as exc:  # keep repairing independent dates/tables
                        attempt["error"] = f"{type(exc).__name__}: {exc}"
                    repair_attempts[table_key].append(attempt)

        if auto_repair and has_issues:
            reports = {
                table_key: CHECK_FUNCTIONS[table_key](
                    start_date=start,
                    end_date=end,
                    ts_code=ts_codes,
                    neighbor_ratio_threshold=neighbor_ratio_threshold,
                    db_path=db_path,
                    connection=conn,
                )
                for table_key in tables
            }
        else:
            reports = initial_reports

        repaired_derived_inputs = [
            table_key
            for table_key in ("daily", "adj_factor")
            if table_key in repair_attempts
            and any(attempt["success"] for attempt in repair_attempts[table_key])
        ]
        if auto_repair and regenerate_derived and repaired_derived_inputs:
            derived_regeneration["requested"] = True
            derived_regeneration["trigger_tables"] = repaired_derived_inputs
            unrepaired_inputs = [
                table_key
                for table_key in repaired_derived_inputs
                if not reports[table_key]["ok"]
            ]
            if unrepaired_inputs:
                derived_regeneration["status"] = "skipped"
                derived_regeneration["reason"] = (
                    "基础数据复查仍有缺口: " + ",".join(unrepaired_inputs)
                )
            else:
                derived_regeneration = _regenerate_derived_tables(
                    downloader,
                    trigger_tables=repaired_derived_inputs,
                )
        elif auto_repair and not regenerate_derived and repaired_derived_inputs:
            derived_regeneration["status"] = "disabled"
            derived_regeneration["trigger_tables"] = repaired_derived_inputs
    finally:
        if own_downloader:
            _close_downloader_connections(downloader)
        if own_connection:
            conn.close()

    base_ok = all(report["ok"] for report in reports.values())
    derived_ok = derived_regeneration["status"] in {
        "not_needed",
        "disabled",
        "completed",
    }
    return {
        "ok": base_ok and derived_ok,
        "start_date": start,
        "end_date": end,
        "ts_codes": ts_codes,
        "tables": tables,
        "auto_repair": auto_repair,
        "regenerate_derived": regenerate_derived,
        "initial_redownload_dates": initial_redownload_dates,
        "redownload_dates": {
            table_key: report["redownload_dates"] for table_key, report in reports.items()
        },
        "repair_attempts": repair_attempts,
        "derived_regeneration": derived_regeneration,
        "initial_reports": initial_reports,
        "reports": reports,
    }


def _regenerate_derived_tables(
    downloader: Any,
    *,
    trigger_tables: Sequence[str],
) -> dict[str, Any]:
    """Rebuild adjusted/original week and month tables after base repair."""
    result: dict[str, Any] = {
        "requested": True,
        "status": "pending",
        "trigger_tables": list(trigger_tables),
        "reason": None,
        "week": None,
        "month": None,
    }
    try:
        from service.service_week import check_daily_adj_consistency

        if not check_daily_adj_consistency(downloader):
            result["status"] = "skipped"
            result["reason"] = "cn_daily/cn_adj_factor 断点不一致或尚未更新到最近收盘日"
            return result
    except Exception as exc:
        result["status"] = "failed"
        result["reason"] = f"派生表前置校验失败: {type(exc).__name__}: {exc}"
        return result

    jobs = (
        (
            "week",
            "update_week",
            ["tbl_cn_week", "tbl_cn_week_origin"],
        ),
        (
            "month",
            "update_month",
            ["tbl_cn_month", "tbl_cn_month_origin"],
        ),
    )
    all_succeeded = True
    for result_key, method_name, generated_tables in jobs:
        job_result = {
            "update_method": method_name,
            "generated_tables": generated_tables,
            "success": False,
            "rows_per_table": None,
            "error": None,
        }
        try:
            rows = getattr(downloader, method_name)()
            job_result["rows_per_table"] = int(rows or 0)
            job_result["success"] = True
        except Exception as exc:
            job_result["error"] = f"{type(exc).__name__}: {exc}"
            all_succeeded = False
        result[result_key] = job_result

    result["status"] = "completed" if all_succeeded else "failed"
    if not all_succeeded:
        result["reason"] = "周线或月线重新生成失败"
    return result


def _load_project_env() -> None:
    """Load simple ``export KEY=value`` entries without evaluating shell code."""
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key or not key.replace("_", "").isalnum():
            continue
        try:
            parsed = shlex.split(raw_value, comments=True, posix=True)
        except ValueError:
            continue
        value = parsed[0] if parsed else ""
        os.environ.setdefault(key, value)


def _close_downloader_connections(downloader: Any) -> None:
    seen: set[int] = set()
    for name in ("conn_basic", "conn_kpl", "conn_news", "conn_index", "conn"):
        conn = getattr(downloader, name, None)
        if conn is None or id(conn) in seen:
            continue
        seen.add(id(conn))
        try:
            conn.close()
        except Exception:
            pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="检查核心日频数据库是否缺日或少拉")
    parser.add_argument(
        "--tables",
        required=True,
        help="逗号分隔: daily,daily_basic,adj_factor（也接受物理表名）",
    )
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--trade-date", "--tradedate", dest="trade_date", help="单日 YYYYMMDD")
    date_group.add_argument(
        "--start-date",
        "--startdate",
        dest="start_date",
        help="区间开始 YYYYMMDD；需同时提供 --end-date",
    )
    parser.add_argument("--end-date", "--enddate", dest="end_date", help="区间结束 YYYYMMDD")
    parser.add_argument(
        "--ts-code",
        "--ts-codes",
        "--tscode",
        dest="ts_code",
        help="单个或逗号分隔股票代码；不填检查全市场",
    )
    parser.add_argument(
        "--neighbor-ratio-threshold",
        type=float,
        default=DEFAULT_NEIGHBOR_RATIO_THRESHOLD,
        help=f"低于相邻交易日较大值的比例即判缺失（默认 {DEFAULT_NEIGHBOR_RATIO_THRESHOLD}）",
    )
    parser.add_argument("--db-path", default=str(DB_PATH_BASIC), help="db_cn_basic.db 路径")
    parser.add_argument(
        "--no-repair",
        action="store_true",
        help="只检查不自动重新下载；默认发现问题后自动修复并复查",
    )
    parser.add_argument(
        "--no-derived-regeneration",
        action="store_true",
        help="修复日线/复权因子后不重算周线和月线；默认自动重算",
    )
    parser.add_argument(
        "--fail-on-issue",
        action="store_true",
        help="发现需重下日期时以退出码2结束",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.start_date and not args.end_date:
        parser.error("使用 --start-date 时必须同时提供 --end-date")
    if args.end_date and not args.start_date:
        parser.error("--end-date 必须和 --start-date 一起使用")

    report = check_database(
        args.tables,
        start_date=args.start_date,
        end_date=args.end_date,
        trade_date=args.trade_date,
        ts_code=args.ts_code,
        neighbor_ratio_threshold=args.neighbor_ratio_threshold,
        auto_repair=not args.no_repair,
        regenerate_derived=not args.no_derived_regeneration,
        db_path=args.db_path,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 2 if args.fail_on_issue and not report["ok"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

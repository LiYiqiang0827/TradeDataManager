"""
query_db: SQLite 数据拉取工具类(只读)

目的:
    给 AI Agent 提供"按需从 online_data_YYYYMM.db 拉数据"的统一接口。
    只取数据,不返回状态汇总 —— 状态检查请用 check_db。

API 列表(按数据条目,fetch_* 系列):
    SNAPSHOT        fetch_snapshot(trade_date, ts_code=..., limit=..., ...)
    ORDERBOOK       fetch_orderbook(trade_date, ts_code=..., limit=..., ...)
    MINUTE          fetch_minute(trade_date, ts_code=..., limit=..., ...)
    AUCTION         fetch_auction(trade_date, ts_code=..., limit=..., ...)
    ZT              fetch_zt(trade_date, ts_code=..., limit=..., ...)
    BREAK           fetch_break(trade_date, ts_code=..., limit=..., ...)
    ANOMALY         fetch_anomaly(trade_date, ts_code=..., limit=..., ...)
    HOT             fetch_hot(trade_date, ts_code=..., limit=..., ...)
    WATCHLIST       fetch_watchlist(trade_date)
    CURSOR          fetch_cursor(stream_key, year_month)
                   fetch_all_cursors(year_month)

    所有 kind 通用查询参数(透传 sqlite_client.query_snapshots):
        ts_code: 单股(可选)
        start_data_ts / end_data_ts: 数据时间窗口(ISO)
        start_save_ts / end_save_ts: 落盘时间窗口(ISO)
        limit: 最多多少条(None = 全量)

只读:全部 connect(readonly=True)。
"""


import sys
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _THIS_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import logging
from typing import Any

from core.sqlite_client import (
    connect,
    query_snapshots,
    query_watchlist,
    list_cursors,
    read_cursor,
    current_year_month,
    table_name_for,
)

_log = logging.getLogger("query_db")

# These daily zt tables were written while Redis' string "False" was coerced
# through bool(value), making both stored flags 1. Source-matched repairs have
# not established every row in these days; callers must not use the flags for
# historical ST filtering. 20261015 is a partial-evidence day: 5,000 rows were
# source-matched, while 6,591 rows had no source coverage in the observed audit.
_ZT_LEGACY_FLAG_UNRELIABLE_DATES = frozenset({
    "20260916", "20260917", "20260918", "20260921", "20260922",
    "20260923", "20260924", "20260928", "20260929", "20260930",
    "20261008", "20261009", "20261012", "20261013", "20261014",
})
_ZT_FLAG_PARTIAL_DATE = "20261015"


# ============================================================
# 工具
# ============================================================


def _ym_from_date(trade_date: str) -> str:
    if len(trade_date) >= 6:
        return trade_date[:6]
    return current_year_month()


def fetch_zt_quality(trade_date: str) -> dict[str, Any]:
    """Return the date-scoped reliability warning for zt boolean flags.

    Unlisted dates are unknown unless a future dated audit adds them to the
    governance record; absence of a warning record is not evidence of clean
    ``is_st``/``is_new`` values.
    """
    normalized = str(trade_date).replace("-", "")
    if normalized in _ZT_LEGACY_FLAG_UNRELIABLE_DATES:
        return {
            "trade_date": normalized,
            "status": "unreliable_legacy_string_bool",
            "fields": ["is_st", "is_new"],
            "warning": "Legacy Redis string booleans were stored as integer 1; do not use these fields for historical ST filtering.",
            "source_recovery": "No historical source evidence is available to reconstruct these rows.",
            "audit_reference": "docs/online_zt_flag_quality_20261015.md",
        }
    if normalized == _ZT_FLAG_PARTIAL_DATE:
        return {
            "trade_date": normalized,
            "status": "partial_source_coverage",
            "fields": ["is_st", "is_new"],
            "warning": "Only 5,000 source-matched keys were observed; 6,591 existing rows had no matching source coverage. Do not use the full-day flags for historical ST filtering.",
            "source_matched_rows_observed": 5000,
            "unverified_rows_observed": 6591,
            "audit_reference": "docs/online_zt_flag_quality_20261015.md",
        }
    return {
        "trade_date": normalized,
        "status": "not_assessed",
        "fields": ["is_st", "is_new"],
        "warning": "No date-specific flag audit is recorded; stored values are not certified for historical ST filtering.",
        "audit_reference": "docs/online_zt_flag_quality_20261015.md",
    }


# ============================================================
# 8 kind 数据拉取(均透传 query_snapshots)
# ============================================================


def _fetch_kind(
    kind: str,
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """通用 kind 拉取"""
    ym = year_month or _ym_from_date(trade_date)
    try:
        conn = connect(ym, readonly=True)
        try:
            return query_snapshots(
                conn,
                kind=kind,
                trade_date=trade_date,
                ts_code=ts_code,
                start_data_ts=start_data_ts,
                end_data_ts=end_data_ts,
                start_save_ts=start_save_ts,
                end_save_ts=end_save_ts,
                limit=limit,
            )
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"fetch_{kind} failed: {e}")
        return []


def fetch_snapshot(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 snapshot 当日数据"""
    return _fetch_kind(
        "snapshot", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


def fetch_orderbook(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 orderbook 当日数据"""
    return _fetch_kind(
        "orderbook", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


def fetch_minute(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 minute 当日数据"""
    return _fetch_kind(
        "minute", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


def fetch_auction(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 auction 当日数据"""
    return _fetch_kind(
        "auction", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


def fetch_zt(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 zt 当日数据"""
    quality = fetch_zt_quality(trade_date)
    _log.warning("fetch_zt quality status for %s: %s", quality["trade_date"], quality["status"])
    return _fetch_kind(
        "zt", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


def fetch_break(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 break 当日数据"""
    return _fetch_kind(
        "break", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


def fetch_anomaly(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 anomaly 当日数据"""
    return _fetch_kind(
        "anomaly", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


def fetch_hot(
    trade_date: str,
    *,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = 100,
    year_month: str | None = None,
) -> list[dict]:
    """拉 hot 当日数据"""
    return _fetch_kind(
        "hot", trade_date,
        ts_code=ts_code,
        start_data_ts=start_data_ts, end_data_ts=end_data_ts,
        start_save_ts=start_save_ts, end_save_ts=end_save_ts,
        limit=limit, year_month=year_month,
    )


# ============================================================
# WATCHLIST
# ============================================================


def fetch_watchlist(trade_date: str, year_month: str | None = None) -> list[dict]:
    """拉 watchlist 当日全量"""
    ym = year_month or _ym_from_date(trade_date)
    try:
        conn = connect(ym, readonly=True)
        try:
            return query_watchlist(conn, trade_date=trade_date)
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"fetch_watchlist failed: {e}")
        return []


# ============================================================
# 高阶:一键转 pandas DataFrame(2026-09-11 新增)
# ============================================================


def to_dataframe(
    kind: str,
    trade_date: str,
    ts_code: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = None,
):
    """一键把 SQLite 数据转 pandas DataFrame

    2026-09-11 重构:不再读 payload_json,直接 SELECT 列(字段全拆列后)
        每张表自带 trade_date / ts_code / data_timestamp / save_timestamp / created_at
        + 各 kind 自己的业务列(last_price / volume / seal_money ...)

    性能(实测 2026-09-11):
        全量 snapshot_20260911(14929 行)≈ 0.05s
        过滤后 200 行 ≈ 0.005s
        minute_bars 长表 46 万行 ≈ 1.5s

    Args:
        kind: snapshot / auction / orderbook / minute / zt / break / anomaly / hot
        trade_date: YYYYMMDD
        ts_code: 单股过滤(可选)
        start_data_ts / end_data_ts: 数据时间窗(ISO 字符串,可选)
        start_save_ts / end_save_ts: 落盘时间窗(ISO 字符串,可选)
        limit: 行数限制(可选)

    Returns:
        pd.DataFrame:列 = [id, trade_date, ts_code, ..., 各 kind 业务列]
        空 DataFrame(表不存在或无匹配)
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("to_dataframe 需要 pandas:pip install pandas")

    table_name = table_name_for(kind, trade_date)
    ym = _ym_from_date(trade_date)
    conn = connect(ym, readonly=True)
    try:
        rows = query_snapshots(
            conn,
            kind=kind,
            trade_date=trade_date,
            ts_code=ts_code,
            start_data_ts=start_data_ts,
            end_data_ts=end_data_ts,
            start_save_ts=start_save_ts,
            end_save_ts=end_save_ts,
            limit=limit,
        )
    finally:
        conn.close()
    df = pd.DataFrame(rows)
    if kind == "zt":
        quality = fetch_zt_quality(trade_date)
        df.attrs["quality"] = quality
        df.attrs["quality_warnings"] = [quality["warning"]]
    return df


# ============================================================
# CURSOR
# ============================================================


def fetch_cursor(stream_key: str, year_month: str | None = None) -> dict[str, Any]:
    """单 stream 落盘游标"""
    ym = year_month or current_year_month()
    try:
        conn = connect(ym, readonly=True)
        try:
            last_id = read_cursor(conn, stream_key)
            return {
                "stream_key": stream_key,
                "year_month": ym,
                "last_id": last_id,
            }
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"fetch_cursor failed: {e}")
        return {"_error": str(e), "stream_key": stream_key}


def fetch_all_cursors(year_month: str | None = None) -> list[dict]:
    """所有 stream 落盘游标"""
    ym = year_month or current_year_month()
    try:
        conn = connect(ym, readonly=True)
        try:
            return list_cursors(conn)
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"fetch_all_cursors failed: {e}")
        return []


# ============================================================
# CLI 自测
# ============================================================


if __name__ == "__main__":
    print("=== WATCHLIST ===")
    print(f"  当日条数: {len(fetch_watchlist('20260911'))}")

    print("\n=== ZT ===")
    rows = fetch_zt("20260911", limit=3)
    print(f"  zt 前 3 条: {len(rows)} 条")
    if rows:
        print(f"  字段: {list(rows[0].keys())}")

    print("\n=== ZT for 600519.SH ===")
    rows = fetch_zt("20260911", ts_code="600519.SH", limit=3)
    print(f"  共 {len(rows)} 条")

    print("\n=== CURSORS ===")
    cursors = fetch_all_cursors()
    print(f"  共 {len(cursors)} 条")
    for c in cursors[:3]:
        print(f"  {c.get('stream_key')}: last_id={c.get('last_id')[:20] if c.get('last_id') else 'None'}...")

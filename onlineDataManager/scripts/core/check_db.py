"""
check_db: SQLite 状态检查工具类(只读)

目的:
    给 AI Agent 提供"检查 online_data_YYYYMM.db 状态"的统一接口。
    只检查,不返回具体数据 —— 拉数据请用 query_db。

API 列表(按数据条目):
    每张表   check_<kind>(trade_date, year_month)   -> row_count + 时间范围 + 去重 ts_code
                  kind: snapshot / orderbook / minute / auction / zt / break / anomaly / hot
    WATCHLIST  check_watchlist(trade_date)         -> row_count + source 分布
    CURSOR     check_cursor(stream_key)            -> 单游标(包含 lag 与 STREAM 实际 ID 无关,纯 SQLite 视角)
    CURSOR     check_cursors(year_month)           -> 所有游标 + 落盘延迟
    META DB    check_db_list()                     -> 所有 db 文件
    META DB    check_table_list(year_month)        -> db 内全部表
    META DB    check_table_info(year_month, table) -> 单表 schema + 行数 + ts_code 数 + 时间范围
    META DB    check_overview(year_month)          -> 8 kind + watchlist + cursor 一键总览

只读:全部 connect(readonly=True)。
"""


import sys
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _THIS_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import logging
import sqlite3
from datetime import datetime
from typing import Any

from core.sqlite_client import (
    db_path_for_month,
    connect,
    list_tables,
    count_snapshots,
    list_cursors,
    read_cursor,
    table_name_for,
    current_year_month,
    query_watchlist,
)

_log = logging.getLogger("check_db")


# ============================================================
# 工具函数
# ============================================================


def _ym_from_date(trade_date: str) -> str:
    """YYYYMMDD -> YYYYMM"""
    if len(trade_date) >= 6:
        return trade_date[:6]
    return current_year_month()


def _rows_to_count_ts_range(conn, table: str) -> dict[str, Any]:
    """通用表统计:row_count / ts_code_count / earliest/latest data_timestamp + save_timestamp

    2026-09-11 修复:
    - 之前用纯 SQL MIN/MAX(字符串字典序,容易出错),现在用 datetime() 函数做正确比较
    - 兼容两种存储格式:ISO 字符串(如 '2026-09-11T10:00:31')和 unix 戳字符串(如 '1789096305.163')
    - SQLite 的 datetime(text) 能解析 ISO 字符串,unix 戳小数点会失败 → 用 CASE 兜底
    """
    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone():
        return {"exists": False, "row_count": 0}

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    ts_code_count = conn.execute(f"SELECT COUNT(DISTINCT ts_code) FROM {table}").fetchone()[0]

    # 2026-09-11 修复:用 datetime() 做正确时间比较(而非字典序)
    # CASE:如果 value 是数字(unix 戳)就 +0 数字比较;否则用 datetime() 解析 ISO 字符串
    # 这样早 10:00 一定 < 晚 15:35(字典序在 1xxx unix 戳 vs 2026- ISO 字符串时反而错)
    try:
        ts_col_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        ts_cols = {row[1]: row[2] for row in ts_col_info}
    except sqlite3.OperationalError:
        ts_cols = {}

    def _min_max(col: str) -> tuple[str | None, str | None]:
        """返回 (earliest, latest) — 兼容 ISO 字符串 + unix 戳 + REAL"""
        if col not in ts_cols:
            return (None, None)
        col_type = ts_cols[col].upper()
        try:
            if "REAL" in col_type or "INT" in col_type:
                # REAL/INT 列,直接 MIN/MAX(数值比较)
                earliest = conn.execute(f"SELECT MIN({col}) FROM {table}").fetchone()[0]
                latest = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]
            else:
                # TEXT 列:用 datetime() 兼容 ISO 字符串,unix 戳数字会失败 → 用 CAST 兜底
                # datetime(text) 解析 ISO 字符串 OK,数字 → NULL → COALESCE 退化成原始值
                earliest = conn.execute(
                    f"SELECT MIN(COALESCE(datetime({col}), {col})) FROM {table}"
                ).fetchone()[0]
                latest = conn.execute(
                    f"SELECT MAX(COALESCE(datetime({col}), {col})) FROM {table}"
                ).fetchone()[0]
            return (earliest, latest)
        except sqlite3.OperationalError:
            return (None, None)

    earliest_dt, latest_dt = _min_max("data_timestamp")
    earliest_st, latest_st = _min_max("save_timestamp")

    def fmt(v):
        if v is None:
            return None
        # 优先 ISO 解析
        if isinstance(v, str):
            try:
                # ISO 字符串(可能含 T,可能空格分隔)
                return datetime.fromisoformat(v.replace("T", " ") if "T" in v else v).isoformat(timespec="milliseconds")
            except (ValueError, TypeError):
                pass
            # fallback:unix 戳字符串
            try:
                return datetime.fromtimestamp(float(v)).isoformat(timespec="milliseconds")
            except (ValueError, TypeError):
                return v
        # REAL/INT 列(MIN/MAX 直接返回的数字)
        try:
            return datetime.fromtimestamp(float(v)).isoformat(timespec="milliseconds")
        except (ValueError, TypeError):
            return str(v)

    return {
        "exists": True,
        "row_count": row_count,
        "ts_code_count": ts_code_count,
        "earliest_data_ts": fmt(earliest_dt),
        "latest_data_ts": fmt(latest_dt),
        "earliest_save_ts": fmt(earliest_st),
        "latest_save_ts": fmt(latest_st),
    }


# ============================================================
# 8 kind 数据表
# ============================================================


def _check_kind(kind: str, trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """通用:检查某 kind 当日表"""
    ym = year_month or _ym_from_date(trade_date)
    table = table_name_for(kind, trade_date)
    try:
        conn = connect(ym, readonly=True)
        try:
            return _rows_to_count_ts_range(conn, table)
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"check_{kind} failed: {e}")
        return {"_error": str(e), "exists": False, "row_count": 0}


# 8 个 kind 的 check_xxx 别名(直接暴露给 AI)
def check_snapshot(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """snapshot 当日表状态"""
    return _check_kind("snapshot", trade_date, year_month)


def check_orderbook(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """orderbook 当日表状态"""
    return _check_kind("orderbook", trade_date, year_month)


def check_minute(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """minute 当日表状态"""
    return _check_kind("minute", trade_date, year_month)


def check_auction(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """auction 当日表状态"""
    return _check_kind("auction", trade_date, year_month)


def check_zt(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """zt 当日表状态"""
    return _check_kind("zt", trade_date, year_month)


def check_break(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """break 当日表状态"""
    return _check_kind("break", trade_date, year_month)


def check_anomaly(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """anomaly 当日表状态"""
    return _check_kind("anomaly", trade_date, year_month)


def check_hot(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """hot 当日表状态"""
    return _check_kind("hot", trade_date, year_month)


# ============================================================
# LIMITPERFORMANCE(2026-09-14 v5 新增)
# ============================================================


def check_limitperformance(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """limitperformance 当日表状态(快照流累积,行数随盘增长)"""
    return _check_kind("limitperformance", trade_date, year_month)


# ============================================================
# WATCHLIST
# ============================================================


def check_watchlist(trade_date: str, year_month: str | None = None) -> dict[str, Any]:
    """watchlist 当日表状态"""
    ym = year_month or _ym_from_date(trade_date)
    try:
        conn = connect(ym, readonly=True)
        try:
            rows = query_watchlist(conn, trade_date=trade_date)
            sources_count = {}
            for r in rows:
                src = r.get("source", "?")
                sources_count[src] = sources_count.get(src, 0) + 1
            return {
                "exists": True,
                "row_count": len(rows),
                "ts_code_count": len(set(r["ts_code"] for r in rows)),
                "sources_count": sources_count,
            }
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"check_watchlist failed: {e}")
        return {"_error": str(e), "exists": False, "row_count": 0}


# ============================================================
# CURSOR
# ============================================================


def check_cursor(stream_key: str, year_month: str | None = None) -> dict[str, Any]:
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
                "exists": last_id is not None,
            }
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"check_cursor failed: {e}")
        return {"_error": str(e), "stream_key": stream_key}


def check_cursors(year_month: str | None = None) -> list[dict[str, Any]]:
    """所有 stream 落盘游标 + lag(分钟)"""
    ym = year_month or current_year_month()
    try:
        conn = connect(ym, readonly=True)
        try:
            cursors = list_cursors(conn)
            now = datetime.now()
            for c in cursors:
                lag = "?"
                if c.get("last_persist_ts"):
                    try:
                        ts = datetime.fromisoformat(c["last_persist_ts"])
                        lag = f"{(now - ts).total_seconds() / 60:.1f}m"
                    except (ValueError, TypeError):
                        pass
                c["lag"] = lag
            return cursors
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"check_cursors failed: {e}")
        return [{"_error": str(e)}]


# ============================================================
# DB 元数据
# ============================================================


def check_db_list() -> list[str]:
    """所有 db 文件对应的 year_month"""
    import os
    from core.sqlite_client import db_path_for_month
    db_dir = db_path_for_month("000000").parent  # 借函数取父目录
    if not db_dir.exists():
        return []
    yms = []
    for f in sorted(db_dir.glob("online_data_*.db")):
        # 文件名形如 online_data_202609.db
        stem = f.stem
        if stem.startswith("online_data_") and len(stem) == len("online_data_YYYYMM"):
            yms.append(stem[len("online_data_"):])
    return yms


def check_table_list(year_month: str | None = None) -> list[str]:
    """某月 db 全部表"""
    ym = year_month or current_year_month()
    try:
        conn = connect(ym, readonly=True)
        try:
            return list_tables(conn)
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"check_table_list failed: {e}")
        return []


def check_table_info(year_month: str | None, table_name: str) -> dict[str, Any]:
    """单表完整信息:row_count + 时间范围 + ts_code 数 + columns"""
    ym = year_month or current_year_month()
    try:
        conn = connect(ym, readonly=True)
        try:
            columns = [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
            if not columns:
                return {"table_name": table_name, "year_month": ym, "exists": False}

            info = {
                "table_name": table_name,
                "year_month": ym,
                "exists": True,
                "columns": columns,
            }
            base = _rows_to_count_ts_range(conn, table_name)
            info.update(base)
            return info
        finally:
            conn.close()
    except Exception as e:
        _log.warning(f"check_table_info failed: {e}")
        return {"_error": str(e), "table_name": table_name}


# ============================================================
# OVERVIEW(一键总览)
# ============================================================


def check_overview(
    trade_date: str | None = None,
    year_month: str | None = None,
) -> dict[str, Any]:
    """一键总览:8 kind + watchlist + cursors + db 文件列表"""
    from datetime import date
    if trade_date is None:
        trade_date = date.today().strftime("%Y%m%d")
    ym = year_month or _ym_from_date(trade_date)

    kinds = {}
    for kind in ["snapshot", "orderbook", "minute", "auction", "zt", "break", "anomaly", "hot"]:
        kinds[kind] = _check_kind(kind, trade_date, ym)

    return {
        "db_files": check_db_list(),
        "year_month": ym,
        "trade_date": trade_date,
        "kinds": kinds,
        "watchlist": check_watchlist(trade_date, ym),
        "cursors": check_cursors(ym),
    }


if __name__ == "__main__":
    import pprint
    pprint.pprint(check_overview("20260911"), width=120)
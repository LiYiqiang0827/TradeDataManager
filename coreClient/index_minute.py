"""Native TDX index OHLC bars; index turnover is never treated as share volume.

Index responses contain breadth fields that the stock parser does not consume.
Always use get_index_bars, including for one-minute and fifteen-minute requests.
The raw index ``vol`` is retained for provenance only. ``amount`` is yuan and
may describe the exchange rather than the index constituents (e.g. 399001).
"""
from __future__ import annotations

import datetime as dt
import math
import os
from pathlib import Path
import sqlite3
from collections import defaultdict

INDEX_CODES = (
    "000001.SH", "399001.SZ", "399006.SZ", "000688.SH",
    "000300.SH", "000016.SH", "000905.SH", "000852.SH",
)
SOURCE = "tdx.get_index_bars"
SHANGHAI = dt.timezone(dt.timedelta(hours=8))


def database_path() -> Path:
    return Path(os.environ.get("TRADING_DATABASE_ROOT", Path.home() / "TradingDatabase")) / "offline" / "db_cn_index_minute.db"


def day_key(value) -> str:
    value = str(value).replace("-", "")
    dt.datetime.strptime(value, "%Y%m%d")
    return value


def expected_labels(period: int) -> set[str]:
    if period not in (1, 15):
        raise ValueError("index period must be 1 or 15 minutes")
    result = set()
    for start in (9 * 60 + 30, 13 * 60):
        for offset in range(period, 121, period):
            hour, minute = divmod(start + offset, 60)
            result.add(f"{hour:02}:{minute:02}")
    return result


def read_index_bars(ts_code, period=1, trade_date=None, start_date=None, end_date=None):
    """Read canonical, complete index sessions without network fallback."""
    import pandas as pd
    if ts_code not in INDEX_CODES:
        raise ValueError(f"unsupported index: {ts_code}")
    expected_labels(period)
    if trade_date and (start_date or end_date):
        raise ValueError("trade_date and date range are mutually exclusive")
    path = database_path()
    columns = ["ts_code", "period", "trade_date", "datetime", "open", "high", "low", "close", "raw_volume", "amount", "up_count", "down_count", "quality_flags", "source", "fetched_at"]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    clauses, params = ["ts_code=?", "period=?"], [ts_code, period]
    if trade_date:
        clauses.append("trade_date=?"); params.append(day_key(trade_date))
    else:
        for key, value, operator in (("start_date", start_date, ">="), ("end_date", end_date, "<=")):
            if value:
                clauses.append(f"trade_date{operator}?"); params.append(day_key(value))
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=30) as conn:
        return pd.read_sql_query("SELECT " + ",".join(columns) + " FROM tbl_cn_index_minute WHERE " + " AND ".join(clauses) + " ORDER BY datetime", conn, params=params)


def ensure_schema(conn):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
      CREATE TABLE IF NOT EXISTS tbl_cn_index_minute (
        ts_code TEXT NOT NULL, period INTEGER NOT NULL CHECK(period IN (1,15)),
        trade_date TEXT NOT NULL, datetime TEXT NOT NULL,
        open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
        raw_volume REAL NOT NULL, amount REAL NOT NULL,
        up_count INTEGER, down_count INTEGER, quality_flags TEXT NOT NULL DEFAULT '[]', source TEXT NOT NULL,
        source_host TEXT NOT NULL, fetched_at TEXT NOT NULL,
        PRIMARY KEY(ts_code,period,datetime)
      );
      CREATE INDEX IF NOT EXISTS ix_index_minute_date ON tbl_cn_index_minute(period,trade_date,ts_code);
      CREATE TABLE IF NOT EXISTS tbl_cn_index_minute_ctrl (
        ts_code TEXT NOT NULL, period INTEGER NOT NULL, trade_date TEXT NOT NULL,
        bar_count INTEGER NOT NULL, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
        PRIMARY KEY(ts_code,period,trade_date)
      );
    """)
    if "quality_flags" not in {row[1] for row in conn.execute("PRAGMA table_info(tbl_cn_index_minute)")}:
        conn.execute("ALTER TABLE tbl_cn_index_minute ADD COLUMN quality_flags TEXT NOT NULL DEFAULT '[]'")


class IndexMinuteDownloader:
    """Use the index decoder and persist only complete, validated sessions."""
    def __init__(self):
        from pytdx.hq import TdxHq_API
        from coreClient.tdx_config import TDX_HISTORY_BAR_IP_POOL, TDX_IP_POOL
        self.api = TdxHq_API(raise_exception=True)
        self.hosts = list(dict.fromkeys([*TDX_HISTORY_BAR_IP_POOL, *TDX_IP_POOL]))
        self.host = None

    def close(self):
        self.api.disconnect()

    def _connect(self, skip=None):
        errors = []
        for host, port in self.hosts:
            if (host, port) == skip:
                continue
            try:
                self.api.disconnect()
                if self.api.connect(host, port, time_out=5):
                    self.host = (host, port)
                    return
            except Exception as exc:
                errors.append(f"{host}:{type(exc).__name__}")
        raise RuntimeError("no usable index history server: " + ", ".join(errors))

    def _page(self, category, market, code, offset):
        if self.host is None:
            self._connect()
        for attempt in range(2):
            try:
                rows = self.api.get_index_bars(category, market, code, offset, 800)
                if rows is None:
                    raise RuntimeError("index source returned None")
                if not rows and offset == 0 and attempt == 0:
                    self._connect(skip=self.host)
                    continue
                return [dict(row) for row in rows]
            except Exception:
                if attempt:
                    raise
                self._connect(skip=self.host)
        raise RuntimeError("index source failed")

    def download(self, ts_code, period, start_date=None, end_date=None, max_pages=80):
        from pytdx.params import TDXParams
        if ts_code not in INDEX_CODES:
            raise ValueError(f"unsupported index: {ts_code}")
        labels = expected_labels(period)
        if not 1 <= max_pages <= 81:
            raise ValueError("max_pages must be 1..81 (TDX offset is uint16)")
        start = day_key(start_date) if start_date else None
        end = day_key(end_date) if end_date else dt.datetime.now(SHANGHAI).strftime("%Y%m%d")
        if start and start > end:
            raise ValueError("start date is after end date")
        category = TDXParams.KLINE_TYPE_1MIN if period == 1 else TDXParams.KLINE_TYPE_15MIN
        market = 1 if ts_code.endswith(".SH") else 0
        selected, seen = {}, set()
        stop_reason, source_start, source_end = "page_limit", None, None
        rejected_labels = 0
        rounding_flags = []
        for page in range(max_pages):
            chunk = self._page(category, market, ts_code[:6], page * 800)
            if not chunk:
                stop_reason = "source_end"; break
            page_days, new_count = [], 0
            for raw in chunk:
                stamp = dt.datetime(int(raw["year"]), int(raw["month"]), int(raw["day"]), int(raw["hour"]), int(raw["minute"]))
                day, label = stamp.strftime("%Y%m%d"), stamp.strftime("%H:%M")
                page_days.append(day)
                source_start = min(source_start or day, day)
                source_end = max(source_end or day, day)
                key = stamp.strftime("%Y-%m-%d %H:%M:%S")
                if key not in seen:
                    seen.add(key); new_count += 1
                if label not in labels:
                    rejected_labels += 1; continue
                if day > end or (start and day < start):
                    continue
                values = {field: float(raw[field]) for field in ("open", "high", "low", "close", "vol", "amount")}
                if not all(math.isfinite(value) for value in values.values()):
                    raise ValueError(f"non-finite index value {ts_code} {key}")
                if min(values[f] for f in ("open", "high", "low", "close")) <= 0 or min(values["vol"], values["amount"]) < 0:
                    raise ValueError(f"invalid index price/turnover {ts_code} {key}")
                boundary_error = max(max(values["open"], values["close"]) - values["high"], values["low"] - min(values["open"], values["close"]), 0)
                quality_flags = "[]"
                if values["high"] < values["low"]:
                    raise ValueError(f"index high is below low {ts_code} {key}")
                if boundary_error:
                    # Native index closing records sometimes differ by one 0.01
                    # point price tick. Retain source OHLC unchanged and audit it.
                    if label != "15:00" or boundary_error > .010001:
                        raise ValueError(f"invalid index OHLC {ts_code} {key}: {values}")
                    quality_flags = '["closing_ohlc_boundary_rounding"]'
                    rounding_flags.append({"datetime": key, "difference_points": boundary_error})
                row = {"ts_code": ts_code, "period": period, "trade_date": day, "datetime": key,
                       **{f: values[f] for f in ("open", "high", "low", "close", "amount")},
                       "raw_volume": values["vol"], "up_count": raw.get("up_count"), "down_count": raw.get("down_count"), "quality_flags": quality_flags}
                if key in selected and selected[key] != row:
                    raise ValueError(f"conflicting duplicate index bar {ts_code} {key}")
                selected[key] = row
            if not new_count:
                stop_reason = "repeated_page"; break
            if start and min(page_days) < start:
                stop_reason = "requested_start"; break
            if len(chunk) < 800:
                stop_reason = "source_end"; break
        if not selected:
            raise RuntimeError(f"no index bars {ts_code} {period}m for requested dates")
        grouped = defaultdict(list)
        for row in sorted(selected.values(), key=lambda row: row["datetime"]):
            grouped[row["trade_date"]].append(row)
        complete, incomplete = [], []
        for day, rows in sorted(grouped.items()):
            actual = {row["datetime"][11:16] for row in rows}
            if actual != labels:
                incomplete.append({"date": day, "bars": len(rows), "missing": sorted(labels - actual)})
            else:
                complete.extend(rows)
        # Source retention may cut the oldest session in half. Never persist it.
        interior = [row for row in incomplete if row["date"] != min(grouped)]
        if interior:
            raise RuntimeError(f"incomplete index sessions {ts_code} {period}m: {interior}")
        if not complete:
            raise RuntimeError(f"no complete index sessions {ts_code} {period}m")
        return complete, {"source": SOURCE, "source_host": f"{self.host[0]}:{self.host[1]}", "stop_reason": stop_reason,
                          "source_start": source_start, "source_end": source_end, "pages": page + 1,
                          "skipped_boundary_sessions": incomplete, "filtered_outside_session": rejected_labels,
                          "closing_rounding_flags": rounding_flags}

    def update(self, ts_code, period, start_date=None, end_date=None, max_pages=80, validator=None):
        rows, report = self.download(ts_code, period, start_date, end_date, max_pages)
        if validator:
            report["validation"] = validator(rows)
        path = database_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        fetched_at = dt.datetime.now(SHANGHAI).isoformat(timespec="seconds")
        fields = ("ts_code", "period", "trade_date", "datetime", "open", "high", "low", "close", "raw_volume", "amount", "up_count", "down_count", "quality_flags")
        days = sorted({row["trade_date"] for row in rows})
        with sqlite3.connect(path, timeout=60) as conn:
            ensure_schema(conn)
            conn.executemany("INSERT OR REPLACE INTO tbl_cn_index_minute (" + ",".join(fields) + ",source,source_host,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             [tuple(row[field] for field in fields) + (SOURCE, report["source_host"], fetched_at) for row in rows])
            conn.executemany("INSERT OR REPLACE INTO tbl_cn_index_minute_ctrl VALUES (?,?,?,?,?,?)",
                             [(ts_code, period, day, 240 if period == 1 else 16, SOURCE, fetched_at) for day in days])
        return {**report, "code": ts_code, "period": period, "rows": len(rows), "days": len(days), "start": days[0], "end": days[-1], "database": str(path)}

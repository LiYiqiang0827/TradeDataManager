"""TDX原生15分钟K线DuckDB存储与查询。

主库文件为 ``offlineDataManager/data/db_fifteenMinute.db``。文件扩展名虽为
``.db``，内部引擎仍是DuckDB。数据按不复权价格保存；``adj_factor``来自本地
日线复权因子表，供上层按需前复权。
"""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence, Union

import duckdb
import pandas as pd

from config.settings import FIFTEEN_MIN_DB_PATH
from core.minute_control import ensure_minute_control, record_minute_update, touch_minute_update


SCHEMA_VERSION = "tdx_fifteen_min_duckdb.v2"
BAR_TIMES = (
    "09:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:15", "11:30",
    "13:15", "13:30", "13:45", "14:00", "14:15", "14:30", "14:45", "15:00",
)
TIME_INDEX = {stamp: index for index, stamp in enumerate(BAR_TIMES)}
STANDARD_COLUMNS = [
    "ts_code", "name", "trade_date", "datetime", "time_idx",
    "open", "high", "low", "close", "vol", "amount", "adj_factor",
]


def normalize_trade_date(value: Union[str, datetime, int]) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    text = str(value).strip().replace("-", "").replace("/", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"非法交易日期: {value!r}，应为YYYYMMDD或YYYY-MM-DD")
    datetime.strptime(text, "%Y%m%d")
    return text


def _database_path(root: Optional[Union[str, Path]] = None) -> Path:
    candidate = Path(root) if root is not None else FIFTEEN_MIN_DB_PATH
    if candidate.exists() and candidate.is_dir():
        return candidate / "db_fifteenMinute.db"
    if not candidate.suffix:
        return candidate / "db_fifteenMinute.db"
    return candidate


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_fifteen_min_schema(
    root: Optional[Union[str, Path]] = None,
) -> Path:
    database = _database_path(root)
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as conn:
        try:
            previous_version = conn.execute(
                "SELECT value FROM fifteen_min_meta WHERE key='schema_version'"
            ).fetchone()
            previous_version = previous_version[0] if previous_version else None
        except Exception:
            previous_version = None
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fifteen_min_bars (
                ts_code VARCHAR NOT NULL,
                name VARCHAR,
                trade_date INTEGER NOT NULL,
                datetime TIMESTAMP NOT NULL,
                time_idx SMALLINT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                adj_factor DOUBLE,
                source VARCHAR NOT NULL DEFAULT 'tdx',
                updated_at TIMESTAMP NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fifteen_min_sync (
                ts_code VARCHAR PRIMARY KEY,
                covered_start INTEGER,
                covered_end INTEGER,
                actual_start INTEGER,
                actual_end INTEGER,
                row_count BIGINT NOT NULL DEFAULT 0,
                status VARCHAR NOT NULL,
                source_host VARCHAR,
                updated_at TIMESTAMP NOT NULL,
                error VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fifteen_min_days (
                ts_code VARCHAR NOT NULL,
                trade_date INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                source_host VARCHAR,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (ts_code, trade_date)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fifteen_min_meta (
                key VARCHAR PRIMARY KEY,
                value VARCHAR,
                updated_at TIMESTAMP NOT NULL
            )
        """)
        ensure_minute_control(conn, "fifteen_minute")
        if previous_version == "tdx_fifteen_min_duckdb.v1":
            # v1在数千万行事实表上维护主键ART索引，历史回补越写越慢。v2依靠
            # 逐股票逐日先删后写保证唯一，完成后再物理排序并建查询索引。
            conn.execute("BEGIN")
            try:
                conn.execute("""
                    CREATE TABLE fifteen_min_bars_v2 (
                        ts_code VARCHAR NOT NULL, name VARCHAR,
                        trade_date INTEGER NOT NULL, datetime TIMESTAMP NOT NULL,
                        time_idx SMALLINT NOT NULL,
                        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                        vol DOUBLE, amount DOUBLE, adj_factor DOUBLE,
                        source VARCHAR NOT NULL, updated_at TIMESTAMP NOT NULL
                    )
                """)
                conn.execute("INSERT INTO fifteen_min_bars_v2 SELECT * FROM fifteen_min_bars")
                conn.execute("DROP TABLE fifteen_min_bars")
                conn.execute("ALTER TABLE fifteen_min_bars_v2 RENAME TO fifteen_min_bars")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        conn.execute(
            "INSERT INTO fifteen_min_meta VALUES ('schema_version', ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            [SCHEMA_VERSION, _utc_now()],
        )
    return database


def _normalize_codes(
    ts_code: Optional[str] = None,
    ts_codes: Optional[Iterable[str]] = None,
) -> list[str]:
    if ts_code is not None and ts_codes is not None:
        raise ValueError("ts_code和ts_codes互斥，只能传一个")
    values = list(ts_codes) if ts_codes is not None else ([ts_code] if ts_code else [])
    return sorted({str(value).strip().upper() for value in values if str(value).strip()})


def _empty_frame(columns: Optional[Sequence[str]] = None) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns) if columns is not None else STANDARD_COLUMNS)


def _rows_to_frame(
    rows: Iterable[Mapping],
    *,
    name: Optional[str] = None,
    adj_factors: Optional[Mapping[str, float]] = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(list(rows))
    if frame.empty:
        return pd.DataFrame(columns=STANDARD_COLUMNS + ["source", "updated_at"])
    required = {"ts_code", "trade_date", "datetime", "open", "high", "low", "close", "vol", "amount"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"TDX fifteenMin行缺少字段: {missing}")

    frame["ts_code"] = frame["ts_code"].astype(str).str.upper()
    frame["trade_date"] = frame["trade_date"].map(normalize_trade_date).astype("int32")
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    if frame["datetime"].isna().any():
        raise ValueError("TDX fifteenMin包含无法解析的datetime")
    labels = frame["datetime"].dt.strftime("%H:%M")
    invalid = sorted(set(labels) - set(BAR_TIMES))
    if invalid:
        raise ValueError(f"TDX fifteenMin包含非标准时间: {invalid}")
    frame["time_idx"] = labels.map(TIME_INDEX).astype("int16")
    for column in ("open", "high", "low", "close", "vol", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if frame[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError("TDX fifteenMin包含空OHLC")
    if ((frame["high"] < frame[["open", "close", "low"]].max(axis=1)) |
            (frame["low"] > frame[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("TDX fifteenMin存在OHLC关系错误")

    frame["name"] = name
    factor_map = {normalize_trade_date(key): value for key, value in (adj_factors or {}).items()}
    frame["adj_factor"] = frame["trade_date"].astype(str).map(factor_map)
    frame["source"] = "tdx"
    frame["updated_at"] = _utc_now()
    frame = frame.drop_duplicates(subset=["ts_code", "datetime"], keep="last")
    return frame[STANDARD_COLUMNS + ["source", "updated_at"]].sort_values(
        ["ts_code", "datetime"]
    ).reset_index(drop=True)


def write_fifteen_min_batch(
    items: Sequence[Mapping],
    *,
    replace: bool = True,
    append_only: bool = False,
    root: Optional[Union[str, Path]] = None,
    connection=None,
) -> dict:
    """把多只股票合并成一个DuckDB事务写入。"""
    database = _database_path(root)
    if connection is None:
        ensure_fifteen_min_schema(database)
    prepared = []
    sync_rows = []
    hosts = {}
    for item in items:
        start = int(normalize_trade_date(item["requested_start"]))
        end = int(normalize_trade_date(item["requested_end"]))
        if start > end:
            raise ValueError(f"requested_start不能晚于requested_end: {start}>{end}")
        frame = _rows_to_frame(
            item["rows"], name=item.get("name"), adj_factors=item.get("adj_factors"),
        )
        if frame.empty:
            continue
        if frame["ts_code"].nunique() != 1:
            raise ValueError("批量写入的每个item只能包含一只股票")
        code = str(frame["ts_code"].iloc[0])
        actual_start = int(frame["trade_date"].min())
        actual_end = int(frame["trade_date"].max())
        host = item.get("source_host")
        hosts[code] = host
        sync_rows.append({
            "ts_code": code,
            "covered_start": start,
            "covered_end": min(end, actual_end),
            "actual_start": actual_start,
            "actual_end": actual_end,
            "status": "downloaded",
            "source_host": host,
        })
        prepared.append(frame)
    if not prepared:
        return {"rows": 0, "symbols": 0, "days": 0, "database": str(database)}

    frame = pd.concat(prepared, ignore_index=True).sort_values(["ts_code", "datetime"])
    now = _utc_now()
    symbols = frame["ts_code"].drop_duplicates().tolist()
    day_counts = frame.groupby(["ts_code", "trade_date"]).size().rename("row_count").reset_index()
    day_counts["source_host"] = day_counts["ts_code"].map(hosts)
    day_counts["updated_at"] = now
    connection_context = (
        duckdb.connect(str(database)) if connection is None else nullcontext(connection)
    )
    with connection_context as conn:
        conn.register("incoming_fifteen_min", frame)
        conn.register("incoming_fifteen_days", day_counts)
        try:
            conn.execute("BEGIN")
            if replace and not append_only:
                conn.execute("""
                    DELETE FROM fifteen_min_bars AS target
                    USING incoming_fifteen_days AS incoming
                    WHERE target.ts_code=incoming.ts_code
                      AND target.trade_date=incoming.trade_date
                """)
            if append_only or replace:
                conn.execute("""
                    INSERT INTO fifteen_min_bars
                    SELECT ts_code, name, trade_date, datetime, time_idx,
                           open, high, low, close, vol, amount, adj_factor,
                           source, updated_at
                    FROM incoming_fifteen_min
                """)
            else:
                conn.execute("""
                    INSERT INTO fifteen_min_bars
                    SELECT incoming.ts_code, incoming.name, incoming.trade_date,
                           incoming.datetime, incoming.time_idx,
                           incoming.open, incoming.high, incoming.low, incoming.close,
                           incoming.vol, incoming.amount, incoming.adj_factor,
                           incoming.source, incoming.updated_at
                    FROM incoming_fifteen_min incoming
                    WHERE NOT EXISTS (
                        SELECT 1 FROM fifteen_min_bars target
                        WHERE target.ts_code=incoming.ts_code
                          AND target.datetime=incoming.datetime
                    )
                """)
            conn.execute("""
                INSERT INTO fifteen_min_days
                SELECT ts_code, trade_date, row_count,
                       CASE WHEN row_count=16 THEN 'complete' ELSE 'partial' END,
                       source_host, updated_at
                FROM incoming_fifteen_days
                ON CONFLICT(ts_code, trade_date) DO UPDATE SET
                    row_count=excluded.row_count, status=excluded.status,
                    source_host=excluded.source_host, updated_at=excluded.updated_at
            """)
            placeholders = ",".join("?" for _ in symbols)
            totals = dict(conn.execute(
                f"SELECT ts_code, COUNT(*) FROM fifteen_min_bars "
                f"WHERE ts_code IN ({placeholders}) GROUP BY ts_code",
                symbols,
            ).fetchall())
            sync_frame = pd.DataFrame(sync_rows)
            sync_frame["row_count"] = sync_frame["ts_code"].map(totals).astype("int64")
            sync_frame["updated_at"] = now
            conn.register("incoming_fifteen_sync", sync_frame)
            conn.execute("""
                INSERT INTO fifteen_min_sync
                SELECT ts_code, covered_start, covered_end, actual_start, actual_end,
                       row_count, status, source_host, updated_at, NULL
                FROM incoming_fifteen_sync
                ON CONFLICT(ts_code) DO UPDATE SET
                    covered_start=LEAST(fifteen_min_sync.covered_start, excluded.covered_start),
                    covered_end=GREATEST(fifteen_min_sync.covered_end, excluded.covered_end),
                    actual_start=LEAST(fifteen_min_sync.actual_start, excluded.actual_start),
                    actual_end=GREATEST(fifteen_min_sync.actual_end, excluded.actual_end),
                    row_count=excluded.row_count,
                    status=excluded.status, source_host=excluded.source_host,
                    updated_at=excluded.updated_at, error=NULL
            """)
            record_minute_update(conn, "fifteen_minute", int(frame["trade_date"].max()))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.unregister("incoming_fifteen_min")
            conn.unregister("incoming_fifteen_days")
            try:
                conn.unregister("incoming_fifteen_sync")
            except Exception:
                pass
    return {
        "rows": len(frame), "symbols": len(symbols), "days": len(day_counts),
        "database": str(database), "actual_start": int(frame["trade_date"].min()),
        "actual_end": int(frame["trade_date"].max()),
    }


def write_fifteen_min_rows(
    rows: Iterable[Mapping],
    *,
    requested_start: Union[str, datetime, int],
    requested_end: Union[str, datetime, int],
    name: Optional[str] = None,
    adj_factors: Optional[Mapping[str, float]] = None,
    source_host: Optional[str] = None,
    replace: bool = True,
    append_only: bool = False,
    root: Optional[Union[str, Path]] = None,
    connection=None,
) -> dict:
    """事务写入一只股票；全市场服务通过batch接口合并多个股票。"""
    return write_fifteen_min_batch(
        [{
            "rows": rows,
            "requested_start": requested_start,
            "requested_end": requested_end,
            "name": name,
            "adj_factors": adj_factors,
            "source_host": source_host,
        }],
        replace=replace,
        append_only=append_only,
        root=root,
        connection=connection,
    )


def repair_fifteen_min_adj_factors(
    factors,
    *,
    root: Optional[Union[str, Path]] = None,
    connection=None,
) -> dict:
    """批量修复同股票同交易日的NULL/旧因子，绝不改动原始OHLCVA。

    ``factors``只能是可靠的当日因子；空值、非正数和非有限值不会被写入，
    不用最新因子跨日回填，也不把未知因子当1。同一键的冲突因子拒绝写入。
    调用方可按日期分批提供DataFrame，并复用现有DuckDB写连接。
    """
    frame = factors.copy() if isinstance(factors, pd.DataFrame) else pd.DataFrame(factors)
    result = {"source_rows": len(frame), "valid_factor_keys": 0,
              "ignored_invalid_factor_rows": 0, "updated_rows": 0}
    if frame.empty:
        return result
    required = {"ts_code", "trade_date", "adj_factor"}
    if not required.issubset(frame.columns):
        raise ValueError(f"15分钟因子修复缺少字段: {sorted(required - set(frame.columns))}")
    frame = frame[["ts_code", "trade_date", "adj_factor"]].copy()
    valid_codes = frame["ts_code"].notna() & frame["ts_code"].astype(str).str.strip().ne("")
    frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="coerce")
    valid_values = frame["adj_factor"].map(
        lambda value: pd.notna(value) and math.isfinite(value) and value > 0
    )
    valid = valid_codes & valid_values
    result["ignored_invalid_factor_rows"] = int((~valid).sum())
    frame = frame.loc[valid].copy()
    if frame.empty:
        return result
    frame["ts_code"] = frame["ts_code"].astype(str).str.strip().str.upper()
    frame["trade_date"] = frame["trade_date"].map(normalize_trade_date).astype("int32")
    if (frame.groupby(["ts_code", "trade_date"])["adj_factor"].nunique() > 1).any():
        raise ValueError("15分钟因子修复存在同股票同日期的冲突因子")
    frame = frame.drop_duplicates(["ts_code", "trade_date"])
    result["valid_factor_keys"] = len(frame)
    database = _database_path(root) if connection is not None else ensure_fifteen_min_schema(root)
    context = nullcontext(connection) if connection is not None else duckdb.connect(str(database))
    with context as conn:
        conn.register("repair_fifteen_factors", frame)
        try:
            conn.execute("BEGIN")
            updated = conn.execute("""
                UPDATE fifteen_min_bars AS b SET adj_factor=f.adj_factor
                FROM repair_fifteen_factors AS f
                WHERE b.trade_date BETWEEN ? AND ?
                  AND b.ts_code=f.ts_code AND b.trade_date=f.trade_date
                  AND b.adj_factor IS DISTINCT FROM f.adj_factor
            """, [int(frame.trade_date.min()), int(frame.trade_date.max())]).fetchone()[0]
            if updated:
                touch_minute_update(conn, "fifteen_minute")
            conn.execute("COMMIT")
            result["updated_rows"] = int(updated)
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.unregister("repair_fifteen_factors")
    return result


def mark_fifteen_min_sync(
    ts_code: str,
    requested_start: Union[str, datetime, int],
    requested_end: Union[str, datetime, int],
    *,
    status: str,
    source_host: Optional[str] = None,
    error: Optional[str] = None,
    root: Optional[Union[str, Path]] = None,
) -> None:
    database = ensure_fifteen_min_schema(root)
    start = int(normalize_trade_date(requested_start))
    end = int(normalize_trade_date(requested_end))
    # 只有已确认的单日停牌空数据才算覆盖。连接异常或长区间返回空不能推进
    # 断点，否则下一次会从错误的covered_end之后开始，永久漏掉历史数据。
    covered_start = start if status == "empty" else None
    covered_end = end if status == "empty" else None
    with duckdb.connect(str(database)) as conn:
        conn.execute("""
            INSERT INTO fifteen_min_sync
            VALUES (?, ?, ?, NULL, NULL, 0, ?, ?, ?, ?)
            ON CONFLICT(ts_code) DO UPDATE SET
                covered_start=CASE WHEN excluded.status='empty' THEN
                    LEAST(fifteen_min_sync.covered_start, excluded.covered_start)
                    ELSE fifteen_min_sync.covered_start END,
                covered_end=CASE WHEN excluded.status='empty' THEN
                    GREATEST(fifteen_min_sync.covered_end, excluded.covered_end)
                    ELSE fifteen_min_sync.covered_end END,
                status=excluded.status, source_host=excluded.source_host,
                updated_at=excluded.updated_at, error=excluded.error
        """, [
            ts_code.upper(), covered_start, covered_end,
            status, source_host, _utc_now(), error,
        ])


def get_fifteen_min_sync(
    root: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    database = ensure_fifteen_min_schema(root)
    with duckdb.connect(str(database), read_only=True) as conn:
        return conn.execute(
            "SELECT * FROM fifteen_min_sync ORDER BY ts_code"
        ).fetchdf()


def get_fifteen_min(
    ts_code: Optional[str] = None,
    ts_codes: Optional[Iterable[str]] = None,
    trade_date: Optional[Union[str, datetime, int]] = None,
    start_date: Optional[Union[str, datetime, int]] = None,
    end_date: Optional[Union[str, datetime, int]] = None,
    columns: Optional[Sequence[str]] = None,
    root: Optional[Union[str, Path]] = None,
    date_ranges: Optional[Sequence[tuple]] = None,
) -> pd.DataFrame:
    database = _database_path(root)
    if not database.is_file():
        raise FileNotFoundError(f"TDX fifteenMin DuckDB不存在: {database}")
    codes = _normalize_codes(ts_code, ts_codes)
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise ValueError("trade_date 与 start_date/end_date 互斥")
    ranges = None
    if date_ranges is not None:
        if trade_date is not None or start_date is not None or end_date is not None:
            raise ValueError("date_ranges 与 trade_date/start_date/end_date 互斥")
        if not codes:
            raise ValueError("date_ranges 查询必须指定股票代码")
        if not date_ranges:
            raise ValueError("date_ranges 不能为空")
        ranges = []
        for pair in date_ranges:
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise ValueError("date_ranges 每项必须为 (start_date, end_date)")
            lo, hi = (normalize_trade_date(value) for value in pair)
            if lo > hi:
                raise ValueError("start_date不能晚于end_date")
            ranges.append((lo, hi))
        merged = []
        for lo, hi in sorted(ranges):
            if merged and lo <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
            else:
                merged.append((lo, hi))
        ranges = merged
    if columns is not None:
        unknown = sorted(set(columns) - set(STANDARD_COLUMNS))
        if unknown:
            raise ValueError(f"未知fifteenMin字段: {unknown}")
    selected = list(columns) if columns is not None else STANDARD_COLUMNS
    with duckdb.connect(str(database), read_only=True) as conn:
        bounds = None
        if ranges is None and trade_date is None and (start_date is None or end_date is None):
            bounds = conn.execute(
                "SELECT MIN(trade_date), MAX(trade_date) FROM fifteen_min_bars"
            ).fetchone()
            if not bounds or bounds[0] is None:
                return _empty_frame(columns)
        if ranges is not None:
            start, end = ranges[0][0], ranges[-1][1]
        elif trade_date is not None:
            start = end = normalize_trade_date(trade_date)
        else:
            if not codes and start_date is None and end_date is None:
                raise ValueError("全市场fifteenMin查询必须指定trade_date或日期范围")
            start = normalize_trade_date(start_date) if start_date is not None else str(bounds[0])
            end = normalize_trade_date(end_date) if end_date is not None else str(bounds[1])
        if start > end:
            raise ValueError(f"start_date不能晚于end_date: {start}>{end}")
        if not codes and start != end:
            raise ValueError("全市场fifteenMin查询只允许单个trade_date；日期范围查询必须指定股票代码")
        ranges = ranges or [(start, end)]
        where = ["(" + " OR ".join("trade_date BETWEEN ? AND ?" for _ in ranges) + ")"]
        params: list = [int(day) for pair in ranges for day in pair]
        if codes:
            where.append("ts_code IN (" + ",".join("?" for _ in codes) + ")")
            params.extend(codes)
        frame = conn.execute(
            f"SELECT {','.join(selected)} FROM fifteen_min_bars "
            f"WHERE {' AND '.join(where)} ORDER BY ts_code, datetime",
            params,
        ).fetchdf()
    return frame.reset_index(drop=True)


def get_fifteen_min_stats(
    root: Optional[Union[str, Path]] = None,
) -> dict:
    database = ensure_fifteen_min_schema(root)
    with duckdb.connect(str(database), read_only=True) as conn:
        row = conn.execute("""
            SELECT COUNT(*), COUNT(DISTINCT ts_code), COUNT(DISTINCT trade_date),
                   MIN(trade_date), MAX(trade_date)
            FROM fifteen_min_bars
        """).fetchone()
        sync = conn.execute(
            "SELECT status, COUNT(*) FROM fifteen_min_sync GROUP BY status ORDER BY status"
        ).fetchall()
    return {
        "database": str(database), "rows": row[0], "symbols": row[1],
        "trade_dates": row[2], "min_date": row[3], "max_date": row[4],
        "sync_status": dict(sync),
    }


def optimize_fifteen_min_database(
    root: Optional[Union[str, Path]] = None,
) -> dict:
    """按股票和时间重写事实表并创建查询索引；用于大批量回补完成后。"""
    database = ensure_fifteen_min_schema(root)
    with duckdb.connect(str(database)) as conn:
        conn.execute("BEGIN")
        try:
            conn.execute("DROP TABLE IF EXISTS fifteen_min_bars_sorted")
            conn.execute("""
                CREATE TABLE fifteen_min_bars_sorted AS
                SELECT * FROM fifteen_min_bars ORDER BY ts_code, datetime
            """)
            conn.execute("DROP TABLE fifteen_min_bars")
            conn.execute("ALTER TABLE fifteen_min_bars_sorted RENAME TO fifteen_min_bars")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute(
            "CREATE INDEX idx_fifteen_min_date ON fifteen_min_bars(trade_date)"
        )
        conn.execute(
            "CREATE INDEX idx_fifteen_min_symbol_date "
            "ON fifteen_min_bars(ts_code, trade_date)"
        )
        conn.execute("ANALYZE fifteen_min_bars")
        conn.execute("CHECKPOINT")
        row = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT ts_code), MIN(trade_date), MAX(trade_date) "
            "FROM fifteen_min_bars"
        ).fetchone()
    return {
        "database": str(database), "rows": row[0], "symbols": row[1],
        "min_date": row[2], "max_date": row[3],
    }

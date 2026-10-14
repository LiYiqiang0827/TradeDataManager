"""全市场15分钟OHLC DuckDB存储。

原始CSV直接按交易日写入DuckDB。导入时丢弃09:30标记行和15:00后的盘后K线，
只保留09:45-11:30、13:15-15:00共16根。
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
from typing import Iterable, Optional, Sequence, Union

import duckdb
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as pacsv
except ImportError as exc:  # pragma: no cover
    raise ImportError("fifteenMin存储需要 pyarrow，请先安装项目依赖") from exc

from config.settings import FIFTEEN_MIN_LEGACY_DB_PATH


SCHEMA_VERSION = "fifteen_min_duckdb.v1"
STANDARD_COLUMNS = [
    "ts_code", "name", "trade_date", "datetime", "time_idx",
    "open", "high", "low", "close", "vol", "amount", "adj_factor",
]
CSV_COLUMNS = {
    "证券代码": "ts_code", "证券名称": "name", "交易日期": "trade_date",
    "分钟时间": "datetime", "开盘价（元）": "open", "最高价（元）": "high",
    "最低价（元）": "low", "收盘价（元）": "close", "成交量": "vol",
    "成交额": "amount", "复权因子": "adj_factor",
}
_DATE_RE = re.compile(r"(?<!\d)(20\d{6})(?!\d)")


def normalize_trade_date(value: Union[str, datetime, int]) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"非法交易日期: {value!r}，应为YYYYMMDD或YYYY-MM-DD")
    datetime.strptime(text, "%Y%m%d")
    return text


def _normalize_codes(
    ts_code: Optional[str] = None,
    ts_codes: Optional[Iterable[str]] = None,
) -> list[str]:
    values = list(ts_codes) if ts_codes is not None else ([ts_code] if ts_code else [])
    return sorted({str(value).strip().upper() for value in values if str(value).strip()})


def _empty_frame(columns: Optional[Sequence[str]] = None) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns or STANDARD_COLUMNS))


def _database_path(
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
) -> Path:
    candidate = Path(catalog_path) if catalog_path is not None else (
        Path(root) if root is not None else FIFTEEN_MIN_LEGACY_DB_PATH
    )
    return candidate if candidate.suffix.lower() == ".duckdb" else candidate / "fifteen_min.duckdb"


def _open_database(
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
):
    database = _database_path(root, catalog_path)
    database.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(database))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fifteen_min_bars (
            ts_code VARCHAR NOT NULL, name VARCHAR, trade_date INTEGER NOT NULL,
            datetime TIMESTAMP NOT NULL, time_idx SMALLINT NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            vol BIGINT, amount DOUBLE, adj_factor DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fifteen_min_ingest_catalog (
            trade_date VARCHAR PRIMARY KEY, source_path VARCHAR NOT NULL,
            source_size UBIGINT NOT NULL, source_mtime_ns UBIGINT NOT NULL,
            source_present BOOLEAN NOT NULL DEFAULT TRUE,
            schema_version VARCHAR NOT NULL, rows BIGINT, stock_count INTEGER,
            bars_per_stock_min SMALLINT, bars_per_stock_max SMALLINT,
            discarded_opening_marker_rows BIGINT, discarded_after_close_rows BIGINT,
            status VARCHAR NOT NULL, discovered_at TIMESTAMP NOT NULL,
            last_checked_at TIMESTAMP NOT NULL, imported_at TIMESTAMP, error VARCHAR,
            source_duplicate_count INTEGER DEFAULT 1, source_candidates JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fifteen_min_source_files (
            source_path VARCHAR PRIMARY KEY, trade_date VARCHAR NOT NULL,
            source_size UBIGINT NOT NULL, source_mtime_ns UBIGINT NOT NULL,
            sha256 VARCHAR, selected BOOLEAN NOT NULL DEFAULT FALSE,
            source_present BOOLEAN NOT NULL DEFAULT TRUE,
            discovered_at TIMESTAMP NOT NULL, last_checked_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fifteen_min_dataset_meta (
            key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL, updated_at TIMESTAMP NOT NULL
        )
        """
    )
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        "INSERT INTO fifteen_min_dataset_meta VALUES ('schema_version', ?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        [SCHEMA_VERSION, now],
    )
    return conn


def get_fifteen_min_catalog(
    catalog_path: Optional[Union[str, Path]] = None, *, status: Optional[str] = None,
) -> pd.DataFrame:
    with _open_database(catalog_path=catalog_path) as conn:
        if status:
            return conn.execute(
                "SELECT * FROM fifteen_min_ingest_catalog WHERE status=? ORDER BY trade_date",
                [status],
            ).fetchdf()
        return conn.execute(
            "SELECT * FROM fifteen_min_ingest_catalog ORDER BY trade_date"
        ).fetchdf()


def get_fifteen_min_source_files(
    catalog_path: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    with _open_database(catalog_path=catalog_path) as conn:
        return conn.execute(
            "SELECT * FROM fifteen_min_source_files ORDER BY trade_date, source_path"
        ).fetchdf()


def _csv_trade_date(path: Path) -> str:
    match = _DATE_RE.search(path.name)
    if not match:
        raise ValueError(f"CSV文件名中没有YYYYMMDD交易日: {path.name}")
    return normalize_trade_date(match.group(1))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _select_source_files(source_root: Path, start: str, end: str) -> dict[str, dict]:
    groups: dict[str, list[Path]] = {}
    for path in sorted(source_root.rglob("*.csv")):
        if path.name.startswith("._"):
            continue
        try:
            day = _csv_trade_date(path)
        except ValueError:
            continue
        if start <= day <= end:
            groups.setdefault(day, []).append(path)
    selected: dict[str, dict] = {}
    for day, paths in groups.items():
        hashes = {str(path): _sha256(path) for path in paths} if len(paths) > 1 else {}
        conflict = len(set(hashes.values())) > 1
        choice = sorted(
            paths,
            key=lambda p: (bool(re.search(r"\(\d+\)", p.stem)), len(p.name), p.name),
        )[0]
        selected[day] = {
            "path": choice, "paths": paths, "hashes": hashes, "conflict": conflict,
        }
    return selected


def scan_fifteen_min_sources(
    csv_root: Union[str, Path],
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
    *,
    start_date: Optional[Union[str, datetime, int]] = None,
    end_date: Optional[Union[str, datetime, int]] = None,
) -> dict:
    source_root = Path(csv_root)
    if not source_root.is_dir():
        raise FileNotFoundError(f"fifteenMin源CSV目录不存在或移动硬盘未挂载: {source_root}")
    start = normalize_trade_date(start_date) if start_date is not None else "00000000"
    end = normalize_trade_date(end_date) if end_date is not None else "99999999"
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    groups = _select_source_files(source_root, start, end)
    present_dates = set(groups)
    source_paths_seen = {str(path) for group in groups.values() for path in group["paths"]}
    discovered = pending = unchanged = 0
    duplicate_files = sum(max(0, len(group["paths"]) - 1) for group in groups.values())
    conflicts = sum(bool(group["conflict"]) for group in groups.values())
    with _open_database(root, catalog_path) as conn:
        for day, group in sorted(groups.items()):
            selected_path = group["path"]
            candidates = [str(item) for item in group["paths"]]
            for candidate in group["paths"]:
                stat = candidate.stat()
                conn.execute(
                    """
                    INSERT INTO fifteen_min_source_files (
                        source_path, trade_date, source_size, source_mtime_ns, sha256,
                        selected, source_present, discovered_at, last_checked_at
                    ) VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?)
                    ON CONFLICT (source_path) DO UPDATE SET
                        trade_date=excluded.trade_date, source_size=excluded.source_size,
                        source_mtime_ns=excluded.source_mtime_ns, sha256=excluded.sha256,
                        selected=excluded.selected, source_present=TRUE,
                        last_checked_at=excluded.last_checked_at
                    """,
                    [str(candidate), day, stat.st_size, stat.st_mtime_ns,
                     group["hashes"].get(str(candidate)), candidate == selected_path, now, now],
                )
            stat = selected_path.stat()
            row = conn.execute(
                "SELECT source_size, source_mtime_ns, schema_version, status "
                "FROM fifteen_min_ingest_catalog WHERE trade_date=?",
                [day],
            ).fetchone()
            is_current = bool(
                row and int(row[0]) == stat.st_size and int(row[1]) == stat.st_mtime_ns
                and row[2] == SCHEMA_VERSION and row[3] == "imported"
            )
            new_status = "source_conflict" if group["conflict"] else (
                "imported" if is_current else "pending"
            )
            if row is None:
                discovered += 1
            elif is_current:
                unchanged += 1
            if new_status == "pending":
                pending += 1
            conn.execute(
                """
                INSERT INTO fifteen_min_ingest_catalog (
                    trade_date, source_path, source_size, source_mtime_ns,
                    source_present, schema_version, status, discovered_at,
                    last_checked_at, error, source_duplicate_count, source_candidates
                ) VALUES (?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (trade_date) DO UPDATE SET
                    source_path=excluded.source_path, source_size=excluded.source_size,
                    source_mtime_ns=excluded.source_mtime_ns, source_present=TRUE,
                    schema_version=excluded.schema_version, status=excluded.status,
                    last_checked_at=excluded.last_checked_at, error=excluded.error,
                    source_duplicate_count=excluded.source_duplicate_count,
                    source_candidates=excluded.source_candidates
                """,
                [day, str(selected_path), stat.st_size, stat.st_mtime_ns, SCHEMA_VERSION,
                 new_status, now, now,
                 "同一交易日存在内容不同的多个CSV" if group["conflict"] else None,
                 len(candidates), json.dumps(candidates, ensure_ascii=False)],
            )
        existing = conn.execute(
            "SELECT trade_date FROM fifteen_min_ingest_catalog WHERE trade_date BETWEEN ? AND ?",
            [start, end],
        ).fetchall()
        missing = [row[0] for row in existing if row[0] not in present_dates]
        if missing:
            conn.executemany(
                "UPDATE fifteen_min_ingest_catalog SET source_present=FALSE, "
                "last_checked_at=? WHERE trade_date=?",
                [(now, day) for day in missing],
            )
        known_sources = conn.execute(
            "SELECT source_path FROM fifteen_min_source_files WHERE trade_date BETWEEN ? AND ?",
            [start, end],
        ).fetchall()
        absent = [row[0] for row in known_sources if row[0] not in source_paths_seen]
        if absent:
            conn.executemany(
                "UPDATE fifteen_min_source_files SET source_present=FALSE, selected=FALSE, "
                "last_checked_at=? WHERE source_path=?",
                [(now, path) for path in absent],
            )
    return {
        "source_root": str(source_root), "database": str(_database_path(root, catalog_path)),
        "discovered": discovered, "pending": pending, "unchanged": unchanged,
        "source_missing": len(missing), "scanned": len(present_dates),
        "source_files": len(source_paths_seen), "duplicate_files": duplicate_files,
        "source_conflicts": conflicts,
    }


def _csv_reader(path: Path):
    column_types = {
        "证券代码": pa.string(), "证券名称": pa.string(), "交易日期": pa.int32(),
        "分钟时间": pa.timestamp("s"), "开盘价（元）": pa.float64(),
        "最高价（元）": pa.float64(), "最低价（元）": pa.float64(),
        "收盘价（元）": pa.float64(), "成交量": pa.float64(),
        "成交额": pa.float64(), "复权因子": pa.float64(),
    }
    return pacsv.open_csv(
        path,
        read_options=pacsv.ReadOptions(block_size=16 * 1024 * 1024, use_threads=True),
        convert_options=pacsv.ConvertOptions(
            column_types=column_types, timestamp_parsers=["%Y-%m-%d %H:%M:%S"],
        ),
    )


def _time_index(datetimes: pa.Array) -> pa.Array:
    wall_minute = pc.add(pc.multiply(pc.hour(datetimes), 60), pc.minute(datetimes))
    allowed = pa.array(
        [585, 600, 615, 630, 645, 660, 675, 690,
         795, 810, 825, 840, 855, 870, 885, 900],
        type=pa.int64(),
    )
    return pc.cast(pc.index_in(wall_minute, value_set=allowed), pa.int16())


def _normalize_batch(batch: pa.RecordBatch, expected_date: str) -> pa.RecordBatch:
    missing = sorted(set(CSV_COLUMNS) - set(batch.schema.names))
    if missing:
        raise ValueError(f"CSV缺少字段: {missing}")
    source = {
        CSV_COLUMNS[name]: batch.column(batch.schema.get_field_index(name))
        for name in CSV_COLUMNS
    }
    hours = pc.hour(source["datetime"])
    minutes = pc.minute(source["datetime"])
    at_or_before_close = pc.or_(
        pc.less(hours, 15), pc.and_(pc.equal(hours, 15), pc.equal(minutes, 0)),
    )
    opening = pc.and_(pc.equal(hours, 9), pc.equal(minutes, 30))
    keep = pc.and_(at_or_before_close, pc.invert(opening))
    source = {name: pc.filter(values, keep) for name, values in source.items()}
    trade_date = pc.cast(source["trade_date"], pa.int32())
    if pc.any(pc.not_equal(trade_date, int(expected_date))).as_py():
        raise ValueError(f"CSV交易日期与文件名不一致: {expected_date}")
    vol_float = source["vol"]
    if pc.any(pc.not_equal(vol_float, pc.floor(vol_float))).as_py():
        raise ValueError("成交量存在非整数值")
    idx = _time_index(source["datetime"])
    if pc.any(pc.invert(pc.is_valid(idx))).as_py():
        raise ValueError("15分钟时间不在标准09:45-11:30或13:15-15:00序列")
    normalized = pa.RecordBatch.from_arrays(
        [source["ts_code"], source["name"], trade_date, source["datetime"], idx,
         source["open"], source["high"], source["low"], source["close"],
         pc.cast(vol_float, pa.int64()), source["amount"], source["adj_factor"]],
        names=STANDARD_COLUMNS,
    )
    if normalized.num_rows:
        high = normalized.column(6)
        low = normalized.column(7)
        opn = normalized.column(5)
        close = normalized.column(8)
        invalid = pc.or_(
            pc.or_(pc.less(high, low), pc.less(high, opn)),
            pc.or_(pc.less(high, close), pc.or_(pc.greater(low, opn), pc.greater(low, close))),
        )
        if pc.any(invalid).as_py():
            raise ValueError("发现不满足 low<=open/close<=high 的15分钟K线")
    return normalized


def import_fifteen_min_csv(
    csv_path: Union[str, Path],
    root: Optional[Union[str, Path]] = None,
    *,
    overwrite: bool = False,
    catalog_path: Optional[Union[str, Path]] = None,
) -> dict:
    source_path = Path(csv_path)
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    trade_date = _csv_trade_date(source_path)
    stat = source_path.stat()
    with _open_database(root, catalog_path) as conn:
        current = conn.execute(
            "SELECT source_size, source_mtime_ns, schema_version, status, rows, stock_count, "
            "bars_per_stock_min, bars_per_stock_max, discarded_opening_marker_rows, "
            "discarded_after_close_rows FROM fifteen_min_ingest_catalog WHERE trade_date=?",
            [trade_date],
        ).fetchone()
        if (
            current and not overwrite and int(current[0]) == stat.st_size
            and int(current[1]) == stat.st_mtime_ns and current[2] == SCHEMA_VERSION
            and current[3] == "imported"
        ):
            return {
                "schema_version": SCHEMA_VERSION, "trade_date": trade_date,
                "rows": current[4], "stock_count": current[5],
                "bars_per_stock_min": current[6], "bars_per_stock_max": current[7],
                "discarded_opening_marker_rows": current[8],
                "discarded_after_close_rows": current[9],
                "discarded_outside_session_rows": (current[8] or 0) + (current[9] or 0),
                "database": str(_database_path(root, catalog_path)),
                "action": "skipped_existing",
            }

    batches: list[pa.RecordBatch] = []
    discarded_opening = discarded_after_close = 0
    for raw_batch in _csv_reader(source_path):
        raw_dt = raw_batch.column(raw_batch.schema.get_field_index("分钟时间"))
        hours = pc.hour(raw_dt)
        minutes = pc.minute(raw_dt)
        opening = pc.and_(pc.equal(hours, 9), pc.equal(minutes, 30))
        after_close = pc.or_(
            pc.greater(hours, 15),
            pc.and_(pc.equal(hours, 15), pc.greater(minutes, 0)),
        )
        discarded_opening += int(pc.sum(pc.cast(opening, pa.int64())).as_py() or 0)
        discarded_after_close += int(pc.sum(pc.cast(after_close, pa.int64())).as_py() or 0)
        normalized = _normalize_batch(raw_batch, trade_date)
        if normalized.num_rows:
            batches.append(normalized)
    if not batches:
        raise ValueError(f"CSV没有有效的盘中15分钟数据: {source_path}")
    table = pa.Table.from_batches(batches)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with _open_database(root, catalog_path) as conn:
        conn.register("incoming_fifteen_min", table)
        duplicate_keys = conn.execute(
            "SELECT COUNT(*) FROM (SELECT ts_code, datetime FROM incoming_fifteen_min "
            "GROUP BY ts_code, datetime HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        if duplicate_keys:
            conn.unregister("incoming_fifteen_min")
            raise ValueError(f"发现重复15分钟主键组: {duplicate_keys}")
        stats = conn.execute(
            "SELECT COUNT(*), MIN(n), MAX(n) FROM ("
            "SELECT ts_code, COUNT(*) n FROM incoming_fifteen_min GROUP BY ts_code)"
        ).fetchone()
        stock_count, bars_min, bars_max = map(int, stats)
        rows = table.num_rows
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DELETE FROM fifteen_min_bars WHERE trade_date=?", [int(trade_date)])
            conn.execute(
                "INSERT INTO fifteen_min_bars SELECT * FROM incoming_fifteen_min "
                "ORDER BY ts_code, datetime"
            )
            conn.execute(
                """
                INSERT INTO fifteen_min_ingest_catalog (
                    trade_date, source_path, source_size, source_mtime_ns,
                    source_present, schema_version, rows, stock_count,
                    bars_per_stock_min, bars_per_stock_max,
                    discarded_opening_marker_rows, discarded_after_close_rows,
                    status, discovered_at, last_checked_at, imported_at, error,
                    source_duplicate_count, source_candidates
                ) VALUES (?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?, 'imported', ?, ?, ?, NULL, 1, ?)
                ON CONFLICT (trade_date) DO UPDATE SET
                    source_path=excluded.source_path, source_size=excluded.source_size,
                    source_mtime_ns=excluded.source_mtime_ns, source_present=TRUE,
                    schema_version=excluded.schema_version, rows=excluded.rows,
                    stock_count=excluded.stock_count,
                    bars_per_stock_min=excluded.bars_per_stock_min,
                    bars_per_stock_max=excluded.bars_per_stock_max,
                    discarded_opening_marker_rows=excluded.discarded_opening_marker_rows,
                    discarded_after_close_rows=excluded.discarded_after_close_rows,
                    status='imported', last_checked_at=excluded.last_checked_at,
                    imported_at=excluded.imported_at, error=NULL
                """,
                [trade_date, str(source_path), stat.st_size, stat.st_mtime_ns,
                 SCHEMA_VERSION, rows, stock_count, bars_min, bars_max,
                 discarded_opening, discarded_after_close, now, now, now,
                 json.dumps([str(source_path)], ensure_ascii=False)],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.unregister("incoming_fifteen_min")
    return {
        "schema_version": SCHEMA_VERSION, "trade_date": trade_date,
        "source_file": str(source_path), "database": str(_database_path(root, catalog_path)),
        "rows": rows, "stock_count": stock_count,
        "bars_per_stock_min": bars_min, "bars_per_stock_max": bars_max,
        "discarded_opening_marker_rows": discarded_opening,
        "discarded_after_close_rows": discarded_after_close,
        "discarded_outside_session_rows": discarded_opening + discarded_after_close,
        "action": "imported",
    }


def _record_failure(
    trade_date: str, error: str,
    root: Optional[Union[str, Path]], catalog_path: Optional[Union[str, Path]],
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with _open_database(root, catalog_path) as conn:
        conn.execute(
            "UPDATE fifteen_min_ingest_catalog SET status='failed', error=?, "
            "last_checked_at=? WHERE trade_date=?",
            [error[:4000], now, trade_date],
        )


def optimize_fifteen_min_database(
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
) -> dict:
    """重写为按股票和时间物理排序的紧凑数据库，原子替换原库。"""
    database = _database_path(root, catalog_path)
    if not database.is_file():
        raise FileNotFoundError(database)
    temporary = database.with_name(database.stem + ".optimize.inprogress.duckdb")
    for stale in (temporary,):
        if stale.exists():
            shutil.rmtree(stale) if stale.is_dir() else stale.unlink()
    with _open_database(temporary) as conn:
        escaped = str(database).replace("'", "''")
        conn.execute(f"ATTACH '{escaped}' AS source_db (READ_ONLY)")
        has_sorted_source = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_catalog='source_db' "
            "AND table_name='fifteen_min_bars_by_symbol'"
        ).fetchone()[0]
        source_table = (
            "source_db.fifteen_min_bars_by_symbol"
            if has_sorted_source else "source_db.fifteen_min_bars"
        )
        order_clause = "" if has_sorted_source else " ORDER BY ts_code, datetime"
        conn.execute(
            f"INSERT INTO fifteen_min_bars SELECT * FROM {source_table}{order_clause}"
        )
        for table in (
            "fifteen_min_ingest_catalog",
            "fifteen_min_source_files",
            "fifteen_min_dataset_meta",
        ):
            conn.execute(f"DELETE FROM {table}")
            conn.execute(f"INSERT INTO {table} SELECT * FROM source_db.{table}")
        conn.execute("ANALYZE fifteen_min_bars")
        conn.execute("DETACH source_db")
        conn.execute("CHECKPOINT")
        rows = conn.execute("SELECT COUNT(*) FROM fifteen_min_bars").fetchone()[0]
    # os.replace在同一文件系统内原子替换，进程中断时原库或新库至少有一个完整存在。
    temporary.replace(database)
    return {
        "database": str(database), "rows": rows,
        "layout": "ordered_by_ts_code_datetime", "optimized": True,
    }


def import_fifteen_min_directory(
    csv_root: Union[str, Path],
    root: Optional[Union[str, Path]] = None,
    *,
    start_date: Optional[Union[str, datetime, int]] = None,
    end_date: Optional[Union[str, datetime, int]] = None,
    overwrite: bool = False,
    catalog_path: Optional[Union[str, Path]] = None,
) -> list[dict]:
    scan_fifteen_min_sources(
        csv_root, root=root, catalog_path=catalog_path,
        start_date=start_date, end_date=end_date,
    )
    start = normalize_trade_date(start_date) if start_date is not None else "00000000"
    end = normalize_trade_date(end_date) if end_date is not None else "99999999"
    with _open_database(root, catalog_path) as conn:
        statuses = "('pending','failed','imported')" if overwrite else "('pending','failed')"
        pending_rows = conn.execute(
            "SELECT trade_date, source_path FROM fifteen_min_ingest_catalog "
            f"WHERE status IN {statuses} AND source_present=TRUE "
            "AND trade_date BETWEEN ? AND ? ORDER BY trade_date",
            [start, end],
        ).fetchall()
    results: list[dict] = []
    for day, source_path in pending_rows:
        try:
            results.append(import_fifteen_min_csv(
                source_path, root=root, overwrite=overwrite, catalog_path=catalog_path,
            ))
        except Exception as exc:
            _record_failure(day, str(exc), root, catalog_path)
            results.append({"trade_date": day, "action": "failed", "error": str(exc)})
    imported_count = sum(item.get("action") == "imported" for item in results)
    if imported_count >= 20 and not any(item.get("action") == "failed" for item in results):
        optimize_fifteen_min_database(root, catalog_path)
    return results


def list_fifteen_min_dates(root: Optional[Union[str, Path]] = None) -> list[str]:
    database = _database_path(root)
    if not database.is_file():
        return []
    with duckdb.connect(str(database), read_only=True) as conn:
        return [str(row[0]) for row in conn.execute(
            "SELECT DISTINCT trade_date FROM fifteen_min_bars ORDER BY trade_date"
        ).fetchall()]


def get_fifteen_min(
    ts_code: Optional[str] = None,
    ts_codes: Optional[Iterable[str]] = None,
    trade_date: Optional[Union[str, datetime, int]] = None,
    start_date: Optional[Union[str, datetime, int]] = None,
    end_date: Optional[Union[str, datetime, int]] = None,
    columns: Optional[Sequence[str]] = None,
    root: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    database = _database_path(root)
    if not database.is_file():
        raise FileNotFoundError(f"fifteenMin DuckDB不存在: {database}")
    codes = _normalize_codes(ts_code, ts_codes)
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise ValueError("trade_date 与 start_date/end_date 互斥")
    if columns is not None:
        unknown = sorted(set(columns) - set(STANDARD_COLUMNS))
        if unknown:
            raise ValueError(f"未知fifteenMin字段: {unknown}")
    selected = list(columns) if columns is not None else STANDARD_COLUMNS
    with duckdb.connect(str(database), read_only=True) as conn:
        bounds = conn.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM fifteen_min_bars"
        ).fetchone()
        if bounds[0] is None:
            return _empty_frame(columns)
        if trade_date is not None:
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
        where = ["trade_date BETWEEN ? AND ?"]
        params: list = [int(start), int(end)]
        if codes:
            where.append("ts_code IN (" + ",".join("?" for _ in codes) + ")")
            params.extend(codes)
        frame = conn.execute(
            f"SELECT {','.join(selected)} FROM fifteen_min_bars "
            f"WHERE {' AND '.join(where)} ORDER BY ts_code, datetime",
            params,
        ).fetchdf()
    return frame.reset_index(drop=True)

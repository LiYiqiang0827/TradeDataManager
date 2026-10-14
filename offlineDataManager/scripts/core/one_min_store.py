"""全市场1分钟OHLC Parquet存储。

数据与旧的 ``policy_minute.db`` 完全独立：

* 原始CSV按交易日导入一个Parquet分区；
* Parquet文件内部按 ``ts_code, datetime`` 排序；
* 查询同时支持全市场单日和单股/多股日期范围；
* 不提供SQLite或在线数据回退。
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Iterable, Optional, Sequence, Union

import duckdb
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - environment error is explicit
    raise ImportError("oneMin存储需要 pyarrow，请先安装项目依赖") from exc

from config.settings import ONE_MIN_CATALOG_PATH, ONE_MIN_PARQUET_ROOT


SCHEMA_VERSION = "one_min_ohlc.v3_240bars"
STANDARD_COLUMNS = [
    "ts_code", "name", "trade_date", "datetime", "time_idx",
    "open", "high", "low", "close", "vol", "amount", "adj_factor",
]
CSV_COLUMNS = {
    "证券代码": "ts_code",
    "证券名称": "name",
    "交易日期": "trade_date",
    "分钟时间": "datetime",
    "开盘价（元）": "open",
    "最高价（元）": "high",
    "最低价（元）": "low",
    "收盘价（元）": "close",
    "成交量": "vol",
    "成交额": "amount",
    "复权因子": "adj_factor",
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


def _partition_dir(root: Path, trade_date: str) -> Path:
    return root / f"year={trade_date[:4]}" / f"month={trade_date[4:6]}" / f"trade_date={trade_date}"


def _manifest_path(root: Path, trade_date: str) -> Path:
    return root / "_manifest" / f"trade_date={trade_date}.json"


def _catalog_path(path: Optional[Union[str, Path]] = None) -> Path:
    return Path(path) if path is not None else ONE_MIN_CATALOG_PATH


def _open_catalog(path: Optional[Union[str, Path]] = None):
    catalog = _catalog_path(path)
    catalog.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(catalog))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS one_min_ingest_catalog (
            trade_date VARCHAR PRIMARY KEY,
            source_path VARCHAR NOT NULL,
            source_size UBIGINT NOT NULL,
            source_mtime_ns UBIGINT NOT NULL,
            source_present BOOLEAN NOT NULL DEFAULT TRUE,
            parquet_path VARCHAR,
            parquet_size UBIGINT,
            schema_version VARCHAR,
            rows BIGINT,
            stock_count INTEGER,
            bars_per_stock_min SMALLINT,
            bars_per_stock_max SMALLINT,
            status VARCHAR NOT NULL,
            discovered_at TIMESTAMP NOT NULL,
            last_checked_at TIMESTAMP NOT NULL,
            imported_at TIMESTAMP,
            error VARCHAR
        )
        """
    )
    for column_sql in (
        "source_duplicate_count INTEGER DEFAULT 1",
        "source_candidates JSON",
    ):
        conn.execute(f"ALTER TABLE one_min_ingest_catalog ADD COLUMN IF NOT EXISTS {column_sql}")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS one_min_source_files (
            source_path VARCHAR PRIMARY KEY,
            trade_date VARCHAR NOT NULL,
            source_size UBIGINT NOT NULL,
            source_mtime_ns UBIGINT NOT NULL,
            sha256 VARCHAR,
            selected BOOLEAN NOT NULL DEFAULT FALSE,
            source_present BOOLEAN NOT NULL DEFAULT TRUE,
            discovered_at TIMESTAMP NOT NULL,
            last_checked_at TIMESTAMP NOT NULL
        )
        """
    )
    return conn


def get_one_min_catalog(
    catalog_path: Optional[Union[str, Path]] = None,
    *,
    status: Optional[str] = None,
) -> pd.DataFrame:
    """读取DuckDB导入目录表。"""
    with _open_catalog(catalog_path) as conn:
        if status:
            return conn.execute(
                "SELECT * FROM one_min_ingest_catalog WHERE status=? ORDER BY trade_date",
                [status],
            ).fetchdf()
        return conn.execute(
            "SELECT * FROM one_min_ingest_catalog ORDER BY trade_date"
        ).fetchdf()


def get_one_min_source_files(
    catalog_path: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    with _open_catalog(catalog_path) as conn:
        return conn.execute(
            "SELECT * FROM one_min_source_files ORDER BY trade_date, source_path"
        ).fetchdf()


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
        # 相同副本优先选择没有“(n)”后缀且文件名最短的原文件。
        choice = sorted(paths, key=lambda p: (bool(re.search(r"\(\d+\)", p.stem)), len(p.name), p.name))[0]
        selected[day] = {
            "path": choice,
            "paths": paths,
            "hashes": hashes,
            "conflict": conflict,
        }
    return selected


def scan_one_min_sources(
    csv_root: Union[str, Path],
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
    *,
    start_date: Optional[Union[str, datetime, int]] = None,
    end_date: Optional[Union[str, datetime, int]] = None,
) -> dict:
    """扫描移动硬盘CSV，将新增、变化、已转换状态写入DuckDB目录表。"""
    source_root = Path(csv_root)
    if not source_root.is_dir():
        raise FileNotFoundError(f"oneMin源CSV目录不存在或移动硬盘未挂载: {source_root}")
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    start = normalize_trade_date(start_date) if start_date is not None else "00000000"
    end = normalize_trade_date(end_date) if end_date is not None else "99999999"
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    discovered = 0
    pending = 0
    unchanged = 0
    groups = _select_source_files(source_root, start, end)
    present_dates = set(groups)
    source_paths_seen = {str(path) for group in groups.values() for path in group["paths"]}
    duplicate_files = sum(max(0, len(group["paths"]) - 1) for group in groups.values())
    conflicts = sum(bool(group["conflict"]) for group in groups.values())
    with _open_catalog(catalog_path) as conn:
        for day, group in sorted(groups.items()):
            path = group["path"]
            candidates = [str(item) for item in group["paths"]]
            for candidate in group["paths"]:
                stat = candidate.stat()
                conn.execute(
                    """
                    INSERT INTO one_min_source_files (
                        source_path, trade_date, source_size, source_mtime_ns, sha256,
                        selected, source_present, discovered_at, last_checked_at
                    ) VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?)
                    ON CONFLICT (source_path) DO UPDATE SET
                        trade_date=excluded.trade_date,
                        source_size=excluded.source_size,
                        source_mtime_ns=excluded.source_mtime_ns,
                        sha256=excluded.sha256,
                        selected=excluded.selected,
                        source_present=TRUE,
                        last_checked_at=excluded.last_checked_at
                    """,
                    [str(candidate), day, stat.st_size, stat.st_mtime_ns,
                     group["hashes"].get(str(candidate)), candidate == path, now, now],
                )
            stat = path.stat()
            target = _partition_dir(base, day) / "part-000.parquet"
            manifest_path = _manifest_path(base, day)
            manifest = None
            if manifest_path.is_file():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    manifest = None
            row = conn.execute(
                "SELECT source_path, source_size, source_mtime_ns, schema_version, status "
                "FROM one_min_ingest_catalog WHERE trade_date=?",
                [day],
            ).fetchone()
            same_registered_source = bool(
                row
                and int(row[1]) == stat.st_size
                and int(row[2]) == stat.st_mtime_ns
                and row[3] == SCHEMA_VERSION
                and row[4] == "imported"
            )
            relocated_existing_source = bool(
                row
                and manifest
                and manifest.get("schema_version") == SCHEMA_VERSION
                and int(manifest.get("source_size", -1)) == stat.st_size
                and manifest.get("source_file") != str(path)
                and manifest.get("status") == "ok"
            )
            is_current = bool(target.is_file() and (same_registered_source or relocated_existing_source))
            new_status = "source_conflict" if group["conflict"] else ("imported" if is_current else "pending")
            if row is None:
                discovered += 1
            elif is_current:
                unchanged += 1
            if new_status == "pending":
                pending += 1
            # 仅移动源文件时不重做Parquet，但同步更新逐日JSON清单中的源路径。
            if is_current and row and row[0] != str(path):
                if manifest_path.is_file():
                    manifest = manifest or json.loads(manifest_path.read_text(encoding="utf-8"))
                    manifest["source_file"] = str(path)
                    manifest["source_size"] = stat.st_size
                    manifest["source_mtime_ns"] = stat.st_mtime_ns
                    temporary = manifest_path.with_suffix(".json.inprogress")
                    temporary.write_text(
                        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                    temporary.replace(manifest_path)
            conn.execute(
                """
                INSERT INTO one_min_ingest_catalog (
                    trade_date, source_path, source_size, source_mtime_ns,
                    source_present, parquet_path, schema_version, status,
                    discovered_at, last_checked_at, error,
                    source_duplicate_count, source_candidates
                ) VALUES (?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (trade_date) DO UPDATE SET
                    source_path=excluded.source_path,
                    source_size=excluded.source_size,
                    source_mtime_ns=excluded.source_mtime_ns,
                    source_present=TRUE,
                    parquet_path=excluded.parquet_path,
                    schema_version=excluded.schema_version,
                    status=excluded.status,
                    last_checked_at=excluded.last_checked_at,
                    error=excluded.error,
                    source_duplicate_count=excluded.source_duplicate_count,
                    source_candidates=excluded.source_candidates
                """,
                [day, str(path), stat.st_size, stat.st_mtime_ns, str(target),
                 SCHEMA_VERSION, new_status, now, now,
                 "同一交易日存在内容不同的多个CSV" if group["conflict"] else None,
                 len(candidates), json.dumps(candidates, ensure_ascii=False)],
            )
        # 只标记目录表中位于本次扫描范围、但源CSV已经不存在的记录。
        existing = conn.execute(
            "SELECT trade_date FROM one_min_ingest_catalog WHERE trade_date BETWEEN ? AND ?",
            [start, end],
        ).fetchall()
        missing = [row[0] for row in existing if row[0] not in present_dates]
        if missing:
            conn.executemany(
                "UPDATE one_min_ingest_catalog SET source_present=FALSE, last_checked_at=? "
                "WHERE trade_date=?",
                [(now, day) for day in missing],
            )
        known_sources = conn.execute(
            "SELECT source_path FROM one_min_source_files WHERE trade_date BETWEEN ? AND ?",
            [start, end],
        ).fetchall()
        absent_sources = [row[0] for row in known_sources if row[0] not in source_paths_seen]
        if absent_sources:
            conn.executemany(
                "UPDATE one_min_source_files SET source_present=FALSE, selected=FALSE, "
                "last_checked_at=? WHERE source_path=?",
                [(now, path) for path in absent_sources],
            )
    return {
        "source_root": str(source_root),
        "discovered": discovered,
        "pending": pending,
        "unchanged": unchanged,
        "source_missing": len(missing),
        "scanned": len(present_dates),
        "source_files": len(source_paths_seen),
        "duplicate_files": duplicate_files,
        "source_conflicts": conflicts,
    }


def _catalog_record_import(
    manifest: dict,
    catalog_path: Optional[Union[str, Path]] = None,
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with _open_catalog(catalog_path) as conn:
        conn.execute(
            """
            UPDATE one_min_ingest_catalog SET
                parquet_path=?, parquet_size=?, schema_version=?, rows=?,
                stock_count=?, bars_per_stock_min=?, bars_per_stock_max=?,
                status='imported', imported_at=?, last_checked_at=?, error=NULL
            WHERE trade_date=?
            """,
            [manifest["target_file"], manifest["target_size"], manifest["schema_version"],
             manifest["rows"], manifest["stock_count"], manifest["bars_per_stock_min"],
             manifest["bars_per_stock_max"], now, now, manifest["trade_date"]],
        )


def _catalog_record_failure(
    trade_date: str,
    error: str,
    catalog_path: Optional[Union[str, Path]] = None,
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with _open_catalog(catalog_path) as conn:
        conn.execute(
            "UPDATE one_min_ingest_catalog SET status='failed', error=?, last_checked_at=? "
            "WHERE trade_date=?",
            [error[:4000], now, trade_date],
        )


def _discover_files(root: Path, start_date: str, end_date: str) -> list[Path]:
    files: list[Path] = []
    for path in root.glob("year=*/month=*/trade_date=*/part-*.parquet"):
        match = _DATE_RE.search(path.parent.name)
        if match and start_date <= match.group(1) <= end_date:
            files.append(path)
    return sorted(files)


def list_one_min_dates(root: Optional[Union[str, Path]] = None) -> list[str]:
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    dates = set()
    if not base.exists():
        return []
    for path in base.glob("year=*/month=*/trade_date=*"):
        match = _DATE_RE.search(path.name)
        if match and any(path.glob("part-*.parquet")):
            dates.add(match.group(1))
    return sorted(dates)


def get_one_min(
    ts_code: Optional[str] = None,
    ts_codes: Optional[Iterable[str]] = None,
    trade_date: Optional[Union[str, datetime, int]] = None,
    start_date: Optional[Union[str, datetime, int]] = None,
    end_date: Optional[Union[str, datetime, int]] = None,
    columns: Optional[Sequence[str]] = None,
    root: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    """读取全市场1分钟OHLC数据，不访问旧SQLite，不联网补数。

    全市场查询必须指定 ``trade_date``；传入股票代码后可以省略日期，
    此时读取当前Parquet数据集中的全部可用交易日。
    """
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    codes = _normalize_codes(ts_code, ts_codes)
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise ValueError("trade_date 与 start_date/end_date 互斥")
    if columns is not None:
        unknown = sorted(set(columns) - set(STANDARD_COLUMNS))
        if unknown:
            raise ValueError(f"未知oneMin字段: {unknown}")
    if not base.exists():
        raise FileNotFoundError(f"oneMin Parquet目录不存在或外置硬盘未挂载: {base}")

    available = list_one_min_dates(base)
    if not available:
        return _empty_frame(columns)
    if trade_date is not None:
        start = end = normalize_trade_date(trade_date)
    else:
        if not codes and start_date is None and end_date is None:
            raise ValueError("全市场oneMin查询必须指定trade_date或日期范围")
        start = normalize_trade_date(start_date) if start_date is not None else available[0]
        end = normalize_trade_date(end_date) if end_date is not None else available[-1]
    if start > end:
        raise ValueError(f"start_date不能晚于end_date: {start}>{end}")

    files = _discover_files(base, start, end)
    if not files:
        return _empty_frame(columns)
    if not codes and start != end:
        raise ValueError("全市场oneMin查询只允许单个trade_date；日期范围查询必须指定股票代码")
    selected = list(columns) if columns is not None else STANDARD_COLUMNS
    # DuckDB对跨大量日文件的Parquet行组过滤显著快于逐文件PyArrow Dataset扫描。
    # 字段名经过白名单校验，条件值始终参数化。
    glob_path = str(base / "year=*" / "month=*" / "trade_date=*" / "part-*.parquet")
    where = ["trade_date BETWEEN ? AND ?"]
    params: list = [glob_path, int(start), int(end)]
    if codes:
        where.append("ts_code IN (" + ",".join("?" for _ in codes) + ")")
        params.extend(codes)
    sql = (
        f"SELECT {','.join(selected)} FROM read_parquet(?, hive_partitioning=false) "
        f"WHERE {' AND '.join(where)}"
    )
    with duckdb.connect(":memory:") as conn:
        frame = conn.execute(sql, params).fetchdf()
    sort_cols = [name for name in ("ts_code", "trade_date", "time_idx", "datetime") if name in frame.columns]
    if sort_cols and not frame.empty:
        frame = frame.sort_values(sort_cols, kind="stable").reset_index(drop=True)
    return frame


def _csv_trade_date(path: Path) -> str:
    match = _DATE_RE.search(path.name)
    if not match:
        raise ValueError(f"CSV文件名中没有YYYYMMDD交易日: {path.name}")
    return normalize_trade_date(match.group(1))


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
            column_types=column_types,
            timestamp_parsers=["%Y-%m-%d %H:%M:%S"],
        ),
    )


def _time_index(datetimes: pa.Array) -> pa.Array:
    hour = pc.hour(datetimes)
    minute = pc.minute(datetimes)
    wall_minute = pc.add(pc.multiply(hour, 60), minute)
    morning = pc.less(hour, 12)
    morning_idx = pc.subtract(wall_minute, 9 * 60 + 31)
    afternoon_idx = pc.add(120, pc.subtract(wall_minute, 13 * 60 + 1))
    return pc.cast(pc.if_else(morning, morning_idx, afternoon_idx), pa.int16())


def _valid_trading_minute(datetimes: pa.Array) -> pa.Array:
    hour = pc.hour(datetimes)
    minute = pc.minute(datetimes)
    morning = pc.or_(
        pc.and_(pc.equal(hour, 9), pc.greater_equal(minute, 31)),
        pc.or_(pc.equal(hour, 10), pc.and_(pc.equal(hour, 11), pc.less_equal(minute, 30))),
    )
    # 统一只保留连续竞价到15:00，盘后固定价格交易不进入数据库。
    afternoon = pc.or_(
        pc.and_(pc.equal(hour, 13), pc.greater_equal(minute, 1)),
        pc.or_(pc.equal(hour, 14), pc.and_(pc.equal(hour, 15), pc.equal(minute, 0))),
    )
    return pc.or_(morning, afternoon)


def _normalize_batch(batch: pa.RecordBatch, expected_date: str) -> pa.RecordBatch:
    missing = sorted(set(CSV_COLUMNS) - set(batch.schema.names))
    if missing:
        raise ValueError(f"CSV缺少字段: {missing}")
    source = {CSV_COLUMNS[name]: batch.column(batch.schema.get_field_index(name)) for name in CSV_COLUMNS}
    trade_date = pc.cast(source["trade_date"], pa.int32())
    if pc.any(pc.not_equal(trade_date, int(expected_date))).as_py():
        raise ValueError(f"CSV交易日期与文件名不一致: {expected_date}")
    valid_session = _valid_trading_minute(source["datetime"])
    source = {name: pc.filter(values, valid_session) for name, values in source.items()}
    trade_date = pc.cast(source["trade_date"], pa.int32())
    vol_float = source["vol"]
    if pc.any(pc.not_equal(vol_float, pc.floor(vol_float))).as_py():
        raise ValueError("成交量存在非整数值")
    vol = pc.cast(vol_float, pa.int64())
    idx = _time_index(source["datetime"])
    if pc.any(pc.or_(pc.less(idx, 0), pc.greater(idx, 239))).as_py():
        raise ValueError("分钟序号超出0-239范围")
    arrays = [
        source["ts_code"], source["name"], trade_date, source["datetime"], idx,
        source["open"], source["high"], source["low"], source["close"],
        vol, source["amount"], source["adj_factor"],
    ]
    return pa.RecordBatch.from_arrays(arrays, names=STANDARD_COLUMNS)


def import_one_min_csv(
    csv_path: Union[str, Path],
    root: Optional[Union[str, Path]] = None,
    *,
    overwrite: bool = False,
) -> dict:
    """幂等导入单个全市场日CSV，成功后原子替换目标Parquet和清单。"""
    source_path = Path(csv_path)
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    trade_date = _csv_trade_date(source_path)
    target_dir = _partition_dir(base, trade_date)
    target = target_dir / "part-000.parquet"
    manifest_path = _manifest_path(base, trade_date)
    if target.exists() and manifest_path.exists() and not overwrite:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        stat = source_path.stat()
        if (
            manifest.get("schema_version") == SCHEMA_VERSION
            and manifest.get("source_size") == stat.st_size
            and manifest.get("source_mtime_ns") == stat.st_mtime_ns
        ):
            manifest["action"] = "skipped_existing"
            return manifest

    target_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target_dir / "part-000.parquet.inprogress"
    sorted_target = target_dir / "part-000.sorted.parquet.inprogress"
    temp_manifest = manifest_path.with_suffix(".json.inprogress")
    writer = None
    rows = 0
    discarded_outside_session_rows = 0
    code_counts: Counter[str] = Counter()
    min_dt = None
    max_dt = None
    try:
        for raw_batch in _csv_reader(source_path):
            batch = _normalize_batch(raw_batch, trade_date)
            discarded_outside_session_rows += raw_batch.num_rows - batch.num_rows
            if batch.num_rows == 0:
                continue
            # 源CSV可能按市场分段而非全局代码排序；计数使用Arrow向量化，
            # 最终统一交给DuckDB全局排序。
            value_counts = pc.value_counts(batch.column(0))
            values = value_counts.field("values").to_pylist()
            frequencies = value_counts.field("counts").to_pylist()
            code_counts.update(dict(zip(values, frequencies)))
            batch_min = pc.min(batch.column(3)).as_py()
            batch_max = pc.max(batch.column(3)).as_py()
            min_dt = batch_min if min_dt is None else min(min_dt, batch_min)
            max_dt = batch_max if max_dt is None else max(max_dt, batch_max)
            high = batch.column(STANDARD_COLUMNS.index("high"))
            low = batch.column(STANDARD_COLUMNS.index("low"))
            opn = batch.column(STANDARD_COLUMNS.index("open"))
            close = batch.column(STANDARD_COLUMNS.index("close"))
            invalid_ohlc = pc.or_(
                pc.or_(pc.less(high, low), pc.less(high, opn)),
                pc.or_(pc.less(high, close), pc.or_(pc.greater(low, opn), pc.greater(low, close))),
            )
            if pc.any(invalid_ohlc).as_py():
                raise ValueError("发现不满足 low<=open/close<=high 的分钟K线")
            if writer is None:
                writer = pq.ParquetWriter(
                    temp_target,
                    batch.schema,
                    compression="zstd",
                    compression_level=3,
                    use_dictionary=["ts_code", "name"],
                    write_statistics=True,
                )
            writer.write_batch(batch, row_group_size=131_072)
            rows += batch.num_rows
        if writer is None:
            raise ValueError(f"CSV没有数据行: {source_path}")
        writer.close()
        writer = None
        escaped_input = str(temp_target).replace("'", "''")
        escaped_output = str(sorted_target).replace("'", "''")
        with duckdb.connect(":memory:") as conn:
            duplicate_keys = conn.execute(
                f"SELECT COUNT(*) FROM ("
                f"SELECT ts_code, datetime FROM read_parquet('{escaped_input}') "
                f"GROUP BY ts_code, datetime HAVING COUNT(*) > 1)"
            ).fetchone()[0]
            if duplicate_keys:
                raise ValueError(f"发现重复分钟主键组: {duplicate_keys}")
            conn.execute(
                f"COPY (SELECT * FROM read_parquet('{escaped_input}') "
                f"ORDER BY ts_code, datetime) TO '{escaped_output}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 131072)"
            )
        sorted_target.replace(temp_target)
        counts = list(code_counts.values())
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "status": "ok",
            "trade_date": trade_date,
            "source_file": str(source_path),
            "source_size": source_path.stat().st_size,
            "source_mtime_ns": source_path.stat().st_mtime_ns,
            "target_file": str(target),
            "target_size": temp_target.stat().st_size,
            "rows": rows,
            "stock_count": len(code_counts),
            "bars_per_stock_min": min(counts),
            "bars_per_stock_max": max(counts),
            "bars_per_stock_distribution": {str(k): v for k, v in sorted(Counter(counts).items())},
            "duplicate_keys": 0,
            "order_violations": None,
            "sorted_during_import": True,
            # 保留旧字段供已有审计脚本兼容；v3含义扩展为所有非标准时段，
            # 包括按统一口径删除的09:30以及盘后记录。
            "discarded_after_close_rows": discarded_outside_session_rows,
            "discarded_outside_session_rows": discarded_outside_session_rows,
            "min_datetime": min_dt.isoformat(sep=" ") if min_dt else None,
            "max_datetime": max_dt.isoformat(sep=" ") if max_dt else None,
            "imported_at": datetime.now(timezone.utc).isoformat(),
        }
        temp_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_target.replace(target)
        temp_manifest.replace(manifest_path)
        manifest["action"] = "imported"
        return manifest
    finally:
        if writer is not None:
            writer.close()
        if temp_target.exists():
            temp_target.unlink()
        if sorted_target.exists():
            sorted_target.unlink()
        if temp_manifest.exists():
            temp_manifest.unlink()


def import_one_min_directory(
    csv_root: Union[str, Path],
    root: Optional[Union[str, Path]] = None,
    *,
    start_date: Optional[Union[str, datetime, int]] = None,
    end_date: Optional[Union[str, datetime, int]] = None,
    overwrite: bool = False,
    catalog_path: Optional[Union[str, Path]] = None,
) -> list[dict]:
    scan_one_min_sources(
        csv_root, root=root, catalog_path=catalog_path,
        start_date=start_date, end_date=end_date,
    )
    start = normalize_trade_date(start_date) if start_date is not None else "00000000"
    end = normalize_trade_date(end_date) if end_date is not None else "99999999"
    with _open_catalog(catalog_path) as conn:
        eligible_statuses = "('pending','failed','imported')" if overwrite else "('pending','failed')"
        pending_rows = conn.execute(
            "SELECT trade_date, source_path FROM one_min_ingest_catalog "
            f"WHERE status IN {eligible_statuses} AND source_present=TRUE "
            "AND trade_date BETWEEN ? AND ? ORDER BY trade_date",
            [start, end],
        ).fetchall()
    results: list[dict] = []
    for day, source_path in pending_rows:
        try:
            result = import_one_min_csv(source_path, root=root, overwrite=overwrite)
            _catalog_record_import(result, catalog_path)
            results.append(result)
        except Exception as exc:
            _catalog_record_failure(day, str(exc), catalog_path)
            results.append({"trade_date": day, "action": "failed", "error": str(exc)})
    return results

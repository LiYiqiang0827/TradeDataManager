"""TDX 1分钟OHLC staging、分区落库与历史口径迁移。"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
from typing import Mapping, Optional, Sequence, Union
import uuid

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config.settings import ONE_MIN_CATALOG_PATH, ONE_MIN_PARQUET_ROOT
from core.one_min_store import (
    SCHEMA_VERSION, STANDARD_COLUMNS, _manifest_path, _open_catalog,
    _partition_dir, list_one_min_dates, normalize_trade_date,
)


TDX_ONE_MIN_START = "20260507"
STOCK_DAY_LINEAGE_VERSION = 1
# TDX's zero-amount float decoder emits this exact subnormal value on some
# suspended stock days. Do not treat an arbitrary small positive amount as zero.
TDX_ZERO_AMOUNT_SENTINEL = 5.877471754111438e-39
RAW_COLUMNS = [
    "ts_code", "trade_date", "datetime", "time_idx",
    "open", "high", "low", "close", "vol", "amount",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_tdx_one_min_catalog(
    catalog_path: Optional[Union[str, Path]] = None,
) -> Path:
    catalog = Path(catalog_path) if catalog_path is not None else ONE_MIN_CATALOG_PATH
    with _open_catalog(catalog) as conn:
        for column_sql in (
            "data_source VARCHAR DEFAULT 'csv'",
            "source_host VARCHAR",
        ):
            conn.execute(
                f"ALTER TABLE one_min_ingest_catalog ADD COLUMN IF NOT EXISTS {column_sql}"
            )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS one_min_tdx_sync (
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
    return catalog


def get_tdx_one_min_sync(
    catalog_path: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    catalog = ensure_tdx_one_min_catalog(catalog_path)
    with duckdb.connect(str(catalog), read_only=True) as conn:
        return conn.execute(
            "SELECT * FROM one_min_tdx_sync ORDER BY ts_code"
        ).fetchdf()


def remove_one_min_partition(
    trade_date: str,
    *,
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
) -> bool:
    """删除一个不应存在的正式分区、清单和目录记录。

    仅由TDX全市场服务在上交所交易日历确认该自然日休市后调用。
    """
    day = normalize_trade_date(trade_date)
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    partition = _partition_dir(base, day)
    manifest = _manifest_path(base, day)
    existed = partition.exists() or manifest.exists()
    shutil.rmtree(partition, ignore_errors=True)
    manifest.unlink(missing_ok=True)
    catalog = ensure_tdx_one_min_catalog(catalog_path)
    with duckdb.connect(str(catalog)) as conn:
        conn.execute("DELETE FROM one_min_ingest_catalog WHERE trade_date=?", [day])
    return existed


def mark_tdx_one_min_sync(
    ts_code: str,
    requested_start: str,
    requested_end: str,
    *,
    rows: Sequence[Mapping] = (),
    status: str,
    source_host: Optional[str] = None,
    error: Optional[str] = None,
    catalog_path: Optional[Union[str, Path]] = None,
) -> None:
    catalog = ensure_tdx_one_min_catalog(catalog_path)
    actual_dates = sorted({
        int(str(item["trade_date"]).replace("-", "")) for item in rows
        if item.get("trade_date") is not None
    })
    covered = status in {"downloaded", "empty"}
    with duckdb.connect(str(catalog)) as conn:
        previous = conn.execute(
            "SELECT covered_start, covered_end, row_count FROM one_min_tdx_sync WHERE ts_code=?",
            [ts_code],
        ).fetchone()
        starts = [int(requested_start)] if covered else []
        ends = [int(requested_end)] if covered else []
        row_count = len(rows)
        if previous:
            if previous[0] is not None:
                starts.append(int(previous[0]))
            if previous[1] is not None:
                ends.append(int(previous[1]))
            row_count += int(previous[2] or 0)
        conn.execute("""
            INSERT INTO one_min_tdx_sync VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ts_code) DO UPDATE SET
                covered_start=excluded.covered_start,
                covered_end=excluded.covered_end,
                actual_start=CASE
                    WHEN one_min_tdx_sync.actual_start IS NULL THEN excluded.actual_start
                    WHEN excluded.actual_start IS NULL THEN one_min_tdx_sync.actual_start
                    ELSE least(one_min_tdx_sync.actual_start, excluded.actual_start) END,
                actual_end=CASE
                    WHEN one_min_tdx_sync.actual_end IS NULL THEN excluded.actual_end
                    WHEN excluded.actual_end IS NULL THEN one_min_tdx_sync.actual_end
                    ELSE greatest(one_min_tdx_sync.actual_end, excluded.actual_end) END,
                row_count=excluded.row_count,
                status=excluded.status,
                source_host=excluded.source_host,
                updated_at=excluded.updated_at,
                error=excluded.error
        """, [
            ts_code, min(starts) if starts else None, max(ends) if ends else None,
            min(actual_dates) if actual_dates else None,
            max(actual_dates) if actual_dates else None,
            row_count, status, source_host, _utc_now(), error,
        ])


def _normalize_tdx_rows(rows: Sequence[Mapping]) -> pd.DataFrame:
    frame = pd.DataFrame(list(rows))
    if frame.empty:
        return pd.DataFrame(columns=RAW_COLUMNS)
    missing = sorted(set(RAW_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"TDX oneMin行缺少字段: {missing}")
    frame = frame[RAW_COLUMNS].copy()
    frame["ts_code"] = frame["ts_code"].astype(str).str.upper()
    frame["trade_date"] = (
        frame["trade_date"].astype(str).str.replace("-", "", regex=False).astype("int32")
    )
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    for column in ("open", "high", "low", "close", "vol", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if frame[["datetime", "open", "high", "low", "close", "vol", "amount"]].isna().any().any():
        raise ValueError("TDX oneMin包含空时间或空OHLCVA")
    labels = frame["datetime"].dt.strftime("%H:%M")
    valid = (
        ((labels >= "09:31") & (labels <= "11:30"))
        | ((labels >= "13:01") & (labels <= "15:00"))
    )
    if not valid.all():
        raise ValueError(f"TDX oneMin包含非标准时点: {sorted(set(labels[~valid]))}")
    expected_idx = frame["datetime"].dt.hour * 60 + frame["datetime"].dt.minute
    expected_idx = expected_idx.where(
        frame["datetime"].dt.hour < 12,
        120 + expected_idx - (13 * 60 + 1),
    ).where(frame["datetime"].dt.hour >= 12, expected_idx - (9 * 60 + 31))
    frame["time_idx"] = expected_idx.astype("int16")
    invalid_ohlc = (
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
    )
    if invalid_ohlc.any():
        raise ValueError("TDX oneMin存在OHLC关系错误")
    if (frame[["vol", "amount"]] < 0).any().any():
        raise ValueError("TDX oneMin存在负成交量或成交额")
    frame["vol"] = frame["vol"].round().astype("int64")
    # 下载入口已经按(ts_code, datetime)去重；正式分区合并时还会再次做窗口去重。
    # staging阶段避免对每批近百万行重复排序，显著降低全市场回补耗时。
    return frame.reset_index(drop=True)


def stage_tdx_one_min_batch(
    items: Sequence[Mapping],
    stage_root: Union[str, Path],
) -> dict:
    """把一批股票结果按交易日写入临时Parquet；最终落库前不改正式分区。"""
    rows = [row for item in items for row in item.get("rows", ())]
    if not rows:
        return {"rows": 0, "dates": []}
    frame = _normalize_tdx_rows(rows)
    root = Path(stage_root)
    token = uuid.uuid4().hex
    quality_by_day: dict[str, list[dict]] = {}
    for row in rows:
        if not row.get("quality_flags"):
            continue
        day_text = str(row.get("trade_date", "")).replace("-", "")
        payload = {
            key: row.get(key)
            for key in ("open", "high", "low", "close", "vol", "amount")
        }
        payload["decoder_corrections"] = list(row.get("decoder_corrections", ()))
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        quality_by_day.setdefault(day_text, []).append({
            "trade_date": int(day_text), "ts_code": str(row["ts_code"]),
            "time_idx": int(row["time_idx"]), "datetime": str(row["datetime"]),
            "quality_flags": json.dumps(sorted(set(row["quality_flags"]))),
            "source": str(row.get("source", "tdx_native_1min")),
            "source_payload_json": payload_json,
            "source_payload_sha256": hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        })
    dates = []
    for day, group in frame.groupby("trade_date", sort=True):
        day_text = str(int(day))
        target_dir = root / f"trade_date={day_text}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"part-{token}.parquet"
        temp = target.with_suffix(".parquet.inprogress")
        table = pa.Table.from_pandas(group[RAW_COLUMNS], preserve_index=False)
        pq.write_table(
            table, temp, compression="zstd", compression_level=3,
            use_dictionary=["ts_code"], row_group_size=131_072,
        )
        temp.replace(target)
        quality_rows = quality_by_day.get(day_text, [])
        if quality_rows:
            quality_dir = root / "_quality_stage" / f"trade_date={day_text}"
            quality_dir.mkdir(parents=True, exist_ok=True)
            quality_target = quality_dir / f"part-{token}.parquet"
            quality_temp = quality_target.with_suffix(".parquet.inprogress")
            pq.write_table(pa.Table.from_pylist(quality_rows), quality_temp, compression="zstd")
            quality_temp.replace(quality_target)
        dates.append(day_text)
    return {"rows": len(frame), "dates": dates}


def _partition_stats(path: Path) -> dict:
    escaped = str(path).replace("'", "''")
    with duckdb.connect(":memory:") as conn:
        summary = conn.execute(f"""
            SELECT count(*) AS rows, count(DISTINCT ts_code) AS stocks,
                   min(datetime), max(datetime), count(*) - count(DISTINCT (ts_code, datetime))
            FROM read_parquet('{escaped}')
        """).fetchone()
        distribution = conn.execute(f"""
            SELECT bars, count(*) FROM (
                SELECT ts_code, count(*) AS bars FROM read_parquet('{escaped}') GROUP BY ts_code
            ) GROUP BY bars ORDER BY bars
        """).fetchall()
        invalid = conn.execute(f"""
            SELECT count(*) FROM read_parquet('{escaped}')
            WHERE time_idx < 0 OR time_idx > 239
               OR NOT ((strftime(datetime, '%H:%M') BETWEEN '09:31' AND '11:30')
                    OR (strftime(datetime, '%H:%M') BETWEEN '13:01' AND '15:00'))
               OR high < greatest(open, close, low)
               OR low > least(open, close, high)
               OR adj_factor IS NULL
        """).fetchone()[0]
    counts = {int(k): int(v) for k, v in distribution}
    return {
        "rows": int(summary[0]), "stock_count": int(summary[1]),
        "min_datetime": summary[2], "max_datetime": summary[3],
        "duplicate_keys": int(summary[4]), "invalid_rows": int(invalid),
        "bars_per_stock_min": min(counts) if counts else 0,
        "bars_per_stock_max": max(counts) if counts else 0,
        "bars_per_stock_distribution": counts,
    }


def _latest_adj_factor_reference(trade_date: str) -> pd.DataFrame:
    """读取每只股票截至指定日最后一个有效复权因子。

    Tushare个别交易日会漏少量股票；复权因子在下一次除权前保持不变，因此
    用不晚于目标日的最近值补齐，比写入空值更符合其业务含义。
    """
    from core.offline_db_client import get_conn

    conn = get_conn("basic")
    try:
        return pd.read_sql_query("""
            SELECT a.ts_code, a.adj_factor
            FROM tbl_cn_adj_factor a
            JOIN (
                SELECT ts_code, max(trade_date) AS max_date
                FROM tbl_cn_adj_factor
                WHERE trade_date <= ?
                GROUP BY ts_code
            ) latest
              ON a.ts_code=latest.ts_code AND a.trade_date=latest.max_date
        """, conn, params=[trade_date])
    finally:
        conn.close()


def _record_tdx_partition(
    day: str,
    target: Path,
    stats: Mapping,
    *,
    source_host: Optional[str],
    data_source: str,
    catalog_path: Optional[Union[str, Path]],
) -> None:
    catalog = ensure_tdx_one_min_catalog(catalog_path)
    now = _utc_now()
    uri = f"tdx://security-bars/1min/{day}"
    with duckdb.connect(str(catalog)) as conn:
        conn.execute("""
            INSERT INTO one_min_ingest_catalog (
                trade_date, source_path, source_size, source_mtime_ns,
                source_present, parquet_path, parquet_size, schema_version,
                rows, stock_count, bars_per_stock_min, bars_per_stock_max,
                status, discovered_at, last_checked_at, imported_at, error,
                source_duplicate_count, source_candidates, data_source, source_host
            ) VALUES (?, ?, 0, 0, TRUE, ?, ?, ?, ?, ?, ?, ?, 'imported', ?, ?, ?, NULL,
                      1, ?, ?, ?)
            ON CONFLICT(trade_date) DO UPDATE SET
                source_path=excluded.source_path, source_size=0, source_mtime_ns=0,
                source_present=TRUE, parquet_path=excluded.parquet_path,
                parquet_size=excluded.parquet_size, schema_version=excluded.schema_version,
                rows=excluded.rows, stock_count=excluded.stock_count,
                bars_per_stock_min=excluded.bars_per_stock_min,
                bars_per_stock_max=excluded.bars_per_stock_max,
                status='imported', last_checked_at=excluded.last_checked_at,
                imported_at=excluded.imported_at, error=NULL,
                source_duplicate_count=1, source_candidates=excluded.source_candidates,
                data_source=excluded.data_source, source_host=excluded.source_host
        """, [
            day, uri, str(target), target.stat().st_size, SCHEMA_VERSION,
            stats["rows"], stats["stock_count"], stats["bars_per_stock_min"],
            stats["bars_per_stock_max"], now, now, now,
                json.dumps([uri]), data_source, source_host,
        ])


def _stock_day_lineage(
    day: str, downloaded_codes: set[str], retained_codes: Sequence[str],
    previous_manifest_path: Path,
) -> dict:
    """Record this merge's sources without guessing an old partition's lineage.

    ``retained_previous_partition_codes`` is the actual fallback operation, not
    proof of a CSV origin. Legacy aggregate counts/source_path cannot establish
    per-stock provenance; only explicit same-day stock lineage is inherited.
    """
    origins = {"tdx": set(downloaded_codes), "csv": set(), "unknown": set()}
    previous_sources: dict = {}
    previous_status = "not_needed"
    previous_hash = None
    if retained_codes:
        previous_status = "missing_manifest"
        if previous_manifest_path.is_file():
            raw = previous_manifest_path.read_bytes()
            previous_hash = hashlib.sha256(raw).hexdigest()
            try:
                previous = json.loads(raw)
                lineage = previous.get("source_lineage") if isinstance(previous, dict) else None
                if lineage is None:
                    previous_status = "legacy_without_stock_lineage"
                elif (isinstance(lineage, dict)
                      and lineage.get("version") == STOCK_DAY_LINEAGE_VERSION
                      and lineage.get("trade_date") == day
                      and isinstance(lineage.get("stock_codes_by_source"), dict)
                      and all(isinstance(lineage["stock_codes_by_source"].get(source), list)
                              and all(isinstance(code, str)
                                      for code in lineage["stock_codes_by_source"][source])
                              for source in origins)):
                    previous_sources = lineage["stock_codes_by_source"]
                    previous_status = "explicit_stock_lineage"
                else:
                    previous_status = "invalid_stock_lineage"
            except (ValueError, UnicodeDecodeError):
                previous_status = "invalid_manifest_json"
    for code in retained_codes:
        matches = [source for source in origins if code in previous_sources.get(source, [])]
        # Missing or contradictory prior provenance stays unknown.
        origins[matches[0] if len(matches) == 1 else "unknown"].add(code)
    return {
        "version": STOCK_DAY_LINEAGE_VERSION, "trade_date": day,
        "tdx_downloaded_codes": sorted(downloaded_codes),
        "retained_previous_partition_codes": sorted(retained_codes),
        "stock_codes_by_source": {source: sorted(codes) for source, codes in origins.items()},
        "previous_manifest_status": previous_status,
        "previous_manifest_sha256": previous_hash,
        "legacy_fallback_fields_note": "csv_fallback_* counts retained rows/stocks; it does not establish CSV origin",
    }


def finalize_tdx_one_min_day(
    trade_date: str,
    stage_root: Union[str, Path],
    *,
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
    source_host: Optional[str] = None,
) -> dict:
    """合并单日staging、补名称/因子，并保存逐股票日来源血缘。

    新增manifest的 ``source_lineage``；旧数量字段继续兼容。旧分区保留数据
    没有逐股来源证据时明确记unknown，不从混合分区的聚合数量猜测CSV来源。
    """
    from core.offline_db_client import get_adj_factor, get_basic, get_day

    day = normalize_trade_date(trade_date)
    stage_dir = Path(stage_root) / f"trade_date={day}"
    stage_files = sorted(stage_dir.glob("part-*.parquet"))
    if not stage_files:
        raise ValueError(f"TDX oneMin staging缺少交易日{day}")
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    target_dir = _partition_dir(base, day)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "part-000.parquet"
    temp = target_dir / "part-000.parquet.inprogress"
    manifest_path = _manifest_path(base, day)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_manifest = manifest_path.with_suffix(".json.inprogress")
    quality_path = base / "_quality" / f"trade_date={day}" / "part-000.parquet"
    quality_temp = quality_path.with_suffix(".parquet.inprogress")
    quality_temp.unlink(missing_ok=True)
    temp.unlink(missing_ok=True)
    temp_manifest.unlink(missing_ok=True)

    basic = get_basic(list_status=["L", "P"], columns=["ts_code", "name"])
    factors = get_adj_factor(
        trade_date=day, columns=["ts_code", "trade_date", "adj_factor"]
    )
    references = []
    if target.is_file():
        escaped_old = str(target).replace("'", "''")
        with duckdb.connect(":memory:") as conn:
            references.append(conn.execute(
                f"SELECT DISTINCT ts_code, name, adj_factor FROM read_parquet('{escaped_old}')"
            ).fetchdf())
    old_ref = pd.concat(references, ignore_index=True).drop_duplicates("ts_code", keep="last") \
        if references else pd.DataFrame(columns=["ts_code", "name", "adj_factor"])
    basic_ref = basic[["ts_code", "name"]].drop_duplicates("ts_code")
    factor_ref = factors[["ts_code", "adj_factor"]].drop_duplicates("ts_code")

    stage_glob = str(stage_dir / "part-*.parquet").replace("'", "''")
    escaped_temp = str(temp).replace("'", "''")
    with duckdb.connect(":memory:") as conn:
        stage_codes = set(
            conn.execute(
                f"SELECT DISTINCT ts_code FROM read_parquet('{stage_glob}')"
            ).fetchnumpy()["ts_code"].tolist()
        )
    expected = get_day(
        trade_date=day, qfq=False, columns=["ts_code", "trade_date"]
    )
    expected_codes = set(expected["ts_code"].astype(str))
    known_factor_codes = set(factor_ref.loc[factor_ref["adj_factor"].notna(), "ts_code"])
    known_factor_codes.update(
        old_ref.loc[old_ref["adj_factor"].notna(), "ts_code"].astype(str)
    )
    missing_factor_codes = expected_codes - known_factor_codes
    if missing_factor_codes:
        latest_factors = _latest_adj_factor_reference(day)
        latest_factors = latest_factors[
            latest_factors["ts_code"].isin(missing_factor_codes)
        ]
        factor_ref = pd.concat(
            [factor_ref, latest_factors[["ts_code", "adj_factor"]]], ignore_index=True
        ).drop_duplicates("ts_code", keep="last")
    unexpected = sorted(stage_codes - expected_codes)
    excluded_placeholders = []
    if unexpected:
        # A server may return 239/240 flat, zero-volume placeholders for a
        # non-trading stock. Exclude only this observed whole-day pattern;
        # genuine trades or incomplete/unknown evidence must still fail closed.
        with duckdb.connect(":memory:") as conn:
            conn.register("unexpected_ref", pd.DataFrame({"ts_code": unexpected}))
            summaries = conn.execute(f"""
                WITH dedup AS (
                    SELECT * EXCLUDE(rn) FROM (
                        SELECT p.*, row_number() OVER (
                            PARTITION BY p.ts_code, p.datetime ORDER BY p.filename DESC
                        ) AS rn
                        FROM read_parquet('{stage_glob}', filename=true) p
                        JOIN unexpected_ref u USING(ts_code)
                    ) WHERE rn=1
                )
                SELECT ts_code, count(*) AS rows,
                       min(least(open, high, low, close)) AS min_price,
                       max(greatest(open, high, low, close)) AS max_price,
                       min(amount) AS min_amount, max(amount) AS max_amount,
                       bool_and(coalesce(
                           isfinite(open) AND isfinite(high) AND isfinite(low)
                           AND isfinite(close) AND open > 0 AND low > 0
                           AND vol=0 AND (amount=0 OR amount=?), false
                       )) AS all_zero_flat_inputs,
                       bool_and(CAST(trade_date AS VARCHAR)=?
                           AND strftime(datetime, '%Y%m%d')=?
                           AND ((strftime(datetime, '%H:%M') BETWEEN '09:31' AND '11:30')
                             OR (strftime(datetime, '%H:%M') BETWEEN '13:01' AND '15:00'))
                       ) AS same_session
                FROM dedup GROUP BY ts_code ORDER BY ts_code
            """, [TDX_ZERO_AMOUNT_SENTINEL, day, day]).fetchall()
        for code, rows, lo, hi, amount_lo, amount_hi, zero_inputs, session in summaries:
            if rows in {239, 240} and zero_inputs and session and lo == hi:
                excluded_placeholders.append({
                    "ts_code": code, "rows": int(rows),
                    "reason": "absent_from_daily_zero_trade_flat_tdx_placeholder",
                    "price": float(lo), "vol": 0,
                    "amount_min": float(amount_lo), "amount_max": float(amount_hi),
                    "evidence": "239_or_240_same_session_bars_all_ohlc_equal_volume_zero_amount_zero_or_exact_tdx_sentinel",
                })
        excluded_codes = {item["ts_code"] for item in excluded_placeholders}
        unresolved = sorted(set(unexpected) - excluded_codes)
        if unresolved:
            raise ValueError(f"TDX oneMin {day}包含日线表无记录且非可证实零成交占位的股票: {unresolved[:20]}")
        stage_codes -= excluded_codes
    missing_codes = sorted(expected_codes - stage_codes)
    if missing_codes and not target.is_file():
        raise ValueError(
            f"TDX oneMin {day}缺{len(missing_codes)}只且没有历史CSV分区可补齐"
        )
    missing_ref = pd.DataFrame({"ts_code": missing_codes})
    if missing_codes:
        escaped_old = str(target).replace("'", "''")
        combined_sql = f"""
            SELECT ts_code, trade_date, datetime, time_idx,
                   open, high, low, close, vol, amount
            FROM dedup
            UNION ALL
            SELECT p.ts_code, p.trade_date, p.datetime, p.time_idx,
                   p.open, p.high, p.low, p.close, p.vol, p.amount
            FROM read_parquet('{escaped_old}') p
            JOIN missing_ref m USING(ts_code)
            WHERE (strftime(p.datetime, '%H:%M') BETWEEN '09:31' AND '11:30')
               OR (strftime(p.datetime, '%H:%M') BETWEEN '13:01' AND '15:00')
        """
    else:
        combined_sql = """
            SELECT ts_code, trade_date, datetime, time_idx,
                   open, high, low, close, vol, amount
            FROM dedup
        """
    with duckdb.connect(":memory:") as conn:
        conn.register("old_ref", old_ref)
        conn.register("basic_ref", basic_ref)
        conn.register("factor_ref", factor_ref)
        conn.register("missing_ref", missing_ref)
        conn.register("excluded_ref", pd.DataFrame({
            "ts_code": pd.Series([item["ts_code"] for item in excluded_placeholders], dtype="str")
        }))
        conn.execute(f"""
            COPY (
                WITH dedup AS (
                    SELECT * EXCLUDE(rn) FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY ts_code, datetime ORDER BY filename DESC
                        ) AS rn
                        FROM read_parquet('{stage_glob}', filename=true)
                    ) WHERE rn=1 AND ts_code NOT IN (SELECT ts_code FROM excluded_ref)
                ), combined AS (
                    {combined_sql}
                )
                SELECT d.ts_code,
                       coalesce(b.name, o.name) AS name,
                       CAST(d.trade_date AS INTEGER) AS trade_date,
                       d.datetime,
                       CAST(
                           CASE
                               WHEN hour(d.datetime) < 12 THEN
                                   hour(d.datetime) * 60 + minute(d.datetime) - (9 * 60 + 31)
                               ELSE
                                   120 + hour(d.datetime) * 60 + minute(d.datetime) - (13 * 60 + 1)
                           END
                           AS SMALLINT
                       ) AS time_idx,
                       d.open, d.high, d.low, d.close,
                       CAST(round(d.vol) AS BIGINT) AS vol, d.amount,
                       coalesce(f.adj_factor, o.adj_factor) AS adj_factor
                FROM combined d
                LEFT JOIN basic_ref b USING(ts_code)
                LEFT JOIN factor_ref f USING(ts_code)
                LEFT JOIN old_ref o USING(ts_code)
                ORDER BY d.ts_code, d.datetime
            ) TO '{escaped_temp}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 131072)
        """)
    quality_stage_dir = stage_dir.parent / "_quality_stage" / f"trade_date={day}"
    quality_files = [quality_stage_dir / path.name for path in stage_files]
    quality_files = [path for path in quality_files if path.is_file()]
    quality_frames = []
    if quality_files:
        quality_frames.extend(pd.read_parquet(path) for path in quality_files)
    if target.is_file() and quality_path.is_file() and missing_codes:
        old_quality = pd.read_parquet(quality_path)
        quality_frames.append(old_quality[old_quality["ts_code"].isin(missing_codes)])
    quality_path.parent.mkdir(parents=True, exist_ok=True)
    if quality_frames:
        quality_frame = pd.concat(quality_frames, ignore_index=True).drop_duplicates(
            ["trade_date", "ts_code", "time_idx"], keep="last"
        ).sort_values(["ts_code", "time_idx"], kind="stable")
        pq.write_table(pa.Table.from_pandas(quality_frame, preserve_index=False), quality_temp,
                       compression="zstd")
        quality_count = len(quality_frame)
        quality_sha256 = hashlib.sha256(quality_temp.read_bytes()).hexdigest()
    else:
        quality_temp.unlink(missing_ok=True)
        quality_count = 0
        quality_sha256 = None
    stats = _partition_stats(temp)
    if stats["rows"] == 0 or stats["duplicate_keys"] or stats["invalid_rows"]:
        temp.unlink(missing_ok=True)
        raise ValueError(f"TDX oneMin {day}分区校验失败: {stats}")
    if stats["stock_count"] != len(expected_codes):
        temp.unlink(missing_ok=True)
        raise ValueError(
            f"TDX oneMin {day}股票数不等于日线实际交易集合: "
            f"output={stats['stock_count']} expected={len(expected_codes)}"
        )
    fallback_rows = stats["rows"] - len(stage_codes) * 240
    data_source = "tdx" if not missing_codes else "tdx_with_csv_fallback"
    manifest = {
        "schema_version": SCHEMA_VERSION, "status": "ok", "data_source": data_source,
        "trade_date": day, "source_file": f"tdx://security-bars/1min/{day}",
        "source_host": source_host, "target_file": str(target),
        "target_size": temp.stat().st_size, **stats,
        "tdx_stock_count": len(stage_codes), "tdx_rows": len(stage_codes) * 240,
        "csv_fallback_stock_count": len(missing_codes),
        "csv_fallback_rows": fallback_rows,
        "source_lineage": _stock_day_lineage(day, stage_codes, missing_codes, manifest_path),
        "excluded_non_trading_placeholders": excluded_placeholders,
        "quality_sidecar": (str(quality_path) if quality_count else None),
        "quality_sidecar_rows": quality_count,
        "quality_sidecar_sha256": quality_sha256,
        "imported_at": datetime.now(timezone.utc).isoformat(),
    }
    pending_manifest = dict(manifest, status="pending_quality_sidecar")
    pending_path = manifest_path.with_suffix(".json.pending")
    pending_path.write_text(json.dumps(pending_manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    pending_path.replace(manifest_path)
    temp.replace(target)
    if quality_temp.is_file():
        quality_temp.replace(quality_path)
    else:
        quality_path.unlink(missing_ok=True)
    temp_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temp_manifest.replace(manifest_path)
    _record_tdx_partition(
        day, target, stats, source_host=source_host, data_source=data_source,
        catalog_path=catalog_path,
    )
    return manifest


def get_one_min_quality(
    trade_date: str,
    *,
    ts_code: Optional[str] = None,
    root: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    """Read source-quality sidecar rows without changing the canonical bar schema."""
    day = normalize_trade_date(trade_date)
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    sidecar = base / "_quality" / f"trade_date={day}" / "part-000.parquet"
    if not sidecar.is_file():
        return pd.DataFrame(columns=[
            "trade_date", "ts_code", "time_idx", "datetime", "quality_flags", "source",
            "source_payload_json", "source_payload_sha256",
        ])
    frame = pd.read_parquet(sidecar)
    if ts_code is not None:
        frame = frame[frame["ts_code"] == str(ts_code).upper()]
    return frame.sort_values(["ts_code", "time_idx"], kind="stable").reset_index(drop=True)


def normalize_existing_one_min_partitions(
    *,
    end_date: str,
    start_date: Optional[str] = None,
    root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
) -> list[dict]:
    """原子重写已有分区：删除09:30并把time_idx统一为0—239。"""
    base = Path(root) if root is not None else ONE_MIN_PARQUET_ROOT
    start = normalize_trade_date(start_date) if start_date else "00000000"
    end = normalize_trade_date(end_date)
    results = []
    for day in [item for item in list_one_min_dates(base) if start <= item <= end]:
        target = _partition_dir(base, day) / "part-000.parquet"
        temp = target.with_suffix(".parquet.inprogress")
        temp.unlink(missing_ok=True)
        escaped_target = str(target).replace("'", "''")
        escaped_temp = str(temp).replace("'", "''")
        with duckdb.connect(":memory:") as conn:
            at_0930 = conn.execute(
                f"SELECT count(*) FROM read_parquet('{escaped_target}') "
                "WHERE strftime(datetime, '%H:%M')='09:30'"
            ).fetchone()[0]
        if at_0930 == 0:
            current_stats = _partition_stats(target)
            if current_stats["duplicate_keys"] or current_stats["invalid_rows"]:
                raise ValueError(f"oneMin {day}已无09:30但分区仍不合法: {current_stats}")
            results.append({"trade_date": day, "action": "skipped_normalized", **current_stats})
            continue
        with duckdb.connect(":memory:") as conn:
            null_factors = conn.execute(
                f"SELECT count(*) FROM read_parquet('{escaped_target}') WHERE adj_factor IS NULL"
            ).fetchone()[0]
            if null_factors:
                conn.register("factor_ref", _latest_adj_factor_reference(day))
                source_sql = f"""
                    SELECT p.* EXCLUDE(adj_factor), coalesce(p.adj_factor, f.adj_factor) AS adj_factor
                    FROM read_parquet('{escaped_target}') p
                    LEFT JOIN factor_ref f USING(ts_code)
                """
            else:
                source_sql = f"SELECT * FROM read_parquet('{escaped_target}')"
            conn.execute(f"""
                COPY (
                    SELECT ts_code, name, CAST(trade_date AS INTEGER) AS trade_date,
                           datetime,
                           CAST(CASE WHEN hour(datetime) < 12
                               THEN hour(datetime) * 60 + minute(datetime) - 571
                               ELSE 120 + hour(datetime) * 60 + minute(datetime) - 781
                           END AS SMALLINT) AS time_idx,
                           open, high, low, close, CAST(vol AS BIGINT) AS vol,
                           amount, adj_factor
                    FROM ({source_sql}) source
                    WHERE (strftime(datetime, '%H:%M') BETWEEN '09:31' AND '11:30')
                       OR (strftime(datetime, '%H:%M') BETWEEN '13:01' AND '15:00')
                    ORDER BY ts_code, datetime
                ) TO '{escaped_temp}'
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 131072)
            """)
        stats = _partition_stats(temp)
        if stats["duplicate_keys"] or stats["invalid_rows"]:
            temp.unlink(missing_ok=True)
            raise ValueError(f"oneMin {day}口径迁移校验失败: {stats}")
        old_rows = pq.read_metadata(target).num_rows
        if old_rows - stats["rows"] != stats["stock_count"]:
            temp.unlink(missing_ok=True)
            raise ValueError(
                f"oneMin {day}应每股删除1根09:30: old={old_rows}, new={stats['rows']}, "
                f"stocks={stats['stock_count']}"
            )
        temp.replace(target)
        manifest_path = _manifest_path(base, day)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {}
        manifest.update({
            "schema_version": SCHEMA_VERSION, "session": "09:31-11:30,13:01-15:00",
            "target_file": str(target), "target_size": target.stat().st_size,
            **stats, "normalized_at": datetime.now(timezone.utc).isoformat(),
        })
        temporary_manifest = manifest_path.with_suffix(".json.inprogress")
        temporary_manifest.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        temporary_manifest.replace(manifest_path)
        with _open_catalog(catalog_path) as conn:
            conn.execute("""
                UPDATE one_min_ingest_catalog SET parquet_size=?, schema_version=?, rows=?,
                    stock_count=?, bars_per_stock_min=?, bars_per_stock_max=?,
                    status='imported', last_checked_at=?, error=NULL
                WHERE trade_date=?
            """, [
                target.stat().st_size, SCHEMA_VERSION, stats["rows"], stats["stock_count"],
                stats["bars_per_stock_min"], stats["bars_per_stock_max"], _utc_now(), day,
            ])
        results.append({"trade_date": day, "old_rows": old_rows, **stats})
    return results

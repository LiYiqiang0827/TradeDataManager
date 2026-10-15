#!/usr/bin/env python3
"""Read-only audit of weekly/monthly OHLCV tables against daily bars and factors.

SQLite is opened in query-only mode. Source rows are projected once without ORDER
BY into bounded DuckDB chunks; all grouping, sorting, and comparisons happen in
DuckDB. No production tables are created or modified.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

FIELDS = ("open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount")
SOURCE_QUERIES = {
    "daily_values": "SELECT ts_code,trade_date,open,high,low,close,vol,amount FROM tbl_cn_day",
    "factor_values": "SELECT ts_code,trade_date,adj_factor FROM tbl_cn_adj_factor",
    "weekly_origin_actual": "SELECT ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount FROM tbl_cn_week_origin",
    "weekly_qfq_actual": "SELECT ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount FROM tbl_cn_week",
    "monthly_origin_actual": "SELECT ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount FROM tbl_cn_month_origin",
    "monthly_qfq_actual": "SELECT ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount FROM tbl_cn_month",
}
STAGING_SCHEMAS = {
    "daily_values": "ts_code VARCHAR, trade_date VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE",
    "factor_values": "ts_code VARCHAR, trade_date VARCHAR, adj_factor DOUBLE",
    **{name: "ts_code VARCHAR, trade_date VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, pre_close DOUBLE, change DOUBLE, pct_chg DOUBLE, vol DOUBLE, amount DOUBLE"
       for name in SOURCE_QUERIES if name.endswith("_actual")},
}


def _quote_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _create_expected(con: duckdb.DuckDBPyConnection, frequency: str, mode: str) -> str:
    if frequency not in {"weekly", "monthly"} or mode not in {"origin", "qfq"}:
        raise ValueError("frequency/mode not supported")
    output = f"{frequency}_{mode}_expected"
    period_key = "strftime(strptime(trade_date,'%Y%m%d'),'%G-W%V')" if frequency == "weekly" else "substr(trade_date,1,6)"
    if mode == "origin":
        base = "SELECT * FROM daily_values WHERE open>0 AND close>0"
    else:
        base = """WITH latest AS (
            SELECT ts_code,arg_max(adj_factor,trade_date) FILTER (WHERE adj_factor IS NOT NULL) latest
            FROM factor_values GROUP BY ts_code
        ), joined AS (
            SELECT d.ts_code,d.trade_date,d.open,d.high,d.low,d.close,d.vol,d.amount,
                   COALESCE(f.adj_factor,1.0) factor,COALESCE(l.latest,1.0) latest
            FROM daily_values d
            LEFT JOIN factor_values f USING(ts_code,trade_date)
            LEFT JOIN latest l USING(ts_code)
        )
        SELECT ts_code,trade_date,open*(factor/latest) AS open,high*(factor/latest) AS high,
               low*(factor/latest) AS low,close*(factor/latest) AS "close",
               vol/(factor/latest) AS vol,amount
        FROM joined WHERE open>0 AND close>0"""
    con.execute(f'DROP TABLE IF EXISTS "{output}"')
    con.execute(f"""CREATE TABLE "{output}" AS
        WITH base AS ({base}), grouped AS (
            SELECT ts_code,{period_key} period_key,
                first(open ORDER BY trade_date) open,
                max(high) high,min(low) low,last(close ORDER BY trade_date) close_value,
                sum(vol) vol,sum(amount) amount,max(trade_date) trade_date
            FROM base GROUP BY ts_code,period_key
        ), linked AS (
            SELECT *,lag(close_value) OVER(PARTITION BY ts_code ORDER BY trade_date) pre_close
            FROM grouped
        )
        SELECT ts_code,trade_date,open,high,low,close_value AS close,pre_close,
               close_value-pre_close change,
               round((close_value-pre_close)/pre_close*100,4) pct_chg,vol,amount
        FROM linked""")
    return output


def _compare_table(con: duckdb.DuckDBPyConnection, expected: str, actual: str, *, chunk_size: int) -> dict:
    missing, extra, common = con.execute(f"""SELECT
        count(*) FILTER (WHERE e.ts_code IS NOT NULL AND a.ts_code IS NULL),
        count(*) FILTER (WHERE e.ts_code IS NULL AND a.ts_code IS NOT NULL),
        count(*) FILTER (WHERE e.ts_code IS NOT NULL AND a.ts_code IS NOT NULL)
        FROM \"{expected}\" e FULL OUTER JOIN \"{actual}\" a USING(ts_code,trade_date)""").fetchone()
    result = {
        "expected_rows": con.execute(f'SELECT count(*) FROM "{expected}"').fetchone()[0],
        "actual_rows": con.execute(f'SELECT count(*) FROM "{actual}"').fetchone()[0],
        "key_missing": missing, "key_extra": extra, "common_rows": common,
        "strict_mismatch_counts": {}, "max_abs_diff": {}, "samples": [],
        "pct_chg_strict_duckdb_rounding_difference_count": 0,
        "pct_chg_numpy_ties_even_mismatch_count": 0,
        "pct_chg_numpy_null_parity_mismatch_count": 0,
    }
    for field in FIELDS:
        tolerance = (f"abs(e.{field}-a.{field}) <= 1e-7 + 1e-9*abs(e.{field})") if field != "pct_chg" else "abs(e.pct_chg-a.pct_chg) <= 1e-7"
        mismatch = (f"((e.{field} IS NULL) <> (a.{field} IS NULL)) OR "
                    f"(e.{field} IS NOT NULL AND a.{field} IS NOT NULL AND NOT ({tolerance}))")
        count = con.execute(f'SELECT count(*) FROM "{expected}" e JOIN "{actual}" a USING(ts_code,trade_date) WHERE {mismatch}').fetchone()[0]
        result["strict_mismatch_counts"][field] = count
        if count:
            maxdiff = con.execute(f'SELECT max(abs(e.{field}-a.{field})) FROM "{expected}" e JOIN "{actual}" a USING(ts_code,trade_date) WHERE {mismatch}').fetchone()[0]
            result["max_abs_diff"][field] = maxdiff
            if len(result["samples"]) < 25:
                rows = con.execute(f'SELECT e.ts_code,e.trade_date,e.{field},a.{field} FROM "{expected}" e JOIN "{actual}" a USING(ts_code,trade_date) WHERE {mismatch} LIMIT {25-len(result["samples"])}').fetchall()
                result["samples"].extend({"ts_code": r[0], "trade_date": r[1], "field": field, "expected": r[2], "actual": r[3]} for r in rows)
    # The production aggregator uses pandas/NumPy round-to-even. DuckDB ROUND uses
    # a different tie rule, so independently verify every pct_chg from close/pre_close.
    cur = con.execute(f'SELECT e.ts_code,e.trade_date,e.close,e.pre_close,a.pct_chg FROM "{expected}" e JOIN "{actual}" a USING(ts_code,trade_date)')
    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        arr = np.asarray(rows, dtype=object)
        close = np.array([np.nan if x is None else x for x in arr[:, 2]], dtype="float64")
        pre_close = np.array([np.nan if x is None else x for x in arr[:, 3]], dtype="float64")
        actual_pct = np.array([np.nan if x is None else x for x in arr[:, 4]], dtype="float64")
        numpy_pct = np.round((close - pre_close) / pre_close * 100, 4)
        result["pct_chg_numpy_null_parity_mismatch_count"] += int(np.count_nonzero(np.isnan(numpy_pct) != np.isnan(actual_pct)))
        result["pct_chg_numpy_ties_even_mismatch_count"] += int(np.count_nonzero((~np.isnan(numpy_pct)) & (~np.isnan(actual_pct)) & (numpy_pct != actual_pct)))
    # Count strict difference between DuckDB's rounded expected pct and the target.
    result["pct_chg_strict_duckdb_rounding_difference_count"] = con.execute(
        f'SELECT count(*) FROM "{expected}" e JOIN "{actual}" a USING(ts_code,trade_date) '
        'WHERE e.pct_chg IS NOT NULL AND a.pct_chg IS NOT NULL AND abs(e.pct_chg-a.pct_chg)>1e-7'
    ).fetchone()[0]
    result["status"] = "pass" if not missing and not extra and not any(result["strict_mismatch_counts"][f] for f in FIELDS if f != "pct_chg") and not result["pct_chg_numpy_ties_even_mismatch_count"] and not result["pct_chg_numpy_null_parity_mismatch_count"] else "mismatch"
    return result


def run(database: Path, staging_db: Path, report_path: Path, spill_dir: Path, *, chunk_size: int = 250_000, resume_stage: bool = False) -> dict:
    started = time.time()
    database = database.expanduser().resolve()
    staging_db = staging_db.expanduser().resolve()
    report_path = report_path.expanduser()
    spill_dir = spill_dir.expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    spill_dir.mkdir(parents=True, exist_ok=True)
    status = {"status": "running", "stage": "init", "mode": "read_only_source", "source_database": str(database),
              "source_size_bytes": database.stat().st_size, "source_mtime_ns": database.stat().st_mtime_ns,
              "staging_duckdb": str(staging_db), "chunk_size": chunk_size, "tables": {}, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    def save(message: str | None = None) -> None:
        status["elapsed_seconds"] = round(time.time() - started, 1)
        if message:
            status["last_progress"] = message
            print(message, flush=True)
        report_path.write_text(json.dumps(status, ensure_ascii=False, indent=2))
    if not resume_stage and staging_db.exists():
        staging_db.unlink()
    con = duckdb.connect(str(staging_db))
    con.execute("SET memory_limit='1GB'")
    con.execute(f"SET temp_directory='{_quote_path(spill_dir)}'")
    con.execute("SET threads=2")
    for table, schema in STAGING_SCHEMAS.items():
        con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({schema})')
    con.execute('CREATE TABLE IF NOT EXISTS audit_source_snapshot (db_path VARCHAR, size_bytes BIGINT, mtime_ns BIGINT, loaded_at VARCHAR)')
    if resume_stage:
        absent = [t for t in SOURCE_QUERIES if not con.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0]]
        meta = con.execute('SELECT db_path,size_bytes,mtime_ns FROM audit_source_snapshot LIMIT 1').fetchone()
        current = (str(database), database.stat().st_size, database.stat().st_mtime_ns)
        if absent or meta != current:
            con.close()
            raise ValueError(f"cannot resume; missing projections={absent}, staged source snapshot={meta}, current={current}")
    else:
        uri = "file:" + str(database).replace("?", "%3F") + "?mode=ro"
        source_before = (database.stat().st_size, database.stat().st_mtime_ns)
        src = sqlite3.connect(uri, uri=True, timeout=30)
        src.execute("PRAGMA query_only=ON")
        src.execute("PRAGMA cache_size=-262144")
        src.execute("BEGIN")
        try:
            for table, query in SOURCE_QUERIES.items():
                status["stage"] = f"load_{table}"; save()
                cur = src.execute(query)  # deliberately no ORDER BY: sequential table scan, no PK random lookups
                con.execute(f'DELETE FROM "{table}"')
                count = 0
                while True:
                    rows = cur.fetchmany(chunk_size)
                    if not rows:
                        break
                    frame = pd.DataFrame.from_records(rows, columns=[d[0] for d in cur.description])
                    con.register("_audit_chunk", frame)
                    con.execute(f'INSERT INTO "{table}" SELECT * FROM _audit_chunk')
                    con.unregister("_audit_chunk")
                    count += len(rows)
                    if count % 1_000_000 < chunk_size:
                        status["tables"][table] = {"loaded_rows": count}
                        save(f"loaded {table}: {count:,} rows")
                    del frame
                status["tables"][table] = {"loaded_rows": count}
                save(f"finished load {table}: {count:,} rows")
        finally:
            src.rollback()
            src.close()
        source_after = (database.stat().st_size, database.stat().st_mtime_ns)
        if source_before != source_after:
            con.close()
            raise RuntimeError(f"source SQLite changed while projected: before={source_before}, after={source_after}")
        con.execute('DELETE FROM audit_source_snapshot')
        con.execute('INSERT INTO audit_source_snapshot VALUES(?,?,?,?)', (str(database), source_after[0], source_after[1], time.strftime("%Y-%m-%dT%H:%M:%S%z")))
    for frequency in ("weekly", "monthly"):
        for mode in ("origin", "qfq"):
            status["stage"] = f"compare_{frequency}_{mode}"; save()
            expected = _create_expected(con, frequency, mode)
            actual = f"{frequency}_{mode}_actual"
            result = _compare_table(con, expected, actual, chunk_size=chunk_size)
            status["tables"][expected] = result
            save(f"compared {expected}: expected={result['expected_rows']:,} actual={result['actual_rows']:,} missing={result['key_missing']:,} extra={result['key_extra']:,}")
    status.update({"status": "complete", "stage": "complete", "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                   "period_grouping": "ISO year/week for weekly; YYYYMM for monthly",
                   "source_contract": "daily rows with open>0 and close>0; origin first/max/min/last OHLC and sum volume/amount; qfq price uses daily factor / per-code latest non-null factor, volume inversely adjusted, amount unchanged; previous period close/change/pct_chg with NumPy ties-to-even 4-decimal rounding",
                   "numeric_tolerance": "abs diff <= 1e-7 + 1e-9*abs(expected) for non-pct fields; NULL parity exact; pct_chg verified row-by-row using numpy.round(...,4)",
                   "snap_ts_compared": False,
                   "partial_period_note": "latest week/month is partial through the latest daily row; comparison uses the daily rows present."})
    save("all four tables complete")
    con.close()
    return status


def main() -> None:
    default_root = Path(os.environ.get("TRADING_DATABASE_ROOT", Path.home() / "TradingDatabase"))
    default_run = Path(tempfile.gettempdir()) / "trading_data_audit" / "week_month"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=default_root / "offline/db_cn_basic.db")
    parser.add_argument("--staging-db", type=Path, default=default_run / "period_compare.duckdb")
    parser.add_argument("--report", type=Path, default=default_run / "week_month_full_compare.json")
    parser.add_argument("--spill-dir", type=Path, default=default_run / "spill")
    parser.add_argument("--chunk-size", type=int, default=250_000)
    parser.add_argument("--resume-stage", action="store_true", help="reuse existing non-empty projected staging tables; no source SQLite scan")
    args = parser.parse_args()
    if args.chunk_size < 1:
        parser.error("--chunk-size must be positive")
    result = run(args.database, args.staging_db, args.report, args.spill_dir, chunk_size=args.chunk_size, resume_stage=args.resume_stage)
    print(json.dumps({"status": result["status"], "tables": {k: v.get("status") for k, v in result["tables"].items() if k.endswith("_expected")}}, ensure_ascii=False))


if __name__ == "__main__":
    main()

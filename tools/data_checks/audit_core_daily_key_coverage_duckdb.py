#!/usr/bin/env python3
"""Stream SQLite daily keys into bounded DuckDB storage and audit key coverage."""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from audit_core_daily_tables import _write_json


TABLES = {
    "daily": "tbl_cn_day",
    "daily_basic": "tbl_cn_daily_basic",
    "adj_factor": "tbl_cn_adj_factor",
}
PAIRS = {
    "daily_missing_daily_basic": ("daily", "daily_basic"),
    "daily_missing_adj_factor": ("daily", "adj_factor"),
    "daily_basic_missing_daily": ("daily_basic", "daily"),
    "adj_factor_missing_daily": ("adj_factor", "daily"),
}


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def run(database: Path, output: Path, temp_dir: Path, start: str, end: str,
        chunk_rows: int, memory_limit: str) -> dict:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "status": "running",
        "mode": "read_only_source_stream_to_temporary_duckdb",
        "database": str(database),
        "start_date": start,
        "end_date": end,
        "chunk_rows": chunk_rows,
        "duckdb_memory_limit": memory_limit,
        "interrupted_then_resumed": True,
        "resume_note": "Original SQLite profiling completed; previous SQLite cross-join attempt was stopped after sustained random-I/O wait. This pass only reloads ts_code/trade_date keys.",
        "source_tables": {},
        "checks": {},
    }
    _write_json(output, state)
    sqlite_uri = "file:" + str(database).replace("?", "%3F") + "?mode=ro"
    duck_path = temp_dir / "core_daily_keys.duckdb"
    if duck_path.exists():
        duck_path.unlink()
    con = duckdb.connect(str(duck_path))
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET temp_directory='{str(temp_dir / 'duckdb_spill')}'")
    con.execute("SET threads=2")
    for key in TABLES:
        con.execute(f'CREATE TABLE "{key}" (ts_code VARCHAR, trade_date VARCHAR)')
    with sqlite3.connect(sqlite_uri, uri=True, timeout=30) as source:
        source.execute("PRAGMA query_only=ON")
        source.execute("BEGIN")
        for key, table in TABLES.items():
            cur = source.execute(
                f'SELECT ts_code,trade_date FROM "{table}" WHERE trade_date BETWEEN ? AND ?',
                (start, end),
            )
            rows = 0
            while True:
                batch = cur.fetchmany(chunk_rows)
                if not batch:
                    break
                frame = pd.DataFrame.from_records(batch, columns=["ts_code", "trade_date"])
                con.register("_key_batch", frame)
                con.execute(f'INSERT INTO "{key}" SELECT ts_code,trade_date FROM _key_batch')
                con.unregister("_key_batch")
                rows += len(batch)
                state["source_tables"][key] = {"physical_table": table, "streamed_rows": rows, "status": "loading"}
                _write_json(output, state)
            state["source_tables"][key] = {"physical_table": table, "streamed_rows": rows, "status": "loaded"}
            _write_json(output, state)
    for label, (source_key, target_key) in PAIRS.items():
        row = con.execute(
            f'SELECT COUNT(*) AS source_rows, '
            f'SUM(CASE WHEN t.ts_code IS NULL THEN 1 ELSE 0 END) AS missing_count '
            f'FROM "{source_key}" s LEFT JOIN "{target_key}" t '
            'ON t.ts_code=s.ts_code AND t.trade_date=s.trade_date'
        ).fetchone()
        missing = int(row[1] or 0)
        sample = []
        if missing:
            sample = [
                {"ts_code": r[0], "trade_date": r[1]}
                for r in con.execute(
                    f'SELECT s.ts_code,s.trade_date FROM "{source_key}" s '
                    f'LEFT JOIN "{target_key}" t ON t.ts_code=s.ts_code AND t.trade_date=s.trade_date '
                    'WHERE t.ts_code IS NULL ORDER BY s.trade_date,s.ts_code LIMIT 25'
                ).fetchall()
            ]
        state["checks"][label] = {
            "source_table": TABLES[source_key],
            "target_table": TABLES[target_key],
            "source_rows": int(row[0] or 0),
            "missing_key_count": missing,
            "sample": sample,
            "status": "complete",
            "completed_at_local": _now(),
        }
        _write_json(output, state)
    con.close()
    state["status"] = "complete"
    state["completed_at_local"] = _now()
    _write_json(output, state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--temp-dir", type=Path, required=True)
    parser.add_argument("--start", default="20150101")
    parser.add_argument("--end", default="20261014")
    parser.add_argument("--chunk-rows", type=int, default=250000)
    parser.add_argument("--memory-limit", default="1GB")
    args = parser.parse_args()
    result = run(args.database, args.output, args.temp_dir, args.start, args.end,
                 args.chunk_rows, args.memory_limit)
    print(json.dumps({"status": result["status"], "checks": result["checks"], "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()

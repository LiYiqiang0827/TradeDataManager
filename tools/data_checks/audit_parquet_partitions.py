#!/usr/bin/env python3
"""Resumable read-only scan of canonical one-minute Parquet partitions.

The scanner reads every column in every row, checks basic OHLCV/session/key
semantics, and checkpoints one partition at a time to an external JSON file.
It does not infer exchange-open dates; compare the emitted dates with a frozen
exchange-calendar snapshot before claiming date coverage.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

import duckdb


REQUIRED_COLUMNS = {
    "ts_code", "name", "trade_date", "datetime", "time_idx",
    "open", "high", "low", "close", "vol", "amount", "adj_factor",
}
PRICE_COLUMNS = ("open", "high", "low", "close")


def _atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def _partition_date(path: Path) -> str | None:
    for part in path.parts:
        if part.startswith("trade_date="):
            value = part.split("=", 1)[1]
            if len(value) == 8 and value.isdigit():
                return value
    return None


def _fingerprint(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _scan_file(path: Path, partition_date: str) -> dict[str, Any]:
    before = _fingerprint(path)
    conn = duckdb.connect(database=":memory:")
    try:
        description = conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        columns = [row[0] for row in description]
        quoted = [f'"{name.replace(chr(34), chr(34) * 2)}"' for name in columns]
        row_hash = "hash(" + ", ".join(quoted) + ")"
        cols_sql = ", ".join(quoted)
        query = f"""
            WITH data AS MATERIALIZED (SELECT * FROM read_parquet(?)),
            stock_counts AS (
                SELECT ts_code, count(*) AS n, count(DISTINCT time_idx) AS distinct_idx
                FROM data GROUP BY ts_code
            ),
            duplicate_datetime AS (
                SELECT coalesce(sum(n - 1), 0) AS n FROM (
                    SELECT count(*) AS n FROM data GROUP BY ts_code, datetime HAVING count(*) > 1
                )
            ),
            duplicate_index AS (
                SELECT coalesce(sum(n - 1), 0) AS n FROM (
                    SELECT count(*) AS n FROM data GROUP BY ts_code, time_idx HAVING count(*) > 1
                )
            ),
            metrics AS (
                SELECT
                    count(*) AS row_count,
                    count(DISTINCT ts_code) AS code_count,
                    min(cast(trade_date AS varchar)) AS min_trade_date,
                    max(cast(trade_date AS varchar)) AS max_trade_date,
                    min(cast(datetime AS varchar)) AS min_datetime,
                    max(cast(datetime AS varchar)) AS max_datetime,
                    count(*) FILTER (WHERE cast(datetime AS date) != strptime(cast(trade_date AS varchar), '%Y%m%d')::date) AS date_mismatch_rows,
                    count(*) FILTER (WHERE trade_date != ?::INTEGER) AS partition_date_mismatch_rows,
                    count(*) FILTER (WHERE strftime(datetime, '%H:%M') NOT BETWEEN '09:31' AND '11:30'
                                      AND strftime(datetime, '%H:%M') NOT BETWEEN '13:01' AND '15:00') AS out_of_session_rows,
                    count(*) FILTER (WHERE time_idx != CASE
                        WHEN strftime(datetime, '%H:%M') BETWEEN '09:31' AND '11:30'
                            THEN hour(datetime) * 60 + minute(datetime) - 571
                        WHEN strftime(datetime, '%H:%M') BETWEEN '13:01' AND '15:00'
                            THEN 120 + hour(datetime) * 60 + minute(datetime) - 781
                        ELSE -1 END) AS time_idx_mismatch_rows,
                    count(*) FILTER (WHERE vol < 0) AS negative_volume_rows,
                    count(*) FILTER (WHERE vol < 0 OR vol != trunc(vol)) AS invalid_volume_rows,
                    count(*) FILTER (WHERE amount < 0) AS negative_amount_rows,
                    count(*) FILTER (WHERE vol = 5.877471754111438e-39) AS pytdx_zero_volume_sentinel_rows,
                    count(*) FILTER (WHERE amount = 5.877471754111438e-39) AS pytdx_zero_amount_sentinel_rows,
                    count(*) FILTER (WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0) AS nonpositive_price_rows,
                    count(*) FILTER (WHERE high < greatest(open, close, low) OR low > least(open, close, high)) AS invalid_ohlc_rows,
                    count(*) FILTER (WHERE adj_factor IS NOT NULL AND adj_factor <= 0) AS nonpositive_adj_factor_rows,
                    sum({row_hash}) AS decode_hash_sum,
                    {', '.join(f'count(*) FILTER (WHERE {c} IS NULL) AS null_{c}' for c in columns)}
                FROM data
            )
            SELECT metrics.*,
                (SELECT n FROM duplicate_datetime) AS duplicate_datetime_rows,
                (SELECT n FROM duplicate_index) AS duplicate_time_idx_rows,
                (SELECT count(*) FROM stock_counts WHERE n != 240) AS non_240_code_count,
                (SELECT min(n) FROM stock_counts) AS min_bars_per_code,
                (SELECT max(n) FROM stock_counts) AS max_bars_per_code,
                (SELECT count(*) FROM stock_counts WHERE distinct_idx != n) AS code_with_duplicate_index_count
            FROM metrics
        """
        row = conn.execute(query, [str(path), int(partition_date)]).fetchone()
        labels = [entry[0] for entry in conn.description]
        result = dict(zip(labels, row))
    finally:
        conn.close()
    after = _fingerprint(path)
    result["input_before"] = before
    result["input_after"] = after
    result["changed_during_scan"] = before != after
    result["columns"] = columns
    result["missing_required_columns"] = sorted(REQUIRED_COLUMNS - set(columns))
    result["extra_columns"] = sorted(set(columns) - REQUIRED_COLUMNS)
    result["status"] = "completed" if not result["changed_during_scan"] else "input_changed_during_scan"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-root", type=Path, required=True, help="schema_v2 directory")
    parser.add_argument("--state-file", type=Path, required=True, help="External resumable JSON output")
    parser.add_argument("--limit-partitions", type=int, help="Diagnostic limit; never a full audit")
    args = parser.parse_args()

    root = args.parquet_root.resolve()
    state_path = args.state_file.resolve()
    try:
        prior = json.loads(state_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        prior = {}
    prior_results = prior.get("partitions", {}) if prior.get("root") == str(args.parquet_root.resolve()) else {}
    files = sorted(p for p in root.rglob("*.parquet") if _partition_date(p))
    if args.limit_partitions is not None:
        files = files[: max(0, args.limit_partitions)]

    state: dict[str, Any] = {
        "updated_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "root": str(root),
        "state_file": str(state_path),
        "mode": "read_only_full_row_decode",
        "audit_version": 2,
        "partition_count_discovered": len(files),
        "calendar_coverage": "pending_frozen_exchange_calendar_comparison",
        "partition_dates_present": [],
        "partitions": {},
    }
    _atomic_json(state_path, state)

    overall_start = time.monotonic()
    failures: list[str] = []
    for idx, path in enumerate(files, 1):
        trade_date = _partition_date(path)
        assert trade_date is not None
        key = str(path.relative_to(root))
        fingerprint = _fingerprint(path)
        old = prior_results.get(key)
        if old and old.get("audit_version") == state["audit_version"] and old.get("status") == "completed" and old.get("input_before") == fingerprint and not old.get("changed_during_scan"):
            state["partitions"][key] = old
            print(f"SKIP {idx}/{len(files)} {trade_date} previously completed", flush=True)
            continue
        state["partitions"][key] = {
            "status": "running",
            "trade_date": trade_date,
            "input_before": fingerprint,
            "started_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        _atomic_json(state_path, state)
        started = time.monotonic()
        try:
            result = _scan_file(path, trade_date)
            result["trade_date"] = trade_date
            result["audit_version"] = state["audit_version"]
            result["elapsed_seconds"] = round(time.monotonic() - started, 3)
            state["partitions"][key] = result
            if result["status"] != "completed" or result["missing_required_columns"]:
                failures.append(key)
            print(
                f"DONE {idx}/{len(files)} {trade_date} rows={result.get('row_count')} "
                f"codes={result.get('code_count')} status={result['status']} "
                f"elapsed={result['elapsed_seconds']}s",
                flush=True,
            )
        except Exception as exc:
            state["partitions"][key] = {
                "status": "error",
                "trade_date": trade_date,
                "input_before": fingerprint,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            failures.append(key)
            print(f"ERROR {idx}/{len(files)} {trade_date} {type(exc).__name__}: {exc}", flush=True)
        _atomic_json(state_path, state)

    completed = sum(v.get("status") == "completed" for v in state["partitions"].values())
    state["partition_dates_present"] = sorted({_partition_date(p) for p in files if _partition_date(p)})
    state["completed_in_this_run"] = completed
    state["run_elapsed_seconds"] = round(time.monotonic() - overall_start, 3)
    state["failed_or_incomplete_paths"] = failures
    state["status"] = "complete_with_findings" if completed == len(files) and not failures else "incomplete"
    state["updated_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    _atomic_json(state_path, state)
    print(
        f"SUMMARY status={state['status']} completed={completed}/{len(files)} "
        f"elapsed={state['run_elapsed_seconds']}s state={state_path}",
        flush=True,
    )
    return 0 if state["status"] == "complete_with_findings" else 2


if __name__ == "__main__":
    raise SystemExit(main())

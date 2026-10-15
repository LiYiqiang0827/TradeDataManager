#!/usr/bin/env python3
"""Resumable supplemental numeric-domain scan for canonical minute Parquet.

This deliberately avoids repeating row hashes and full-column semantic work
from audit_parquet_partitions.py. It checks every numeric column for NaN/Inf,
and counts known pytdx zero sentinels and amount-with-zero-volume rows.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import time
from typing import Any

import duckdb


SENTINEL = 5.877471754111438e-39


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def partition_date(path: Path) -> str | None:
    for part in path.parts:
        if part.startswith("trade_date="):
            value = part.split("=", 1)[1]
            if len(value) == 8 and value.isdigit():
                return value
    return None


def fingerprint(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def scan(path: Path) -> dict[str, Any]:
    before = fingerprint(path)
    conn = duckdb.connect(database=":memory:")
    try:
        description = conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        numeric = [(name, typ) for name, typ, *_ in description if any(
            marker in typ.upper() for marker in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "HUGEINT")
        )]
        if not {"vol", "amount"}.issubset({row[0] for row in description}):
            raise ValueError("required vol/amount columns missing")
        q = lambda name: '"' + name.replace('"', '""') + '"'
        nonfinite = ", ".join(
            f"count(*) FILTER (WHERE {q(name)} IS NOT NULL AND NOT isfinite(try_cast({q(name)} AS DOUBLE))) AS {q(name + '_nonfinite')}"
            for name, _ in numeric
        )
        sql = f"""
            SELECT count(*) AS row_count,
                count(*) FILTER (WHERE vol = {SENTINEL}) AS pytdx_zero_volume_sentinel_rows,
                count(*) FILTER (WHERE amount = {SENTINEL}) AS pytdx_zero_amount_sentinel_rows,
                count(*) FILTER (WHERE vol = 0 AND amount <> 0) AS amount_nonzero_when_vol_zero_rows,
                {nonfinite}
            FROM read_parquet(?)
        """
        row = conn.execute(sql, [str(path)]).fetchone()
        labels = [column[0] for column in conn.description]
        metrics = dict(zip(labels, row))
    finally:
        conn.close()
    after = fingerprint(path)
    return {
        **metrics,
        "numeric_columns": numeric,
        "input_before": before,
        "input_after": after,
        "changed_during_scan": before != after,
        "status": "completed" if before == after else "input_changed_during_scan",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-root", type=Path, required=True)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--limit-partitions", type=int)
    args = parser.parse_args()
    root = args.parquet_root.resolve()
    state_path = args.state_file.resolve()
    try:
        prior = json.loads(state_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        prior = {}
    previous = prior.get("partitions", {}) if prior.get("root") == str(root) else {}
    files = sorted(p for p in root.rglob("*.parquet") if partition_date(p))
    if args.limit_partitions is not None:
        files = files[:max(args.limit_partitions, 0)]
    state: dict[str, Any] = {
        "updated_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "root": str(root),
        "mode": "read_only_numeric_supplement",
        "partition_count_discovered": len(files),
        "partitions": {},
    }
    write_json(state_path, state)
    start = time.monotonic()
    failures: list[str] = []
    for idx, path in enumerate(files, 1):
        key = str(path.relative_to(root))
        current = fingerprint(path)
        old = previous.get(key)
        if old and old.get("status") == "completed" and old.get("input_before") == current:
            state["partitions"][key] = old
            print(f"SKIP {idx}/{len(files)} {partition_date(path)}", flush=True)
        else:
            try:
                result = scan(path)
                state["partitions"][key] = {"trade_date": partition_date(path), **result}
                if result["status"] != "completed":
                    failures.append(key)
                print(
                    f"DONE {idx}/{len(files)} {partition_date(path)} rows={result['row_count']} "
                    f"vol_sentinel={result['pytdx_zero_volume_sentinel_rows']} "
                    f"amount_sentinel={result['pytdx_zero_amount_sentinel_rows']} "
                    f"amount_with_zero_vol={result['amount_nonzero_when_vol_zero_rows']}",
                    flush=True,
                )
            except Exception as exc:
                state["partitions"][key] = {
                    "trade_date": partition_date(path), "status": "error",
                    "error_type": type(exc).__name__, "error": str(exc),
                    "input_before": current,
                }
                failures.append(key)
        state["updated_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        write_json(state_path, state)
    completed = sum(row.get("status") == "completed" for row in state["partitions"].values())
    totals: dict[str, int] = {}
    for result in state["partitions"].values():
        if result.get("status") == "completed":
            for metric in ("pytdx_zero_volume_sentinel_rows", "pytdx_zero_amount_sentinel_rows", "amount_nonzero_when_vol_zero_rows"):
                totals[metric] = totals.get(metric, 0) + int(result[metric])
            for metric, value in result.items():
                if metric.endswith("_nonfinite"):
                    totals[metric] = totals.get(metric, 0) + int(value)
    state["completed"] = completed
    state["totals"] = totals
    state["failed_paths"] = failures
    state["elapsed_seconds"] = round(time.monotonic() - start, 3)
    state["status"] = "complete" if completed == len(files) and not failures else "incomplete"
    state["updated_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    write_json(state_path, state)
    print(f"SUMMARY {state['status']} {completed}/{len(files)} totals={totals}", flush=True)
    return 0 if state["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())

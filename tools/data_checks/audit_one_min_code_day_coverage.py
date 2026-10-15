#!/usr/bin/env python3
"""Compare canonical one-minute code-days with daily bars, excluding all audit/stage trees."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import duckdb

from audit_core_daily_tables import _write_json


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _manifest_inventory(root: Path) -> tuple[list[tuple[str, Path, dict]], dict]:
    parts = sorted(root.glob("year=*/month=*/trade_date=*/part-*.parquet"))
    manifests = sorted((root / "_manifest").glob("trade_date=*.json"))
    by_date = {}
    for path in manifests:
        item = json.loads(path.read_text(encoding="utf-8"))
        by_date[str(item["trade_date"])] = item
    canonical = {}
    issues = {"missing_target": [], "extra_part": [], "size_mismatch": [], "duplicate_resolved_part": []}
    resolved_parts: dict[Path, Path] = {}
    for path in parts:
        resolved = path.resolve()
        if resolved in resolved_parts:
            issues["duplicate_resolved_part"].append(str(path))
        resolved_parts[resolved] = path
        day = path.parent.name.split("=", 1)[1]
        if day in canonical:
            issues["extra_part"].append(str(path))
        canonical[day] = (resolved, path)
    listed = {}
    for day, item in by_date.items():
        target_text = item.get("target_file")
        if not target_text:
            issues["missing_target"].append({"date": day, "reason": "manifest_target_file_missing"})
            continue
        target = Path(target_text).resolve()
        listed[day] = target
        if not target.is_file():
            issues["missing_target"].append({"date": day, "path": str(target)})
            continue
        if int(item.get("target_size", -1)) != target.stat().st_size:
            issues["size_mismatch"].append({
                "date": day, "manifest_bytes": item.get("target_size"), "actual_bytes": target.stat().st_size,
            })
    extra_manifestless = sorted(set(canonical) - set(by_date))
    if extra_manifestless:
        issues["extra_part"].extend(extra_manifestless)
    for day, target in listed.items():
        if day not in canonical or target != canonical[day][0]:
            issues["missing_target"].append({"date": day, "path": str(target), "reason": "not_same_as_canonical_glob"})
    summary = {
        "canonical_glob_part_files": len(parts),
        "resolved_unique_part_files": len(resolved_parts),
        "manifest_files": len(manifests),
        "manifest_dates": len(by_date),
        "manifest_target_paths": len(listed),
        "manifest_target_size_mismatches": len(issues["size_mismatch"]),
        "path_issues": issues,
        "canonical_dates": sorted(canonical),
    }
    canonical_rows = [(day, canonical[day][1], by_date[day]) for day in sorted(canonical) if day in by_date]
    return canonical_rows, summary


def run(parquet_root: Path, duckdb_keys: Path, output: Path, start: str, end: str,
        memory_limit: str = "1GB") -> dict:
    output.parent.mkdir(parents=True, exist_ok=True)
    partitions, inventory = _manifest_inventory(parquet_root)
    issues = inventory["path_issues"]
    if any(issues.values()) or inventory["canonical_glob_part_files"] != inventory["manifest_dates"]:
        raise RuntimeError("canonical part/manifest inventory is inconsistent; no Parquet scan started")
    state = {
        "status": "running",
        "mode": "canonical_parquet_code_day_projection_only",
        "parquet_root": str(parquet_root),
        "daily_keys_duckdb": str(duckdb_keys),
        "start_date": start,
        "end_date": end,
        "inventory": {k: v for k, v in inventory.items() if k != "canonical_dates"},
        "current_date": None,
        "completed_dates": 0,
        "physical_rows_scanned": 0,
        "code_day_rows_inserted": 0,
        "per_date": {},
    }
    _write_json(output, state)
    con = duckdb.connect(str(duckdb_keys))
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET threads=2")
    con.execute("CREATE TABLE IF NOT EXISTS one_min_code_days (ts_code VARCHAR, trade_date VARCHAR)")
    existing = con.execute("SELECT COUNT(*) FROM one_min_code_days").fetchone()[0]
    if existing:
        raise RuntimeError("temporary one_min_code_days table is not empty; choose a fresh DuckDB key file")
    for day, path, manifest in partitions:
        if day < start or day > end:
            continue
        state["current_date"] = day
        _write_json(output, state)
        rows, = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=false)", [str(path)]
        ).fetchone()
        con.execute(
            "INSERT INTO one_min_code_days "
            "SELECT DISTINCT ts_code, CAST(trade_date AS VARCHAR) "
            "FROM read_parquet(?, hive_partitioning=false)", [str(path)]
        )
        code_days, = con.execute("SELECT COUNT(*) FROM one_min_code_days WHERE trade_date=?", [day]).fetchone()
        state["physical_rows_scanned"] += int(rows)
        state["code_day_rows_inserted"] += int(code_days)
        state["completed_dates"] += 1
        state["per_date"][day] = {
            "physical_rows": int(rows),
            "distinct_code_days": int(code_days),
            "manifest_rows": manifest.get("rows"),
            "manifest_stock_count": manifest.get("stock_count"),
            "manifest_status": manifest.get("status"),
        }
        state["current_date"] = None
        _write_json(output, state)
    range_clause = "trade_date BETWEEN ? AND ?"
    expected_count = con.execute(f"SELECT COUNT(*) FROM daily WHERE {range_clause}", [start, end]).fetchone()[0]
    actual_count = con.execute(f"SELECT COUNT(*) FROM one_min_code_days WHERE {range_clause}", [start, end]).fetchone()[0]
    missing_by_date = con.execute(
        "SELECT d.trade_date, COUNT(*) AS missing_code_days "
        "FROM daily d LEFT JOIN one_min_code_days m ON m.ts_code=d.ts_code AND m.trade_date=d.trade_date "
        f"WHERE d.{range_clause} AND m.ts_code IS NULL GROUP BY d.trade_date ORDER BY d.trade_date",
        [start, end],
    ).fetchall()
    extra_by_date = con.execute(
        "SELECT m.trade_date, COUNT(*) AS extra_code_days "
        "FROM one_min_code_days m LEFT JOIN daily d ON d.ts_code=m.ts_code AND d.trade_date=m.trade_date "
        f"WHERE m.{range_clause} AND d.ts_code IS NULL GROUP BY m.trade_date ORDER BY m.trade_date",
        [start, end],
    ).fetchall()
    missing_sample = con.execute(
        "SELECT d.trade_date,d.ts_code FROM daily d LEFT JOIN one_min_code_days m "
        "ON m.ts_code=d.ts_code AND m.trade_date=d.trade_date "
        f"WHERE d.{range_clause} AND m.ts_code IS NULL ORDER BY d.trade_date,d.ts_code LIMIT 100",
        [start, end],
    ).fetchall()
    extra_sample = con.execute(
        "SELECT m.trade_date,m.ts_code FROM one_min_code_days m LEFT JOIN daily d "
        "ON d.ts_code=m.ts_code AND d.trade_date=m.trade_date "
        f"WHERE m.{range_clause} AND d.ts_code IS NULL ORDER BY m.trade_date,m.ts_code LIMIT 100",
        [start, end],
    ).fetchall()
    state["comparison"] = {
        "daily_code_day_count": int(expected_count),
        "canonical_one_min_code_day_count": int(actual_count),
        "daily_code_days_missing_one_min": int(sum(row[1] for row in missing_by_date)),
        "one_min_code_days_missing_daily": int(sum(row[1] for row in extra_by_date)),
        "missing_by_date": [{"trade_date": r[0], "count": int(r[1])} for r in missing_by_date],
        "extra_by_date": [{"trade_date": r[0], "count": int(r[1])} for r in extra_by_date],
        "missing_sample_first_100": [{"trade_date": r[0], "ts_code": r[1]} for r in missing_sample],
        "extra_sample_first_100": [{"trade_date": r[0], "ts_code": r[1]} for r in extra_sample],
        "interpretation": "A daily key absent from minutes is a candidate only; listing/suspension/source scope must be checked before calling it missing data.",
    }
    con.close()
    state["status"] = "complete"
    state["completed_at_local"] = _now()
    _write_json(output, state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-root", type=Path, required=True)
    parser.add_argument("--duckdb-keys", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start", default="20250102")
    parser.add_argument("--end", default="20261014")
    parser.add_argument("--memory-limit", default="1GB")
    args = parser.parse_args()
    result = run(args.parquet_root, args.duckdb_keys, args.output, args.start, args.end, args.memory_limit)
    print(json.dumps({"status": result["status"], "completed_dates": result["completed_dates"], "comparison": result["comparison"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()

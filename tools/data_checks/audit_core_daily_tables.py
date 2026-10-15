#!/usr/bin/env python3
"""Read-only whole-calendar audit for daily, daily-basic, and adj-factor tables."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from coreClient._paths import database_root

SCRIPTS = ROOT / "offlineDataManager" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from core.check_database import check_database  # noqa: E402

TABLES = {
    "daily": "tbl_cn_day",
    "daily_basic": "tbl_cn_daily_basic",
    "adj_factor": "tbl_cn_adj_factor",
}


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_schema(conn: sqlite3.Connection, table: str) -> list[dict]:
    rows = conn.execute(f"PRAGMA table_info({_identifier(table)})").fetchall()
    return [
        {"name": row[1], "type": row[2], "not_null": bool(row[3]), "primary_key_order": row[5]}
        for row in rows
    ]


def _table_profile(conn: sqlite3.Connection, table: str, schema: list[dict], start: str, end: str) -> dict:
    numeric = [
        item["name"] for item in schema
        if any(token in item["type"].upper() for token in ("INT", "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"))
    ]
    expressions = ["COUNT(*) AS row_count"]
    names = ["row_count"]
    for index, column in enumerate(schema):
        name = column["name"]
        alias = f"n{index}"
        expressions.append(f"SUM(CASE WHEN {_identifier(name)} IS NULL THEN 1 ELSE 0 END) AS {alias}")
        names.append(alias)
        if name in numeric:
            min_alias, max_alias, inf_alias, type_alias = f"min{index}", f"max{index}", f"nonfinite{index}", f"type_mismatch{index}"
            col = _identifier(name)
            expressions.extend([
                f"MIN({col}) AS {min_alias}",
                f"MAX({col}) AS {max_alias}",
                f"SUM(CASE WHEN typeof({col}) IN ('real','integer') AND "
                f"({col} > 1.7976931348623157e308 OR {col} < -1.7976931348623157e308) "
                f"THEN 1 ELSE 0 END) AS {inf_alias}",
                f"SUM(CASE WHEN {col} IS NOT NULL AND typeof({col}) NOT IN ('integer','real') THEN 1 ELSE 0 END) AS {type_alias}",
            ])
            names.extend([min_alias, max_alias, inf_alias, type_alias])
    row = conn.execute(f"SELECT {', '.join(expressions)} FROM {_identifier(table)}").fetchone()
    values = dict(zip(names, row))
    nulls, ranges, nonfinite = {}, {}, {}
    for index, column in enumerate(schema):
        name = column["name"]
        nulls[name] = int(values[f"n{index}"] or 0)
        if name in numeric:
            ranges[name] = {"min": values[f"min{index}"], "max": values[f"max{index}"]}
            nonfinite[name] = int(values[f"nonfinite{index}"] or 0)
    pk = [item["name"] for item in sorted(schema, key=lambda item: item["primary_key_order"]) if item["primary_key_order"]]
    predicates = {
        "tbl_cn_day": {
            "invalid_ohlc_domain": "open<=0 OR high<=0 OR low<=0 OR close<=0 OR low>high OR low>open OR low>close OR high<open OR high<close",
            "negative_volume_or_amount": "vol<0 OR amount<0",
        },
        "tbl_cn_daily_basic": {
            "nonpositive_close": "close<=0",
            "negative_share_or_market_value": "total_share<0 OR float_share<0 OR free_share<0 OR total_mv<0 OR circ_mv<0",
        },
        "tbl_cn_adj_factor": {
            "nonpositive_factor": "adj_factor<=0",
        },
    }[table]
    domain_counts = {}
    domain_samples = {}
    date_col = _identifier("trade_date")
    for name, predicate in predicates.items():
        domain_counts[name] = int(conn.execute(
            f"SELECT COUNT(*) FROM {_identifier(table)} WHERE {date_col} BETWEEN ? AND ? AND ({predicate})",
            (start, end),
        ).fetchone()[0])
        domain_samples[name] = [dict(row) for row in conn.execute(
            f"SELECT ts_code, trade_date FROM {_identifier(table)} WHERE {date_col} BETWEEN ? AND ? AND ({predicate}) LIMIT 25",
            (start, end),
        ).fetchall()]
    numeric_type_mismatches = {
        name: int(values[f"type_mismatch{index}"] or 0)
        for index, name in enumerate(column["name"] for column in schema)
        if name in numeric
    }
    # Date validity and calendar agreement are checked over all distinct dates,
    # then mapped back to row counts so an invalid date cannot hide in a sparse day.
    date_counts = conn.execute(
        f"SELECT {date_col}, COUNT(*) FROM {_identifier(table)} GROUP BY {date_col} ORDER BY {date_col}"
    ).fetchall()
    invalid_dates = []
    for date_value, count in date_counts:
        text = str(date_value)
        try:
            datetime.strptime(text, "%Y%m%d")
        except ValueError:
            invalid_dates.append({"trade_date": text, "rows": int(count)})
    nonopen = conn.execute(
        f"SELECT t.trade_date, COUNT(*) FROM {_identifier(table)} t "
        "LEFT JOIN tbl_cn_tradecal c ON c.exchange='SSE' AND c.cal_date=t.trade_date AND c.is_open=1 "
        "WHERE c.cal_date IS NULL GROUP BY t.trade_date ORDER BY t.trade_date"
    ).fetchall()
    return {
        "row_count": int(values["row_count"]),
        "null_count_by_column": nulls,
        "numeric_min_max": ranges,
        "nonfinite_count_by_numeric_column": nonfinite,
        "numeric_storage_type_mismatch_by_column": numeric_type_mismatches,
        "primary_key_columns": pk,
        "domain_violation_counts_in_requested_range": domain_counts,
        "domain_violation_samples": domain_samples,
        "invalid_trade_date_values": invalid_dates,
        "rows_on_dates_without_sse_open_calendar": [
            {"trade_date": str(date), "rows": int(count)} for date, count in nonopen
        ],
    }


def _cross_table_keys(conn: sqlite3.Connection, start: str, end: str) -> dict:
    checks = {
        "daily_missing_daily_basic": ("tbl_cn_day", "tbl_cn_daily_basic"),
        "daily_missing_adj_factor": ("tbl_cn_day", "tbl_cn_adj_factor"),
        "daily_basic_missing_daily": ("tbl_cn_daily_basic", "tbl_cn_day"),
        "adj_factor_missing_daily": ("tbl_cn_adj_factor", "tbl_cn_day"),
    }
    result = {}
    for label, (source, target) in checks.items():
        join = (
            f"SELECT COUNT(*) FROM {_identifier(source)} s LEFT JOIN {_identifier(target)} t "
            "ON t.ts_code=s.ts_code AND t.trade_date=s.trade_date "
            "WHERE s.trade_date BETWEEN ? AND ? AND t.ts_code IS NULL"
        )
        count = int(conn.execute(join, (start, end)).fetchone()[0])
        sample = conn.execute(
            f"SELECT s.ts_code, s.trade_date FROM {_identifier(source)} s LEFT JOIN {_identifier(target)} t "
            "ON t.ts_code=s.ts_code AND t.trade_date=s.trade_date "
            "WHERE s.trade_date BETWEEN ? AND ? AND t.ts_code IS NULL ORDER BY s.trade_date,s.ts_code LIMIT 25",
            (start, end),
        ).fetchall()
        result[label] = {"missing_key_count": count, "sample": [dict(row) for row in sample]}
    return result


def run(start: str, end: str, output: Path) -> dict:
    db_path = database_root() / "offline" / "db_cn_basic.db"
    if not db_path.is_file():
        raise FileNotFoundError(f"canonical basic DB not found: {db_path}")
    state = {
        "status": "running",
        "mode": "read_only",
        "database": str(db_path),
        "start_date": start,
        "end_date": end,
        "tables": {},
        "limitations": [
            "Neighbor-count candidates are suspects only; listing, suspension, and source completeness require separate evidence.",
            "This checks SSE open-date row coverage and stored numeric/null domains; it does not validate every value against an external source.",
        ],
    }
    _write_json(output, state)
    uri = "file:" + str(db_path).replace("?", "%3F") + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        for key, physical in TABLES.items():
            schema = _table_schema(conn, physical)
            if not schema:
                raise RuntimeError(f"table not found: {physical}")
            profile = _table_profile(conn, physical, schema, start, end)
            coverage = check_database(
                key,
                start_date=start,
                end_date=end,
                auto_repair=False,
                regenerate_derived=False,
                db_path=db_path,
                connection=conn,
                neighbor_ratio_threshold=0.90,
            )
            state["tables"][key] = {
                "physical_table": physical,
                "schema": schema,
                "profile": profile,
                "calendar_coverage": coverage["reports"][key],
                "neighbor_ratio_candidates_are_not_confirmed_gaps": True,
                "status": "scanned",
            }
            if key == "daily":
                candidate_days = coverage["reports"][key]["redownload_dates"]
                suspension = {}
                for date in candidate_days:
                    rows = conn.execute(
                        "SELECT suspend_type, COUNT(*) FROM tbl_cn_suspend "
                        "WHERE trade_date=? GROUP BY suspend_type ORDER BY suspend_type",
                        (date,),
                    ).fetchall()
                    suspension[date] = {
                        "suspension_rows_by_type": {str(item[0]): int(item[1]) for item in rows},
                        "sample": [dict(item) for item in conn.execute(
                            "SELECT ts_code, suspend_timing, suspend_type FROM tbl_cn_suspend "
                            "WHERE trade_date=? ORDER BY ts_code LIMIT 30", (date,),
                        ).fetchall()],
                    }
                state["tables"][key]["candidate_date_suspension_evidence"] = suspension
            _write_json(output, state)
        state["cross_table_key_coverage"] = _cross_table_keys(conn, start, end)
        _write_json(output, state)
    state["status"] = "complete_with_candidates" if any(
        table["calendar_coverage"]["redownload_dates"] for table in state["tables"].values()
    ) else "complete"
    state["completed_at_local"] = datetime.now().astimezone().isoformat(timespec="seconds")
    _write_json(output, state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="20150101")
    parser.add_argument("--end", default="20261014")
    parser.add_argument("--output", type=Path, required=True, help="External JSON progress/receipt path")
    args = parser.parse_args()
    result = run(args.start, args.end, args.output)
    print(json.dumps({
        "status": result["status"],
        "database": result["database"],
        "tables": {
            key: {
                "rows": value["profile"]["row_count"],
                "open_dates": value["calendar_coverage"]["checked_trade_dates"],
                "candidate_dates": len(value["calendar_coverage"]["redownload_dates"]),
            }
            for key, value in result["tables"].items()
        },
        "output": str(args.output),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()

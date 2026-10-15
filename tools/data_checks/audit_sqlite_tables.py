#!/usr/bin/env python3
"""Read-only semantic/storage audit for the non-core offline SQLite tables.

The daily, daily-basic and adjustment-factor core tables are owned by the
separate ``audit_core_daily_tables.py`` scan. Policy minute tables are covered
by their own source/structure audit. This tool profiles complete tables without
asserting economic correctness where no field contract is documented.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
from typing import Any


TABLES: dict[str, tuple[str, ...]] = {
    "db_cn_basic.db": (
        "tbl_basic_ctrl", "tbl_cn_basic", "tbl_cn_cyq_perf", "tbl_cn_margin",
        "tbl_cn_margin_detail", "tbl_cn_moneyflow", "tbl_cn_month",
        "tbl_cn_month_origin", "tbl_cn_stk_limit", "tbl_cn_suspend",
        "tbl_cn_tradecal", "tbl_cn_week", "tbl_cn_week_origin",
    ),
    "db_cn_kpl.db": (
        "tbl_cn_block_trade", "tbl_cn_ggt_daily", "tbl_cn_hsgt_top10",
        "tbl_cn_kpl_concept_cons", "tbl_cn_kpl_limit_performance", "tbl_cn_kpl_list",
        "tbl_cn_limit_list", "tbl_cn_top_inst", "tbl_cn_top_list", "tbl_kpl_ctrl",
    ),
    "db_cn_news.db": ("tbl_cctv_news", "tbl_major_news", "tbl_news", "tbl_news_ctrl"),
    "db_cn_index.db": ("tbl_cn_index_basic", "tbl_cn_index_daily", "tbl_index_ctrl"),
    "policy_ticks.db": ("tbl_tick", "tbl_tick_ctrl"),
}

TRADE_DATE_TABLES = {
    "tbl_cn_cyq_perf", "tbl_cn_margin", "tbl_cn_margin_detail", "tbl_cn_moneyflow",
    "tbl_cn_month", "tbl_cn_month_origin", "tbl_cn_stk_limit", "tbl_cn_suspend",
    "tbl_cn_week", "tbl_cn_week_origin", "tbl_cn_block_trade", "tbl_cn_ggt_daily",
    "tbl_cn_hsgt_top10", "tbl_cn_kpl_concept_cons", "tbl_cn_kpl_limit_performance",
    "tbl_cn_kpl_list", "tbl_cn_limit_list", "tbl_cn_top_inst", "tbl_cn_top_list",
    "tbl_cn_index_daily", "tbl_tick",
}
DATE_ONLY_COLUMNS = {
    "tbl_cn_basic": ("list_date", "delist_date"),
    "tbl_cn_index_basic": ("base_date", "list_date"),
    "tbl_basic_ctrl": ("max_date",),
    "tbl_kpl_ctrl": ("max_date",),
    "tbl_index_ctrl": ("max_date",),
    "tbl_news_ctrl": ("max_date",),
}
TIMESTAMP_COLUMNS = {"tbl_cctv_news": ("datetime",), "tbl_major_news": ("datetime",), "tbl_news": ("datetime",)}

PRICE_TABLES = {
    "tbl_cn_month": ("open", "high", "low", "close", "pre_close"),
    "tbl_cn_month_origin": ("open", "high", "low", "close", "pre_close"),
    "tbl_cn_week": ("open", "high", "low", "close", "pre_close"),
    "tbl_cn_week_origin": ("open", "high", "low", "close", "pre_close"),
    "tbl_cn_index_daily": ("open", "high", "low", "close", "pre_close"),
}
NONNEGATIVE_COLUMNS = {
    **{table: ("vol", "amount") for table in PRICE_TABLES},
    "tbl_cn_moneyflow": (
        "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
        "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
        "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
        "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
    ),
}


def _qi(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _date_value(raw: Any, *, timestamp: bool = False) -> date | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        raise ValueError("empty")
    formats = ["%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"]
    if timestamp:
        formats = [
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f",
            "%Y%m%d%H%M%S", "%Y%m%d %H:%M:%S",
        ] + formats
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise ValueError("unsupported date format") from exc


def _timestamp_audit(conn: sqlite3.Connection, table: str, column: str) -> dict[str, Any]:
    """Validate supported timestamp shapes with SQLite SQL, not Python UDFs."""
    qtable, qcol = _qi(table), _qi(column)
    x = f"trim({qcol})"
    compact_date = f"(length({x})=8 AND {x} NOT GLOB '*[^0-9]*')"
    compact_stamp = f"(length({x})=14 AND {x} NOT GLOB '*[^0-9]*')"
    compact_spaced_stamp = (
        f"(length({x})=17 AND substr({x},1,8) NOT GLOB '*[^0-9]*' AND substr({x},9,1)=' ')")
    iso_date_prefix = f"(length({x})>=10 AND substr({x},5,1)='-' AND substr({x},8,1)='-')"
    iso_stamp_shape = (
        f"({iso_date_prefix} AND length({x})>=19 AND substr({x},11,1) IN (' ','T') "
        f"AND substr({x},14,1)=':' AND substr({x},17,1)=':')"
    )
    iso_minute_shape = (
        f"({iso_date_prefix} AND length({x})=16 AND substr({x},11,1) IN (' ','T') "
        f"AND substr({x},14,1)=':')"
    )
    compact_date_sql = f"substr({x},1,4)||'-'||substr({x},5,2)||'-'||substr({x},7,2)"
    compact_date_valid = f"(strftime('%Y%m%d',{compact_date_sql})=substr({x},1,8))"
    compact_time_valid = (
        f"(CAST(substr({x},9,2) AS INTEGER) BETWEEN 0 AND 23 "
        f"AND substr({x},9,2) NOT GLOB '*[^0-9]*' "
        f"AND CAST(substr({x},11,2) AS INTEGER) BETWEEN 0 AND 59 "
        f"AND substr({x},11,2) NOT GLOB '*[^0-9]*' "
        f"AND CAST(substr({x},13,2) AS INTEGER) BETWEEN 0 AND 59 "
        f"AND substr({x},13,2) NOT GLOB '*[^0-9]*')"
    )
    compact_spaced_time_valid = (
        f"(substr({x},10,2) NOT GLOB '*[^0-9]*' AND CAST(substr({x},10,2) AS INTEGER) BETWEEN 0 AND 23 "
        f"AND substr({x},13,2) NOT GLOB '*[^0-9]*' AND CAST(substr({x},13,2) AS INTEGER) BETWEEN 0 AND 59 "
        f"AND substr({x},16,2) NOT GLOB '*[^0-9]*' AND CAST(substr({x},16,2) AS INTEGER) BETWEEN 0 AND 59)"
    )
    iso_valid = f"(strftime('%Y-%m-%d %H:%M:%S',{x}) IS NOT NULL)"
    iso_minute_valid = (
        f"(substr({x},5,1)='-' AND substr({x},8,1)='-' "
        f"AND substr({x},1,4) NOT GLOB '*[^0-9]*' "
        f"AND substr({x},6,2) NOT GLOB '*[^0-9]*' "
        f"AND substr({x},9,2) NOT GLOB '*[^0-9]*' "
        f"AND CAST(substr({x},1,4) AS INTEGER) BETWEEN 1 AND 9999 "
        f"AND CAST(substr({x},6,2) AS INTEGER) BETWEEN 1 AND 12 "
        f"AND CAST(substr({x},9,2) AS INTEGER) BETWEEN 1 AND "
        f"CASE CAST(substr({x},6,2) AS INTEGER) "
        f"WHEN 2 THEN CASE WHEN (CAST(substr({x},1,4) AS INTEGER)%4=0 "
        f"AND (CAST(substr({x},1,4) AS INTEGER)%100<>0 OR CAST(substr({x},1,4) AS INTEGER)%400=0)) "
        f"THEN 29 ELSE 28 END WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30 ELSE 31 END "
        f"AND substr({x},11,1) IN (' ','T') AND substr({x},14,1)=':' "
        f"AND substr({x},12,2) NOT GLOB '*[^0-9]*' "
        f"AND substr({x},15,2) NOT GLOB '*[^0-9]*' "
        f"AND CAST(substr({x},12,2) AS INTEGER) BETWEEN 0 AND 23 "
        f"AND CAST(substr({x},15,2) AS INTEGER) BETWEEN 0 AND 59)"
    )
    counters = conn.execute(
        f"SELECT COUNT(*) AS all_rows, "
        f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS null_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}='' THEN 1 ELSE 0 END) AS blank_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND {compact_date} THEN 1 ELSE 0 END) AS compact_date_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND ({compact_stamp} OR {compact_spaced_stamp}) THEN 1 ELSE 0 END) AS compact_timestamp_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND {iso_stamp_shape} AND {iso_valid} THEN 1 ELSE 0 END) AS valid_iso_timestamp_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND {iso_minute_shape} AND {iso_minute_valid} THEN 1 ELSE 0 END) AS valid_iso_minute_timestamp_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND ({compact_stamp} OR {compact_spaced_stamp}) AND NOT {compact_date_valid} THEN 1 ELSE 0 END) AS invalid_compact_event_date_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND {compact_stamp} AND {compact_date_valid} AND NOT {compact_time_valid} THEN 1 ELSE 0 END) + "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND {compact_spaced_stamp} AND {compact_date_valid} AND NOT {compact_spaced_time_valid} THEN 1 ELSE 0 END) AS invalid_compact_event_time_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND {iso_date_prefix} AND NOT (({iso_stamp_shape} AND {iso_valid}) OR ({iso_minute_shape} AND {iso_minute_valid})) THEN 1 ELSE 0 END) AS invalid_iso_timestamp_rows, "
        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {x}<>'' AND NOT ({compact_date} OR {compact_stamp} OR {compact_spaced_stamp} OR {iso_date_prefix}) THEN 1 ELSE 0 END) AS unknown_format_rows "
        f"FROM {qtable}"
    ).fetchone()
    return {key: int(value or 0) for key, value in zip(
        ("all_rows", "null_rows", "blank_rows", "compact_date_rows", "compact_timestamp_rows",
         "valid_iso_timestamp_rows", "valid_iso_minute_timestamp_rows",
         "invalid_compact_event_date_rows", "invalid_compact_event_time_rows",
         "invalid_iso_timestamp_rows", "unknown_format_rows"), counters,
    )}


def _timestamp_profile(conn: sqlite3.Connection, table: str, column: str) -> dict[str, Any]:
    """Summarize event dates by compact/ISO date prefix and separately audit time shape."""
    raw_col = _qi(column)
    value = f"trim({raw_col})"
    date_expr = (
        f"CASE WHEN length({value})=8 AND {value} NOT GLOB '*[^0-9]*' THEN {value} "
        f"WHEN length({value})>=14 AND substr({value},1,8) NOT GLOB '*[^0-9]*' THEN substr({value},1,8) "
        f"WHEN substr({value},5,1)='-' THEN substr({value},1,10) ELSE {value} END"
    )
    grouped = conn.execute(
        f"SELECT {date_expr},COUNT(*) FROM {_qi(table)} "
        f"WHERE {raw_col} IS NOT NULL GROUP BY {date_expr} ORDER BY {date_expr}"
    ).fetchall()
    invalid, parsed = [], []
    rows_with_dates = 0
    for raw, count in grouped:
        rows_with_dates += int(count)
        try:
            parsed_date = _date_value(raw, timestamp=True)
        except ValueError:
            invalid.append({"value": str(raw)[:80], "rows": int(count)})
            continue
        parsed.append((parsed_date, int(count)))
    return {
        "role": "event_timestamp",
        "nonnull_rows": rows_with_dates,
        "distinct_event_date_groups": len(grouped),
        "invalid_nonnull_values": invalid,
        "invalid_event_date_rows": sum(item["rows"] for item in invalid),
        "parsed_min": min((value for value, _ in parsed), default=None).isoformat() if parsed else None,
        "parsed_max": max((value for value, _ in parsed), default=None).isoformat() if parsed else None,
        "calendar_comparison": {"status": "not_applicable", "reason": "not a trade-date field"},
        "timestamp_format_checks": _timestamp_audit(conn, table, column),
    }


def _columns(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    return [
        {"name": row[1], "declared_type": row[2], "not_null": bool(row[3]), "pk_order": row[5]}
        for row in conn.execute(f"PRAGMA table_xinfo({_qi(table)})").fetchall()
        if row[6] == 0
    ]


def _profile(conn: sqlite3.Connection, table: str, open_dates: set[date],
             calendar_range: tuple[date, date] | None) -> dict[str, Any]:
    columns = _columns(conn, table)
    if not columns:
        return {"status": "not_evaluated", "reason": "table missing or has no visible columns"}
    expressions = ["COUNT(*) AS row_count"]
    aliases: dict[str, tuple[int, str]] = {}
    for index, col in enumerate(columns):
        name = col["name"]
        quoted = _qi(name)
        for suffix, predicate in (
            ("null", f"{quoted} IS NULL"),
            ("integer", f"typeof({quoted})='integer'"),
            ("real", f"typeof({quoted})='real'"),
            ("text", f"typeof({quoted})='text'"),
            ("blob", f"typeof({quoted})='blob'"),
        ):
            alias = f"c{index}_{suffix}"
            expressions.append(f"SUM(CASE WHEN {predicate} THEN 1 ELSE 0 END) AS {alias}")
            aliases[alias] = (index, suffix)
        declared = col["declared_type"].upper()
        if any(token in declared for token in ("INT", "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")):
            alias = f"c{index}_nonfinite"
            expressions.append(
                f"SUM(CASE WHEN typeof({quoted}) IN ('integer','real') AND "
                f"({quoted}>1.7976931348623157e308 OR {quoted}<-1.7976931348623157e308) "
                f"THEN 1 ELSE 0 END) AS {alias}"
            )
            aliases[alias] = (index, "nonfinite")
            alias = f"c{index}_declared_numeric_other_storage"
            expressions.append(
                f"SUM(CASE WHEN {quoted} IS NOT NULL AND typeof({quoted}) NOT IN ('integer','real') "
                f"THEN 1 ELSE 0 END) AS {alias}"
            )
            aliases[alias] = (index, "declared_numeric_other_storage")
    row = conn.execute(f"SELECT {', '.join(expressions)} FROM {_qi(table)}").fetchone()
    values = dict(zip(["row_count"] + list(aliases), row))
    column_stats = {}
    for index, col in enumerate(columns):
        stats = {kind: int(values.get(f"c{index}_{kind}") or 0)
                 for kind in ("null", "integer", "real", "text", "blob")}
        stats["declared_numeric_nonfinite"] = int(values.get(f"c{index}_nonfinite") or 0)
        stats["declared_numeric_other_storage"] = int(values.get(f"c{index}_declared_numeric_other_storage") or 0)
        column_stats[col["name"]] = stats

    pk = [col["name"] for col in sorted(columns, key=lambda col: col["pk_order"]) if col["pk_order"]]
    key_report: dict[str, Any] = {"columns": pk, "status": "not_applicable"}
    if pk:
        null_predicate = " OR ".join(f"{_qi(name)} IS NULL" for name in pk)
        duplicate_group = ", ".join(_qi(name) for name in pk)
        key_report = {
            "columns": pk,
            "status": "checked",
            "null_key_rows": int(conn.execute(
                f"SELECT COUNT(*) FROM {_qi(table)} WHERE {null_predicate}"
            ).fetchone()[0]),
            "duplicate_groups": int(conn.execute(
                f"SELECT COUNT(*) FROM (SELECT 1 FROM {_qi(table)} WHERE NOT ({null_predicate}) "
                f"GROUP BY {duplicate_group} HAVING COUNT(*)>1)"
            ).fetchone()[0]),
        }

    date_columns: dict[str, dict[str, Any]] = {}
    roles: dict[str, tuple[str, bool]] = {}
    if "trade_date" in {col["name"] for col in columns} and table in TRADE_DATE_TABLES:
        roles["trade_date"] = ("trade_date", False)
    if "cal_date" in {col["name"] for col in columns} and table == "tbl_cn_tradecal":
        roles["cal_date"] = ("calendar_date", False)
    for name in DATE_ONLY_COLUMNS.get(table, ()):
        if name in {col["name"] for col in columns}:
            roles[name] = ("calendar_or_listing_date", False)
    for name in TIMESTAMP_COLUMNS.get(table, ()):
        if name in {col["name"] for col in columns}:
            roles[name] = ("event_timestamp", True)
    if table == "tbl_cn_tradecal" and "pretrade_date" in {col["name"] for col in columns}:
        roles["pretrade_date"] = ("previous_open_date_reference", False)
    for name, (role, is_timestamp) in roles.items():
        if is_timestamp:
            date_columns[name] = _timestamp_profile(conn, table, name)
            continue
        date_expr = _qi(name)
        grouped = conn.execute(
            f"SELECT {date_expr},COUNT(*) FROM {_qi(table)} "
            f"WHERE {_qi(name)} IS NOT NULL GROUP BY {date_expr} ORDER BY {date_expr}"
        ).fetchall()
        invalid, parsed, nonopen, out_of_range = [], [], [], []
        rows_with_dates = 0
        for raw, count in grouped:
            rows_with_dates += int(count)
            try:
                parsed_date = _date_value(raw, timestamp=is_timestamp)
            except ValueError:
                invalid.append({"value": str(raw)[:80], "rows": int(count)})
                continue
            parsed.append((parsed_date, int(count)))
            if role == "trade_date" or role == "previous_open_date_reference":
                if calendar_range and parsed_date < calendar_range[0] or calendar_range and parsed_date > calendar_range[1]:
                    out_of_range.append({"date": parsed_date.isoformat(), "rows": int(count)})
                elif parsed_date not in open_dates:
                    nonopen.append({"date": parsed_date.isoformat(), "rows": int(count)})
        date_columns[name] = {
            "role": role,
            "nonnull_rows": rows_with_dates,
            "distinct_raw_values": len(grouped),
            "invalid_nonnull_values": invalid,
            "parsed_min": min((value for value, _ in parsed), default=None).isoformat() if parsed else None,
            "parsed_max": max((value for value, _ in parsed), default=None).isoformat() if parsed else None,
            "calendar_comparison": (
                {"status": "checked", "reference": "SSE trade calendar",
                 "not_open_date_rows": sum(x["rows"] for x in nonopen),
                 "not_open_date_samples": nonopen[:25],
                 "outside_reference_range_rows": sum(x["rows"] for x in out_of_range),
                 "outside_reference_range_samples": out_of_range[:25]}
                if role == "trade_date" or role == "previous_open_date_reference"
                else {"status": "not_applicable", "reason": "not a trade-date field"}
            ),
        }

    domain = {}
    if table in PRICE_TABLES:
        price_cols = [c for c in PRICE_TABLES[table] if c in {col["name"] for col in columns}]
        price_invalid = " OR ".join(f"{_qi(c)}<=0" for c in price_cols)
        domain["nonpositive_price_rows"] = int(conn.execute(
            f"SELECT COUNT(*) FROM {_qi(table)} WHERE {price_invalid}"
        ).fetchone()[0]) if price_invalid else 0
        if {"open", "high", "low", "close"} <= {col["name"] for col in columns}:
            domain["invalid_ohlc_rows"] = int(conn.execute(
                f"SELECT COUNT(*) FROM {_qi(table)} WHERE open<=0 OR high<=0 OR low<=0 OR close<=0 "
                "OR low>high OR low>open OR low>close OR high<open OR high<close"
            ).fetchone()[0])
    nonnegative = [c for c in NONNEGATIVE_COLUMNS.get(table, ()) if c in {col["name"] for col in columns}]
    if nonnegative:
        domain["negative_contract_values"] = {
            col: int(conn.execute(f"SELECT COUNT(*) FROM {_qi(table)} WHERE {_qi(col)}<0").fetchone()[0])
            for col in nonnegative
        }
    domain_status = "checked_documented_fields" if domain else "not_evaluated_no_domain_rule"
    return {
        "status": "scanned",
        "timestamp_audit_version": 3,
        "row_count": int(values["row_count"]),
        "columns": columns,
        "column_storage_and_null_counts": column_stats,
        "primary_key_check": key_report,
        "business_date_checks": date_columns,
        "domain_check_status": domain_status,
        "domain_checks": domain,
        "interpretation_limit": "stored-value checks do not establish source truth or completeness; no blanket nonnegative rule is applied to signed fields such as PE/change/net-flow/heat text",
    }


def _load_open_calendar(path: Path) -> tuple[set[date], tuple[date, date] | None, dict[str, Any]]:
    if not path.is_file():
        return set(), None, {"status": "unavailable", "path": str(path)}
    uri = "file:" + str(path.resolve()).replace("?", "%3F") + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.execute("PRAGMA query_only=ON")
        dates = {
            parsed for (raw,) in conn.execute(
                "SELECT cal_date FROM tbl_cn_tradecal WHERE exchange='SSE' AND is_open=1"
            ) if (parsed := _date_value(raw)) is not None
        }
    bounds = (min(dates), max(dates)) if dates else None
    return dates, bounds, {"status": "loaded", "path": str(path), "open_dates": len(dates),
                           "min_date": bounds[0].isoformat() if bounds else None,
                           "max_date": bounds[1].isoformat() if bounds else None}


def _save(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _empty_result(database_root: Path, calendar_meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 3,
        "writer_version": 3,
        "status": "running",
        "mode": "read_only",
        "database_root": str(database_root),
        "calendar": calendar_meta,
        "excluded_tables": {
            "db_cn_basic.db": ["tbl_cn_day", "tbl_cn_daily_basic", "tbl_cn_adj_factor"],
            "policy_minute.db": "audited separately by source/structure tool",
        },
        "tables": {},
        "limitations": [
            "Coverage describes rows stored for each table; no every-trading-day completeness is inferred unless a documented rule exists.",
            "News content truth is not externally verified; coverage is based on stored source timestamps and table rows.",
            "Negative PE, change, net-flow, and TEXT heat/status fields are not rejected by generic sign or numeric rules.",
            "Calendar comparisons are limited to explicitly mapped trade_date fields and the available SSE calendar range.",
            "News timestamp checks distinguish event-date validity from complete timestamp format/time validity; unknown shapes remain unknown.",
        ],
    }


def run(database_root: Path, output: Path, *, table_keys: set[str] | None = None,
        resume: bool = False, invalidated: set[str] | None = None,
        timestamp_only: bool = False,
        base_receipt: Path | None = None) -> dict[str, Any]:
    basic_path = database_root / "offline" / "db_cn_basic.db"
    open_dates, calendar_range, calendar_meta = _load_open_calendar(basic_path)
    prior_receipt = None
    prior_receipt_sha256 = None
    prior_receipt_path = None
    if resume and output.is_file():
        prior_receipt_path = output
        prior_receipt_sha256 = _sha256_file(output)
        prior_receipt = json.loads(output.read_text(encoding="utf-8"))
        result = json.loads(output.read_text(encoding="utf-8"))
        if Path(result.get("database_root", "")).resolve() != database_root.resolve():
            raise ValueError("resume receipt database_root mismatch")
        result["schema_version"] = 3
        result["writer_version"] = 3
        result.setdefault("tables", {})
        result["status"] = "running"
        result["calendar"] = calendar_meta
    else:
        result = _empty_result(database_root, calendar_meta)
    if timestamp_only:
        selected_keys = sorted(table_keys or {
            f"{filename}:{table}" for filename, tables in TABLES.items() for table in tables
        })
        baseline_path = base_receipt or prior_receipt_path
        baseline = None
        baseline_sha256 = None
        if baseline_path is not None:
            baseline = json.loads(baseline_path.read_text(encoding="utf-8")) if baseline_path != output else prior_receipt
            if baseline is None or Path(baseline.get("database_root", "")).resolve() != database_root.resolve():
                raise ValueError("timestamp-only baseline receipt database_root mismatch")
            baseline_sha256 = _sha256_file(baseline_path)
        if base_receipt is not None and prior_receipt_sha256 is None:
            prior_receipt_sha256 = baseline_sha256
            prior_receipt_path = base_receipt
        expected_keys = {f"{filename}:{table}" for filename, tables in TABLES.items() for table in tables}
        baseline_tables = baseline.get("tables", {}) if baseline else {}
        baseline_complete = bool(
            baseline
            and baseline.get("status") == "complete"
            and set(baseline_tables) == expected_keys
            and all(item.get("status") == "scanned" for item in baseline_tables.values())
        )
        result["audit_provenance"] = {
            "mode": "timestamp_only_resume",
            "writer_version": 3,
            "rescan_status": "running",
            "selected_tables": selected_keys,
            "selected_timestamp_columns": {
                key: list(TIMESTAMP_COLUMNS.get(key.split(":", 1)[1], ())) for key in selected_keys
            },
            "baseline_receipt": ({
                "path": str(baseline_path),
                "sha256": baseline_sha256,
                "schema_version": baseline.get("schema_version"),
                "status": baseline.get("status"),
                "table_count": len(baseline_tables),
            } if baseline else None),
            "carried_forward_profile_count": len(set(baseline_tables) - set(selected_keys)),
            "composite_coverage_status": "complete" if baseline_complete else "partial_or_unverified",
            "coverage_note": (
                "Complete base profiles are carried forward; only selected timestamp checks are recomputed."
                if baseline_complete else
                "This timestamp-only run does not establish complete coverage unless its baseline is independently complete."
            ),
        }
    else:
        result["schema_version"] = 3
        result["writer_version"] = 3
    _save(output, result)
    missing = []
    invalidated = invalidated or set()
    for filename, tables in TABLES.items():
        path = database_root / "offline" / filename
        if not path.is_file():
            for table in tables:
                key = f"{filename}:{table}"
                if table_keys is not None and key not in table_keys:
                    continue
                result["tables"][key] = {"status": "not_audited_missing_database"}
            missing.append(str(path))
            continue
        uri = "file:" + str(path.resolve()).replace("?", "%3F") + "?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=120) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            conn.execute("PRAGMA busy_timeout=120000")
            existing = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )}
            for table in tables:
                key = f"{filename}:{table}"
                if table_keys is not None and key not in table_keys:
                    continue
                old_item = result["tables"].get(key)
                if (resume and not timestamp_only and old_item and old_item.get("status") == "scanned"
                        and key not in invalidated):
                    continue
                if table not in existing:
                    result["tables"][key] = {"status": "not_audited_missing_table"}
                elif timestamp_only:
                    item = dict(old_item or {"status": "timestamp_checks_only"})
                    date_checks = dict(item.get("business_date_checks", {}))
                    for column in TIMESTAMP_COLUMNS.get(table, ()):
                        date_checks[column] = _timestamp_profile(conn, table, column)
                    item["business_date_checks"] = date_checks
                    item["timestamp_audit_version"] = 3
                    result["tables"][key] = item
                else:
                    result["tables"][key] = _profile(conn, table, open_dates, calendar_range)
                _save(output, result)
    is_partial_run = table_keys is not None or timestamp_only
    result["status"] = (
        "partial_with_missing_inputs" if is_partial_run and missing else
        "partial" if is_partial_run else
        "complete_with_missing_inputs" if missing else "complete"
    )
    result["missing_databases"] = missing
    if timestamp_only:
        result["audit_provenance"]["rescan_status"] = "complete"
    _save(output, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-root", type=Path, default=Path("/Users/nickzhang/TradingDatabase"))
    parser.add_argument("--output", type=Path, required=True, help="external JSON progress/receipt path")
    parser.add_argument("--resume", action="store_true", help="reuse already-scanned table profiles from the output receipt")
    parser.add_argument("--tables", help="comma-separated whitelist, e.g. db_cn_news.db:tbl_news")
    parser.add_argument("--invalidate", help="comma-separated table keys to rescan despite --resume")
    parser.add_argument("--timestamp-only", action="store_true", help="recompute only mapped full timestamp/date checks")
    parser.add_argument("--base-receipt", type=Path, help="prior full audit receipt used as the timestamp-only baseline")
    args = parser.parse_args()
    table_keys = set(args.tables.split(",")) if args.tables else None
    invalidated = set(args.invalidate.split(",")) if args.invalidate else set()
    known_keys = {f"{filename}:{table}" for filename, tables in TABLES.items() for table in tables}
    for key in (table_keys or set()) | invalidated:
        if key not in known_keys:
            parser.error(f"unknown table key: {key}")
    result = run(args.database_root, args.output, table_keys=table_keys, resume=args.resume,
                 invalidated=invalidated, timestamp_only=args.timestamp_only,
                 base_receipt=args.base_receipt)
    summary = {"status": result["status"], "table_count": len(result["tables"]),
               "scanned": sum(item.get("status") == "scanned" for item in result["tables"].values()),
               "output": str(args.output)}
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if result["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())

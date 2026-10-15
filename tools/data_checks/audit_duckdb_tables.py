#!/usr/bin/env python3
"""Read-only structural and semantic audit for the seven persistent DuckDBs.

The audit inventories every base table and scans every row for NULL and non-finite
numeric values. Domain checks are deliberately limited to documented contracts;
unknown or unavailable business rules are reported as not evaluated.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

import duckdb


DATABASES = {
    "legacy_fifteen_min": "offline/db_fifteenMinute.db",
    "major_news_events": "offline/db_major_news_events.duckdb",
    "news_events": "offline/db_news_events.duckdb",
    "theme_graph": "offline/db_theme_graph.duckdb",
    "fifteen_min": "offline/fifteenMinute/fifteen_min.duckdb",
    "one_min_catalog": "offline/oneMinute/catalog.duckdb",
    "theme_graph_2025_snapshot": (
        "offline/research_snapshots/fifteen_min_core_reactivation_2026/"
        "db_theme_graph_2025.duckdb"
    ),
}

KEYS: dict[str, dict[str, tuple[str, ...]]] = {
    "legacy_fifteen_min": {
        "fifteen_min_bars": ("ts_code", "trade_date", "time_idx"),
        "fifteen_min_days": ("ts_code", "trade_date"),
        "fifteen_min_meta": ("key",),
        "fifteen_min_sync": ("ts_code",),
    },
    "major_news_events": {
        "dim_major_news_entity": ("entity_id",),
        "dim_major_news_theme_tag": ("tag_id",),
        "fact_major_news_analysis": ("semantic_event_id",),
        "fact_major_news_daily_analysis": ("event_date",),
        "fact_major_news_semantic_event": ("semantic_event_id",),
        "fact_news_event": ("event_id",),
        "fact_news_insight": ("event_id",),
        "news_llm_run": ("run_id",),
        "pipeline_state": ("key",),
        "rel_major_news_entity": ("semantic_event_id", "entity_id"),
        "rel_major_news_semantic_member": ("semantic_event_id", "event_id"),
        "rel_major_news_theme": ("semantic_event_id", "tag_id"),
        "rel_news_event_member": ("item_id",),
    },
    "news_events": {
        "fact_news_event": ("event_id",),
        "fact_news_insight": ("event_id",),
        "news_llm_run": ("run_id",),
        "pipeline_state": ("key",),
        "rel_news_event_member": ("item_id",),
    },
    "theme_graph": {
        "dim_stock": ("ts_code",),
        "dim_theme": ("theme_id",),
        "dim_theme_taxonomy": ("theme_id",),
        "fact_limit_event": ("event_id",),
        "fact_membership_snapshot": ("trade_date", "theme_id", "ts_code"),
        "fact_theme_daily": ("trade_date", "theme_id"),
        "fact_theme_episode": ("episode_id",),
        "graph_build_run": ("run_id",),
        "graph_meta": ("key",),
        "llm_analysis": ("analysis_id",),
        "source_checkpoint": ("source_name",),
        "source_conflict": ("conflict_id",),
        "source_day_manifest": ("source_name", "trade_date"),
        "theme_alias": ("theme_id", "alias", "source", "valid_from"),
        "rel_limit_theme": ("event_id", "theme_id", "attribution_role"),
        "rel_theme_stock_membership": ("theme_id", "ts_code", "valid_from"),
    },
    "fifteen_min": {
        "fifteen_min_bars": ("ts_code", "trade_date", "time_idx"),
        "fifteen_min_dataset_meta": ("key",),
        "fifteen_min_ingest_catalog": ("trade_date",),
        "fifteen_min_source_files": ("source_path",),
    },
    "one_min_catalog": {
        "one_min_ingest_catalog": ("trade_date",),
        "one_min_source_files": ("source_path",),
        "one_min_tdx_sync": ("ts_code",),
    },
    "theme_graph_2025_snapshot": {
        "dim_stock": ("ts_code",),
        "dim_theme": ("theme_id",),
        "dim_theme_taxonomy": ("theme_id",),
        "fact_limit_event": ("event_id",),
        "fact_membership_snapshot": ("trade_date", "theme_id", "ts_code"),
        "fact_theme_daily": ("trade_date", "theme_id"),
        "fact_theme_episode": ("episode_id",),
        "graph_build_run": ("run_id",),
        "graph_meta": ("key",),
        "llm_analysis": ("analysis_id",),
        "source_checkpoint": ("source_name",),
        "source_conflict": ("conflict_id",),
        "source_day_manifest": ("source_name", "trade_date"),
        "theme_alias": ("theme_id", "alias", "source", "valid_from"),
        "rel_limit_theme": ("event_id", "theme_id", "attribution_role"),
        "rel_theme_stock_membership": ("theme_id", "ts_code", "valid_from"),
    },
}

FIFTEEN_MIN_TIMES = (
    "09:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:15", "11:30",
    "13:15", "13:30", "13:45", "14:00", "14:15", "14:30", "14:45", "15:00",
)


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _count(con: duckdb.DuckDBPyConnection, sql: str, args: list[Any] | None = None) -> int:
    return int(con.execute(sql, args or []).fetchone()[0] or 0)


def _load_calendar(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"path": str(path), "status": "unknown", "open_dates": []}
    if not path.is_file():
        result["reason"] = "reference calendar database is missing"
        return result
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as con:
            schema = con.execute("PRAGMA table_info(tbl_cn_tradecal)").fetchall()
            names = {row[1] for row in schema}
            required = {"cal_date", "exchange", "is_open"}
            if not required <= names:
                result["reason"] = "tbl_cn_tradecal schema does not match expected reference"
                return result
            rows = con.execute(
                "SELECT cal_date FROM tbl_cn_tradecal "
                "WHERE exchange='SSE' AND is_open=1 ORDER BY cal_date"
            ).fetchall()
        result.update({
            "status": "available",
            "open_dates": [str(row[0]) for row in rows],
            "open_date_count": len(rows),
            "min_date": str(rows[0][0]) if rows else None,
            "max_date": str(rows[-1][0]) if rows else None,
            "reference_table": "tbl_cn_tradecal(exchange='SSE',is_open=1)",
        })
    except Exception as exc:  # reports only error type, never database contents
        result["reason"] = f"reference calendar read failed: {type(exc).__name__}"
    return result


def _columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[dict[str, str]]:
    return [
        {"name": row[0], "type": row[1], "nullable": row[2]}
        for row in con.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema=? AND table_name=? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
    ]


def _date_parse_expr(name: str, dtype: str) -> str | None:
    low = name.lower()
    if low.endswith("_json") or low in {"sources_json", "output_json"}:
        return None
    if not (
        low.endswith("date") or "_date" in low or low in {
            "valid_at", "computed_as_of", "covered_start", "covered_end",
            "actual_start", "actual_end", "max_trade_date", "last_complete_date",
        }
    ):
        return None
    col = _q(name)
    dtype_upper = dtype.upper()
    if "DATE" in dtype_upper or "TIMESTAMP" in dtype_upper:
        return f"TRY_CAST({col} AS DATE)"
    token = f"REGEXP_EXTRACT(TRIM(CAST({col} AS VARCHAR)), '^([0-9]{{4}}[-/]?[0-9]{{2}}[-/]?[0-9]{{2}})', 1)"
    normalized = f"REPLACE(REPLACE({token}, '-', ''), '/', '')"
    return f"TRY_STRPTIME({normalized}, '%Y%m%d')::DATE"


def _audit_table(
    con: duckdb.DuckDBPyConnection,
    database_key: str,
    schema: str,
    table: str,
    open_dates: set[str] | None,
    calendar_range: tuple[str, str] | None,
) -> dict[str, Any]:
    cols = _columns(con, schema, table)
    colnames = {item["name"] for item in cols}
    row_count = _count(con, f"SELECT COUNT(*) FROM {_q(schema)}.{_q(table)}")
    aggregate: list[str] = ["COUNT(*) AS rows"]
    null_aliases: dict[str, str] = {}
    nonfinite_aliases: dict[str, str] = {}
    for col in cols:
        name, dtype = col["name"], col["type"].upper()
        null_alias = f"null_{len(null_aliases)}"
        null_aliases[name] = null_alias
        aggregate.append(f"COUNT(*) FILTER (WHERE {_q(name)} IS NULL) AS {_q(null_alias)}")
        if any(marker in dtype for marker in ("DOUBLE", "FLOAT", "REAL")):
            nonfinite_alias = f"nonfinite_{len(nonfinite_aliases)}"
            nonfinite_aliases[name] = nonfinite_alias
            aggregate.append(f"COUNT(*) FILTER (WHERE {_q(name)} IS NOT NULL "
                             f"AND NOT ISFINITE({_q(name)})) AS {_q(nonfinite_alias)}")
    cursor = con.execute(
        f"SELECT {', '.join(aggregate)} FROM {_q(schema)}.{_q(table)}"
    )
    metric_row = dict(zip((description[0] for description in cursor.description), cursor.fetchone()))
    null_counts = {name: int(metric_row[alias] or 0) for name, alias in null_aliases.items()}
    nonfinite = {name: int(metric_row[alias] or 0)
                 for name, alias in nonfinite_aliases.items() if metric_row[alias]}

    key_cols = KEYS.get(database_key, {}).get(table)
    key_result: dict[str, Any] = {"columns": list(key_cols) if key_cols else None,
                                 "null_rows": None, "duplicate_rows": None}
    if key_cols and set(key_cols) <= colnames:
        null_predicate = " OR ".join(f"{_q(c)} IS NULL" for c in key_cols)
        key_result["null_rows"] = _count(
            con, f"SELECT COUNT(*) FROM {_q(schema)}.{_q(table)} WHERE {null_predicate}"
        )
        grouped = ", ".join(_q(c) for c in key_cols)
        key_result["duplicate_rows"] = _count(
            con,
            f"SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n "
            f"FROM {_q(schema)}.{_q(table)} GROUP BY {grouped} HAVING COUNT(*)>1)",
        )
    elif key_cols:
        key_result["status"] = "not_evaluated_missing_key_columns"

    date_checks: dict[str, Any] = {}
    for col in cols:
        expr = _date_parse_expr(col["name"], col["type"])
        if expr is None:
            continue
        name = col["name"]
        invalid = _count(
            con, f"SELECT COUNT(*) FROM {_q(schema)}.{_q(table)} "
                 f"WHERE {_q(name)} IS NOT NULL AND ({expr}) IS NULL"
        )
        minimum, maximum = con.execute(
            f"SELECT MIN({expr}), MAX({expr}) FROM {_q(schema)}.{_q(table)} "
            f"WHERE {_q(name)} IS NOT NULL"
        ).fetchone()
        date_checks[name] = {
            "invalid_nonnull": invalid,
            "min": str(minimum) if minimum is not None else None,
            "max": str(maximum) if maximum is not None else None,
        }
        if open_dates is not None and name in {
            "trade_date", "max_trade_date", "last_complete_date", "actual_start", "actual_end",
        }:
            unique_values = con.execute(
                f"SELECT DISTINCT STRFTIME({expr}, '%Y%m%d') FROM {_q(schema)}.{_q(table)} "
                f"WHERE {_q(name)} IS NOT NULL AND ({expr}) IS NOT NULL"
            ).fetchall()
            date_values = {str(row[0]) for row in unique_values if row[0] is not None}
            outside = sorted(value for value in date_values if calendar_range and
                             not (calendar_range[0] <= value <= calendar_range[1]))
            unexpected_closed = sorted(value for value in date_values
                                       if value not in open_dates and
                                       (not calendar_range or calendar_range[0] <= value <= calendar_range[1]))
            date_checks[name]["calendar"] = {
                "status": "checked",
                "reference": "SSE trade calendar",
                "distinct_dates": len(date_values),
                "outside_reference_range": outside,
                "not_open_dates": unexpected_closed,
            }
        elif name in {"trade_date", "max_trade_date", "last_complete_date"}:
            date_checks[name]["calendar"] = {"status": "unknown", "reason": "reference calendar unavailable"}

    json_invalid: dict[str, int] = {}
    for col in cols:
        if col["name"].lower().endswith("_json") or col["name"].lower() == "output_json":
            json_invalid[col["name"]] = _count(
                con, f"SELECT COUNT(*) FROM {_q(schema)}.{_q(table)} "
                     f"WHERE {_q(col['name'])} IS NOT NULL "
                     f"AND TRY_CAST({_q(col['name'])} AS JSON) IS NULL"
            )

    return {
        "table": f"{schema}.{table}",
        "columns": cols,
        "rows": row_count,
        "null_counts": null_counts,
        "nonfinite_numeric": nonfinite,
        "candidate_key": key_result,
        "date_checks": date_checks,
        "invalid_json_counts": json_invalid,
        "domain_checks": {},
    }


def _fk_check(con, child: str, child_cols: tuple[str, ...], parent: str,
              parent_cols: tuple[str, ...], schema: str = "main") -> dict[str, Any]:
    child_exists = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema=? AND table_name=?",
        [schema, child],
    ).fetchone()
    parent_exists = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema=? AND table_name=?",
        [schema, parent],
    ).fetchone()
    if not child_exists or not parent_exists:
        return {"not_evaluated": 1}
    ccols = ", ".join(f"c.{_q(c)}" for c in child_cols)
    pcols = ", ".join(f"p.{_q(c)}" for c in parent_cols)
    nonnull = " AND ".join(f"c.{_q(c)} IS NOT NULL" for c in child_cols)
    joins = " AND ".join(f"c.{_q(a)}=p.{_q(b)}" for a, b in zip(child_cols, parent_cols))
    result = {
        "null_key_rows": _count(con, f"SELECT COUNT(*) FROM {_q(schema)}.{_q(child)} c WHERE NOT ({nonnull})"),
        "orphan_rows": _count(
            con, f"SELECT COUNT(*) FROM {_q(schema)}.{_q(child)} c WHERE {nonnull} "
                 f"AND NOT EXISTS (SELECT 1 FROM {_q(schema)}.{_q(parent)} p WHERE {joins})"
        ),
        "orphan_distinct_keys": _count(
            con, f"SELECT COUNT(*) FROM (SELECT DISTINCT {ccols} "
                 f"FROM {_q(schema)}.{_q(child)} c WHERE {nonnull} "
                 f"AND NOT EXISTS (SELECT 1 FROM {_q(schema)}.{_q(parent)} p WHERE {joins}))"
        ),
    }
    if result["orphan_rows"]:
        samples = con.execute(
            f"SELECT DISTINCT {ccols} FROM {_q(schema)}.{_q(child)} c WHERE {nonnull} "
            f"AND NOT EXISTS (SELECT 1 FROM {_q(schema)}.{_q(parent)} p WHERE {joins}) "
            f"ORDER BY {ccols} LIMIT 20"
        ).fetchall()
        result["orphan_key_samples"] = [
            {key: (str(value) if value is not None else None)
             for key, value in zip(child_cols, row)}
            for row in samples
        ]
    return result


def _classify_limit_event_stock_orphans(con, schema: str = "main") -> dict[str, Any]:
    """Separate blown-only event codes from missing limit-up stock identities.

    `dim_stock` is not required for a stock whose only fact events are `炸板`.
    This is an audit classification only; it does not insert or infer dimension
    rows from current stock metadata.
    """
    tables = {
        row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=?", [schema]
        ).fetchall()
    }
    if not {"fact_limit_event", "dim_stock"} <= tables:
        return {"status": "not_evaluated", "reason": "fact_limit_event or dim_stock missing"}
    rows = con.execute(f"""
        WITH orphan_codes AS (
          SELECT e.ts_code,
                 COUNT(*) AS event_rows,
                 COUNT(*) FILTER (WHERE e.tag='炸板') AS blown_rows,
                 COUNT(*) FILTER (WHERE e.tag='涨停') AS limitup_rows,
                 COUNT(*) FILTER (WHERE e.tag IS NULL OR e.tag NOT IN ('炸板','涨停')) AS other_rows
          FROM {_q(schema)}.fact_limit_event e
          WHERE e.ts_code IS NOT NULL
            AND NOT EXISTS (
              SELECT 1 FROM {_q(schema)}.dim_stock d WHERE d.ts_code=e.ts_code
            )
          GROUP BY e.ts_code
        )
        SELECT
          COUNT(*) AS orphan_codes,
          COUNT(*) FILTER (WHERE blown_rows>0 AND limitup_rows=0 AND other_rows=0) AS blown_only_codes,
          COALESCE(SUM(event_rows) FILTER (WHERE blown_rows>0 AND limitup_rows=0 AND other_rows=0),0) AS blown_only_rows,
          COUNT(*) FILTER (WHERE limitup_rows>0) AS missing_limitup_dim_codes,
          COALESCE(SUM(limitup_rows),0) AS missing_limitup_event_rows,
          COUNT(*) FILTER (WHERE blown_rows=0 AND limitup_rows=0 OR other_rows>0 AND limitup_rows=0) AS other_or_unknown_codes,
          COALESCE(SUM(event_rows) FILTER (WHERE limitup_rows=0 AND (blown_rows=0 OR other_rows>0)),0) AS other_or_unknown_rows
        FROM orphan_codes
    """).fetchone()
    samples = {}
    for category, condition in (
        ("expected_blown_only", "blown_rows>0 AND limitup_rows=0 AND other_rows=0"),
        ("true_missing_limitup_dim", "limitup_rows>0"),
        ("other_or_unknown_tag", "limitup_rows=0 AND (blown_rows=0 OR other_rows>0)"),
    ):
        samples[category] = [
            {"ts_code": str(row[0]), "event_rows": int(row[1]), "blown_rows": int(row[2]),
             "limitup_rows": int(row[3]), "other_rows": int(row[4])}
            for row in con.execute(f"""
                SELECT e.ts_code, COUNT(*) event_rows,
                       COUNT(*) FILTER (WHERE e.tag='炸板') blown_rows,
                       COUNT(*) FILTER (WHERE e.tag='涨停') limitup_rows,
                       COUNT(*) FILTER (WHERE e.tag IS NULL OR e.tag NOT IN ('炸板','涨停')) other_rows
                FROM {_q(schema)}.fact_limit_event e
                WHERE e.ts_code IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM {_q(schema)}.dim_stock d WHERE d.ts_code=e.ts_code)
                GROUP BY e.ts_code HAVING {condition}
                ORDER BY e.ts_code LIMIT 20
            """).fetchall()
        ]
    return {
        "status": "checked",
        "orphan_codes": int(rows[0] or 0),
        "expected_blown_only_codes": int(rows[1] or 0),
        "expected_blown_only_event_rows": int(rows[2] or 0),
        "true_missing_limitup_dim_codes": int(rows[3] or 0),
        "true_missing_limitup_event_rows": int(rows[4] or 0),
        "other_or_unknown_tag_codes": int(rows[5] or 0),
        "other_or_unknown_tag_event_rows": int(rows[6] or 0),
        "samples": samples,
        "interpretation": "blown-only codes are expected not to require dim_stock; limit-up rows without dim_stock remain true missing identities; other tags are unresolved",
    }


def _audit_fifteen_min(con: duckdb.DuckDBPyConnection, table_reports: list[dict[str, Any]]) -> dict[str, Any]:
    tables = {entry["table"].split(".", 1)[1]: entry for entry in table_reports}
    if "fifteen_min_bars" not in tables:
        return {"status": "not_evaluated", "reason": "bar table missing"}
    table = "fifteen_min_bars"
    cols = {column["name"] for column in tables[table]["columns"]}
    needed = {"ts_code", "trade_date", "datetime", "time_idx", "open", "high", "low", "close", "vol", "amount"}
    if not needed <= cols:
        return {"status": "not_evaluated", "reason": "required fifteen-minute bar columns missing"}
    con.execute("CREATE TEMP TABLE _audit_expected_15m(time_idx INTEGER, clock VARCHAR)")
    con.executemany("INSERT INTO _audit_expected_15m VALUES (?,?)", list(enumerate(FIFTEEN_MIN_TIMES)))
    sqltable = f"main.{_q(table)}"
    result = {
        "status": "checked",
        "timestamp_trade_date_mismatches": _count(
            con, f"SELECT COUNT(*) FROM {sqltable} WHERE CAST(datetime AS DATE) "
                 "<> TRY_STRPTIME(CAST(trade_date AS VARCHAR), '%Y%m%d')::DATE"
        ),
        "time_idx_clock_mismatches": _count(
            con, f"SELECT COUNT(*) FROM {sqltable} b LEFT JOIN _audit_expected_15m e USING(time_idx) "
                 "WHERE e.time_idx IS NULL OR STRFTIME(b.datetime,'%H:%M')<>e.clock"
        ),
        "invalid_ohlc_rows": _count(
            con, f"SELECT COUNT(*) FROM {sqltable} WHERE open IS NULL OR high IS NULL OR low IS NULL "
                 "OR close IS NULL OR NOT (high>=greatest(open,close,low) "
                 "AND low<=least(open,close,high))"
        ),
        "negative_volume_or_amount_rows": _count(
            con, f"SELECT COUNT(*) FROM {sqltable} WHERE COALESCE(vol,0)<0 OR COALESCE(amount,0)<0"
        ),
        "nonpositive_price_rows": _count(
            con, f"SELECT COUNT(*) FROM {sqltable} WHERE open<=0 OR high<=0 OR low<=0 OR close<=0"
        ),
        "incomplete_code_day_groups": _count(
            con, f"SELECT COUNT(*) FROM (SELECT ts_code,trade_date,COUNT(*) n,COUNT(DISTINCT time_idx) ni,"
                 f"MIN(time_idx) lo,MAX(time_idx) hi FROM {sqltable} GROUP BY ts_code,trade_date "
                 "HAVING n<>16 OR ni<>16 OR lo<>0 OR hi<>15)"
        ),
    }
    result["incomplete_group_samples"] = [
        {"ts_code": row[0], "trade_date": int(row[1]), "rows": int(row[2]),
         "distinct_time_idx": int(row[3]), "min_time_idx": int(row[4]),
         "max_time_idx": int(row[5])}
        for row in con.execute(
            f"SELECT ts_code,trade_date,COUNT(*),COUNT(DISTINCT time_idx),MIN(time_idx),MAX(time_idx) "
            f"FROM {sqltable} GROUP BY ts_code,trade_date "
            f"HAVING COUNT(*)<>16 OR COUNT(DISTINCT time_idx)<>16 OR MIN(time_idx)<>0 OR MAX(time_idx)<>15 "
            f"ORDER BY trade_date,ts_code LIMIT 20"
        ).fetchall()
    ]
    return result


def _audit_catalog_links(con: duckdb.DuckDBPyConnection, database_key: str) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    if database_key in {"legacy_fifteen_min", "fifteen_min"}:
        day_table = "fifteen_min_days" if database_key == "legacy_fifteen_min" else "fifteen_min_ingest_catalog"
        exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='main' AND table_name=?", [day_table]).fetchone()[0]
        bars_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='fifteen_min_bars'").fetchone()[0]
        if exists and bars_exists:
            if database_key == "legacy_fifteen_min":
                checks["daily_catalog_bar_count_mismatches"] = _count(
                    con, f"SELECT COUNT(*) FROM {day_table} d LEFT JOIN "
                         "(SELECT ts_code,trade_date,COUNT(*) n FROM fifteen_min_bars GROUP BY 1,2) b "
                         "USING(ts_code,trade_date) WHERE d.row_count<>COALESCE(b.n,0)"
                )
            else:
                checks["daily_catalog_bar_count_mismatches"] = _count(
                    con, f"SELECT COUNT(*) FROM {day_table} d LEFT JOIN "
                         "(SELECT trade_date,SUM(per_code) n,COUNT(DISTINCT ts_code) stocks,"
                         "MIN(per_code) minbars,MAX(per_code) maxbars FROM "
                         "(SELECT trade_date,ts_code,COUNT(*) per_code FROM fifteen_min_bars GROUP BY 1,2) "
                         "GROUP BY trade_date) b USING(trade_date) "
                         "WHERE d.rows IS NOT NULL AND d.rows<>COALESCE(b.n,0) "
                         "OR d.stock_count IS NOT NULL AND d.stock_count<>COALESCE(b.stocks,0) "
                         "OR d.bars_per_stock_min IS NOT NULL AND d.bars_per_stock_min<>COALESCE(b.minbars,0) "
                         "OR d.bars_per_stock_max IS NOT NULL AND d.bars_per_stock_max<>COALESCE(b.maxbars,0)"
                )
    return checks


def _relationships(con: duckdb.DuckDBPyConnection, database_key: str) -> dict[str, Any]:
    rules: dict[str, list[tuple[str, tuple[str, ...], str, tuple[str, ...]]]] = {
        "news_events": [
            ("fact_news_insight", ("event_id",), "fact_news_event", ("event_id",)),
            ("rel_news_event_member", ("event_id",), "fact_news_event", ("event_id",)),
        ],
        "major_news_events": [
            ("fact_major_news_analysis", ("semantic_event_id",), "fact_major_news_semantic_event", ("semantic_event_id",)),
            ("rel_major_news_semantic_member", ("semantic_event_id",), "fact_major_news_semantic_event", ("semantic_event_id",)),
            ("rel_major_news_semantic_member", ("event_id",), "fact_news_event", ("event_id",)),
            ("rel_major_news_theme", ("semantic_event_id",), "fact_major_news_semantic_event", ("semantic_event_id",)),
            ("rel_major_news_theme", ("tag_id",), "dim_major_news_theme_tag", ("tag_id",)),
            ("rel_major_news_entity", ("semantic_event_id",), "fact_major_news_semantic_event", ("semantic_event_id",)),
            ("rel_major_news_entity", ("entity_id",), "dim_major_news_entity", ("entity_id",)),
            ("rel_news_event_member", ("event_id",), "fact_news_event", ("event_id",)),
            ("fact_news_insight", ("event_id",), "fact_news_event", ("event_id",)),
        ],
        "theme_graph": [
            ("dim_theme", ("parent_theme_id",), "dim_theme", ("theme_id",)),
            ("dim_theme_taxonomy", ("theme_id",), "dim_theme", ("theme_id",)),
            ("fact_membership_snapshot", ("theme_id",), "dim_theme", ("theme_id",)),
            ("fact_membership_snapshot", ("ts_code",), "dim_stock", ("ts_code",)),
            ("fact_theme_daily", ("theme_id",), "dim_theme", ("theme_id",)),
            ("fact_theme_daily", ("episode_id",), "fact_theme_episode", ("episode_id",)),
            ("fact_theme_episode", ("theme_id",), "dim_theme", ("theme_id",)),
            ("fact_limit_event", ("ts_code",), "dim_stock", ("ts_code",)),
            ("rel_limit_theme", ("event_id",), "fact_limit_event", ("event_id",)),
            ("rel_limit_theme", ("theme_id",), "dim_theme", ("theme_id",)),
            ("theme_alias", ("theme_id",), "dim_theme", ("theme_id",)),
            ("rel_theme_stock_membership", ("theme_id",), "dim_theme", ("theme_id",)),
            ("rel_theme_stock_membership", ("ts_code",), "dim_stock", ("ts_code",)),
        ],
        "theme_graph_2025_snapshot": [],
    }
    if database_key == "theme_graph_2025_snapshot":
        rules[database_key] = rules["theme_graph"]
    output: dict[str, Any] = {}
    for child, child_cols, parent, parent_cols in rules.get(database_key, []):
        name = f"{child}({','.join(child_cols)})->{parent}({','.join(parent_cols)})"
        output[name] = _fk_check(con, child, child_cols, parent, parent_cols)
        if (database_key in {"theme_graph", "theme_graph_2025_snapshot"}
                and child == "fact_limit_event" and child_cols == ("ts_code",)
                and parent == "dim_stock"):
            output[name]["orphan_classification"] = _classify_limit_event_stock_orphans(con)
    return output


def _derived_checks(con: duckdb.DuckDBPyConnection, database_key: str) -> dict[str, Any]:
    if database_key not in {"theme_graph", "theme_graph_2025_snapshot"}:
        return {"status": "not_applicable"}
    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    if not {"fact_limit_event", "rel_limit_theme", "fact_theme_daily"} <= tables:
        return {"status": "not_evaluated", "reason": "required source/derived tables missing"}
    mismatches = con.execute("""
        WITH source AS (
          SELECT e.trade_date, r.theme_id,
                 COUNT(*) FILTER (WHERE e.tag='涨停' AND r.attribution_role='primary') AS limits,
                 COUNT(*) FILTER (WHERE e.tag='炸板' AND r.attribution_role='auxiliary') AS breaks
          FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
          WHERE (e.tag='涨停' AND r.attribution_role='primary')
             OR (e.tag='炸板' AND r.attribution_role='auxiliary')
          GROUP BY e.trade_date,r.theme_id
        ), compared AS (
          SELECT COALESCE(s.trade_date,d.trade_date) trade_date,
                 COALESCE(s.theme_id,d.theme_id) theme_id,
                 COALESCE(s.limits,0) limits,d.limit_up_count,
                 COALESCE(s.breaks,0) breaks,d.break_count
          FROM source s FULL OUTER JOIN fact_theme_daily d
            ON s.trade_date=d.trade_date AND s.theme_id=d.theme_id
        )
        SELECT COUNT(*) FILTER (WHERE limits IS DISTINCT FROM limit_up_count) AS limit_count_mismatches,
               COUNT(*) FILTER (WHERE breaks IS DISTINCT FROM break_count) AS break_count_mismatches,
               COUNT(*) AS compared_groups
        FROM compared
    """).fetchone()
    return {
        "status": "checked",
        "daily_groups_compared": int(mismatches[2] or 0),
        "limit_up_count_mismatches": int(mismatches[0] or 0),
        "break_count_mismatches": int(mismatches[1] or 0),
        "other_derived_metrics": "not_evaluated_without replaying documented scoring state machine",
    }


def _db_header(path: Path) -> dict[str, str]:
    with path.open("rb") as handle:
        raw = handle.read(16)
    return {"first_16_bytes_hex": raw.hex(), "duckdb_signature_present": raw[8:12] == b"DUCK"}


def audit_database(name: str, path: Path, calendar: dict[str, Any]) -> dict[str, Any]:
    header = _db_header(path)
    if not header["duckdb_signature_present"]:
        return {"path": str(path), "status": "not_audited_wrong_header", "header": header}
    open_dates = set(calendar.get("open_dates", [])) if calendar.get("status") == "available" else None
    bounds = (calendar.get("min_date"), calendar.get("max_date")) if open_dates is not None else None
    con = duckdb.connect(str(path), read_only=True)
    try:
        objects = con.execute(
            "SELECT table_schema,table_name,table_type FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY 1,2"
        ).fetchall()
        tables = []
        for schema, table, table_type in objects:
            if table_type != "BASE TABLE":
                continue
            tables.append(_audit_table(con, name, schema, table, open_dates, bounds))
        return {
            "path": str(path), "size_bytes": path.stat().st_size,
            "header": header, "status": "scanned",
            "base_table_count": len(tables), "tables": tables,
            "relationships": _relationships(con, name),
            "fifteen_min_domain": _audit_fifteen_min(con, tables)
                if name in {"legacy_fifteen_min", "fifteen_min"} else {"status": "not_applicable"},
            "catalog_links": _audit_catalog_links(con, name),
            "derived_relationships": _derived_checks(con, name),
        }
    finally:
        con.close()


def _summarize(audit: dict[str, Any]) -> dict[str, int]:
    totals = {"databases": 0, "tables": 0, "rows": 0, "tables_with_nulls": 0,
              "null_values": 0, "nonfinite_values": 0, "duplicate_key_rows": 0,
              "key_null_rows": 0, "invalid_date_values": 0, "invalid_json_values": 0,
              "calendar_closed_distinct_dates": 0, "fk_orphan_rows": 0}
    for db in audit["databases"].values():
        if db.get("status") != "scanned":
            continue
        totals["databases"] += 1
        for table in db["tables"]:
            totals["tables"] += 1
            totals["rows"] += table["rows"]
            nulls = sum(table["null_counts"].values())
            totals["null_values"] += nulls
            totals["tables_with_nulls"] += int(nulls > 0)
            totals["nonfinite_values"] += sum(table["nonfinite_numeric"].values())
            key = table["candidate_key"]
            totals["duplicate_key_rows"] += int(key.get("duplicate_rows") or 0)
            totals["key_null_rows"] += int(key.get("null_rows") or 0)
            totals["invalid_date_values"] += sum(x["invalid_nonnull"] for x in table["date_checks"].values())
            totals["invalid_json_values"] += sum(table["invalid_json_counts"].values())
            for check in table["date_checks"].values():
                totals["calendar_closed_distinct_dates"] += len(check.get("calendar", {}).get("not_open_dates", []))
        for check in db["relationships"].values():
            totals["fk_orphan_rows"] += int(check.get("orphan_rows", 0))
    return totals


def _markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# DuckDB table audit (read-only)", "",
        f"Generated: `{audit['generated_at']}`  ",
        f"TradingDatabase root: `{audit['database_root']}`  ",
        f"Calendar reference: `{audit['calendar'].get('path')}` ({audit['calendar'].get('status')})", "",
        "This report scans every base table in the seven persistent DuckDB files. It records full-table row, NULL, finite-number, candidate-key, date and JSON counts. `scanned` means the read completed; it does not mean economic/business correctness passed. Checks without a documented source rule are marked `not_evaluated`.", "",
        "## Summary", "",
        "| Databases | Base tables | Rows scanned | NULL values | Non-finite values | Candidate-key duplicate rows | Key NULL rows | Invalid dates | Invalid JSON | Closed distinct dates | FK orphan rows |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        "| " + " | ".join(str(value) for value in audit["summary"].values()) + " |", "",
    ]
    for name, db in audit["databases"].items():
        lines += [f"## `{name}`", "", f"Path: `{db['path']}`; status: `{db['status']}`; tables: {db.get('base_table_count', 0)}.", ""]
        if db.get("status") != "scanned":
            continue
        lines += ["| Table | Rows | Candidate key duplicate rows | Key NULL rows | Non-finite numeric | Invalid dates / calendar | Invalid JSON |",
                  "|---|---:|---:|---:|---:|---|---:|"]
        for table in db["tables"]:
            key = table["candidate_key"]
            dates = "; ".join(
                f"{column}: invalid={result['invalid_nonnull']}, calendar="
                f"{result.get('calendar',{}).get('status','n/a')}, "
                f"closed={len(result.get('calendar',{}).get('not_open_dates',[]))}"
                for column, result in table["date_checks"].items()
            ) or "n/a"
            lines.append(
                f"| `{table['table']}` | {table['rows']} | {key.get('duplicate_rows')} | "
                f"{key.get('null_rows')} | {sum(table['nonfinite_numeric'].values())} | "
                f"{dates} | {sum(table['invalid_json_counts'].values())} |"
            )
        for table in db["tables"]:
            nonzero_nulls = {key: value for key, value in table["null_counts"].items() if value}
            if nonzero_nulls or table["nonfinite_numeric"] or table["invalid_json_counts"]:
                lines += ["", f"- `{table['table']}` NULL counts: `{json.dumps(nonzero_nulls, ensure_ascii=False, sort_keys=True)}`; non-finite: `{json.dumps(table['nonfinite_numeric'], sort_keys=True)}`; invalid JSON: `{json.dumps(table['invalid_json_counts'], sort_keys=True)}`."]
        lines += ["", f"Fifteen-minute contract: `{json.dumps(db.get('fifteen_min_domain'), ensure_ascii=False, sort_keys=True)}`.",
                  f"Catalog checks: `{json.dumps(db.get('catalog_links'), ensure_ascii=False, sort_keys=True)}`.",
                  f"Cross-table references: `{json.dumps(db.get('relationships'), ensure_ascii=False, sort_keys=True)}`.",
                  f"Derived consistency: `{json.dumps(db.get('derived_relationships'), ensure_ascii=False, sort_keys=True)}`.", ""]
    lines += [
        "## Limits", "",
        "- Calendar consistency uses the local read-only `db_cn_basic.db:tbl_cn_tradecal` SSE open-day set and is limited to its date range. Calendar absence or out-of-range values are not treated as passes.",
        "- The auditor checks keys and references listed in its code; these are candidate/documented keys, not a declaration of undocumented database constraints.",
        "- Theme graph daily limit/break counts are compared to the source facts using the implementation's documented join/filter. Other lifecycle/score calculations are explicitly not replayed. Fact-to-dimension orphans are reported as reference discrepancies for review; they are not silently repaired.",
        "- Legacy policy minute/tick SQLite databases and Parquet partitions are outside this DuckDB report. No source values are rewritten.",
        "",
    ]
    return "\n".join(lines)


def run_audit(database_root: Path, calendar_db: Path) -> dict[str, Any]:
    calendar = _load_calendar(calendar_db)
    databases = {}
    for name, relative_path in DATABASES.items():
        path = database_root / relative_path
        if not path.exists():
            databases[name] = {"path": str(path), "status": "not_audited_missing"}
            continue
        try:
            databases[name] = audit_database(name, path, calendar)
        except Exception as exc:
            databases[name] = {
                "path": str(path), "status": "scan_failed",
                "error_type": type(exc).__name__,
            }
    result = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_root": str(database_root),
        "calendar": {k: v for k, v in calendar.items() if k != "open_dates"},
        "calendar_open_date_count": len(calendar.get("open_dates", [])),
        "databases": databases,
    }
    result["summary"] = _summarize(result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-root", type=Path, default=Path("/Users/nickzhang/TradingDatabase"))
    parser.add_argument("--calendar-db", type=Path,
                        default=Path("/Users/nickzhang/TradingDatabase/offline/db_cn_basic.db"))
    parser.add_argument("--json-output", type=Path, required=True,
                        help="write detailed machine-readable audit result")
    parser.add_argument("--markdown-output", type=Path, required=True,
                        help="write concise table-level report")
    args = parser.parse_args(argv)
    audit = run_audit(args.database_root, args.calendar_db)
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    args.markdown_output.write_text(_markdown(audit), encoding="utf-8")
    print(json.dumps({"summary": audit["summary"], "database_status": {
        name: db.get("status") for name, db in audit["databases"].items()
    }}, ensure_ascii=False))
    return 0 if all(db.get("status") == "scanned" for db in audit["databases"].values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())

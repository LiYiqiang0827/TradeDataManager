#!/usr/bin/env python3
"""Build a review-only merged SQLite copy without treating rowids as business keys.

The mini database is copied first and wins on identical business keys. Rows that
exist only in the local repaired source are inserted with surrogate INTEGER PKs
omitted so SQLite allocates fresh ids. Tables without one unambiguous non-rowid
UNIQUE key, key rows containing NULL, schema drift, and alternate-unique-index
collisions are quarantined in the report and make the output non-promotable.
Source files are opened read-only and never changed.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import quote


VOLATILE_PAYLOAD_COLUMNS = {"snap_ts", "created_at", "updated_at", "ingested_at", "ingest_ts"}
PROTECTED_CONTROL_TABLES = {
    "online_stream_cursor", "online_stream_cursors", "stream_cursor",
    "service_state", "scheduler_state", "checkpoint", "checkpoints",
    "online_stream_timestamp_quarantine",
}
MAX_CONFLICT_SAMPLES_PER_TABLE = 10


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _uri(path: Path) -> str:
    # SQLite expects file:/absolute/path, not the RFC file:/// URI form here.
    return "file:" + quote(str(path.resolve()), safe="/:") + "?mode=ro"


def _table_names(conn: sqlite3.Connection, schema: str = "main") -> set[str]:
    return {row[0] for row in conn.execute(
        f"SELECT name FROM {schema}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )}


def _table_info(conn: sqlite3.Connection, table: str, schema: str) -> list[tuple[Any, ...]]:
    return conn.execute(f"PRAGMA {schema}.table_info({_q(table)})").fetchall()


def _auto_integer_pk(info: list[tuple[Any, ...]], create_sql: str | None) -> set[str]:
    pk = [row for row in info if int(row[5]) > 0]
    if len(pk) == 1 and str(pk[0][2]).strip().upper() == "INTEGER":
        # In SQLite a single INTEGER PRIMARY KEY aliases rowid and is generated
        # when omitted, whether AUTOINCREMENT was spelled explicitly or not.
        return {str(pk[0][1])}
    return set()


def _business_unique_indexes(
    conn: sqlite3.Connection, table: str, schema: str, auto_ids: set[str]
) -> set[tuple[str, ...]]:
    result: set[tuple[str, ...]] = set()
    for seq, name, unique, origin, partial in conn.execute(
        f"PRAGMA {schema}.index_list({_q(table)})"
    ):
        if not unique or partial:
            continue
        fields = conn.execute(f"PRAGMA {schema}.index_info({_q(name)})").fetchall()
        if not fields or any(row[2] is None or int(row[1]) < 0 for row in fields):
            continue  # expression or rowid-only index: no portable business key
        cols = tuple(str(row[2]) for row in fields)
        if set(cols) & auto_ids:
            continue
        result.add(cols)
    # Composite PKs are represented by an autoindex; include explicitly for
    # SQLite builds that omit that metadata row. Integer rowid PK is excluded.
    pk = sorted((int(row[5]), str(row[1])) for row in _table_info(conn, table, schema) if int(row[5]) > 0)
    if len(pk) > 1:
        result.add(tuple(name for _, name in pk))
    return result


def _schema_signature(info: list[tuple[Any, ...]]) -> list[tuple[str, str, int, int]]:
    return [(str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5])) for row in info]


def _matching_keys(conn: sqlite3.Connection, table: str) -> tuple[tuple[str, ...] | None, set[str], str | None]:
    mini_info = _table_info(conn, table, "main")
    local_info = _table_info(conn, table, "local_src")
    if _schema_signature(mini_info) != _schema_signature(local_info):
        return None, set(), "schema_mismatch"
    mini_sql = conn.execute(
        "SELECT sql FROM main.sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    local_sql = conn.execute(
        "SELECT sql FROM local_src.sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    mini_auto = _auto_integer_pk(mini_info, mini_sql[0] if mini_sql else None)
    local_auto = _auto_integer_pk(local_info, local_sql[0] if local_sql else None)
    if mini_auto != local_auto:
        return None, set(), "surrogate_key_mismatch"
    mini_keys = _business_unique_indexes(conn, table, "main", mini_auto)
    local_keys = _business_unique_indexes(conn, table, "local_src", local_auto)
    common = mini_keys & local_keys
    if len(common) != 1:
        return None, mini_auto, "no_business_unique_index" if not common else "ambiguous_business_unique_indexes"
    return next(iter(common)), mini_auto, None


def _and_equal(left_alias: str, right_alias: str, cols: tuple[str, ...]) -> str:
    # IS is NULL-safe. Null business keys are separately rejected before use.
    return " AND ".join(f"{left_alias}.{_q(c)} IS {right_alias}.{_q(c)}" for c in cols)


def _columns(info: list[tuple[Any, ...]]) -> list[str]:
    return [str(row[1]) for row in info]


def _is_protected_control_table(table: str) -> bool:
    lowered = table.lower()
    return (lowered in PROTECTED_CONTROL_TABLES or
            lowered.endswith(("_cursor", "_cursors", "_checkpoint", "_checkpoints", "_state")))


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value[:128].hex(), "truncated": len(value) > 128}
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, (str, int, float, type(None), bool)):
        return value
    return repr(value)


def _conflict_samples(conn: sqlite3.Connection, table: str, key: tuple[str, ...],
                      payload_cols: list[str], join: str, same_payload: str) -> list[dict[str, Any]]:
    select = [f"m.{_q(c)} AS mini__{c}" for c in key]
    select += [f"m.{_q(c)} AS mini_payload__{c}" for c in payload_cols]
    select += [f"l.{_q(c)} AS local_payload__{c}" for c in payload_cols]
    rows = conn.execute(
        f"SELECT {','.join(select)} FROM local_src.{_q(table)} l "
        f"JOIN main.{_q(table)} m ON {join} WHERE NOT ({same_payload}) LIMIT ?",
        (MAX_CONFLICT_SAMPLES_PER_TABLE,),
    ).fetchall()
    samples: list[dict[str, Any]] = []
    for row in rows:
        key_value = {col: _json_value(row[f"mini__{col}"]) for col in key}
        differences = {}
        for col in payload_cols:
            mini_value = row[f"mini_payload__{col}"]
            local_value = row[f"local_payload__{col}"]
            if mini_value != local_value:
                differences[col] = {"mini": _json_value(mini_value), "local": _json_value(local_value)}
        samples.append({"business_key": key_value, "differing_columns": differences})
    return samples


def _copy_local_only_table(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    ddl = conn.execute(
        "SELECT sql FROM local_src.sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()[0]
    if not ddl:
        return {"table": table, "status": "quarantined", "reason": "missing_create_sql"}
    conn.execute(ddl)
    info = _table_info(conn, table, "local_src")
    cols = _columns(info)
    cols_sql = ",".join(_q(c) for c in cols)
    conn.execute(f"INSERT INTO main.{_q(table)} ({cols_sql}) SELECT {cols_sql} FROM local_src.{_q(table)}")
    object_rows = conn.execute(
        "SELECT sql FROM local_src.sqlite_master WHERE tbl_name=? AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name",
        (table,),
    ).fetchall()
    for (sql,) in object_rows:
        conn.execute(sql)
    count = int(conn.execute(f"SELECT COUNT(*) FROM local_src.{_q(table)}").fetchone()[0])
    return {"table": table, "status": "copied_local_only", "rows": count}


def _merge_table(conn: sqlite3.Connection, table: str, replace: bool) -> dict[str, Any]:
    mini_info = _table_info(conn, table, "main")
    local_info = _table_info(conn, table, "local_src")
    mini_count = int(conn.execute(f"SELECT COUNT(*) FROM main.{_q(table)}").fetchone()[0])
    local_count = int(conn.execute(f"SELECT COUNT(*) FROM local_src.{_q(table)}").fetchone()[0])
    if replace:
        if _schema_signature(mini_info) != _schema_signature(local_info):
            return {"table": table, "status": "quarantined", "reason": "schema_mismatch"}
        conn.execute(f"DELETE FROM main.{_q(table)}")
        cols = _columns(mini_info)
        cols_sql = ",".join(_q(c) for c in cols)
        conn.execute(f"INSERT INTO main.{_q(table)} ({cols_sql}) SELECT {cols_sql} FROM local_src.{_q(table)}")
        return {"table": table, "status": "replaced_from_local", "mini_rows": mini_count, "local_rows": local_count}
    if local_count == 0:
        return {"table": table, "status": "local_empty_passthrough", "mini_rows": mini_count,
                "local_rows": 0, "reason": "no local rows to merge"}
    if _schema_signature(mini_info) != _schema_signature(local_info):
        return {"table": table, "status": "quarantined", "reason": "schema_mismatch",
                "mini_rows": mini_count, "local_rows": local_count}

    key, auto_ids, reason = _matching_keys(conn, table)
    if reason:
        return {"table": table, "status": "quarantined", "reason": reason,
                "mini_rows": mini_count, "local_rows": local_count}
    assert key is not None
    key_null = " OR ".join(f"{_q(c)} IS NULL" for c in key)
    null_count = int(conn.execute(
        f"SELECT COUNT(*) FROM main.{_q(table)} WHERE {key_null}"
    ).fetchone()[0]) + int(conn.execute(
        f"SELECT COUNT(*) FROM local_src.{_q(table)} WHERE {key_null}"
    ).fetchone()[0])
    if null_count:
        return {"table": table, "status": "quarantined", "reason": "null_business_key_values",
                "business_key": list(key), "null_key_rows": null_count,
                "mini_rows": mini_count, "local_rows": local_count}

    join = _and_equal("m", "l", key)
    payload_cols = [c for c in _columns(mini_info) if c not in auto_ids and c not in VOLATILE_PAYLOAD_COLUMNS]
    if not payload_cols:
        return {"table": table, "status": "quarantined", "reason": "no_payload_columns",
                "business_key": list(key), "mini_rows": mini_count, "local_rows": local_count}
    same_payload = " AND ".join(f"m.{_q(c)} IS l.{_q(c)}" for c in payload_cols)
    duplicate_count, conflict_count = conn.execute(
        f"SELECT SUM(CASE WHEN {same_payload} THEN 1 ELSE 0 END), "
        f"SUM(CASE WHEN NOT ({same_payload}) THEN 1 ELSE 0 END) "
        f"FROM local_src.{_q(table)} l JOIN main.{_q(table)} m ON {join}"
    ).fetchone()
    duplicate_count, conflict_count = int(duplicate_count or 0), int(conflict_count or 0)
    conflict_samples = _conflict_samples(conn, table, key, payload_cols, join, same_payload) if conflict_count else []
    local_only = int(conn.execute(
        f"SELECT COUNT(*) FROM local_src.{_q(table)} l WHERE NOT EXISTS "
        f"(SELECT 1 FROM main.{_q(table)} m WHERE {join})"
    ).fetchone()[0])
    insert_cols = [c for c in _columns(mini_info) if c not in auto_ids]
    cols_sql = ",".join(_q(c) for c in insert_cols)
    select_sql = ",".join(f"l.{_q(c)}" for c in insert_cols)
    try:
        conn.execute("SAVEPOINT merge_table")
        conn.execute(
            f"INSERT INTO main.{_q(table)} ({cols_sql}) "
            f"SELECT {select_sql} FROM local_src.{_q(table)} l WHERE NOT EXISTS "
            f"(SELECT 1 FROM main.{_q(table)} m WHERE {join})"
        )
        conn.execute("RELEASE merge_table")
    except sqlite3.IntegrityError as exc:
        conn.execute("ROLLBACK TO merge_table")
        conn.execute("RELEASE merge_table")
        return {"table": table, "status": "quarantined", "reason": "alternate_unique_constraint_conflict",
                "detail": str(exc), "business_key": list(key), "same_payload_duplicates": duplicate_count,
                "business_key_payload_conflicts": conflict_count, "conflict_samples": conflict_samples,
                "local_only_candidates": local_only,
                "mini_rows": mini_count, "local_rows": local_count}
    return {"table": table, "status": "merged", "business_key": list(key),
            "same_payload_duplicates": duplicate_count,
            "business_key_payload_conflicts_kept_mini": conflict_count,
            "conflict_samples": conflict_samples,
            "local_only_candidates": local_only, "local_only_inserted": local_only,
            "mini_rows": mini_count, "local_rows": local_count}


def merge_databases(mini_path: Path, local_path: Path, output_path: Path,
                    include_tables: set[str],
                    replace_tables: set[str] | None = None) -> dict[str, Any]:
    mini_path, local_path, output_path = mini_path.resolve(), local_path.resolve(), output_path.resolve()
    if mini_path == local_path or output_path in {mini_path, local_path}:
        raise ValueError("mini, local, and output paths must be three distinct files")
    if not mini_path.is_file() or not local_path.is_file():
        raise FileNotFoundError("both source databases must exist")
    if not include_tables:
        raise ValueError("an explicit reviewed domain-table allowlist is required")
    protected = sorted(table for table in include_tables if _is_protected_control_table(table))
    if protected:
        raise ValueError(f"control tables cannot be merged; Mini remains authoritative: {protected}")
    if not set(replace_tables or ()).issubset(include_tables):
        raise ValueError("replace tables must also be in the explicit include allowlist")
    if output_path.exists():
        raise FileExistsError(f"refusing to overwrite existing output: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(_uri(mini_path), uri=True)
    dest = sqlite3.connect(output_path)
    try:
        source.backup(dest)
    finally:
        dest.close()
        source.close()

    conn = sqlite3.connect(output_path, uri=True)
    conn.row_factory = sqlite3.Row
    report: dict[str, Any] = {"mini_source": str(mini_path), "local_source": str(local_path),
                              "output": str(output_path), "sources_unchanged": True,
                              "include_tables": sorted(include_tables),
                              "ready_to_promote": False, "tables": []}
    completed = False
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("ATTACH DATABASE ? AS local_src", (_uri(local_path),))
        mini_tables, local_tables = _table_names(conn, "main"), _table_names(conn, "local_src")
        known_tables = mini_tables | local_tables
        unknown_requested = include_tables - known_tables
        if unknown_requested:
            raise ValueError(f"allowlisted tables absent from both sources: {sorted(unknown_requested)}")
        missing_replace_tables = set(replace_tables or ()) - (mini_tables & local_tables)
        if missing_replace_tables:
            raise ValueError(f"replace tables must exist in both sources: {sorted(missing_replace_tables)}")
        report["local_only_tables"] = sorted((local_tables - mini_tables) & include_tables)
        report["mini_only_tables"] = sorted((mini_tables - local_tables) & include_tables)
        report["excluded_local_only_tables"] = sorted(local_tables - mini_tables - include_tables)
        report["excluded_mini_tables_preserved"] = sorted(mini_tables - include_tables)
        conn.execute("BEGIN IMMEDIATE")
        for table in sorted((mini_tables & local_tables) & include_tables):
            report["tables"].append(_merge_table(conn, table, table in (replace_tables or set())))
        for table in sorted((local_tables - mini_tables) & include_tables):
            try:
                report["tables"].append(_copy_local_only_table(conn, table))
            except sqlite3.Error as exc:
                report["tables"].append({"table": table, "status": "quarantined",
                                         "reason": "local_only_copy_failed", "detail": str(exc)})
        quarantined = [row["table"] for row in report["tables"] if row["status"] == "quarantined"]
        review_required = [row["table"] for row in report["tables"]
                           if row.get("business_key_payload_conflicts_kept_mini", 0) > 0]
        report["quarantined_tables"] = quarantined
        report["review_required_tables"] = review_required
        conn.commit()
        integrity_rows = [str(row[0]) for row in conn.execute("PRAGMA integrity_check").fetchall()]
        fk_rows = [tuple(row) for row in conn.execute("PRAGMA foreign_key_check").fetchall()]
        report["integrity_check"] = {"ok": integrity_rows == ["ok"], "results": integrity_rows}
        report["foreign_key_check"] = {"ok": not fk_rows, "violations": fk_rows[:100],
                                        "violation_count": len(fk_rows)}
        report["ready_to_promote"] = (
            not quarantined and not review_required and
            report["integrity_check"]["ok"] and report["foreign_key_check"]["ok"]
        )
        completed = True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        if not completed:
            output_path.unlink(missing_ok=True)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mini", type=Path, required=True, help="Mini-priority source SQLite file (read-only)")
    parser.add_argument("--local", type=Path, required=True, help="Locally repaired source SQLite file (read-only)")
    parser.add_argument("--output", type=Path, required=True, help="New isolated output file; must not exist")
    parser.add_argument("--report", type=Path, required=True, help="JSON report path")
    parser.add_argument("--include-table", action="append", required=True,
                        help="reviewed domain table allowlist entry (repeat per table); control tables are rejected")
    parser.add_argument("--replace-table", action="append", default=[],
                        help="replace this complete table from local source (repeat per table)")
    args = parser.parse_args()
    if args.report.exists():
        parser.error(f"refusing to overwrite existing report: {args.report}")
    report = merge_databases(args.mini, args.local, args.output, set(args.include_table), set(args.replace_table))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": report["output"], "ready_to_promote": report["ready_to_promote"],
                      "quarantined_tables": report["quarantined_tables"],
                      "review_required_tables": report["review_required_tables"]}, ensure_ascii=False))
    return 0 if report["ready_to_promote"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

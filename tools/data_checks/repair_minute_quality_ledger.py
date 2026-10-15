#!/usr/bin/env python3
"""Safely append missing minute quality ledger rows from a verified sidecar.

This tool never edits minute bar rows. It is dry-run by default and requires an
explicit existing SQLite path and date. Use --apply only inside a coordinated
maintenance window with no database writers.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

QUALITY_SQL = """CREATE TABLE IF NOT EXISTS minute_source_quality (
 trade_date TEXT NOT NULL, ts_code TEXT NOT NULL, time_idx INTEGER NOT NULL,
 datetime TEXT NOT NULL, quality_code TEXT NOT NULL, source TEXT NOT NULL,
 source_payload_json TEXT NOT NULL, source_timestamp TEXT, detected_at TEXT NOT NULL,
 PRIMARY KEY (trade_date, ts_code, time_idx, quality_code))"""
_MARKET_TZ = ZoneInfo("Asia/Shanghai")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def json_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def normalize_bar_datetime(value) -> str | None:
    """Normalize a bar timestamp to market-local second/microsecond precision."""
    if value is None:
        return None
    try:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if isinstance(value, datetime):
            parsed = value
        else:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(_MARKET_TZ).replace(tzinfo=None)
        base = parsed.strftime("%Y-%m-%d %H:%M:%S")
        fraction = f"{parsed.microsecond:06d}".rstrip("0")
        return f"{base}.{fraction}" if fraction else base
    except (TypeError, ValueError, OverflowError):
        return None


def load_verified_sidecar(parquet_root: Path, trade_date: str):
    manifest_path = parquet_root / "_manifest" / f"trade_date={trade_date}.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"missing canonical manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "ok" or manifest.get("trade_date") != trade_date:
        raise ValueError("manifest status/date mismatch")
    # The canonical data path is nested by year and month; the manifest is
    # authoritative for the sidecar path and content hash.
    sidecar_value = manifest.get("quality_sidecar")
    if not sidecar_value:
        raise ValueError("manifest has no quality_sidecar")
    sidecar = Path(sidecar_value).expanduser().resolve()
    root = parquet_root.resolve()
    try:
        sidecar.relative_to(root)
    except ValueError as exc:
        raise ValueError("quality sidecar path escapes parquet root") from exc
    if not sidecar.is_file():
        raise FileNotFoundError(f"missing quality sidecar: {sidecar}")
    actual_sha = sha256(sidecar)
    if actual_sha != manifest.get("quality_sidecar_sha256"):
        raise ValueError("quality sidecar SHA256 does not match manifest")
    df = pd.read_parquet(sidecar)
    if len(df) != int(manifest.get("quality_sidecar_rows", -1)):
        raise ValueError("quality sidecar row count does not match manifest")
    required = {
        "trade_date", "ts_code", "time_idx", "datetime", "quality_flags",
        "source", "source_payload_json", "source_payload_sha256",
    }
    if not required.issubset(df.columns):
        raise ValueError(f"quality sidecar missing columns: {sorted(required - set(df.columns))}")
    if set(df["trade_date"].astype(str)) != {trade_date}:
        raise ValueError("quality sidecar contains a different trade_date")
    if df.duplicated(["trade_date", "ts_code", "time_idx"]).any():
        raise ValueError("duplicate quality sidecar keys")
    bad_hashes = []
    for idx, row in df.iterrows():
        payload = str(row["source_payload_json"])
        if json_hash(payload) != str(row["source_payload_sha256"]):
            bad_hashes.append(idx)
        try:
            flags = json.loads(str(row["quality_flags"]))
        except Exception as exc:
            raise ValueError(f"invalid quality_flags at row {idx}") from exc
        if not isinstance(flags, list) or not all(isinstance(x, str) for x in flags):
            raise ValueError(f"quality_flags must be a string list at row {idx}")
        try:
            json.loads(payload)
        except Exception as exc:
            raise ValueError(f"invalid source payload JSON at row {idx}") from exc
    if bad_hashes:
        raise ValueError(f"{len(bad_hashes)} source_payload_sha256 mismatch(es)")
    target_value = manifest.get("target_file")
    if not target_value:
        raise ValueError("manifest has no canonical target_file")
    target = Path(target_value).expanduser().resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise ValueError("canonical target path escapes parquet root") from exc
    if not target.is_file() or target.stat().st_size != int(manifest.get("target_size", -1)):
        raise ValueError("canonical primary partition is missing or its size differs from manifest")
    return manifest_path, manifest, sidecar, actual_sha, df


def prepare_rows(df: pd.DataFrame, target_bars: dict[tuple[str, int], str | None], trade_date: str, detected_at: str):
    """Build candidates only for sidecar bars present in the target minute table."""
    rows = {}
    matched_keys = set()
    unmatched_sidecar_keys = 0
    datetime_conflicts = []
    for rec in df.itertuples(index=False):
        code = str(rec.ts_code)
        time_idx = int(rec.time_idx)
        bar_key = (code, time_idx)
        if bar_key not in target_bars:
            unmatched_sidecar_keys += 1
            continue
        sidecar_dt = normalize_bar_datetime(rec.datetime)
        target_dt = target_bars[bar_key]
        if sidecar_dt is None or target_dt is None or sidecar_dt != target_dt:
            datetime_conflicts.append({
                "ts_code": code,
                "time_idx": time_idx,
                "sidecar_datetime": str(rec.datetime),
                "target_datetime": target_dt,
                "reason": "invalid_or_mismatched_bar_datetime",
            })
            continue
        matched_keys.add(bar_key)
        flags = json.loads(str(rec.quality_flags))
        when = sidecar_dt
        for flag in flags:
            key = (trade_date, code, time_idx, flag)
            value = (
                trade_date, code, time_idx, when, flag,
                str(rec.source), str(rec.source_payload_json), None, detected_at,
            )
            previous = rows.get(key)
            if previous is not None and previous != value:
                raise ValueError(f"conflicting quality records for {key}")
            rows[key] = value
    return list(rows.items()), {
        "matched_bar_keys": len(matched_keys),
        "matched_codes": len({code for code, _ in matched_keys}),
        "unmatched_sidecar_bar_keys": unmatched_sidecar_keys,
        "datetime_conflict_bar_keys": len(datetime_conflicts),
        "datetime_conflict_samples": datetime_conflicts[:25],
    }


def _existing_ledger_rows(conn: sqlite3.Connection, trade_date: str) -> dict[tuple[str, str, int, str], tuple]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='minute_source_quality'"
    ).fetchone()
    if not exists:
        return {}
    columns = {row[1] for row in conn.execute("PRAGMA table_info(minute_source_quality)")}
    required = {"trade_date", "ts_code", "time_idx", "datetime", "quality_code", "source",
                "source_payload_json", "source_timestamp", "detected_at"}
    if not required.issubset(columns):
        raise ValueError(f"minute_source_quality schema missing columns: {sorted(required - columns)}")
    rows = conn.execute(
        "SELECT trade_date,ts_code,time_idx,datetime,quality_code,source,source_payload_json,"
        "source_timestamp,detected_at FROM minute_source_quality WHERE trade_date=?",
        (trade_date,),
    ).fetchall()
    result = {}
    for row in rows:
        key = (str(row[0]), str(row[1]), int(row[2]), str(row[4]))
        if key in result:
            raise ValueError(f"duplicate existing quality ledger key: {key}")
        result[key] = tuple(row)
    return result


def reconcile_candidates(candidate_items, existing_rows):
    """Return missing-only inserts and same-key conflicts; existing rows win."""
    inserts, conflicts, unchanged = [], [], 0
    for key, candidate in candidate_items:
        existing = existing_rows.get(key)
        if existing is None:
            inserts.append(candidate)
            continue
        # detected_at is the repair/import time and naturally differs on reruns;
        # preserve the existing value while comparing source evidence fields.
        existing_evidence = existing[:8]
        candidate_evidence = candidate[:8]
        if existing_evidence == candidate_evidence:
            unchanged += 1
        else:
            conflicts.append({
                "key": {"trade_date": key[0], "ts_code": key[1], "time_idx": key[2], "quality_code": key[3]},
                "differing_fields": [
                    name for name, old, new in zip(
                        ("trade_date", "ts_code", "time_idx", "datetime", "quality_code", "source",
                         "source_payload_json", "source_timestamp"), existing_evidence, candidate_evidence,
                    ) if old != new
                ],
                "existing_payload_sha256": json_hash(str(existing[6])),
                "candidate_payload_sha256": json_hash(str(candidate[6])),
            })
    return inserts, conflicts, unchanged


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True, help="existing online SQLite DB; never created by this tool")
    ap.add_argument("--trade-date", required=True, help="YYYYMMDD")
    ap.add_argument("--parquet-root", required=True, help="canonical schema_v2 root")
    ap.add_argument("--receipt", required=True, help="JSON output path, outside the Git repository")
    ap.add_argument("--apply", action="store_true", help="append only missing matching bar/flag keys; preserve existing rows")
    args = ap.parse_args()
    if not re.fullmatch(r"\d{8}", args.trade_date):
        ap.error("--trade-date must be YYYYMMDD")
    db = Path(args.db).expanduser().resolve()
    if not db.is_file():
        raise FileNotFoundError(f"database does not exist: {db}")
    root = Path(args.parquet_root).expanduser().resolve()
    manifest_path, manifest, sidecar, sidecar_sha, df = load_verified_sidecar(root, args.trade_date)
    mode = "rw" if args.apply else "ro"
    conn = sqlite3.connect(f"file:{db}?mode={mode}", uri=True, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    table = f"minute_{args.trade_date}"
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if not exists:
        conn.close()
        raise ValueError(f"target table does not exist: {table}")
    key_check = conn.execute(
        f"SELECT count(*), count(DISTINCT ts_code), "
        f"sum(CASE WHEN trade_date<>? THEN 1 ELSE 0 END) FROM {quote_ident(table)}",
        (args.trade_date,),
    ).fetchone()
    duplicate_keys = conn.execute(
        f"SELECT count(*) FROM (SELECT ts_code,time_idx FROM {quote_ident(table)} "
        "GROUP BY ts_code,time_idx HAVING count(*)>1)"
    ).fetchone()[0]
    if key_check[0] and (duplicate_keys or key_check[2]):
        conn.close()
        raise ValueError(f"target minute table date/key check failed: counts={key_check}, duplicate_keys={duplicate_keys}")
    target_bars = {
        (str(code), int(time_idx)): normalize_bar_datetime(bar_datetime)
        for code, time_idx, bar_datetime in conn.execute(
            f"SELECT ts_code,time_idx,datetime FROM {quote_ident(table)}"
        )
    }
    target_codes = {code for code, _ in target_bars}
    detected_at = datetime.now().astimezone().isoformat(timespec="seconds")
    candidate_items, selected = prepare_rows(df, target_bars, args.trade_date, detected_at)
    existing_rows = _existing_ledger_rows(conn, args.trade_date)
    ledger_rows, conflicts, unchanged_rows = reconcile_candidates(candidate_items, existing_rows)
    flag_counts = Counter(row[4] for row in ledger_rows)
    receipt = {
        "status": "planned",
        "mode": "apply" if args.apply else "dry_run",
        "db": str(db),
        "trade_date": args.trade_date,
        "manifest": str(manifest_path),
        "manifest_sha256": sha256(manifest_path),
        "canonical_partition": manifest.get("target_file"),
        "quality_sidecar": str(sidecar),
        "quality_sidecar_sha256": sidecar_sha,
        "sidecar_rows": int(len(df)),
        "target_rows": key_check[0],
        "target_codes": len(target_codes),
        "sidecar_bar_keys_in_target": selected["matched_bar_keys"],
        "sidecar_codes_in_target": selected["matched_codes"],
        "sidecar_bar_keys_excluded_not_in_target": selected["unmatched_sidecar_bar_keys"],
        "sidecar_bar_keys_excluded_datetime_conflict": selected["datetime_conflict_bar_keys"],
        "datetime_conflict_samples": selected["datetime_conflict_samples"],
        "old_ledger_rows_for_date": len(existing_rows),
        "candidate_ledger_rows": len(candidate_items),
        "new_ledger_rows": len(ledger_rows),
        "already_present_identical_rows": unchanged_rows,
        "same_key_payload_conflicts": len(conflicts),
        "conflict_samples": conflicts[:25],
        "flag_counts": dict(flag_counts),
        "generated_at": detected_at,
        "minute_bars_modified": False,
    }
    if args.apply:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(QUALITY_SQL)
            conn.executemany(
                "INSERT INTO minute_source_quality (trade_date,ts_code,time_idx,datetime,quality_code,source,source_payload_json,source_timestamp,detected_at) VALUES (?,?,?,?,?,?,?,?,?)",
                ledger_rows,
            )
            actual = conn.execute("SELECT count(*) FROM minute_source_quality WHERE trade_date=?",
                                  (args.trade_date,)).fetchone()[0]
            expected_total = len(existing_rows) + len(ledger_rows)
            if actual != expected_total:
                raise ValueError(f"ledger count mismatch: expected {expected_total}, got {actual}")
            conn.commit()
            receipt["status"] = "committed"
            receipt["postwrite_ledger_rows_for_date"] = actual
            receipt["inserted_rows"] = len(ledger_rows)
        except Exception:
            conn.rollback()
            conn.close()
            raise
    conn.close()
    out = Path(args.receipt).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    tmp.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, out)
    print(json.dumps({"status": receipt["status"], "target_codes": len(target_codes),
                      "ledger_rows": len(ledger_rows), "receipt": str(out)}, ensure_ascii=False))


if __name__ == "__main__":
    main()

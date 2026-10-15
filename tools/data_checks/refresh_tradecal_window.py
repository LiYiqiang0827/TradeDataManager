#!/usr/bin/env python3
"""Refresh only the forward SSE/SZSE exchange-calendar window.

Load the configured environment before importing TushareClient, for example:
    source "$TRADING_AGENT_ENV_FILE"
    python tools/data_checks/refresh_tradecal_window.py

The request always starts at Shanghai today (never at a ctrl/max-date
watermark), validates both exchanges, and upserts only provider-returned dates
on/after today. A JSON receipt records pre-write rows, gaps, provider hash, and
post-write fingerprint. No market-data endpoint is called.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")
EXCHANGES = ("SSE", "SZSE")
TABLE = "tbl_cn_tradecal"


def _rows_from_frame(exchange: str, frame) -> list[dict[str, str | None]]:
    if frame is None:
        return []
    rows = []
    for raw in frame.to_dict(orient="records"):
        row = {key: (None if value is None or str(value) == "nan" else str(value))
               for key, value in raw.items()}
        row["exchange"] = exchange
        rows.append(row)
    return rows


def validate_calendar_rows(
    results: dict[str, list[dict[str, str | None]]],
    start_date: str,
    requested_end: str,
    *,
    minimum_forward_days: int = 7,
) -> tuple[list[dict[str, str | None]], str]:
    """Validate both exchange responses and return sorted accepted rows/end."""
    if set(results) != set(EXCHANGES):
        raise ValueError("both SSE and SZSE responses are required")
    by_exchange: dict[str, dict[str, dict[str, str | None]]] = {}
    for exchange in EXCHANGES:
        by_date: dict[str, dict[str, str | None]] = {}
        for row in results[exchange]:
            day = row.get("cal_date")
            if not isinstance(day, str) or len(day) != 8 or not day.isdigit():
                raise ValueError(f"invalid cal_date for {exchange}")
            if row.get("exchange") != exchange or row.get("is_open") not in {"0", "1"}:
                raise ValueError(f"invalid exchange/is_open value for {exchange} {day}")
            if not start_date <= day <= requested_end:
                raise ValueError(f"provider row outside requested range: {exchange} {day}")
            if day in by_date:
                raise ValueError(f"duplicate provider row: {exchange} {day}")
            by_date[day] = row
        if not by_date:
            raise ValueError(f"empty provider response for {exchange}")
        by_exchange[exchange] = by_date

    accepted_end = min(max(by_exchange[ex]) for ex in EXCHANGES)
    start = dt.datetime.strptime(start_date, "%Y%m%d").date()
    stop = dt.datetime.strptime(accepted_end, "%Y%m%d").date()
    expected = []
    cursor = start
    while cursor <= stop:
        expected.append(cursor.strftime("%Y%m%d"))
        cursor += dt.timedelta(days=1)
    if len(expected) - 1 < minimum_forward_days:
        raise ValueError("provider calendar does not cover a full next-week window")

    rows = []
    for exchange in EXCHANGES:
        missing = [day for day in expected if day not in by_exchange[exchange]]
        if missing:
            raise ValueError(f"calendar has gaps for {exchange}: {missing[:5]}")
    for day in expected:
        sse = by_exchange["SSE"][day]
        szse = by_exchange["SZSE"][day]
        if sse.get("is_open") != szse.get("is_open"):
            raise ValueError(f"exchange open flags disagree on {day}")
        if sse.get("pretrade_date") != szse.get("pretrade_date"):
            raise ValueError(f"exchange pretrade dates disagree on {day}")
        rows.extend((sse, szse))
    return rows, accepted_end


def _canonical_hash(rows) -> str:
    packed = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(packed).hexdigest()


def refresh_calendar(
    *, db_path: Path, receipt_path: Path, start_date: str, requested_end: str,
    provider_rows: dict[str, list[dict[str, str | None]]], fetched_at: str,
    provider_request_sha256: str,
) -> dict:
    accepted, accepted_end = validate_calendar_rows(provider_rows, start_date, requested_end)
    conn = sqlite3.connect(f"file:{quote(str(db_path), safe='/')}?mode=rw", uri=True, timeout=5)
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        info = conn.execute(f"PRAGMA table_info({TABLE})").fetchall()
        expected_cols = {"cal_date", "exchange", "is_open", "pretrade_date", "snap_ts"}
        if not expected_cols.issubset({row[1] for row in info}):
            raise RuntimeError(f"{TABLE} schema is incomplete")
        pk = {row[1]: row[5] for row in info if row[5]}
        if pk.get("cal_date") != 1 or pk.get("exchange") != 2:
            raise RuntimeError(f"unexpected {TABLE} primary key")

        before = conn.execute(
            f"SELECT cal_date,exchange,is_open,pretrade_date,snap_ts FROM {TABLE} "
            "WHERE cal_date BETWEEN ? AND ? ORDER BY cal_date,exchange",
            (start_date, accepted_end),
        ).fetchall()
        before_hash = _canonical_hash(before)
        before_keys = {(r[0], r[1]) for r in before}
        expected_keys = {(r["cal_date"], r["exchange"]) for r in accepted}
        missing_before = sorted(expected_keys - before_keys)
        before_by_key = {(r[0], r[1]): (r[2], r[3]) for r in before}
        changed_existing_rows = sum(
            1 for row in accepted
            if (row["cal_date"], row["exchange"]) in before_by_key
            and before_by_key[(row["cal_date"], row["exchange"])]
            != (int(row["is_open"]), row.get("pretrade_date"))
        )
        now = dt.datetime.now(SHANGHAI).isoformat(timespec="seconds")
        receipt = {
            "status": "pending_transaction",
            "fetched_at": fetched_at,
            "written_at": now,
            "provider": "TushareClient.trade_cal",
            "provider_request_sha256": provider_request_sha256,
            "database": str(db_path),
            "table": TABLE,
            "range": {"start": start_date, "end": accepted_end, "requested_end": requested_end},
            "original_rows_in_range": before,
            "missing_keys_before": [list(k) for k in missing_before],
            "original_rows_sha256": before_hash,
            "accepted_provider_rows": len(accepted),
        }
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")

        values = [
            (r["cal_date"], r["exchange"], int(r["is_open"]), r.get("pretrade_date"), now)
            for r in accepted
        ]
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            f"INSERT INTO {TABLE}(cal_date,exchange,is_open,pretrade_date,snap_ts) "
            "VALUES(?,?,?,?,?) ON CONFLICT(cal_date,exchange) DO UPDATE SET "
            "is_open=excluded.is_open,pretrade_date=excluded.pretrade_date,snap_ts=excluded.snap_ts",
            values,
        )
        conn.commit()
        after = conn.execute(
            f"SELECT cal_date,exchange,is_open,pretrade_date,snap_ts FROM {TABLE} "
            "WHERE cal_date BETWEEN ? AND ? ORDER BY cal_date,exchange",
            (start_date, accepted_end),
        ).fetchall()
        receipt.update({
            "status": "committed",
            "upserted_rows": len(values),
            "changed_existing_keys": changed_existing_rows,
            "refreshed_existing_keys": len(before_keys & expected_keys),
            "after_rows_in_range": len(after),
            "after_rows_sha256": _canonical_hash(after),
            "sse_is_open_today": next(int(r["is_open"]) for r in accepted if r["exchange"] == "SSE" and r["cal_date"] == start_date),
        })
        receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
        return receipt
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-root", default=os.environ.get("TRADING_DATABASE_ROOT", str(Path.home() / "TradingDatabase")))
    parser.add_argument("--start-date", help="defaults to Asia/Shanghai today")
    parser.add_argument("--end-date", help="defaults to 90 days after start")
    parser.add_argument("--receipt", help="JSON audit receipt path; defaults to /private/tmp")
    args = parser.parse_args(argv)

    if not os.environ.get("TRADING_AGENT_TUSHARE_TOKEN"):
        parser.error("TRADING_AGENT_TUSHARE_TOKEN missing; load the configured private env before running")
    start = args.start_date or dt.datetime.now(SHANGHAI).strftime("%Y%m%d")
    start_day = dt.datetime.strptime(start, "%Y%m%d").date()
    end = args.end_date or (start_day + dt.timedelta(days=90)).strftime("%Y%m%d")
    fetched_at = dt.datetime.now(SHANGHAI).isoformat(timespec="seconds")

    # Import only after env validation: tushare_config reads the token at import.
    repo = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo))
    from coreClient.tushare_client import TushareClient

    result_rows = {}
    source_material = []
    for exchange in EXCHANGES:
        frame = TushareClient().trade_cal(exchange=exchange, start_date=start, end_date=end)
        rows = _rows_from_frame(exchange, frame)
        result_rows[exchange] = rows
        source_material.extend(rows)
    provider_hash = _canonical_hash(sorted(source_material, key=lambda r: (r["cal_date"], r["exchange"])))
    receipt = Path(args.receipt) if args.receipt else Path("/private/tmp") / f"tradecal_refresh_{start}_{dt.datetime.now(SHANGHAI):%H%M%S}.json"
    db_path = Path(args.database_root).expanduser() / "offline" / "db_cn_basic.db"
    done = refresh_calendar(
        db_path=db_path, receipt_path=receipt, start_date=start, requested_end=end,
        provider_rows=result_rows, fetched_at=fetched_at,
        provider_request_sha256=provider_hash,
    )
    print(json.dumps({"receipt": str(receipt), "status": done["status"], "range": done["range"],
                      "upserted_rows": done["upserted_rows"], "sse_is_open_today": done["sse_is_open_today"],
                      "after_rows_sha256": done["after_rows_sha256"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Update all eight dashboard indices from TDX's index-specific K-line decoder.

Canonical storage: TradingDatabase/offline/db_cn_index_minute.db. Legacy policy
index rows stay read-only. Both 1-minute and native 15-minute sessions are updated.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))
from coreClient.index_minute import INDEX_CODES, IndexMinuteDownloader, database_path, day_key
from service.common import latest_completed_trade_date


def validate_sessions(rows, basic_path, daily_path, end):
    days = sorted({row["trade_date"] for row in rows})
    with sqlite3.connect(basic_path) as conn:
        expected = {str(row[0]) for row in conn.execute("SELECT cal_date FROM tbl_cn_tradecal WHERE exchange='SSE' AND is_open=1 AND cal_date BETWEEN ? AND ?", (days[0], end))}
    missing = sorted(expected - set(days))
    if missing:
        raise ValueError(f"missing trading sessions: {missing}")
    if days[-1] != end:
        raise ValueError(f"source not current: {days[-1]} != {end}")
    code = rows[0]["ts_code"]
    closes = {row["trade_date"]: row["close"] for row in rows}
    with sqlite3.connect(daily_path) as conn:
        daily = conn.execute("SELECT trade_date,close FROM tbl_cn_index_daily WHERE ts_code=? AND trade_date BETWEEN ? AND ?", (code, days[0], end)).fetchall()
    differences = [(day, abs(closes[day] / float(close) - 1), abs(closes[day] - float(close))) for day, close in daily if day in closes and close]
    bad = [row for row in differences if row[1] > .001]
    if bad:
        raise ValueError(f"index close disagrees with daily reference (>0.1%): {bad[:8]}")
    return {"expected_sessions": len(expected), "missing_sessions": missing, "daily_close_checks": len(differences),
            "max_close_difference_points": max((row[2] for row in differences), default=0),
            "max_close_difference_pct": max((row[1] * 100 for row in differences), default=0)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--all-history", action="store_true", help="Download all complete history sessions the source currently retains")
    parser.add_argument("--ts-codes", default=",".join(INDEX_CODES))
    parser.add_argument("--max-pages", type=int, default=80)
    args = parser.parse_args()
    if args.trade_date and (args.start_date or args.end_date or args.all_history):
        parser.error("--trade-date cannot be combined with a date range/all-history")
    codes = [code.strip().upper() for code in args.ts_codes.split(",") if code.strip()]
    if not codes or any(code not in INDEX_CODES for code in codes):
        parser.error("only the eight configured dashboard indices are supported")
    data_root = database_path().parent
    basic_path, daily_path = data_root / "db_cn_basic.db", data_root / "db_cn_index.db"
    with sqlite3.connect(basic_path) as conn:
        end = day_key(args.trade_date or args.end_date) if (args.trade_date or args.end_date) else latest_completed_trade_date(conn)
    report = {"started_at": dt.datetime.now().astimezone().isoformat(), "end_date": end, "results": [], "errors": [],
              "volume_contract": "raw_volume is TDX index raw data, NOT shares; amount is source-published turnover in yuan and can use an exchange-wide scope"}
    downloader = IndexMinuteDownloader()
    try:
        for code in codes:
            for period in (1, 15):
                try:
                    start = args.trade_date or args.start_date
                    if not start and not args.all_history and database_path().exists():
                        with sqlite3.connect(database_path()) as conn:
                            last = conn.execute("SELECT MAX(trade_date) FROM tbl_cn_index_minute_ctrl WHERE ts_code=? AND period=?", (code, period)).fetchone()[0]
                        if last:
                            start = (dt.datetime.strptime(last, "%Y%m%d") - dt.timedelta(days=7)).strftime("%Y%m%d")
                    item = downloader.update(code, period, start, end, args.max_pages,
                        validator=lambda rows: validate_sessions(rows, basic_path, daily_path, end))
                    report["results"].append(item)
                    print(json.dumps(item, ensure_ascii=False), flush=True)
                except Exception as exc:
                    item = {"code": code, "period": period, "error": f"{type(exc).__name__}: {exc}"}
                    report["errors"].append(item)
                    print(json.dumps(item, ensure_ascii=False), file=sys.stderr, flush=True)
    finally:
        downloader.close()
    report["finished_at"] = dt.datetime.now().astimezone().isoformat()
    audit_root = data_root / "index_minute_downloads"
    audit_root.mkdir(exist_ok=True)
    audit_path = audit_root / (dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".json")
    audit_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print("audit=" + str(audit_path), flush=True)
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

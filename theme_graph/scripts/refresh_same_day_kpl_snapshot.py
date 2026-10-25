from __future__ import annotations
import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
from coreClient.kpl_client import KPLClient, add_exchange_suffix_series
from loguru import logger

p = argparse.ArgumentParser(description="Fetch same-day KPL limit-up API data without unrelated Tushare initialization")
p.add_argument("--trade-date", required=True, help="YYYYMMDD; same-day realtime API only")
p.add_argument("--db", default=str(Path.home()/"TradingDataBase/offline/db_cn_kpl.db"))
p.add_argument("--calendar-db", default=str(Path.home()/"TradingDataBase/offline/db_cn_basic.db"))
a = p.parse_args()
date = a.trade_date.replace("-", "")
if len(date) != 8 or not date.isdigit():
    raise SystemExit("trade-date must be YYYYMMDD")
now = datetime.now(ZoneInfo("Asia/Shanghai"))
if now.strftime("%Y%m%d") != date:
    raise SystemExit("this refresh script only calls the realtime API for the current Shanghai date")

cal = sqlite3.connect(f"file:{Path(a.calendar_db).expanduser()}?mode=ro", uri=True)
calrow = cal.execute("SELECT is_open FROM tbl_cn_tradecal WHERE exchange='SSE' AND cal_date=?", (date,)).fetchone()
cal.close()
if calrow is None or calrow[0] != 1:
    raise SystemExit(f"SSE calendar does not confirm {date} as an open day")

# Avoid leaking KPL credentials through the client's startup log.
logger.remove()
client = KPLClient()
frames = []
board_counts = {}
errors = []
for board_type in (1, 2, 3, 4, 5):
    try:
        result = client.call("HomeDingPan", "DailyLimitPerformance", PidType=board_type, Type=1, st=2000, Order=0)
        raw = result.get("info", []) if isinstance(result, dict) else result
        frame = client._parse_limit_performance(raw, now.strftime("%Y-%m-%d"), board_type)
        board_counts[str(board_type)] = len(frame)
        if len(frame):
            frames.append(frame)
    except Exception as e:
        errors.append(f"board_type={board_type}: {type(e).__name__}: {e}")
if errors:
    raise SystemExit("one or more board-type API requests failed; rejecting partial day: " + "; ".join(errors))
if not frames:
    raise SystemExit(f"KPL realtime API returned no rows for open day {date}; do not infer zero limit-ups")
df = pd.concat(frames, ignore_index=True)
df["ts_code"] = add_exchange_suffix_series(df["ts_code"])
codes = df["ts_code"].astype(str)
dupes = codes[codes.duplicated()].tolist()
if dupes:
    raise SystemExit("duplicate stock codes returned across board types: " + ",".join(dupes))
for value in df.get("lu_time", pd.Series(dtype=object)).dropna():
    try:
        epoch = int(float(value))
        source_date = datetime.fromtimestamp(epoch, timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    except Exception as e:
        raise SystemExit(f"unparseable realtime lu_time {value!r}: {e}")
    if source_date != date:
        raise SystemExit(f"KPL realtime lu_time belongs to {source_date}, expected {date}")

snap_ts = now.isoformat(timespec="seconds")
dbpath = Path(a.db).expanduser()
conn = sqlite3.connect(dbpath)
cols = [r[1] for r in conn.execute("PRAGMA table_info(tbl_cn_kpl_limit_performance)")]
required = {"trade_date", "ts_code", "name", "board_type", "board_count", "lu_time", "theme", "limit_reason", "is_break", "amplitude", "turnover_rate", "limit_order", "lu_limit_order", "net_change", "main_in", "main_out", "amount", "free_float", "close_price", "pct_chg", "board_period", "theme_id", "sector_id", "snap_ts"}
if not required.issubset(cols):
    raise SystemExit("KPL target table schema is missing required columns")
write_cols = [c for c in cols if c in required]
records=[]
for row in df.to_dict(orient="records"):
    row["trade_date"] = date
    row["snap_ts"] = snap_ts
    row["ts_code"] = str(row["ts_code"])
    records.append(tuple(None if pd.isna(row.get(c)) else row.get(c) for c in write_cols))
placeholders=",".join("?" for _ in write_cols)
col_sql=",".join(f'"{c}"' for c in write_cols)
updates=",".join(f'"{c}"=excluded."{c}"' for c in write_cols if c not in ("trade_date","ts_code"))
conn.execute(f"INSERT INTO tbl_cn_kpl_limit_performance ({col_sql}) VALUES ({placeholders}) ON CONFLICT(trade_date,ts_code) DO UPDATE SET {updates}", records[0])
for row in records[1:]:
    conn.execute(f"INSERT INTO tbl_cn_kpl_limit_performance ({col_sql}) VALUES ({placeholders}) ON CONFLICT(trade_date,ts_code) DO UPDATE SET {updates}", row)
conn.commit()
count, latest = conn.execute("SELECT count(*), max(snap_ts) FROM tbl_cn_kpl_limit_performance WHERE trade_date=?", (date,)).fetchone()
conn.close()
if int(count) != len(records) or latest != snap_ts:
    raise SystemExit(f"post-write verification mismatch: api_rows={len(records)}, db_rows={count}, latest_snap_ts={latest}")
print(__import__("json").dumps({"status":"ok","trade_date":date,"api_rows":len(records),"board_type_counts":board_counts,"snap_ts":snap_ts,"database":str(dbpath)},ensure_ascii=False))

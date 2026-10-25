from __future__ import annotations
import argparse
import json
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

p = argparse.ArgumentParser(description="Build today's limit-up packet from same-day KPL API capture")
p.add_argument("--db", default=str(Path.home() / "TradingDataBase/offline/db_cn_kpl.db"))
p.add_argument("--out", required=True)
p.add_argument("--trade-date", required=True, help="YYYYMMDD")
a = p.parse_args()
date = a.trade_date.replace("-", "")
if len(date) != 8 or not date.isdigit():
    raise SystemExit("trade-date must be YYYYMMDD")
uri = f"file:{Path(a.db).expanduser()}?mode=ro"
conn = sqlite3.connect(uri, uri=True)
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT trade_date, ts_code, name, board_type, board_count, lu_time, theme,
           limit_reason, is_break, amplitude, turnover_rate, limit_order,
           lu_limit_order, net_change, main_in, main_out, amount, free_float,
           close_price, pct_chg, board_period, theme_id, sector_id, snap_ts
      FROM tbl_cn_kpl_limit_performance
     WHERE trade_date = ?
     ORDER BY board_count DESC, lu_time ASC, ts_code ASC
""", (date,)).fetchall()
if not rows:
    raise SystemExit(f"no same-day KPL API data for {date}; do not infer zero limit-ups")
if len({r["ts_code"] for r in rows}) != len(rows):
    raise SystemExit(f"duplicate stock codes in KPL source for {date}")
snapshots = sorted({str(r["snap_ts"]) for r in rows if r["snap_ts"]})
if not snapshots:
    raise SystemExit("KPL rows have no snap_ts; source freshness cannot be verified")

def clean(v):
    if v is None:
        return None
    if isinstance(v, (int, float, str)):
        return v
    return str(v)

def format_time(value):
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    try:
        numeric = int(float(raw))
    except ValueError:
        return raw if len(raw.split(":")) in (2, 3) else None
    if abs(numeric) >= 100_000_000:
        return datetime.fromtimestamp(numeric, ZoneInfo("Asia/Shanghai")).strftime("%H:%M:%S")
    return raw

limitups = []
theme_counts = Counter()
for r in rows:
    theme = (r["theme"] or "").strip()
    reason = (r["limit_reason"] or "").strip()
    primary = theme or "其他/归因待核"
    theme_counts[primary] += 1
    limitups.append({
        "ts_code": r["ts_code"], "name": r["name"] or "未知",
        "status_raw": None, "continuous_board": r["board_count"],
        "lu_time": format_time(r["lu_time"]), "ld_time": None,
        "open_time": None, "last_time": None, "lu_desc": primary,
        "theme_raw": reason, "normalized_themes": theme,
        "amount": r["amount"], "turnover_rate": r["turnover_rate"],
        "bid_amount": r["lu_limit_order"], "is_break": r["is_break"],
        "board_period": r["board_period"], "amplitude": r["amplitude"],
        "close_price": r["close_price"], "pct_chg": r["pct_chg"],
        "source_snap_ts": r["snap_ts"],
    })
packet = {
    "trade_date": date, "cutoff": "收盘后；以同日 KPL API 捕获快照为准（Asia/Shanghai）",
    "source": "TradingDataBase/offline/db_cn_kpl.db tbl_cn_kpl_limit_performance; KPLClient HomeDingPan/DailyLimitPerformance same-day endpoint",
    "source_kind": "same_day_kpl_api",
    "source_snapshot_ts": snapshots[-1],
    "limitup_stock_count": len(limitups), "limitups": limitups,
    "theme_metrics": [
        {"canonical_name": name, "limit_up_count": count,
         "metric_source": "same-day KPL API primary theme label; not canonical DuckDB theme breadth"}
        for name, count in sorted(theme_counts.items(), key=lambda x: (-x[1], x[0]))
    ],
    "theme_metrics_note": "API primary-theme label counts only; offline canonical breadth, ladder points, height and lifecycle metrics are not yet available.",
}
out = Path(a.out).expanduser()
out.mkdir(parents=True, exist_ok=True)
target = out / f"{date}.json"
target.write_text(json.dumps(packet, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
print(json.dumps({"path": str(target), "trade_date": date, "rows": len(limitups), "themes": len(theme_counts), "source_snapshot_ts": snapshots[-1], "source_kind": packet["source_kind"]}, ensure_ascii=False))

"""Persisted index week/month OHLCV, rebuilt offline from canonical daily rows.

Stored vol/amount use index-daily units (lots/thousand yuan). Readers are read-only.
Current-period tick overlays are in memory and never write unfinished bars to SQLite.
"""
from __future__ import annotations
import argparse
import calendar
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import sqlite3

PERIOD_TABLES = {"weekly": "tbl_cn_index_weekly", "monthly": "tbl_cn_index_monthly"}
PRICE_FIELDS = ("open", "high", "low", "close")
DATA_FIELDS = (*PRICE_FIELDS, "vol", "amount")
COLUMNS = ("ts_code", "period_key", "period_start", "period_end", "trade_date", "first_trade_date",
           *DATA_FIELDS, "pre_close", "change", "pct_chg", "trading_days",
           *("before_last_"+key for key in (*DATA_FIELDS, "trade_date", "trading_days")),
           "source_hash", "snap_ts")


def _schema():
    statements = []
    for table in PERIOD_TABLES.values():
        statements.append(f"""CREATE TABLE IF NOT EXISTS {table} (
            ts_code TEXT NOT NULL, period_key TEXT NOT NULL, period_start TEXT NOT NULL,
            period_end TEXT NOT NULL, trade_date TEXT NOT NULL, first_trade_date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            vol REAL NOT NULL, amount REAL NOT NULL, pre_close REAL, change REAL, pct_chg REAL,
            trading_days INTEGER NOT NULL,
            before_last_open REAL, before_last_high REAL, before_last_low REAL, before_last_close REAL,
            before_last_vol REAL NOT NULL, before_last_amount REAL NOT NULL,
            before_last_trade_date TEXT, before_last_trading_days INTEGER NOT NULL,
            source_hash TEXT NOT NULL, snap_ts TEXT NOT NULL,
            PRIMARY KEY(ts_code,period_key))""")
        statements.append(f"CREATE INDEX IF NOT EXISTS ix_{table}_date ON {table}(trade_date,ts_code)")
        statements.append(f"CREATE INDEX IF NOT EXISTS ix_{table}_code_date ON {table}(ts_code,trade_date)")
    return ";\n".join(statements)+";\n"


SCHEMA_SQL_INDEX_PERIOD = _schema()


def database_path():
    from coreClient._paths import offline_data_dir
    return offline_data_dir()/"db_cn_index.db"


def day_key(value):
    if isinstance(value, (dt.date, dt.datetime)): return value.strftime("%Y%m%d")
    value = str(value).replace("-", "")
    dt.datetime.strptime(value, "%Y%m%d")
    return value


def _period(value):
    if value not in PERIOD_TABLES: raise ValueError("index period must be weekly or monthly")
    return value


def period_bounds(day, period):
    date = dt.datetime.strptime(day_key(day), "%Y%m%d").date()
    if _period(period) == "weekly":
        start = date-dt.timedelta(days=date.weekday()); end = start+dt.timedelta(days=6)
        key = start.strftime("%Y%m%d")
    else:
        start = date.replace(day=1); end = date.replace(day=calendar.monthrange(date.year,date.month)[1])
        key = date.strftime("%Y%m")
    return key, start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def valid_ohlc(row):
    return all(isinstance(row.get(key),(int,float)) and math.isfinite(row[key]) and row[key]>0 for key in PRICE_FIELDS)


def _number(value):
    return float(value) if isinstance(value,(int,float)) and math.isfinite(value) else 0.0


def _summarize(rows):
    if not rows: return {**dict.fromkeys(PRICE_FIELDS), "vol":0.0,"amount":0.0,"trade_date":None,"trading_days":0}
    return {"open":rows[0]["open"],"high":max(row["high"] for row in rows),
            "low":min(row["low"] for row in rows),"close":rows[-1]["close"],
            "vol":sum(_number(row.get("vol")) for row in rows),
            "amount":sum(_number(row.get("amount")) for row in rows),
            "trade_date":rows[-1]["trade_date"],"trading_days":len(rows)}


def _period_source_hash(rows, previous_close):
    prior = previous_close if previous_close is not None else rows[0].get("pre_close")
    prior = float(prior) if isinstance(prior,(int,float)) and math.isfinite(prior) and prior>0 else None
    signature = [[row.get(key) for key in ("trade_date",*DATA_FIELDS)] for row in rows]
    return hashlib.sha256(json.dumps([prior,signature],separators=(",",":"),allow_nan=False).encode()).hexdigest()


def _record(code, period, rows, previous_close, stamp):
    key,start,end = period_bounds(rows[0]["trade_date"],period)
    total = _summarize(rows); prefix = _summarize(rows[:-1])
    prior = previous_close if previous_close is not None else rows[0].get("pre_close")
    prior = float(prior) if isinstance(prior,(int,float)) and math.isfinite(prior) and prior>0 else None
    change = total["close"]-prior if prior is not None else None
    values = {"ts_code":code,"period_key":key,"period_start":start,"period_end":end,
              "first_trade_date":rows[0]["trade_date"],**total,"pre_close":prior,"change":change,
              "pct_chg":change/prior*100 if prior is not None else None,"snap_ts":stamp}
    values.update({"before_last_"+key:value for key,value in prefix.items()})
    values["source_hash"] = _period_source_hash(rows, previous_close)
    return values


class IndexPeriodStore:
    def __init__(self, path=None): self.path = Path(path) if path else database_path()

    def _connect(self, write=False):
        if not self.path.is_file(): raise FileNotFoundError(f"Index database does not exist: {self.path}")
        conn = sqlite3.connect(self.path.resolve().as_uri()+("?mode=rw" if write else "?mode=ro"),uri=True,timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def refresh(self, ts_codes=None):
        """Read one consistent daily snapshot; upsert changed derived periods atomically.

        Scan daily history offline so historical corrections/backfills are reflected as well.
        Never delete or alter canonical daily observations. Identical periods are not rewritten.
        """
        with self._connect(True) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            for statement in SCHEMA_SQL_INDEX_PERIOD.split(";"):
                if statement.strip(): conn.execute(statement)
            conn.commit(); conn.execute("BEGIN IMMEDIATE")
            codes = sorted(set(ts_codes or [row[0] for row in conn.execute("SELECT DISTINCT ts_code FROM tbl_cn_index_daily WHERE ts_code IS NOT NULL")]))
            stamp = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(timespec="seconds")
            report = {"codes":len(codes),"weekly":0,"monthly":0,"changed":0,"reaggregated":0,"reused":0,"excluded_daily_rows":0,"latest_date":None}
            for code in codes:
                raw = [dict(row) for row in conn.execute("SELECT trade_date,open,high,low,close,pre_close,vol,amount FROM tbl_cn_index_daily WHERE ts_code=? ORDER BY trade_date",(code,))]
                rows = [row for row in raw if valid_ohlc(row)]
                report["excluded_daily_rows"] += len(raw)-len(rows)
                if not rows: continue
                report["latest_date"] = max(report["latest_date"] or "",rows[-1]["trade_date"])
                for period,table in PERIOD_TABLES.items():
                    groups = {}
                    for row in rows: groups.setdefault(period_bounds(row["trade_date"],period)[0],[]).append(row)
                    existing_hashes={row[0]:row[1] for row in conn.execute(f"SELECT period_key,source_hash FROM {table} WHERE ts_code=?",(code,))}
                    records=[]; previous_close=None
                    for period_key,group in groups.items():
                        # Validate all source groups so historical repairs are detected,
                        # but aggregate OHLCV/prefixes only for changed periods.
                        if existing_hashes.get(period_key)==_period_source_hash(group,previous_close):
                            report["reused"]+=1
                        else:
                            records.append(_record(code,period,group,previous_close,stamp))
                            report["reaggregated"]+=1
                        previous_close=group[-1]["close"]
                    before=conn.total_changes
                    updates=",".join(f"{key}=excluded.{key}" for key in COLUMNS if key not in ("ts_code","period_key"))
                    sql=f"INSERT INTO {table} ({','.join(COLUMNS)}) VALUES ({','.join('?' for _ in COLUMNS)}) ON CONFLICT(ts_code,period_key) DO UPDATE SET {updates} WHERE {table}.source_hash<>excluded.source_hash"
                    conn.executemany(sql,[[record.get(key) for key in COLUMNS] for record in records])
                    existing={row[0] for row in conn.execute(f"SELECT period_key FROM {table} WHERE ts_code=?",(code,))}
                    conn.executemany(f"DELETE FROM {table} WHERE ts_code=? AND period_key=?",[(code,key) for key in existing-set(groups)])
                    report["changed"] += conn.total_changes-before; report[period] += len(groups)
            conn.commit()
        return report

    def rows(self, period, ts_code=None, trade_date=None, start_date=None, end_date=None, limit=None):
        table=PERIOD_TABLES[_period(period)];where=[];params=[]
        if ts_code:
            codes=[ts_code] if isinstance(ts_code,str) else list(ts_code)
            where.append("ts_code IN ("+",".join("?" for _ in codes)+")");params.extend(codes)
        for field,value,operator in (("trade_date",trade_date,"="),("trade_date",start_date,">="),("trade_date",end_date,"<=")):
            if value: where.append(field+operator+"?");params.append(day_key(value))
        sql=f"SELECT {','.join(COLUMNS)} FROM {table}"+(" WHERE "+" AND ".join(where) if where else "")+" ORDER BY trade_date DESC,ts_code"
        if limit is not None:
            if int(limit)<1: raise ValueError("limit must be positive")
            sql+=" LIMIT ?";params.append(int(limit))
        with self._connect() as conn: return [dict(row) for row in conn.execute(sql,params)]

    def read(self, period, **kwargs):
        import pandas as pd
        result=pd.DataFrame(self.rows(period,**kwargs),columns=COLUMNS)
        result.attrs["units"]={"vol":"lots","amount":"thousand_yuan"}
        result.attrs["source"]="daily_preaggregated"
        return result

    def as_of_rows(self, code, period, end_date, start_date=None, limit=None):
        """Reuse stored periods; fit only one partial period for historical review dates."""
        day=day_key(end_date);table=PERIOD_TABLES[_period(period)]
        rows=self.rows(period,ts_code=code,start_date=start_date,end_date=day,limit=limit)
        key,start,_=period_bounds(day,period)
        with self._connect() as conn:
            current=conn.execute(f"SELECT * FROM {table} WHERE ts_code=? AND period_key=?",(code,key)).fetchone()
            if current and current["trade_date"]>day:
                daily=[dict(row) for row in conn.execute("SELECT trade_date,open,high,low,close,pre_close,vol,amount FROM tbl_cn_index_daily WHERE ts_code=? AND trade_date BETWEEN ? AND ? ORDER BY trade_date",(code,start,day))]
                daily=[row for row in daily if valid_ohlc(row)]
                if daily: rows.insert(0,_record(code,period,daily,current["pre_close"],current["snap_ts"]))
        return sorted(rows[:limit] if limit else rows,key=lambda row:row["trade_date"])


def overlay_index_period(rows, tick_daily, period):
    """Overlay a native-unit daily tick on saved period bars without aggregating daily history.

    The saved before-last-day prefix prevents double-counting if offline data already
    contains today's daily bar. Calling repeatedly with newer ticks is idempotent.
    """
    output=[dict(row) for row in rows]
    if not tick_daily or not valid_ohlc(tick_daily): return output
    day=day_key(tick_daily["trade_date"]);key,start,end=period_bounds(day,period)
    existing=next((row for row in output if row["period_key"]==key),None)
    if existing and existing["trade_date"]>day: raise ValueError("Period contains future daily data")
    if existing and existing["trade_date"]==day:
        base={field:existing.get("before_last_"+field) for field in (*DATA_FIELDS,"trade_date","trading_days")}
    elif existing: base={field:existing.get(field) for field in (*DATA_FIELDS,"trade_date","trading_days")}
    else: base=_summarize([])
    has_base=bool(base.get("trading_days"))
    previous=existing.get("pre_close") if existing else (output[-1]["close"] if output else tick_daily.get("pre_close"))
    row={**(existing or {}),"ts_code":tick_daily["ts_code"],"period_key":key,"period_start":start,"period_end":end,
         "trade_date":day,"first_trade_date":existing["first_trade_date"] if existing and has_base else day,
         "open":base["open"] if has_base else tick_daily["open"],
         "high":max(base["high"],tick_daily["high"]) if has_base else tick_daily["high"],
         "low":min(base["low"],tick_daily["low"]) if has_base else tick_daily["low"],"close":tick_daily["close"],
         "vol":_number(base.get("vol"))+_number(tick_daily.get("vol")),
         "amount":_number(base.get("amount"))+_number(tick_daily.get("amount")),
         "trading_days":int(base.get("trading_days") or 0)+1,"pre_close":previous,"source":"realtime_overlay"}
    row["change"]=row["close"]-previous if previous else None
    row["pct_chg"]=row["change"]/previous*100 if previous else None
    row.update({"before_last_"+field:value for field,value in base.items()})
    output=[old for old in output if old["period_key"]!=key]+[row]
    return sorted(output,key=lambda record:record["trade_date"])


def main():
    parser=argparse.ArgumentParser(description="Rebuild persisted index weekly/monthly bars from local daily data")
    parser.add_argument("--codes",nargs="*",help="Optional index codes; default all stored indices")
    args=parser.parse_args()
    print(json.dumps(IndexPeriodStore().refresh(args.codes),ensure_ascii=False))


if __name__=="__main__": main()

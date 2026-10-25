"""Create isolated SYNTHETIC market inputs; refuse an existing data directory.

Uses production schema and the real historical packet builder. No API/credentials needed.
"""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "offlineDataManager/scripts"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    if root.exists():
        parser.error("Use a new directory; existing databases are never overwritten")
    root.mkdir(parents=True)
    os.environ["TRADING_DATABASE_ROOT"] = str(root)
    from config.settings import SCHEMA_SQL_BASIC, SCHEMA_SQL_KPL, SCHEMA_SQL_INDEX
    import duckdb
    offline = root / "offline"; offline.mkdir()
    # An explicitly empty minute dataset is distinct from a missing external disk.
    (offline / "oneMinute/parquet/schema_v2").mkdir(parents=True)
    from core.tdx_fifteen_min_store import ensure_fifteen_min_schema
    ensure_fifteen_min_schema(offline / "db_fifteenMinute.db")
    for name, schema in [("db_cn_basic.db", SCHEMA_SQL_BASIC), ("db_cn_kpl.db", SCHEMA_SQL_KPL), ("db_cn_index.db", SCHEMA_SQL_INDEX)]:
        with sqlite3.connect(offline / name) as conn:
            conn.executescript(schema)
    names = {"600000.SH": "演示甲", "000001.SZ": "演示乙"}
    days = ["20261014", "20261015", "20261016"]
    events = []
    with sqlite3.connect(offline / "db_cn_basic.db") as conn:
        for code, name in names.items():
            conn.execute("INSERT INTO tbl_cn_basic(ts_code,symbol,name,list_status) VALUES (?,?,?,'L')", (code, code[:6], name))
            for index, day in enumerate(days):
                previous = 10 if index < 2 or code == "000001.SZ" else 11
                sealed = index == 2 or index == 1 and code == "600000.SH"
                close = round(previous * 1.1, 2) if sealed else previous
                conn.execute("INSERT INTO tbl_cn_day(ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount) VALUES (?,?,?,?,?,?,?,?,?,?)", (code,day,previous,close,previous*.99,close,previous,10 if sealed else 0,10000,10000))
                conn.execute("INSERT INTO tbl_cn_adj_factor(ts_code,trade_date,adj_factor) VALUES (?,?,1)", (code,day))
                conn.execute("INSERT INTO tbl_cn_stk_limit(ts_code,trade_date,up_limit,down_limit) VALUES (?,?,?,?)", (code,day,round(previous*1.1,2),round(previous*.9,2)))
                conn.execute("INSERT INTO tbl_cn_daily_basic(ts_code,trade_date,turnover_rate_f,total_mv,circ_mv,volume_ratio) VALUES (?,?,5,500000,300000,1.2)", (code,day))
                if sealed:
                    board = 2 if code == "600000.SH" and index == 2 else 1
                    events.append((day,code,name,board,"09:30:00" if code == "600000.SH" else "09:35:00"))
        for index,day in enumerate(days):
            conn.execute("INSERT INTO tbl_cn_tradecal(cal_date,exchange,is_open,pretrade_date) VALUES (?,'SSE',1,?)", (day,days[max(0,index-1)]))
    with sqlite3.connect(offline / "db_cn_kpl.db") as conn:
        # The production schema may be extended by the KPL client on first write.
        columns={r[1] for r in conn.execute("PRAGMA table_info(tbl_cn_kpl_limit_performance)")}
        for name,kind in [("board_period","TEXT")]:
            if name not in columns: conn.execute(f"ALTER TABLE tbl_cn_kpl_limit_performance ADD COLUMN {name} {kind}")
        for day,code,name,board,time in events:
            conn.execute("INSERT INTO tbl_cn_kpl_limit_performance(trade_date,ts_code,name,theme,limit_reason,board_count,board_period,lu_time) VALUES (?,?,?,?,?,?,?,?)", (day,code,name,"示例题材A","演示行情标签，与分析结论不同",board,"首板" if board==1 else f"{board}连板",time))
    # Minimal input fact tables consumed by the existing historical packet builder.
    with duckdb.connect(str(offline / "db_theme_graph.duckdb")) as conn:
        conn.execute("CREATE TABLE fact_limit_event(event_id VARCHAR,trade_date VARCHAR,ts_code VARCHAR,name VARCHAR,status_raw VARCHAR,continuous_board INTEGER,lu_time VARCHAR,ld_time VARCHAR,open_time VARCHAR,last_time VARCHAR,lu_desc VARCHAR,theme_raw VARCHAR,amount DOUBLE,turnover_rate DOUBLE,bid_amount DOUBLE,tag VARCHAR)")
        conn.execute("CREATE TABLE dim_stock(ts_code VARCHAR,name VARCHAR)")
        conn.execute("CREATE TABLE rel_limit_theme(event_id VARCHAR,source_label VARCHAR)")
        conn.execute("CREATE TABLE dim_theme(theme_id VARCHAR,canonical_name VARCHAR)")
        conn.execute("CREATE TABLE fact_theme_daily(trade_date VARCHAR,theme_id VARCHAR,limit_up_count INTEGER,break_count INTEGER,max_board_height INTEGER,first_board_count INTEGER,multi_board_count INTEGER,ladder_points DOUBLE,seal_rate DOUBLE,heat_score DOUBLE,lifecycle_state VARCHAR)")
        conn.execute("INSERT INTO dim_theme VALUES ('DEMO','示例题材A')")
        for code,name in names.items(): conn.execute("INSERT INTO dim_stock VALUES (?,?)", [code,name])
        for day,code,name,board,time in events:
            conn.execute("INSERT INTO fact_limit_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [day+code,day,code,name,"首板" if board==1 else f"{board}连板",board,time,None,None,time,"示例题材A","示例题材A",10000000,5,1000000,"涨停"])
        for day,count,height in [("20261015",1,1),("20261016",2,2)]:
            conn.execute("INSERT INTO fact_theme_daily VALUES (?,?,?,?,?,?,?,?,?,?,?)", [day,"DEMO",count,0,height,1,count-1,height,1,1,"synthetic"])
    reports=root / "theme_graph/每日涨停原因"; reports.mkdir(parents=True)
    for day in days[1:]: shutil.copy2(Path(__file__).with_name(day+'.md'), reports/(day+'.md'))
    subprocess.run([sys.executable,str(ROOT/'theme_graph/scripts/build_daily_limitup_packet.py'),"--db",str(offline/'db_theme_graph.duckdb'),"--out",str(reports/'分析输入包'),"--start","20261015","--end","20261016"],check=True)
    print("SYNTHETIC data ready:", root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

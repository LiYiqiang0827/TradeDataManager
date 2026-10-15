import importlib.util
import sqlite3
from pathlib import Path


SCRIPT = Path(__file__).with_name("audit_core_daily_tables.py")
SPEC = importlib.util.spec_from_file_location("audit_core_daily_tables", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_audit_profiles_domains_calendar_and_cross_table_keys(tmp_path, monkeypatch):
    db_root = tmp_path / "database"
    offline = db_root / "offline"
    offline.mkdir(parents=True)
    db = offline / "db_cn_basic.db"
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE tbl_cn_tradecal (cal_date TEXT, exchange TEXT, is_open INTEGER, pretrade_date TEXT, snap_ts TEXT, PRIMARY KEY(cal_date,exchange));
            CREATE TABLE tbl_cn_day (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, vol REAL, amount REAL, PRIMARY KEY(ts_code,trade_date));
            CREATE TABLE tbl_cn_daily_basic (ts_code TEXT, trade_date TEXT, close REAL, total_share REAL, float_share REAL, free_share REAL, total_mv REAL, circ_mv REAL, pe REAL, pb REAL, PRIMARY KEY(ts_code,trade_date));
            CREATE TABLE tbl_cn_adj_factor (ts_code TEXT, trade_date TEXT, adj_factor REAL, PRIMARY KEY(ts_code,trade_date));
            CREATE TABLE tbl_cn_suspend (trade_date TEXT, ts_code TEXT, suspend_timing TEXT, suspend_type TEXT, PRIMARY KEY(trade_date,ts_code));
        """)
        conn.execute("INSERT INTO tbl_cn_tradecal VALUES ('20260512','SSE',1,'20260511','test')")
        conn.execute("INSERT INTO tbl_cn_day VALUES ('000001.SZ','20260512',10,9,11,8,-1,-2)")
        conn.execute("INSERT INTO tbl_cn_daily_basic VALUES ('000001.SZ','20260512',8,1,1,1,1,1,-1,-1)")
        conn.execute("INSERT INTO tbl_cn_adj_factor VALUES ('600000.SH','20260512',0)")
    monkeypatch.setattr(module, "database_root", lambda: db_root)
    output = tmp_path / "audit.json"
    result = module.run("20260512", "20260512", output)
    daily = result["tables"]["daily"]["profile"]
    basic = result["tables"]["daily_basic"]["profile"]
    factor = result["tables"]["adj_factor"]["profile"]
    assert daily["domain_violation_counts_in_requested_range"] == {
        "invalid_ohlc_domain": 1, "negative_volume_or_amount": 1,
    }
    assert basic["domain_violation_counts_in_requested_range"]["nonpositive_close"] == 0
    assert factor["domain_violation_counts_in_requested_range"]["nonpositive_factor"] == 1
    assert result["cross_table_key_coverage"]["daily_missing_daily_basic"]["missing_key_count"] == 0
    assert result["cross_table_key_coverage"]["daily_missing_adj_factor"]["missing_key_count"] == 1
    assert result["cross_table_key_coverage"]["adj_factor_missing_daily"]["missing_key_count"] == 1

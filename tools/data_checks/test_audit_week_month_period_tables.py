import importlib.util
from pathlib import Path

import duckdb
import pytest

MODULE_PATH = Path(__file__).with_name("audit_week_month_period_tables.py")
spec = importlib.util.spec_from_file_location("period_audit", MODULE_PATH)
period_audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(period_audit)


@pytest.fixture
def period_source():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE daily_values(ts_code VARCHAR,trade_date VARCHAR,open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,vol DOUBLE,amount DOUBLE)")
    con.execute("CREATE TABLE factor_values(ts_code VARCHAR,trade_date VARCHAR,adj_factor DOUBLE)")
    con.executemany("INSERT INTO daily_values VALUES(?,?,?,?,?,?,?,?)", [
        ("000001.SZ", "20201231", 9.0, 11.0, 8.0, 10.0, 100.0, 1000.0),
        ("000001.SZ", "20210101", 10.0, 13.0, 9.0, 12.0, 200.0, 2400.0),
        ("000001.SZ", "20210104", 12.0, 14.0, 10.0, 10.5, 300.0, 3150.0),
    ])
    con.executemany("INSERT INTO factor_values VALUES(?,?,?)", [
        ("000001.SZ", "20201231", 2.0),
        ("000001.SZ", "20210101", 3.0),
        ("000001.SZ", "20210104", 6.0),
    ])
    yield con
    con.close()


def test_iso_week_crosses_calendar_year_and_computes_previous_period(period_source):
    con = period_source
    table = period_audit._create_expected(con, "weekly", "origin")
    rows = con.execute(f'SELECT trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount FROM "{table}" ORDER BY trade_date').fetchall()
    assert len(rows) == 2
    assert rows[0] == ("20210101", 9.0, 13.0, 8.0, 12.0, None, None, None, 300.0, 3400.0)
    assert rows[1] == ("20210104", 12.0, 14.0, 10.0, 10.5, 12.0, -1.5, -12.5, 300.0, 3150.0)


def test_qfq_adjusts_prices_and_reverses_volume_but_not_amount(period_source):
    con = period_source
    table = period_audit._create_expected(con, "weekly", "qfq")
    rows = con.execute(f'SELECT trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount FROM "{table}" ORDER BY trade_date').fetchall()
    assert len(rows) == 2
    first, second = rows
    assert first[0] == "20210101"
    assert first[1:5] == pytest.approx((3.0, 6.5, 8.0 / 3.0, 6.0))
    assert first[5:8] == (None, None, None)
    assert first[8:] == (700.0, 3400.0)
    assert second[0] == "20210104"
    assert second[1:5] == pytest.approx((12.0, 14.0, 10.0, 10.5))
    assert second[5:] == pytest.approx((6.0, 4.5, 75.0, 300.0, 3150.0))


def test_monthly_aggregation_uses_calendar_month_and_prior_period_close(period_source):
    con = period_source
    table = period_audit._create_expected(con, "monthly", "origin")
    rows = con.execute(f'SELECT trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount FROM "{table}" ORDER BY trade_date').fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "20201231"
    assert rows[1] == ("20210104", 10.0, 14.0, 9.0, 10.5, 10.0, 0.5, 5.0, 500.0, 5550.0)

import os
import sqlite3

import pytest


os.environ.setdefault("TRADING_AGENT_TUSHARE_TOKEN", "test-token-not-used")

from config.settings import SCHEMA_SQL_BASIC
from core.offline_db_client import get_ctrl, get_month, get_week, replace_tables, update_ctrl
from core.offline_downloader import CNDataDown
from core.periodic_kline_rebuild import rebuild_period_tables


def build_downloader():
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA_SQL_BASIC)
    day_rows = [
        ("000001.SZ", "20260105", 100, 110, 90, 105, 100, 1000),
        ("000001.SZ", "20260109", 106, 120, 100, 115, 200, 2000),
        ("000001.SZ", "20260112", 60, 65, 55, 62, 300, 3000),
        ("000001.SZ", "20260116", 63, 70, 60, 68, 400, 4000),
    ]
    conn.executemany(
        "INSERT INTO tbl_cn_day "
        "(ts_code,trade_date,open,high,low,close,vol,amount) "
        "VALUES (?,?,?,?,?,?,?,?)",
        day_rows,
    )
    conn.executemany(
        "INSERT INTO tbl_cn_adj_factor (ts_code,trade_date,adj_factor) VALUES (?,?,?)",
        [
            ("000001.SZ", "20260105", 1.0),
            ("000001.SZ", "20260109", 1.0),
            ("000001.SZ", "20260112", 2.0),
            ("000001.SZ", "20260116", 2.0),
        ],
    )
    conn.commit()
    downloader = CNDataDown.__new__(CNDataDown)
    downloader.conn_basic = conn
    return downloader, conn


def test_week_update_writes_qfq_and_origin_tables():
    downloader, conn = build_downloader()
    try:
        inserted = downloader.update_week("20260105", "20260116")

        adjusted = get_week(ts_code="000001.SZ", qfq=True, conn=conn)
        origin = get_week(ts_code="000001.SZ", qfq=False, conn=conn)

        assert inserted == 2
        assert len(adjusted) == len(origin) == 2
        assert origin.iloc[0]["open"] == pytest.approx(100)
        assert origin.iloc[0]["high"] == pytest.approx(120)
        assert origin.iloc[0]["close"] == pytest.approx(115)
        assert origin.iloc[0]["vol"] == pytest.approx(300)
        assert adjusted.iloc[0]["open"] == pytest.approx(50)
        assert adjusted.iloc[0]["high"] == pytest.approx(60)
        assert adjusted.iloc[0]["close"] == pytest.approx(57.5)
        assert adjusted.iloc[0]["vol"] == pytest.approx(600)
        assert origin.iloc[1]["pre_close"] == pytest.approx(115)
        assert adjusted.iloc[1]["pre_close"] == pytest.approx(57.5)
    finally:
        conn.close()


def test_month_update_writes_qfq_and_origin_tables():
    downloader, conn = build_downloader()
    try:
        inserted = downloader.update_month("20260105", "20260116")

        adjusted = get_month(ts_code="000001.SZ", qfq=True, conn=conn)
        origin = get_month(ts_code="000001.SZ", qfq=False, conn=conn)

        assert inserted == 1
        assert len(adjusted) == len(origin) == 1
        assert origin.iloc[0]["open"] == pytest.approx(100)
        assert origin.iloc[0]["high"] == pytest.approx(120)
        assert origin.iloc[0]["low"] == pytest.approx(55)
        assert origin.iloc[0]["close"] == pytest.approx(68)
        assert origin.iloc[0]["vol"] == pytest.approx(1000)
        assert adjusted.iloc[0]["open"] == pytest.approx(50)
        assert adjusted.iloc[0]["high"] == pytest.approx(70)
        assert adjusted.iloc[0]["low"] == pytest.approx(45)
        assert adjusted.iloc[0]["close"] == pytest.approx(68)
        assert adjusted.iloc[0]["vol"] == pytest.approx(1300)
    finally:
        conn.close()


def test_paired_replace_rolls_back_both_tables_on_failure():
    _, conn = build_downloader()
    try:
        conn.execute(
            "INSERT INTO tbl_cn_week (ts_code,trade_date,close) VALUES ('OLD','20260101',1)"
        )
        conn.execute(
            "INSERT INTO tbl_cn_week_origin (ts_code,trade_date,close) "
            "VALUES ('OLD','20260101',1)"
        )
        conn.commit()

        import pandas as pd

        good = pd.DataFrame([{"ts_code": "NEW", "trade_date": "20260102", "close": 2}])
        duplicate = pd.DataFrame(
            [
                {"ts_code": "NEW", "trade_date": "20260102", "close": 2},
                {"ts_code": "NEW", "trade_date": "20260102", "close": 3},
            ]
        )
        with pytest.raises(sqlite3.IntegrityError):
            replace_tables(
                conn,
                [("tbl_cn_week", good), ("tbl_cn_week_origin", duplicate)],
            )

        assert conn.execute("SELECT ts_code FROM tbl_cn_week").fetchone()[0] == "OLD"
        assert conn.execute("SELECT ts_code FROM tbl_cn_week_origin").fetchone()[0] == "OLD"
    finally:
        conn.close()


def test_basic_ctrl_never_moves_backwards():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE tbl_basic_ctrl (key TEXT PRIMARY KEY, max_date TEXT, updated_at TEXT)"
    )
    try:
        update_ctrl(conn, "cn_daily", "20260924")
        update_ctrl(conn, "cn_daily", "20260910")
        assert get_ctrl(conn, "cn_daily") == "20260924"
    finally:
        conn.close()


@pytest.mark.parametrize("freq,period", [("weekly", "week"), ("monthly", "month")])
@pytest.mark.parametrize("bounds", [(None, None), ("20251229", "20260112"), ("20260112", None)])
def test_batched_rebuild_matches_full_history_reference(freq, period, bounds):
    """Batching must not reset pre_close or change full-history qfq baselines."""
    import pandas as pd
    from pandas.testing import assert_frame_equal

    downloader, conn = build_downloader()
    try:
        conn.execute("DELETE FROM tbl_cn_day")
        conn.execute("DELETE FROM tbl_cn_adj_factor")
        day_rows, factors = [], []
        for stock in range(5):
            code = f"{stock:06d}.SZ"
            # Cross the ISO week-year boundary, with deliberately unsorted input.
            for position, date in enumerate(["20260112", "20251231", "20260102", "20251229", "20260105"]):
                price = 10.0 + stock + position / 10
                day_rows.append((code, date, price, price + 1, price - 1, price + .5, 100 + position, 1000 + position))
                if stock != 1 and date != "20260102":
                    factors.append((code, date, 1.0 + position / 10))
            # A latest factor outside the requested daily range remains the baseline.
            if stock != 1:
                factors.extend([(code, "20260202", 2.0), (code, "20260203", None)])
            # Invalid placeholders must not affect high/low or period counts.
            day_rows.append((code, "20260106", 0, 999, 0, 0, 999, 999))
        conn.executemany(
            "INSERT INTO tbl_cn_day (ts_code,trade_date,open,high,low,close,vol,amount) VALUES (?,?,?,?,?,?,?,?)",
            day_rows,
        )
        conn.executemany("INSERT INTO tbl_cn_adj_factor (ts_code,trade_date,adj_factor) VALUES (?,?,?)", factors)
        conn.commit()
        day = pd.read_sql_query("SELECT ts_code,trade_date,open,high,low,close,vol,amount FROM tbl_cn_day", conn)
        if all(bounds):
            day = day[day.trade_date.between(*bounds)]
        day = day.sort_values(["ts_code", "trade_date"])
        day = day[(day.open > 0) & (day.close > 0)].copy()
        expected_origin = downloader._aggregate_daily_to_freq(day, freq=freq)
        adj = pd.read_sql_query("SELECT ts_code,trade_date,adj_factor FROM tbl_cn_adj_factor ORDER BY ts_code,trade_date", conn)
        latest = adj.groupby("ts_code")["adj_factor"].last().reset_index(name="adj_factor_latest")
        day = day.merge(adj, on=["ts_code", "trade_date"], how="left").merge(latest, on="ts_code", how="left")
        ratio = day.adj_factor.fillna(1) / day.adj_factor_latest.fillna(1)
        for column in ["open", "high", "low", "close"]:
            day[column] *= ratio
        day["vol"] /= ratio
        expected_qfq = downloader._aggregate_daily_to_freq(day, freq=freq)

        counts = rebuild_period_tables(
            conn, freq=freq, aggregate=downloader._aggregate_daily_to_freq,
            start_date=bounds[0], end_date=bounds[1], batch_size=2,
        )
        for suffix, expected in [("", expected_qfq), ("_origin", expected_origin)]:
            table = f"tbl_cn_{period}{suffix}"
            actual = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY ts_code,trade_date", conn)
            columns = [column for column in actual.columns if column != "snap_ts"]
            assert_frame_equal(actual[columns], expected[columns], check_dtype=False)
            assert counts[table] == len(expected)
        assert not conn.in_transaction
        assert [row[1] for row in conn.execute("PRAGMA database_list")] == ["main"]
    finally:
        conn.close()


@pytest.mark.parametrize("failure", ["aggregate", "second_table"])
def test_batched_rebuild_preserves_pair_and_cleans_up_on_failure(failure):
    downloader, conn = build_downloader()
    try:
        for table in ["tbl_cn_week", "tbl_cn_week_origin"]:
            conn.execute(f"INSERT INTO {table} (ts_code,trade_date,close) VALUES ('OLD','20250101',99)")
        if failure == "second_table":
            conn.execute(
                "CREATE TRIGGER reject_origin BEFORE INSERT ON tbl_cn_week_origin "
                "BEGIN SELECT RAISE(ABORT, 'injected write failure'); END"
            )
        conn.commit()

        def aggregate(day, freq):
            if failure == "aggregate":
                raise RuntimeError("injected aggregation failure")
            return downloader._aggregate_daily_to_freq(day, freq)

        with pytest.raises((RuntimeError, sqlite3.IntegrityError), match="injected"):
            rebuild_period_tables(conn, freq="weekly", aggregate=aggregate, batch_size=1)
        for table in ["tbl_cn_week", "tbl_cn_week_origin"]:
            assert conn.execute(f"SELECT ts_code,close FROM {table}").fetchall() == [("OLD", 99)]
        assert not conn.in_transaction
        assert [row[1] for row in conn.execute("PRAGMA database_list")] == ["main"]
    finally:
        conn.close()


def test_batched_rebuild_empty_range_keeps_existing_pair():
    downloader, conn = build_downloader()
    try:
        for table in ["tbl_cn_month", "tbl_cn_month_origin"]:
            conn.execute(f"INSERT INTO {table} (ts_code,trade_date,close) VALUES ('OLD','20250101',99)")
        conn.commit()
        assert downloader.update_month("20270101", "20270131") == 0
        for table in ["tbl_cn_month", "tbl_cn_month_origin"]:
            assert conn.execute(f"SELECT ts_code,close FROM {table}").fetchall() == [("OLD", 99)]
        assert not conn.in_transaction
    finally:
        conn.close()


def test_batched_rebuild_limits_materialized_stocks(monkeypatch):
    import pandas as pd

    downloader, conn = build_downloader()
    try:
        for stock in range(1, 6):
            conn.execute(
                "INSERT INTO tbl_cn_day (ts_code,trade_date,open,high,low,close,vol,amount) "
                "VALUES (?, '20260105', 10, 11, 9, 10, 100, 1000)",
                (f"{stock + 1:06d}.SZ",),
            )
        conn.commit()
        original = pd.read_sql_query
        observed = []

        def bounded_read(sql, *args, **kwargs):
            result = original(sql, *args, **kwargs)
            observed.append(result.ts_code.nunique())
            assert result.ts_code.nunique() <= 2
            return result

        monkeypatch.setattr(pd, "read_sql_query", bounded_read)
        counts = rebuild_period_tables(conn, freq="weekly", aggregate=downloader._aggregate_daily_to_freq, batch_size=2)
        assert max(observed) == 2
        assert counts["tbl_cn_week"] == 7
    finally:
        conn.close()


def test_batched_rebuild_invalid_prices_preserve_existing_pair():
    downloader, conn = build_downloader()
    try:
        conn.execute("UPDATE tbl_cn_day SET open=0,close=0")
        for table in ["tbl_cn_week", "tbl_cn_week_origin"]:
            conn.execute(f"INSERT INTO {table} (ts_code,trade_date,close) VALUES ('OLD','20250101',99)")
        conn.commit()
        with pytest.raises(ValueError, match="没有有效聚合数据"):
            downloader.update_week()
        for table in ["tbl_cn_week", "tbl_cn_week_origin"]:
            assert conn.execute(f"SELECT ts_code,close FROM {table}").fetchall() == [("OLD", 99)]
        assert not conn.in_transaction
    finally:
        conn.close()


def test_batched_rebuild_does_not_rollback_callers_pending_transaction():
    downloader, conn = build_downloader()
    try:
        conn.execute("UPDATE tbl_cn_day SET amount=123 WHERE trade_date='20260105'")
        with pytest.raises(ValueError, match="未提交事务"):
            downloader.update_week()
        assert conn.in_transaction
        assert conn.execute("SELECT amount FROM tbl_cn_day WHERE trade_date='20260105'").fetchone()[0] == 123
    finally:
        conn.close()

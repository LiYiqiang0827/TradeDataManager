import sqlite3

import pytest

from core.check_database import check_database


TABLES = ("tbl_cn_day", "tbl_cn_daily_basic", "tbl_cn_adj_factor")


@pytest.fixture
def conn():
    database = sqlite3.connect(":memory:")
    database.execute(
        "CREATE TABLE tbl_cn_tradecal ("
        "cal_date TEXT NOT NULL, exchange TEXT NOT NULL, is_open INTEGER, "
        "PRIMARY KEY (cal_date, exchange))"
    )
    for date, is_open in (
        ("20260901", 1),
        ("20260902", 1),
        ("20260903", 1),
        ("20260904", 0),
        ("20260905", 1),
    ):
        database.execute(
            "INSERT INTO tbl_cn_tradecal VALUES (?, 'SSE', ?)",
            (date, is_open),
        )
    database.execute(
        "CREATE TABLE tbl_basic_ctrl (key TEXT PRIMARY KEY, max_date TEXT, updated_at TEXT)"
    )
    database.executemany(
        "INSERT INTO tbl_basic_ctrl (key,max_date) VALUES (?,?)",
        [("cn_daily", "20260905"), ("cn_adj_factor", "20260905")],
    )
    for table in TABLES:
        database.execute(
            f"CREATE TABLE {table} ("
            "trade_date TEXT NOT NULL, ts_code TEXT NOT NULL, "
            "PRIMARY KEY (trade_date, ts_code))"
        )
    database.commit()
    yield database
    database.close()


def add_rows(conn, table, date, count):
    conn.executemany(
        f"INSERT OR IGNORE INTO {table} (trade_date, ts_code) VALUES (?, ?)",
        [(date, f"{index:06d}.SZ") for index in range(count)],
    )
    conn.commit()


def test_missing_open_day_is_marked_for_redownload(conn):
    add_rows(conn, "tbl_cn_day", "20260901", 10)
    add_rows(conn, "tbl_cn_day", "20260903", 10)

    report = check_database(
        "daily",
        trade_date="20260902",
        connection=conn,
        auto_repair=False,
    )

    assert report["redownload_dates"] == {"daily": ["20260902"]}
    assert report["reports"]["daily"]["days"][0]["reasons"] == ["missing_open_day"]


def test_closed_sse_day_does_not_require_data(conn):
    report = check_database(
        "daily",
        trade_date="20260904",
        connection=conn,
        auto_repair=False,
    )

    assert report["ok"] is True
    assert report["reports"]["daily"]["checked_trade_dates"] == 0


def test_low_count_compared_with_neighbors_is_marked(conn):
    add_rows(conn, "tbl_cn_day", "20260901", 10)
    add_rows(conn, "tbl_cn_day", "20260902", 2)
    add_rows(conn, "tbl_cn_day", "20260903", 10)

    report = check_database(
        "tbl_cn_day",
        trade_date="2026-09-02",
        connection=conn,
        auto_repair=False,
    )
    day = report["reports"]["daily"]["days"][0]

    assert day["neighbor_baseline"] == 10
    assert day["neighbor_ratio"] == 0.2
    assert day["reasons"] == ["low_count_vs_neighbors"]


def test_requested_codes_are_checked_individually(conn):
    for date in ("20260901", "20260902", "20260903"):
        add_rows(conn, "tbl_cn_day", date, 1)

    report = check_database(
        "daily",
        tradedate="20260902",
        tscode=["000000.SZ", "000001.SZ"],
        connection=conn,
        auto_repair=False,
    )
    day = report["reports"]["daily"]["days"][0]

    assert day["missing_ts_codes"] == ["000001.SZ"]
    assert "missing_ts_codes" in day["reasons"]


def test_multiple_table_aliases_share_one_api(conn):
    for table in TABLES:
        add_rows(conn, table, "20260901", 3)
        add_rows(conn, table, "20260902", 3)
        add_rows(conn, table, "20260903", 3)

    report = check_database(
        ["daily", "dailybasic", "复权因子"],
        start_date="20260901",
        end_date="20260903",
        connection=conn,
        auto_repair=False,
    )

    assert report["ok"] is True
    assert report["tables"] == ["daily", "daily_basic", "adj_factor"]


def test_auto_repair_downloads_and_rechecks(conn):
    add_rows(conn, "tbl_cn_day", "20260901", 10)
    add_rows(conn, "tbl_cn_day", "20260903", 10)

    class FakeDownloader:
        conn_basic = conn

        def __init__(self):
            self.derived_calls = []

        def update_daily(self, start_date, end_date):
            assert start_date == end_date == "20260902"
            add_rows(conn, "tbl_cn_day", start_date, 10)
            return 10

        def update_week(self):
            self.derived_calls.append("week")
            return 123

        def update_month(self):
            self.derived_calls.append("month")
            return 45

    downloader = FakeDownloader()
    report = check_database(
        "daily",
        trade_date="20260902",
        connection=conn,
        downloader=downloader,
    )

    assert report["initial_redownload_dates"] == {"daily": ["20260902"]}
    assert report["redownload_dates"] == {"daily": []}
    assert report["repair_attempts"]["daily"][0]["inserted_rows"] == 10
    assert report["derived_regeneration"]["status"] == "completed"
    assert report["derived_regeneration"]["week"]["generated_tables"] == [
        "tbl_cn_week",
        "tbl_cn_week_origin",
    ]
    assert report["derived_regeneration"]["month"]["generated_tables"] == [
        "tbl_cn_month",
        "tbl_cn_month_origin",
    ]
    assert downloader.derived_calls == ["week", "month"]
    assert report["ok"] is True


def test_daily_basic_repair_does_not_regenerate_week_month(conn):
    add_rows(conn, "tbl_cn_daily_basic", "20260901", 10)
    add_rows(conn, "tbl_cn_daily_basic", "20260903", 10)

    class FakeDownloader:
        def update_daily_basic(self, start_date, end_date):
            add_rows(conn, "tbl_cn_daily_basic", start_date, 10)
            return 10

    report = check_database(
        "daily_basic",
        trade_date="20260902",
        connection=conn,
        downloader=FakeDownloader(),
    )

    assert report["ok"] is True
    assert report["derived_regeneration"]["status"] == "not_needed"


def test_invalid_date_modes_are_rejected(conn):
    with pytest.raises(ValueError, match="不能与"):
        check_database(
            "daily",
            trade_date="20260902",
            start_date="20260901",
            end_date="20260903",
            connection=conn,
            auto_repair=False,
        )


def test_duplicate_alias_arguments_are_rejected(conn):
    with pytest.raises(ValueError, match="不能同时使用"):
        check_database(
            "daily",
            trade_date="20260902",
            tradedate="20260902",
            connection=conn,
            auto_repair=False,
        )

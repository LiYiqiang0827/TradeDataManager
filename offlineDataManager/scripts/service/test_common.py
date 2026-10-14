import sqlite3
from types import SimpleNamespace

import pytest

from service.common import resolve_date_range


def args(**changes):
    values = {"trade_date": None, "start_date": None, "end_date": "20260923"}
    values.update(changes)
    return SimpleNamespace(**values)


def test_kpl_increment_reads_kpl_ctrl_table():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE tbl_kpl_ctrl (key TEXT PRIMARY KEY, max_date TEXT, updated_at TEXT)")
    conn.execute(
        "INSERT INTO tbl_kpl_ctrl(key, max_date) VALUES (?, ?)",
        ("cn_top_list", "20260922"),
    )

    start, end, _ = resolve_date_range(
        conn,
        "cn_top_list",
        args(),
        ctrl_store="kpl",
    )

    assert (start, end) == ("20260922", "20260923")


def test_basic_increment_keeps_existing_ctrl_behavior():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE tbl_basic_ctrl (key TEXT PRIMARY KEY, max_date TEXT, updated_at TEXT)")
    conn.execute(
        "INSERT INTO tbl_basic_ctrl(key, max_date) VALUES (?, ?)",
        ("cn_daily", "20260921"),
    )

    start, end, _ = resolve_date_range(conn, "cn_daily", args())

    assert (start, end) == ("20260921", "20260923")


def test_unknown_ctrl_store_is_rejected():
    conn = sqlite3.connect(":memory:")
    with pytest.raises(ValueError, match="ctrl_store"):
        resolve_date_range(conn, "cn_daily", args(), ctrl_store="news")

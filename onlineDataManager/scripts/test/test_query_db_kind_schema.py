"""Regression tests for kind-specific timestamp columns in online SQLite reads."""
from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from core import query_db, sqlite_client


def _install_readonly_db(monkeypatch, statements):
    raw_conn = sqlite3.connect(":memory:")
    raw_conn.row_factory = sqlite3.Row
    for statement, rows in statements:
        raw_conn.execute(statement)
        if rows:
            raw_conn.executemany(rows[0], rows[1])
    raw_conn.commit()

    class KeepOpen:
        def __getattr__(self, name):
            return getattr(raw_conn, name)

        def close(self):
            pass

    monkeypatch.setattr(query_db, "connect", lambda _ym, readonly=True: KeepOpen())
    return raw_conn


def test_fetch_minute_uses_data_timestamp_and_bar_ordering(monkeypatch):
    conn = _install_readonly_db(monkeypatch, [
        ("CREATE TABLE minute_20261015(trade_date TEXT, ts_code TEXT, time_idx INTEGER, datetime TEXT, data_timestamp TEXT, price REAL, vol REAL, created_at TEXT)", (
            "INSERT INTO minute_20261015 VALUES (?,?,?,?,?,?,?,?)",
            [
                ("20261015", "600000.SH", 1, "2026-10-15 09:31:00", "2026-10-15T09:31:02", 10.1, 20, "2026-10-15T09:31:03"),
                ("20261015", "600000.SH", 0, "2026-10-15 09:30:00", "2026-10-15T09:30:02", 10.0, 10, "2026-10-15T09:30:03"),
                ("20261015", "000001.SZ", 0, "2026-10-15 09:30:00", "2026-10-15T09:30:02", 12.0, 30, "2026-10-15T09:30:03"),
            ],
        )),
    ])
    try:
        rows = query_db.fetch_minute(
            "20261015", ts_code="600000.SH", start_data_ts="2026-10-15T09:30:30",
            end_data_ts="2026-10-15T09:32:00", start_save_ts="2026-10-15T09:30:00",
            limit=None,
        )
        assert len(rows) == 1
        assert rows[0]["time_idx"] == 1
        assert rows[0]["datetime"] == "2026-10-15 09:31:00"
    finally:
        conn.close()


def test_fetch_zt_uses_zt_timestamp_created_at_and_logs_quality(monkeypatch, caplog):
    conn = _install_readonly_db(monkeypatch, [
        ("CREATE TABLE zt_20260916(trade_date TEXT, ts_code TEXT, is_st INTEGER, is_new INTEGER, zt_timestamp TEXT, created_at TEXT)", (
            "INSERT INTO zt_20260916 VALUES (?,?,?,?,?,?)",
            [
                ("20260916", "600000.SH", 1, 1, "2026-09-16T09:30:00.000", "2026-09-16T09:30:01"),
                ("20260916", "000001.SZ", 1, 1, "2026-09-16T09:31:00.000", "2026-09-16T09:31:01"),
            ],
        )),
    ])
    try:
        with caplog.at_level(logging.WARNING, logger="query_db"):
            rows = query_db.fetch_zt(
                "20260916", start_data_ts="2026-09-16T09:30:30",
                start_save_ts="2026-09-16T09:30:30", limit=None,
            )
        assert [row["ts_code"] for row in rows] == ["000001.SZ"]
        assert "unreliable_legacy_string_bool" in caplog.text
    finally:
        conn.close()


def test_to_dataframe_reuses_kind_schema_query_and_attaches_zt_quality(monkeypatch):
    conn = _install_readonly_db(monkeypatch, [
        ("CREATE TABLE zt_20260916(trade_date TEXT, ts_code TEXT, is_st INTEGER, is_new INTEGER, zt_timestamp TEXT, created_at TEXT)", (
            "INSERT INTO zt_20260916 VALUES (?,?,?,?,?,?)",
            [("20260916", "600000.SH", 1, 1, "2026-09-16T09:30:00.000", "2026-09-16T09:30:01")],
        )),
        ("CREATE TABLE minute_20261015(trade_date TEXT, ts_code TEXT, time_idx INTEGER, datetime TEXT, data_timestamp TEXT, price REAL, vol REAL, created_at TEXT)", (
            "INSERT INTO minute_20261015 VALUES (?,?,?,?,?,?,?,?)",
            [("20261015", "600000.SH", 0, "2026-10-15 09:30:00", "2026-10-15T09:30:02", 10.0, 10, "2026-10-15T09:30:03")],
        )),
    ])
    try:
        zt = query_db.to_dataframe("zt", "20260916", limit=None)
        minute = query_db.to_dataframe(
            "minute", "20261015", start_data_ts="2026-10-15T09:30:01", limit=None,
        )
        assert list(zt["ts_code"]) == ["600000.SH"]
        assert zt.attrs["quality"]["status"] == "unreliable_legacy_string_bool"
        assert zt.attrs["quality"]["audit_reference"] == "docs/online_zt_flag_quality_20261015.md"
        assert "do not use" in zt.attrs["quality_warnings"][0]
        assert list(minute["time_idx"]) == [0]
    finally:
        conn.close()


def test_zt_quality_reports_unknown_for_unlisted_days():
    result = query_db.fetch_zt_quality("20261016")

    assert result["status"] == "not_assessed"
    assert result["audit_reference"] == "docs/online_zt_flag_quality_20261015.md"
    assert "not certified" in result["warning"]


def test_zt_partial_day_reports_observed_source_coverage_not_full_verification():
    result = query_db.fetch_zt_quality("2026-10-15")

    assert result["status"] == "partial_source_coverage"
    assert result["source_matched_rows_observed"] == 5000
    assert result["unverified_rows_observed"] == 6591
    assert "observed" in result["warning"]


def test_snapshot_index_uses_shared_snapshot_timestamp_column():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE snapshot_index_20261015(trade_date TEXT, ts_code TEXT, snapshot_timestamp TEXT, created_at TEXT)")
    conn.execute("INSERT INTO snapshot_index_20261015 VALUES ('20261015','000001.SH','2026-10-15T09:30:00','2026-10-15T09:30:01')")
    try:
        rows = sqlite_client.query_snapshots(
            conn, kind="snapshot_index", trade_date="20261015", limit=None,
        )
        assert len(rows) == 1
        assert rows[0]["ts_code"] == "000001.SH"
    finally:
        conn.close()

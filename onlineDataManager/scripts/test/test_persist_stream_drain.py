"""Isolated regressions for all STREAM-backed persistence families."""
from __future__ import annotations

import logging
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPTS = Path(__file__).resolve().parents[1]
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import core.logger as core_logger


def _quiet_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"test.{name}")
    logger.setLevel(logging.DEBUG)
    return logger


core_logger.setup_logger = _quiet_logger
from core import persist_client  # noqa: E402
from core import sqlite_client  # noqa: E402


class PaginatedStream:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def xread(self, streams, *, count, block=None):
        stream_key, last_id = next(iter(streams.items()))
        start = int(last_id.split("-", 1)[0])
        page = self.rows[start : start + count]
        self.calls.append((stream_key, last_id, count, block, len(page)))
        return [(stream_key, page)] if page else []

    def hincrby(self, *_args):
        return 1


class FakeRedis:
    def __init__(self, rows):
        self.r = PaginatedStream(rows)


def _messages(count: int, kind: str = "zt"):
    return [
        (f"{index}-0", {
            "ts_code": f"{index % 5000:06d}.SZ",
            f"{kind}_timestamp": "2026-10-15T10:00:00+08:00",
            "event": str(index),
        })
        for index in range(1, count + 1)
    ]


def _install_fakes(monkeypatch, *, fail_batch: int | None = None):
    raw_conn = sqlite3.connect(":memory:")

    class KeepOpenConnection:
        def __getattr__(self, name):
            return getattr(raw_conn, name)

        def close(self):
            pass

    conn = KeepOpenConnection()
    cursor = ["0-0"]
    batches = []
    monkeypatch.setattr(persist_client, "connect", lambda _month: conn)
    monkeypatch.setattr(persist_client, "read_cursor", lambda _conn, _key: cursor[0])

    def update_cursor(_conn, *, last_id, **_kwargs):
        cursor[0] = last_id

    def insert(_conn, **kwargs):
        batch_no = len(batches) + 1
        batches.append((kwargs["kind"], len(kwargs["snapshots"])))
        return -1 if batch_no == fail_batch else len(kwargs["snapshots"])

    monkeypatch.setattr(persist_client, "update_cursor", update_cursor)
    monkeypatch.setattr(persist_client, "insert_snapshots_batch", insert)
    return raw_conn, cursor, batches


def test_persist_kind_drains_zt_stream_beyond_one_thousand(monkeypatch):
    raw_conn, cursor, batches = _install_fakes(monkeypatch)
    redis = FakeRedis(_messages(2401, "zt"))
    try:
        inserted = persist_client.persist_kind(
            redis, kind="zt", trade_date="20261015", drain=True,
        )
        assert inserted == 2401
        assert batches == [("zt", 1000), ("zt", 1000), ("zt", 401)]
        assert [call[4] for call in redis.r.calls] == [1000, 1000, 401]
        assert cursor[0] == "2401-0"
    finally:
        raw_conn.close()


def test_non_drain_stream_persists_one_batch_and_minute_stays_zset(monkeypatch):
    raw_conn, _cursor, batches = _install_fakes(monkeypatch)
    redis = FakeRedis(_messages(1500, "break"))
    minute_calls = []
    monkeypatch.setattr(
        persist_client, "persist_minute",
        lambda _redis, *, trade_date: minute_calls.append(trade_date) or 7,
    )
    try:
        assert persist_client.persist_kind(
            redis, kind="break", trade_date="20261015", drain=False,
        ) == 1000
        assert len(redis.r.calls) == 1
        assert batches == [("break", 1000)]
        assert persist_client.persist_kind(
            redis, kind="minute", trade_date="20261015", drain=True,
        ) == 7
        assert minute_calls == ["20261015"]
        assert len(redis.r.calls) == 1
    finally:
        raw_conn.close()


def test_failed_stream_batch_does_not_advance_its_cursor(monkeypatch):
    raw_conn, cursor, batches = _install_fakes(monkeypatch, fail_batch=2)
    redis = FakeRedis(_messages(2100, "hot"))
    try:
        inserted = persist_client.persist_kind(
            redis, kind="hot", trade_date="20261015", drain=True,
        )
        assert inserted == -1
        assert batches == [("hot", 1000), ("hot", 1000)]
        assert cursor[0] == "1000-0"
    finally:
        raw_conn.close()


def test_anomaly_stream_uses_paginated_drain(monkeypatch):
    raw_conn, cursor, batches = _install_fakes(monkeypatch)
    redis = FakeRedis(_messages(1001, "anomaly"))
    try:
        assert persist_client.persist_kind(
            redis, kind="anomaly", trade_date="20261015", drain=True,
        ) == 1001
        assert batches == [("anomaly", 1000), ("anomaly", 1)]
        assert cursor[0] == "1001-0"
    finally:
        raw_conn.close()


def test_limitperformance_drain_and_max_batch_cap(monkeypatch):
    raw_conn, cursor, batches = _install_fakes(monkeypatch)
    redis = FakeRedis(_messages(2500, "limitperformance"))
    try:
        inserted = persist_client.persist_kind(
            redis, kind="limitperformance", trade_date="20261015",
            drain=True, max_batches=2,
        )
        assert inserted == 2000
        assert batches == [("limitperformance", 1000), ("limitperformance", 1000)]
        assert len(redis.r.calls) == 2
        assert cursor[0] == "2000-0"
    finally:
        raw_conn.close()


class OnePageRedis:
    def __init__(self, rows):
        self.rows = rows
        self.r = self
        self.increments = []
        self.calls = []

    def xread(self, streams, *, count, block=None):
        key, last_id = next(iter(streams.items()))
        start = int(last_id.split("-", 1)[0])
        page = [row for row in self.rows if int(row[0].split("-", 1)[0]) > start][:count]
        self.calls.append((key, last_id, count, len(page)))
        return [(key, page)] if page else []

    def hincrby(self, *args):
        self.increments.append(args)
        return 1


def _real_sqlite_persist(monkeypatch):
    raw_conn = sqlite3.connect(":memory:")
    raw_conn.row_factory = sqlite3.Row
    raw_conn.executescript(sqlite_client.CURSOR_TABLE_SCHEMA)

    class KeepOpenConnection:
        def __getattr__(self, name):
            return getattr(raw_conn, name)

        def close(self):
            pass

    monkeypatch.setattr(persist_client, "connect", lambda _month: KeepOpenConnection())
    return raw_conn


def test_stream_persist_preserves_normalized_epoch_iso_and_payload_fallback(monkeypatch):
    conn = _real_sqlite_persist(monkeypatch)
    event1 = datetime(2026, 10, 15, 9, 30, 1, tzinfo=ZoneInfo("Asia/Shanghai"))
    event2 = datetime(2026, 10, 15, 9, 30, 2, tzinfo=ZoneInfo("Asia/Shanghai"))
    event3 = datetime(2026, 10, 15, 9, 30, 3, 456000, tzinfo=ZoneInfo("Asia/Shanghai"))
    rows = [
        ("1-0", {
            "ts_code": "600000.SH", "zt_timestamp": str(int(event1.timestamp())),
            "name": "top-level", "payload": json.dumps({"ts_code": "000001.SZ", "name": "payload"}),
        }),
        ("2-0", {
            "payload": json.dumps({"thscode": "000002.SZ", "zt_timestamp": str(int(event2.timestamp() * 1000)), "name": "payload-fallback"}),
        }),
        ("3-0", {
            "ts_code": "000003.SZ", "zt_timestamp": event3.isoformat(), "name": "iso-source",
        }),
    ]
    redis = OnePageRedis(rows)
    try:
        inserted = persist_client.persist_zt(redis, trade_date="20261015", drain=True)
        assert inserted == 3
        stored = conn.execute(
            "SELECT ts_code, zt_timestamp, name FROM zt_20261015 ORDER BY ts_code"
        ).fetchall()
        by_code = {row["ts_code"]: row for row in stored}
        assert set(by_code) == {"600000.SH", "000002.SZ", "000003.SZ"}
        assert by_code["600000.SH"]["zt_timestamp"] == event1.replace(tzinfo=None).isoformat(timespec="milliseconds")
        assert by_code["600000.SH"]["name"] == "top-level"
        assert by_code["000002.SZ"]["zt_timestamp"] == event2.replace(tzinfo=None).isoformat(timespec="milliseconds")
        assert by_code["000002.SZ"]["name"] == "payload-fallback"
        assert by_code["000003.SZ"]["zt_timestamp"] == event3.replace(tzinfo=None).isoformat(timespec="milliseconds")
        assert by_code["000003.SZ"]["name"] == "iso-source"
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='online_stream_timestamp_quarantine'"
        ).fetchone() is None
    finally:
        conn.close()


def test_stream_persist_quarantines_missing_invalid_and_wrong_day_timestamps(monkeypatch):
    conn = _real_sqlite_persist(monkeypatch)
    rows = [
        ("1-0", {"ts_code": "600000.SH", "zt_timestamp": "not-a-time", "name": "bad"}),
        ("2-0", {"ts_code": "000001.SZ", "zt_timestamp": "2026-10-14T15:00:00+08:00", "name": "wrong-day"}),
    ]
    redis = OnePageRedis(rows)
    try:
        inserted = persist_client.persist_zt(redis, trade_date="20261015", drain=True)
        assert inserted == 0
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='zt_20261015'"
        ).fetchone() is None
        quarantine = conn.execute(
            "SELECT stream_id, reason, source_timestamp FROM online_stream_timestamp_quarantine ORDER BY stream_id"
        ).fetchall()
        assert [(row["stream_id"], row["reason"]) for row in quarantine] == [
            ("1-0", "missing_or_invalid_source_timestamp"),
            ("2-0", "source_date_mismatch"),
        ]
        assert quarantine[0]["source_timestamp"] == "not-a-time"
        assert persist_client.read_cursor(conn, "online:zt:stream") == "2-0"
        assert len(redis.increments) == 2
    finally:
        conn.close()


def test_drain_continues_across_all_quarantined_full_batch(monkeypatch):
    conn = _real_sqlite_persist(monkeypatch)
    rows = [
        (f"{index}-0", {"ts_code": f"00000{index}.SZ", "zt_timestamp": "not-a-time"})
        for index in range(1, 4)
    ]
    redis = OnePageRedis(rows)
    try:
        inserted = persist_client._persist_stream_to_sqlite(
            redis, conn, kind="zt", stream_key="online:zt:stream",
            trade_date="20261015", batch_size=2, drain=True,
        )
        assert inserted == 0
        assert [call[3] for call in redis.calls] == [2, 1]
        assert conn.execute(
            "SELECT COUNT(*) FROM online_stream_timestamp_quarantine"
        ).fetchone()[0] == 3
        assert persist_client.read_cursor(conn, "online:zt:stream") == "3-0"
    finally:
        conn.close()

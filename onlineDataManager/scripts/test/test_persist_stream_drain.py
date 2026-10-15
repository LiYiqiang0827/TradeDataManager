"""Isolated regressions for all STREAM-backed persistence families."""
from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

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

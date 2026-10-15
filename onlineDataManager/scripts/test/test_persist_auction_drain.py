"""Isolated regression test for complete auction STREAM draining."""
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

    def xread(self, streams, *, count, block):
        stream_key, last_id = next(iter(streams.items()))
        start = int(last_id.split("-", 1)[0])
        page = self.rows[start:start + count]
        self.calls.append((last_id, count, block, len(page)))
        return [(stream_key, page)] if page else []

    def hincrby(self, *_args):
        return 1


class FakeRedis:
    def __init__(self, rows):
        self.r = PaginatedStream(rows)


def test_auction_drain_consumes_more_than_one_thousand_messages(monkeypatch):
    messages = [
        (f"{index}-0", {
            "ts_code": f"{index % 5000:06d}.SZ",
            "auction_timestamp": "2026-10-15T09:25:00.000+08:00",
            "auction_request_stage": "final",
            "auction_price": "10.0",
        })
        for index in range(1, 2402)
    ]
    redis = FakeRedis(messages)
    raw_conn = sqlite3.connect(":memory:")

    class KeepOpenConnection:
        def __getattr__(self, name):
            return getattr(raw_conn, name)

        def close(self):
            pass

    conn = KeepOpenConnection()
    cursor = ["0-0"]
    inserted_batches = []
    monkeypatch.setattr(persist_client, "connect", lambda _month: conn)
    monkeypatch.setattr(persist_client, "_get_cursor", lambda *_args: cursor[0])
    monkeypatch.setattr(persist_client, "update_cursor",
                        lambda _conn, *, last_id, **_kwargs: cursor.__setitem__(0, last_id))
    monkeypatch.setattr(persist_client, "insert_snapshots_batch",
                        lambda _conn, **kwargs: inserted_batches.append(len(kwargs["snapshots"]))
                        or len(kwargs["snapshots"]))

    try:
        inserted = persist_client.persist_stream_kind(
            redis, kind="auction", trade_date="20261015", batch_size=1000, drain=True,
        )
        assert inserted == 2401
        assert inserted_batches == [1000, 1000, 401]
        assert [call[3] for call in redis.r.calls] == [1000, 1000, 401]
        assert cursor[0] == "2401-0"
    finally:
        raw_conn.close()

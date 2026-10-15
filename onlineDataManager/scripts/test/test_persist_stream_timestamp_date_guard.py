from __future__ import annotations

import logging
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import core.logger as core_logger


def _quiet_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"test.{name}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
    return logger


core_logger.setup_logger = _quiet_logger
from core import persist_client  # noqa: E402


class FakeStream:
    def __init__(self, messages):
        self.messages = messages

    def xread(self, *_args, **_kwargs):
        return [("online:snapshot:stream", self.messages)]


class FakeRedis:
    def __init__(self, messages):
        class RedisCommands(FakeStream):
            def __init__(self, incoming):
                super().__init__(incoming)
                self.counters = []

            def hincrby(self, key, field, amount):
                self.counters.append((key, field, amount))
                return 1

        self.r = RedisCommands(messages)

    def incr_meta(self, key):
        self.rejections.append(key)


def _message(stream_id: str, *, ts_code="600000.SH", timestamp=..., timestamp_field="snapshot_timestamp"):
    fields = {"ts_code": ts_code, "last_price": "11.2"}
    if timestamp is not ...:
        fields[timestamp_field] = timestamp
    return stream_id, fields


def _run(monkeypatch, messages, trade_date="20261014", kind="snapshot"):
    raw_conn = sqlite3.connect(":memory:")

    class KeepOpenConnection:
        def __getattr__(self, name):
            return getattr(raw_conn, name)

        def close(self):
            pass

    conn = KeepOpenConnection()
    captured = []
    cursors = []
    monkeypatch.setattr(persist_client, "connect", lambda _month: conn)
    monkeypatch.setattr(persist_client, "_get_cursor", lambda *_args: "0-0")
    monkeypatch.setattr(persist_client, "insert_snapshots_batch",
                        lambda _conn, **kwargs: captured.extend(kwargs["snapshots"]) or len(kwargs["snapshots"]))
    monkeypatch.setattr(persist_client, "update_cursor", lambda *_a, **kw: cursors.append(kw))
    redis = FakeRedis(messages)
    inserted = persist_client.persist_stream_kind(redis, kind=kind, trade_date=trade_date)
    if raw_conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='online_stream_timestamp_quarantine'").fetchone():
        quarantine = raw_conn.execute(
            "SELECT stream_id,kind,target_trade_date,ts_code,reason,source_timestamp,payload_json,payload_sha256 "
            "FROM online_stream_timestamp_quarantine ORDER BY stream_id"
        ).fetchall()
    else:
        quarantine = []
    raw_conn.close()
    return inserted, captured, redis.r.counters, cursors, quarantine


def test_mixed_prior_and_target_day_stream_rows_only_persist_target_date(monkeypatch):
    inserted, captured, counters, cursors, quarantine = _run(monkeypatch, [
        _message("1-0", timestamp="2026-10-13T15:00:00+08:00"),
        _message("2-0", timestamp="2026-10-14T09:31:00+08:00", ts_code="000001.SZ"),
    ])
    assert inserted == 1
    assert [row["ts_code"] for row in captured] == ["000001.SZ"]
    assert captured[0]["snapshot_timestamp"] == "2026-10-14T09:31:00.000+08:00"
    assert counters == [("online:meta", "snapshot_persist_reject_source_date_mismatch", 1)]
    assert cursors[-1]["last_id"] == "2-0"
    assert quarantine[0][:6] == ("1-0", "snapshot", "20261014", "600000.SH",
                                 "source_date_mismatch", "2026-10-13T15:00:00+08:00")


def test_valid_same_day_event_is_preserved_when_persisted_later(monkeypatch):
    # The stored event date, not wall-clock save time, determines the target day.
    monkeypatch.setattr(persist_client.time, "time", lambda: 1792076400.0)
    inserted, captured, counters, _cursors, quarantine = _run(monkeypatch, [
        _message("1-0", timestamp="2026-10-14T15:00:00.000+08:00"),
    ])
    assert inserted == 1
    assert captured[0]["snapshot_timestamp"] == "2026-10-14T15:00:00.000+08:00"
    assert counters == []
    assert quarantine == []


def test_missing_invalid_and_nonfinite_source_timestamps_are_rejected_without_fallback(monkeypatch):
    inserted, captured, counters, _cursors, quarantine = _run(monkeypatch, [
        _message("1-0"),
        _message("2-0", timestamp="not-a-timestamp"),
        _message("3-0", timestamp="NaN"),
    ])
    assert inserted == 0
    assert captured == []
    assert counters == [("online:meta", "snapshot_persist_reject_missing_or_invalid_source_timestamp", 1)] * 3
    assert len(quarantine) == 3
    for row in quarantine:
        assert row[4] == "missing_or_invalid_source_timestamp"
        payload = json.loads(row[6])
        assert payload["last_price"] == "11.2"
        assert payload.get("snapshot_timestamp") in (None, "not-a-timestamp", "NaN")
        assert hashlib.sha256(row[6].encode("utf-8")).hexdigest() == row[7]


def test_epoch_millisecond_timestamp_is_checked_in_shanghai_calendar(monkeypatch):
    inserted, captured, counters, _cursors, quarantine = _run(monkeypatch, [
        _message("1-0", timestamp=1791941460000),
    ])
    assert inserted == 1
    assert captured[0]["snapshot_timestamp"].startswith("2026-10-14T")
    assert counters == []
    assert quarantine == []


@pytest.mark.parametrize("kind", ["auction", "snapshot", "orderbook", "snapshot_index"])
def test_all_stream_snapshot_kinds_quarantine_cross_date_rows(monkeypatch, kind):
    field = f"{kind}_timestamp"
    inserted, captured, counters, _cursors, quarantine = _run(monkeypatch, [
        _message("1-0", timestamp="2026-10-13T15:00:00+08:00", timestamp_field=field),
    ], kind=kind)
    assert inserted == 0
    assert captured == []
    assert counters == [("online:meta", f"{kind}_persist_reject_source_date_mismatch", 1)]
    assert quarantine[0][1] == kind


def test_quarantine_storage_failure_does_not_advance_stream_cursor(monkeypatch):
    conn = sqlite3.connect(":memory:")
    cursors = []
    monkeypatch.setattr(persist_client, "connect", lambda _month: conn)
    monkeypatch.setattr(persist_client, "_get_cursor", lambda *_args: "0-0")
    monkeypatch.setattr(persist_client, "_persist_timestamp_quarantine",
                        lambda *_args: (_ for _ in ()).throw(sqlite3.OperationalError("isolated failure")))
    monkeypatch.setattr(persist_client, "update_cursor", lambda *args, **kwargs: cursors.append(kwargs))
    redis = FakeRedis([_message("1-0", timestamp="bad")])
    try:
        with pytest.raises(sqlite3.OperationalError, match="isolated failure"):
            persist_client.persist_stream_kind(redis, kind="snapshot", trade_date="20261014")
        assert cursors == []
    finally:
        conn.close()

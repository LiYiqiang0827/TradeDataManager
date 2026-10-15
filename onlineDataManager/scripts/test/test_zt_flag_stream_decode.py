"""Exercise real Redis field encoding and SQLite zt extraction in memory."""
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
    logger.propagate = True
    return logger


core_logger.setup_logger = _quiet_logger
from core import persist_client  # noqa: E402
from coreClient.redis_client import RedisBase  # noqa: E402


class FakeXRead:
    def __init__(self, entries):
        self.entries = entries

    def xread(self, streams, *, count, block):
        key = next(iter(streams))
        return [(key, self.entries)]


class FakeRedis:
    def __init__(self, entries):
        self.r = FakeXRead(entries)


def test_zt_flags_roundtrip_redis_field_strings_into_sqlite(monkeypatch, caplog):
    raw_conn = sqlite3.connect(":memory:")

    class KeepOpen:
        def __getattr__(self, name):
            return getattr(raw_conn, name)

        def close(self):
            pass

    conn = KeepOpen()
    monkeypatch.setattr(persist_client, "connect", lambda _month: conn)
    monkeypatch.setattr(persist_client, "read_cursor", lambda *_args: "0-0")
    monkeypatch.setattr(persist_client, "update_cursor", lambda *_args, **_kwargs: None)

    cases = [
        ("600000.SH", False, "0"),
        ("600001.SH", True, "1"),
        ("600002.SH", 0, 1),
        ("600003.SH", 1, 0),
        ("600004.SH", "false", "yes"),
        ("600005.SH", "maybe", None),
        ("600006.SH", float("inf"), 2),
    ]
    entries = []
    expected = {}
    for index, (ts_code, is_st, is_new) in enumerate(cases, 1):
        fields = {
            "ts_code": RedisBase._encode_value(ts_code),
            "zt_timestamp": RedisBase._encode_value(1792050000.0),
        }
        if is_st is not None:
            fields["is_st"] = RedisBase._encode_value(is_st)
        if is_new is not None:
            fields["is_new"] = RedisBase._encode_value(is_new)
        entries.append((f"{index}-0", fields))
        expected[ts_code] = (
            {False: 0, True: 1, "0": 0, "1": 1, "false": 0, "maybe": None}.get(is_st),
            {"0": 0, "1": 1, "yes": 1, 1: 1, 0: 0, 2: None}.get(is_new),
        )
    # The exact bool/string source encodings are the values sent by Redis XADD.
    assert RedisBase._encode_value(False) == "False"
    assert RedisBase._encode_value("0") == "0"

    try:
        with caplog.at_level(logging.WARNING, logger="core.sqlite_client"):
            inserted = persist_client.persist_zt(FakeRedis(entries), trade_date="20261015")
        assert inserted == len(cases)
        stored = {
            ts_code: (is_st, is_new)
            for ts_code, is_st, is_new in raw_conn.execute(
                "SELECT ts_code, is_st, is_new FROM zt_20261015"
            )
        }
        assert stored == expected
        assert "unsupported" in caplog.text
    finally:
        raw_conn.close()

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

# Importing persist_client normally creates a file logger under the production data
# tree. Keep this isolated test entirely in memory and on pytest's captured logger.
import core.logger as core_logger


def _test_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"test.{name}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
    return logger


core_logger.setup_logger = _test_logger
from core import persist_client  # noqa: E402


class FakeRedis:
    def __init__(self, bars_by_code: dict[str, list[dict]]):
        self.bars_by_code = bars_by_code

    def get_watchlist(self) -> list[str]:
        return list(self.bars_by_code)

    def get_minute_bars(self, ts_code: str) -> list[dict]:
        return self.bars_by_code[ts_code]


def _install_memory_sqlite(monkeypatch):
    connections: list[sqlite3.Connection] = []
    captured_rows: list[tuple] = []
    original_insert = persist_client.insert_snapshots_batch

    def connect(_month: str) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        connections.append(conn)
        return conn

    def capture_insert(conn: sqlite3.Connection, **kwargs) -> int:
        inserted = original_insert(conn, **kwargs)
        table = f"minute_{kwargs['trade_date']}"
        captured_rows.extend(
            conn.execute(
                f"SELECT ts_code, trade_date, time_idx, datetime "
                f"FROM {table} ORDER BY ts_code, time_idx"
            ).fetchall()
        )
        return inserted

    monkeypatch.setattr(persist_client, "connect", connect)
    monkeypatch.setattr(persist_client, "insert_snapshots_batch", capture_insert)
    return connections, captured_rows


def _bar(dt: str | None, idx: int = 0) -> dict:
    return {
        "source": "tdx_native_1min",
        "time_idx": idx,
        "datetime": dt,
        "price": 10.0 + idx,
        "vol": 100 + idx,
        "data_timestamp": "2026-10-14T09:31:01",
    }


def test_mixed_old_and_current_day_payload_only_persists_current_day(monkeypatch):
    connections, captured_rows = _install_memory_sqlite(monkeypatch)
    redis = FakeRedis({
        "000001.SZ": [
            _bar("2026-10-13 09:31:00", idx=0),
            _bar("2026-10-14 09:32:00", idx=1),
        ],
        "600000.SH": [_bar("2026-10-12 09:31:00", idx=0)],
    })

    inserted = persist_client.persist_minute(redis, trade_date="20261014")

    assert inserted == 1
    assert captured_rows == [
        ("000001.SZ", "20261014", 1, "2026-10-14 09:32:00"),
    ]
    for conn in connections:
        conn.close()


def test_missing_or_invalid_datetime_is_rejected(monkeypatch, caplog):
    connections, captured_rows = _install_memory_sqlite(monkeypatch)
    redis = FakeRedis({
        "000001.SZ": [
            _bar(None, idx=0),
            _bar("not-a-datetime", idx=1),
            _bar("09:33:00", idx=2),
            _bar("2026-10-14", idx=3),  # date-only is not a minute timestamp
        ],
    })

    with caplog.at_level(logging.WARNING):
        inserted = persist_client.persist_minute(redis, trade_date="20261014")

    assert inserted == 0
    assert captured_rows == []
    assert "missing, invalid, or mismatched datetime" in caplog.text
    assert "bars=4" in caplog.text
    for conn in connections:
        conn.close()


def test_stale_bar_does_not_claim_unique_key_before_later_current_bar(monkeypatch):
    connections, captured_rows = _install_memory_sqlite(monkeypatch)
    redis = FakeRedis({"000001.SZ": [_bar("2026-10-13 09:31:00", idx=0)]})

    stale_inserted = persist_client.persist_minute(redis, trade_date="20261014")
    assert stale_inserted == 0
    assert captured_rows == []

    redis.bars_by_code["000001.SZ"] = [_bar("2026-10-14 09:31:00", idx=0)]
    current_inserted = persist_client.persist_minute(redis, trade_date="20261014")

    assert current_inserted == 1
    assert captured_rows == [
        ("000001.SZ", "20261014", 0, "2026-10-14 09:31:00"),
    ]
    for conn in connections:
        conn.close()

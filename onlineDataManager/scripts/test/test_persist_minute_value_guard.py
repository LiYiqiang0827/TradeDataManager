from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import core.logger as core_logger


def _test_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"test.{name}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
    return logger


core_logger.setup_logger = _test_logger
from core import persist_client  # noqa: E402


class FakeRedis:
    def get_watchlist(self):
        return ["000001.SZ"]

    def get_minute_bars(self, ts_code):
        return [
            {"datetime": "2026-10-14 09:31:00", "time_idx": 0, "price": 10.0, "vol": 20},
            {"datetime": "2026-10-14 09:32:00", "time_idx": 1, "price": -0.85, "vol": 20},
            {"datetime": "2026-10-14 09:33:00", "time_idx": 2, "price": 10.1, "vol": -2},
        ]


def test_persistence_boundary_skips_nonpositive_price_and_negative_volume(monkeypatch, caplog):
    inserted_rows = []

    def fake_connect(_month):
        return sqlite3.connect(":memory:")

    def fake_insert(conn, *, kind, trade_date, snapshots):
        inserted_rows.extend(snapshots)
        return len(snapshots)

    monkeypatch.setattr(persist_client, "connect", fake_connect)
    monkeypatch.setattr(persist_client, "insert_snapshots_batch", fake_insert)

    with caplog.at_level(logging.WARNING):
        inserted = persist_client.persist_minute(FakeRedis(), trade_date="20261014")

    assert inserted == 1
    assert [row["time_idx"] for row in inserted_rows] == [0]
    assert "price_nonpositive" in caplog.text
    assert "vol_negative" in caplog.text

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SCRIPTS = ROOT / "onlineDataManager" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from coreClient import ths_client
from core import sqlite_client
from service import service_writeredis_auction


def test_fetch_auction_keeps_native_fields_stage_and_timestamp_semantics(monkeypatch):
    commands = []

    def fake_run_cli(args):
        commands.append(args)
        return {"data": {
            "auction_phase": "closed", "data_status": "final",
            "timestamp": 1792031100123,
            "item": [{
                "thscode": "000001.SZ", "auction_price": 11.5, "auction_pct": 0.0,
                "auction_volume": 10188.35, "auction_amount": 11716602.5,
                "auction_unmatched": 1967.65, "auction_turnover_pct": 0.0053,
                "auction_yesterday_ratio_pct": 82.4, "auction_volume_ratio": 1.12,
                "pre_close_price": 11.5, "open_price": 11.5, "last_price": 11.27,
                "name": "平安银行", "float_market_cap": 123456789.0,
            }],
        }}

    monkeypatch.setattr(ths_client, "_run_cli", fake_run_cli)
    monkeypatch.setattr(ths_client.time, "sleep", lambda _seconds: None)
    rows = ths_client.fetch_auction_snapshots(["000001.SZ"], stage="final")
    assert commands[0][commands[0].index("--stage") + 1] == "final"
    row = rows[0]
    assert row["ts_code"] == "000001.SZ"
    assert row["auction_timestamp"] == 1792031100123
    assert row["auction_timestamp_source"] == "api_response_assembly"
    assert row["auction_request_stage"] == "final"
    assert row["auction_price"] == 11.5
    assert row["auction_volume"] == 10188.35
    assert row["auction_amount"] == 11716602.5
    assert row["auction_yesterday_ratio_pct"] == 82.4
    assert row["auction_volume_ratio"] == 1.12
    assert row["float_market_cap"] == 123456789.0
    assert row["name"] == "平安银行"
    assert row["last_price"] == 11.27


def test_auction_sqlite_persists_native_fields_without_guessing_generic_volume_or_turnover():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    snapshot = {
        "ts_code": "000001.SZ", "auction_timestamp": "2026-10-15T09:25:00.123",
        "auction_timestamp_source": "api_response_assembly", "auction_request_stage": "final",
        "auction_phase": "closed", "data_status": "final", "auction_price": 11.5,
        "auction_pct": 0.0, "auction_volume": 10188.35, "auction_amount": 11716602.5,
        "auction_unmatched": 1967.65, "auction_turnover_pct": 0.0053,
        "auction_yesterday_ratio_pct": 82.4, "auction_volume_ratio": 1.12,
        "pre_close_price": 11.5, "open_price": 11.5, "last_price": 11.27,
        "name": "平安银行", "float_market_cap": 123456789.0,
    }
    inserted = sqlite_client.insert_snapshots_batch(
        conn, kind="auction", trade_date="20261015", snapshots=[snapshot],
    )
    assert inserted == 1
    row = conn.execute("SELECT * FROM auction_20261015").fetchone()
    assert row["auction_price"] == 11.5
    assert row["auction_volume"] == 10188.35
    assert row["auction_amount"] == 11716602.5
    assert row["auction_turnover_pct"] == 0.0053
    assert row["auction_yesterday_ratio_pct"] == 82.4
    assert row["auction_volume_ratio"] == 1.12
    assert row["name"] == "平安银行"
    assert row["float_market_cap"] == 123456789.0
    assert row["last_price"] == 11.27
    assert row["volume"] is None
    assert row["turnover"] is None
    assert row["auction_timestamp_source"] == "api_response_assembly"
    assert row["auction_request_stage"] == "final"


def test_ensure_table_adds_native_columns_to_existing_auction_table_without_rewriting_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE auction_20261015 (
          id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT NOT NULL, ts_code TEXT NOT NULL,
          volume INTEGER, turnover INTEGER, last_price REAL, price_change REAL,
          price_change_ratio_pct REAL, open_price REAL, high_price REAL, low_price REAL,
          prev_price REAL, auction_timestamp TEXT NOT NULL, auction_phase TEXT, data_status TEXT,
          created_at TEXT NOT NULL DEFAULT 'legacy', UNIQUE(ts_code, auction_timestamp)
        )
    """)
    conn.execute("CREATE INDEX idx_auction_20261015_ts_code ON auction_20261015(ts_code)")
    conn.execute("CREATE INDEX idx_auction_20261015_auction_ts ON auction_20261015(auction_timestamp)")
    conn.execute("CREATE INDEX idx_auction_20261015_trade_date ON auction_20261015(trade_date)")
    conn.execute("INSERT INTO auction_20261015(trade_date,ts_code,auction_timestamp,turnover) VALUES('20261015','600000.SH','old-ts',NULL)")
    sqlite_client.ensure_table(conn, "auction", "20261015")
    row = conn.execute("SELECT ts_code,turnover,auction_price,auction_amount FROM auction_20261015").fetchone()
    assert row == ("600000.SH", None, None, None)
    columns = {r[1] for r in conn.execute("PRAGMA table_info(auction_20261015)")}
    assert {"auction_price", "auction_volume", "auction_amount", "auction_timestamp_source"} <= columns


def test_writer_cli_rejects_unsupported_stage(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["service_writeredis_auction", "--stage", "predicted", "--once"])
    with pytest.raises(SystemExit) as exc:
        service_writeredis_auction.main()
    assert exc.value.code == 2

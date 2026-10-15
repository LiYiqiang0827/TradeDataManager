from __future__ import annotations

import logging
import struct
from datetime import datetime

from pytdx.parser.get_minute_time_data import GetMinuteTimeData as PytdxMinuteParser
from pytdx.helper import get_volume

import coreClient.tdx_client as tdx_module
from coreClient.minute_validation import correct_pytdx_zero_volume_sentinel, minute_value_error
from coreClient.tdx_client import TdxClient


def _native_bar(hour: int, minute: int, *, close: float = 10.0, vol: float = 100.0,
                amount: float = 1000.0) -> dict:
    today = datetime.now()
    return {
        "year": today.year, "month": today.month, "day": today.day,
        "hour": hour, "minute": minute,
        "open": close, "high": close, "low": close, "close": close,
        "vol": vol, "amount": amount,
    }


class FakeNativeMinuteApi:
    def __init__(self, rows):
        self.rows = rows

    def get_security_bars(self, category, market, code, start, count):
        return self.rows if start == 0 else []

    def get_minute_time_data(self, market, code):
        raise AssertionError("legacy date-less decoder must not be called")


def test_live_minute_uses_native_bars_maps_close_and_keeps_share_volume(monkeypatch, caplog):
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 800)
    client = TdxClient.__new__(TdxClient)
    client.api = FakeNativeMinuteApi([
        _native_bar(9, 31, close=10.25, vol=120, amount=1230),
        _native_bar(9, 32, close=0, vol=120, amount=0),
        _native_bar(9, 33, close=10.3, vol=-1, amount=0),
        _native_bar(9, 34, close=10.35, vol=5.877471754111438e-39,
                    amount=5.877471754111438e-39),
    ])
    client.ip, client.port = "fake.local", 7709

    with caplog.at_level(logging.WARNING, logger="tdx_client"):
        rows = client.get_minute_kline("000001.SZ")

    assert len(rows) == 2
    assert rows[0]["price"] == 10.25
    assert rows[0]["vol"] == 120
    assert rows[0]["datetime"].endswith("09:31:00")
    assert rows[1]["price"] == 10.35
    assert rows[1]["vol"] == 0
    assert "open_nonpositive" in caplog.text
    assert "vol_negative" in caplog.text


def test_live_minute_drops_current_forming_and_future_bars(monkeypatch, caplog):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 15, 10, 0, 30, tzinfo=tz)

    monkeypatch.setattr(tdx_module, "datetime", FrozenDateTime)
    client = TdxClient.__new__(TdxClient)
    client.api = FakeNativeMinuteApi([
        {**_native_bar(9, 59), "year": 2026, "month": 10, "day": 15},
        {**_native_bar(10, 0), "year": 2026, "month": 10, "day": 15},
        {**_native_bar(10, 1), "year": 2026, "month": 10, "day": 15},
    ])
    client.ip, client.port = "fake.local", 7709

    with caplog.at_level(logging.WARNING, logger="tdx_client"):
        rows = client.get_minute_kline("000001.SZ")

    assert [row["datetime"] for row in rows] == ["2026-10-15 09:59:00"]
    assert "forming or future native bars" in caplog.text
    assert "count=2" in caplog.text


def test_live_minute_preserves_zero_volume_bar_and_marks_soft_quality_flag(caplog):
    client = TdxClient.__new__(TdxClient)
    client.api = FakeNativeMinuteApi([_native_bar(9, 31, close=14.33, vol=0, amount=100.31)])
    client.ip, client.port = "fake.local", 7709

    with caplog.at_level(logging.WARNING, logger="tdx_client"):
        rows = client.get_minute_kline("920000.BJ")

    assert len(rows) == 1
    row = rows[0]
    assert row["price"] == 14.33
    assert row["vol"] == 0
    assert row["source_amount"] == 100.31
    assert row["source_ohlc"] == {"open": 14.33, "high": 14.33, "low": 14.33, "close": 14.33}
    assert row["quality_flags"] == ["amount_nonzero_with_zero_volume"]
    assert "retained source-quality flagged bars" in caplog.text


def test_pytdx_signed_delta_decoder_can_emit_nonpositive_price():
    body = struct.pack("<H2x", 1) + bytes((0xD5, 0x01, 0x00, 0x01))
    decoded = PytdxMinuteParser(None).parseResponse(body)
    assert decoded[0]["price"] == -0.85
    assert minute_value_error(decoded[0]) == "price_nonpositive"


def test_pytdx_zero_volume_encoding_is_exactly_corrected():
    decoded_zero = get_volume(0)
    assert decoded_zero == 5.877471754111438e-39
    assert correct_pytdx_zero_volume_sentinel(decoded_zero) == 0
    assert correct_pytdx_zero_volume_sentinel(1e-30) == 1e-30


def test_history_minute_uses_native_bars_and_preserves_legacy_fields(monkeypatch):
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 800)
    row = _native_bar(9, 31, close=10.25, vol=120, amount=1230)
    row.update(year=2026, month=10, day=14)
    client = TdxClient.__new__(TdxClient)
    client.api = FakeNativeMinuteApi([row])
    client.ip, client.port = "fake.local", 7709

    assert client.get_history_minute("000001.SZ", 20261014) == [{
        "ts_code": "000001.SZ",
        "source": "tdx_native_1min",
        "trade_date": "2026-10-14",
        "datetime": "2026-10-14 09:31:00",
        "time_idx": 0,
        "price": 10.25,
        "vol": 120,
    }]

from __future__ import annotations

import coreClient.tdx_client as tdx_module
from coreClient.tdx_client import TdxClient


def _bar(day: int, hour: int, minute: int, price: float) -> dict:
    return {
        "year": day // 10000, "month": day // 100 % 100, "day": day % 100,
        "hour": hour, "minute": minute,
        "open": price, "high": price + 0.2, "low": price - 0.1,
        "close": price + 0.1, "vol": 1000.0, "amount": 10000.0,
    }


class FakePagedApi:
    def __init__(self, pages):
        self.pages = pages

    def get_security_bars(self, category, market, code, start, count):
        return self.pages.get(start, [])


def _client(api) -> TdxClient:
    client = TdxClient.__new__(TdxClient)
    client.api = api
    client.ip = "working.example"
    client.port = 7709
    return client


def test_download_one_minute_returns_full_ohlc_and_240_bar_indices(monkeypatch, caplog):
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 4)
    api = FakePagedApi({
        0: [
            _bar(20260924, 9, 30, 9.9),
            _bar(20260924, 9, 31, 10.0),
            _bar(20260924, 11, 30, 10.1),
            _bar(20260924, 13, 1, 10.2),
        ],
        4: [
            _bar(20260924, 9, 31, 99.0),  # overlapping page; first value wins
            _bar(20260923, 15, 0, 9.8),
            _bar(20260924, 15, 0, 10.3),
        ],
    })
    rows = _client(api).download_one_minute("000001.SZ", trade_date="20260924")
    assert [row["time_idx"] for row in rows] == [0, 119, 120, 239]
    assert rows[0]["datetime"] == "2026-09-24 09:31:00"
    assert rows[-1]["datetime"] == "2026-09-24 15:00:00"
    assert rows[-1]["amount"] == 10000.0
    assert set(rows[-1]) >= {"open", "high", "low", "close", "vol", "amount"}
    assert rows[0]["close"] == 10.1
    assert "deduplicated bars" in caplog.text
    assert "conflicting=1" in caplog.text


def test_download_one_minute_filters_date_range_and_validates_arguments(monkeypatch):
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 4)
    api = FakePagedApi({0: [
        _bar(20260922, 9, 31, 9.9),
        _bar(20260923, 9, 31, 10.0),
        _bar(20260924, 9, 31, 10.1),
    ]})
    client = _client(api)
    rows = client.download_one_minute(
        "600000.SH", start_date="20260923", end_date="20260924"
    )
    assert [row["trade_date"] for row in rows] == ["2026-09-23", "2026-09-24"]
    try:
        client.download_one_minute(
            "600000.SH", trade_date="20260924", start_date="20260901"
        )
    except ValueError as exc:
        assert "互斥" in str(exc)
    else:
        raise AssertionError("互斥日期参数应报错")


def test_exact_zero_decoder_sentinel_is_corrected_and_recorded(monkeypatch):
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 4)
    row = _bar(20260924, 14, 59, 10.0)
    row["vol"] = row["amount"] = 2.0 ** -127
    result = _client(FakePagedApi({0: [row]})).download_one_minute(
        "600000.SH", trade_date="20260924",
    )
    assert len(result) == 1
    assert result[0]["vol"] == 0 and result[0]["amount"] == 0
    assert "pytdx_zero_volume_sentinel_corrected" in result[0]["decoder_corrections"]
    assert "pytdx_zero_amount_sentinel_corrected" in result[0]["decoder_corrections"]
    assert set(result[0]["decoder_corrections"]).issubset(result[0]["quality_flags"])

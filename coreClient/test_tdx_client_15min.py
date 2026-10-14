from __future__ import annotations

import coreClient.tdx_client as tdx_module
from coreClient.tdx_client import TdxClient


def _bar(day: int, hour: int, minute: int, price: float) -> dict:
    return {
        "year": day // 10000,
        "month": day // 100 % 100,
        "day": day % 100,
        "hour": hour,
        "minute": minute,
        "open": price,
        "high": price + 0.2,
        "low": price - 0.1,
        "close": price + 0.1,
        "vol": 1000.0,
        "amount": 10000.0,
    }


class FakePagedApi:
    def __init__(self, pages: dict[int, list[dict]]) -> None:
        self.pages = pages

    def get_security_bars(self, category, market, code, start, count):
        return self.pages.get(start, [])


def _client(api) -> TdxClient:
    client = TdxClient.__new__(TdxClient)
    client.api = api
    client.ip = "working.example"
    client.port = 7709
    return client


def test_get_history_15min_pages_filters_and_resets_time_idx(monkeypatch) -> None:
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 4)
    api = FakePagedApi({
        0: [
            _bar(20260923, 9, 45, 10.0),
            _bar(20260923, 10, 0, 10.1),
            _bar(20260924, 9, 45, 10.2),
            _bar(20260924, 10, 0, 10.3),
        ],
        4: [
            _bar(20260921, 9, 45, 9.8),
            _bar(20260921, 10, 0, 9.9),
            _bar(20260922, 9, 45, 10.0),
            _bar(20260922, 10, 0, 10.1),
        ],
    })

    rows = _client(api).get_history_15min(
        "000001.SZ", start_date="2026-09-22", end_date="20260923"
    )

    assert [row["trade_date"] for row in rows] == [
        "2026-09-22", "2026-09-22", "2026-09-23", "2026-09-23"
    ]
    assert [row["time_idx"] for row in rows] == [0, 1, 0, 1]
    assert rows[0]["datetime"] == "2026-09-22 09:45:00"
    assert rows[-1]["amount"] == 10000.0


def test_get_history_15min_trade_date_and_invalid_arguments(monkeypatch) -> None:
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 4)
    api = FakePagedApi({0: [_bar(20260924, 9, 45, 10.0)]})
    client = _client(api)

    rows = client.get_history_15min("600000.SH", trade_date=20260924)
    assert len(rows) == 1
    assert rows[0]["ts_code"] == "600000.SH"

    try:
        client.get_history_15min(
            "600000.SH", trade_date=20260924, start_date=20260901
        )
    except ValueError as exc:
        assert "互斥" in str(exc)
    else:
        raise AssertionError("互斥日期参数应报错")


class FakeFailoverApi:
    def __init__(self) -> None:
        self.connected_host = "stale.example"

    def get_security_bars(self, category, market, code, start, count):
        if self.connected_host == "shtdx.gtjas.com":
            return [_bar(20260924, 9, 45, 10.0)]
        return []

    def disconnect(self) -> None:
        return None

    def connect(self, host, port, time_out=5):
        self.connected_host = host
        return self


def test_get_history_15min_uses_dedicated_bar_failover(monkeypatch) -> None:
    monkeypatch.setattr(
        tdx_module, "TDX_HISTORY_BAR_IP_POOL", [("shtdx.gtjas.com", 7709)]
    )
    client = _client(FakeFailoverApi())
    client.ip = "stale.example"

    rows = client.get_history_15min("000001.SZ", trade_date=20260924)

    assert len(rows) == 1
    assert client.ip == "shtdx.gtjas.com"


def test_download_fifteen_minute_is_public_download_alias(monkeypatch) -> None:
    monkeypatch.setattr(tdx_module, "TDX_KLINE_PAGE_SIZE", 4)
    client = _client(FakePagedApi({0: [_bar(20260924, 9, 45, 10.0)]}))
    rows = client.download_fifteen_minute("600000.SH", trade_date="20260924")
    assert len(rows) == 1
    assert rows[0]["datetime"] == "2026-09-24 09:45:00"

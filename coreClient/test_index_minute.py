"""Regression checks for index decoder selection and persistence acceptance."""
import datetime as dt
import pytest
import coreClient.index_minute as module


def source_rows(period=1):
    return [{"year": 2026, "month": 10, "day": 21,
             "hour": int(label[:2]), "minute": int(label[3:]),
             "open": 3713.02, "high": 3713.04, "low": 3713.01, "close": 3713.03,
             "vol": 12345.0, "amount": 1234500.0, "up_count": 800, "down_count": 1400}
            for label in sorted(module.expected_labels(period))]


def client(rows):
    class IndexApi:
        def get_index_bars(self, category, market, code, offset, count):
            assert market == 1 and code == "000001"
            return rows if offset == 0 else []
        def get_security_bars(self, *args):
            raise AssertionError("stock decoder must never parse index bytes")
    result = module.IndexMinuteDownloader.__new__(module.IndexMinuteDownloader)
    result.api = IndexApi()
    result.host = ("fake.test", 7709)
    return result


@pytest.mark.parametrize("period,expected", [(1, 240), (15, 16)])
def test_index_decoder_and_complete_session(period, expected):
    rows, report = client(source_rows(period)).download("000001.SH", period, end_date="20261021")
    assert len(rows) == expected
    assert rows[-1]["datetime"] == "2026-10-21 15:00:00"
    assert rows[-1]["raw_volume"] == 12345
    assert rows[-1]["amount"] == 1234500
    assert "vol" not in rows[-1]
    assert report["source"] == "tdx.get_index_bars"


def test_after_close_and_future_days_are_excluded():
    rows = source_rows()
    rows += [{**rows[-1], "minute": 15}, {**rows[-1], "day": 22}]
    result, report = client(rows).download("000001.SH", 1, end_date="20261021")
    assert len(result) == 240
    assert report["filtered_outside_session"] == 1


def test_incomplete_session_is_not_persisted(tmp_path, monkeypatch):
    path = tmp_path / "missing.db"
    monkeypatch.setattr(module, "database_path", lambda: path)
    with pytest.raises(RuntimeError, match="no complete"):
        client(source_rows()[:-1]).update("000001.SH", 1, end_date="20261021")
    assert not path.exists()


def test_bad_ohlc_is_not_persisted(tmp_path, monkeypatch):
    path = tmp_path / "bad.db"
    monkeypatch.setattr(module, "database_path", lambda: path)
    rows = source_rows()
    rows[0]["close"] = 50000
    with pytest.raises(ValueError, match="invalid index OHLC"):
        client(rows).update("000001.SH", 1, end_date="20261021")
    assert not path.exists()


def test_reference_validation_precedes_writes(tmp_path, monkeypatch):
    path = tmp_path / "unaccepted.db"
    monkeypatch.setattr(module, "database_path", lambda: path)
    def reject(rows):
        raise ValueError("reference mismatch")
    with pytest.raises(ValueError, match="reference mismatch"):
        client(source_rows()).update("000001.SH", 1, end_date="20261021", validator=reject)
    assert not path.exists()


def test_store_and_historical_date_boundary(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "database_path", lambda: tmp_path / "accepted.db")
    client(source_rows()).update("000001.SH", 1, end_date="20261021")
    assert module.read_index_bars("000001.SH", end_date="20261020").empty
    frame = module.read_index_bars("000001.SH", trade_date="20261021")
    assert len(frame) == 240
    assert "raw_volume" in frame and "amount" in frame and "volume" not in frame
    assert float(frame.iloc[-1]["amount"]) == 1234500


def test_closing_rounding_flag_keeps_source_values():
    rows = source_rows()
    rows[-1]["close"] = 3713.05
    result, report = client(rows).download("000001.SH", 1, end_date="20261021")
    assert result[-1]["high"] == 3713.04
    assert result[-1]["close"] == 3713.05
    assert "closing_ohlc_boundary_rounding" in result[-1]["quality_flags"]
    assert len(report["closing_rounding_flags"]) == 1


def test_legacy_online_facade_uses_index_source_and_declares_currency_unit(monkeypatch):
    import pandas as pd
    import coreClient.data_provider as provider
    monkeypatch.setattr(provider, "_validate_online_minute", lambda **kwargs: (["000001.SH"], ["20261021"]))
    monkeypatch.setattr(provider, "get_index_oneMin", lambda *args, **kwargs: pd.DataFrame([
        {"ts_code": "000001.SH", "trade_date": "20261021", "datetime": "2026-10-21 15:00:00",
         "close": 3713.03, "raw_volume": 100000, "amount": 10000000}
    ]))
    def reject_stock_decoder(*args, **kwargs):
        raise AssertionError("index facade must never call stock TDX adapter")
    monkeypatch.setattr(provider, "_online_loop_tdx", reject_stock_decoder)
    frame = provider.get_minute_index("000001.SH", trade_date="20261021", source="online")
    assert frame.iloc[0]["price"] == 3713.03
    assert frame.iloc[0]["vol"] == 1000
    assert frame.iloc[0]["volume_unit"] == "turnover_10000_yuan"
    assert frame.iloc[0]["time_idx"] == 239

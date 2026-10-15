"""Regression coverage for malformed Tushare margin_detail response rows."""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from core import offline_downloader


_COLUMNS = [
    "trade_date", "ts_code", "rzye", "rqye", "rzmre", "rqyl",
    "rzche", "rqchl", "rqmcl", "rzrqye",
]


def _row(**changes):
    values = {column: None for column in _COLUMNS}
    values.update(trade_date="20231016", ts_code="600000.SH")
    values.update(changes)
    return values


def _downloader(monkeypatch, frame):
    instance = offline_downloader.CNDataDown.__new__(offline_downloader.CNDataDown)
    instance.client = SimpleNamespace(margin_detail=Mock(return_value=frame))
    instance.conn_basic = object()
    upserts = []
    ctrl_updates = []
    def capture_upsert(*args, **_kwargs):
        upserts.append(args[1])
        return len(args[1])

    monkeypatch.setattr(offline_downloader, "upsert_df", capture_upsert)
    monkeypatch.setattr(offline_downloader, "snap_ts", lambda: "2026-10-15T12:00:00")
    monkeypatch.setattr(offline_downloader, "update_ctrl", lambda *args: ctrl_updates.append(args))
    return instance, upserts, ctrl_updates


def test_margin_detail_error_message_row_is_rejected_before_upsert(monkeypatch):
    bad = pd.DataFrame([_row(
        trade_date="20231015",
        ts_code="没有找到符合条件的数据！.SZ",
    )])
    instance, upserts, ctrl_updates = _downloader(monkeypatch, bad)

    inserted = instance.update_margin_detail(start_date="20231015", end_date="20231015")

    assert inserted == 0
    assert upserts == []
    assert ctrl_updates == []
    instance.client.margin_detail.assert_called_once()


def test_margin_detail_allows_nullable_financial_metrics(monkeypatch):
    good = pd.DataFrame([_row()])
    instance, upserts, ctrl_updates = _downloader(monkeypatch, good)

    inserted = instance.update_margin_detail(start_date="20231016", end_date="20231016")

    assert inserted == 1
    assert len(upserts) == 1
    assert upserts[0].loc[0, "rzye"] is None
    assert upserts[0].loc[0, "snap_ts"] == "2026-10-15T12:00:00"
    assert len(ctrl_updates) == 1


def test_margin_detail_rejects_missing_endpoint_columns():
    incomplete = pd.DataFrame([{"trade_date": "20231016", "ts_code": "600000.SH"}])

    try:
        offline_downloader._validate_margin_detail_response(incomplete, "20231016", "20231016")
    except ValueError as exc:
        assert "missing required columns" in str(exc)
    else:
        raise AssertionError("missing margin_detail endpoint columns must fail closed")


def test_margin_detail_mixed_valid_and_error_rows_rejects_page_without_watermark(monkeypatch):
    mixed = pd.DataFrame([
        _row(trade_date="20231016", ts_code="600000.SH", rzye=123.0),
        _row(trade_date="20231015", ts_code="没有找到符合条件的数据！.SZ"),
    ])
    instance, upserts, ctrl_updates = _downloader(monkeypatch, mixed)

    inserted = instance.update_margin_detail(start_date="20231015", end_date="20231031")

    assert inserted == 0
    assert upserts == []
    assert ctrl_updates == []


@pytest.mark.parametrize(
    ("trade_date", "ts_code"),
    [
        ("20230230", "600000.SH"),
        ("20231017", "600000.SH"),
        ("20231016", "60000.SH"),
        ("20231016", "600000.XX"),
    ],
)
def test_margin_detail_rejects_invalid_or_out_of_range_key_fields(trade_date, ts_code):
    frame = pd.DataFrame([_row(trade_date=trade_date, ts_code=ts_code)])

    with pytest.raises(ValueError, match="malformed row"):
        offline_downloader._validate_margin_detail_response(frame, "20231016", "20231016")


@pytest.mark.parametrize("ts_code", ["000001.SZ", "900901.SH", "430047.BJ"])
def test_margin_detail_accepts_valid_exchange_code_shapes(ts_code):
    frame = pd.DataFrame([_row(ts_code=ts_code)])

    accepted = offline_downloader._validate_margin_detail_response(frame, "20231016", "20231016")

    assert len(accepted) == 1
    assert accepted.iloc[0]["ts_code"] == ts_code

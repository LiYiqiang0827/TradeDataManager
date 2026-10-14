"""Calendar and source-date safeguards, using only in-memory databases."""

import os
import sqlite3
from datetime import datetime
from unittest.mock import Mock

import pandas as pd
import pytest

os.environ.setdefault("TRADING_AGENT_TUSHARE_TOKEN", "test-token-not-used")

from config.settings import SCHEMA_SQL_BASIC, SCHEMA_SQL_KPL
from core import offline_downloader as module
from core.offline_db_client import get_ctrl_kpl


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 8, 20, 0, 0, tzinfo=tz)


@pytest.fixture
def downloader(monkeypatch):
    monkeypatch.setattr(module, "datetime", FixedDateTime)
    down = module.CNDataDown.__new__(module.CNDataDown)
    down.conn_basic = sqlite3.connect(":memory:")
    down.conn_basic.executescript(SCHEMA_SQL_BASIC)
    down.conn_kpl = sqlite3.connect(":memory:")
    down.conn_kpl.executescript(SCHEMA_SQL_KPL)
    down._kpl_client = Mock()
    down.conn_basic.executemany(
        "INSERT INTO tbl_cn_tradecal(cal_date,exchange,is_open) VALUES (?,'SSE',?)",
        [("20260930", 1)] + [(f"202610{day:02d}", int(day == 8)) for day in range(1, 9)],
    )
    yield down
    down.conn_basic.close()
    down.conn_kpl.close()


def rows(date="2026-10-08", *, clock=None):
    value = clock if clock is not None else int(pd.Timestamp(f"{date} 09:31:00+08:00").timestamp())
    return pd.DataFrame([{"ts_code": "000001.SZ", "name": "测试", "lu_time": value}])


def stored_count(down):
    return down.conn_kpl.execute("SELECT COUNT(*) FROM tbl_cn_kpl_limit_performance").fetchone()[0]


def test_open_today_uses_realtime_and_advances_ctrl(downloader):
    downloader._kpl_client.fetch_realtime_limit_performance.return_value = rows()
    assert downloader.update_kpl_limit_performance(trade_date="2026-10-08") == 1
    downloader._kpl_client.fetch_realtime_limit_performance.assert_called_once_with()
    downloader._kpl_client.get_daily_limit_performance.assert_not_called()
    assert get_ctrl_kpl("cn_kpl_limit_performance", conn=downloader.conn_kpl) == "20261008"


def test_holiday_does_not_request_write_or_advance(downloader):
    assert downloader.update_kpl_limit_performance("20261001", "20261007") == 0
    assert not downloader._kpl_client.mock_calls
    assert stored_count(downloader) == 0
    assert get_ctrl_kpl("cn_kpl_limit_performance", conn=downloader.conn_kpl) is None


def test_mixed_range_requests_only_open_dates(downloader):
    downloader._kpl_client.get_daily_limit_performance.return_value = rows("2026-09-30")
    downloader._kpl_client.fetch_realtime_limit_performance.return_value = rows()
    assert downloader.update_kpl_limit_performance("20260930", "20261008") == 2
    downloader._kpl_client.get_daily_limit_performance.assert_called_once_with("2026-09-30")
    downloader._kpl_client.fetch_realtime_limit_performance.assert_called_once_with()
    assert stored_count(downloader) == 2


def test_unknown_day_rejects_entire_range_before_requests(downloader):
    downloader.conn_basic.execute("DELETE FROM tbl_cn_tradecal WHERE cal_date='20261002'")
    with pytest.raises(ValueError, match="日历未知"):
        downloader.update_kpl_limit_performance("20260930", "20261008")
    assert not downloader._kpl_client.mock_calls
    assert stored_count(downloader) == 0


def test_unknown_calendar_flag_is_not_treated_as_open(downloader):
    downloader.conn_basic.execute("UPDATE tbl_cn_tradecal SET is_open=NULL WHERE cal_date='20261008'")
    with pytest.raises(ValueError, match="日历未知"):
        downloader.update_kpl_limit_performance(trade_date="20261008")
    assert not downloader._kpl_client.mock_calls


def test_previous_session_epoch_rejects_whole_day(downloader):
    downloader._kpl_client.fetch_realtime_limit_performance.return_value = pd.concat(
        [rows(), rows("2026-09-30").assign(ts_code="000002.SZ")], ignore_index=True,
    )
    assert downloader.update_kpl_limit_performance(trade_date="20261008") == 0
    assert stored_count(downloader) == 0
    assert get_ctrl_kpl("cn_kpl_limit_performance", conn=downloader.conn_kpl) is None


@pytest.mark.parametrize("clock", ["09:31:00", "01:31:00"])
def test_historical_clock_only_does_not_invent_source_date(downloader, clock):
    downloader._kpl_client.get_daily_limit_performance.return_value = rows("2026-09-30", clock=clock)
    assert downloader.update_kpl_limit_performance(trade_date="20260930") == 1
    assert stored_count(downloader) == 1


def test_millisecond_epoch_checks_same_shanghai_date(downloader):
    frame = rows()
    frame["lu_time"] *= 1000
    downloader._kpl_client.fetch_realtime_limit_performance.return_value = frame
    assert downloader.update_kpl_limit_performance(trade_date="20261008") == 1


def test_empty_open_day_leaves_ctrl_pending(downloader):
    downloader._kpl_client.fetch_realtime_limit_performance.return_value = pd.DataFrame()
    assert downloader.update_kpl_limit_performance(trade_date="20261008") == 0
    assert get_ctrl_kpl("cn_kpl_limit_performance", conn=downloader.conn_kpl) is None


def test_reversed_range_is_rejected_before_requests(downloader):
    with pytest.raises(ValueError, match="起始日期晚于结束日期"):
        downloader.update_kpl_limit_performance("20261008", "20260930")
    assert not downloader._kpl_client.mock_calls

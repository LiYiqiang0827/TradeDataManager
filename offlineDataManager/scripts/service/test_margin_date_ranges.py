"""Late-data overlap and explicit historical repair ranges do not touch a real DB."""

import os
import sqlite3
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

os.environ.setdefault("TRADING_AGENT_TUSHARE_TOKEN", "test-token-not-used")

from service import service_margin, service_margin_detail


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE tbl_basic_ctrl(key TEXT PRIMARY KEY,max_date TEXT,updated_at TEXT)")
    connection.executemany(
        "INSERT INTO tbl_basic_ctrl(key,max_date) VALUES (?,?)",
        [("cn_margin", "20261001"), ("cn_margin_detail", "20261001")],
    )
    yield connection
    connection.close()


def args(**changes):
    values = {"trade_date": None, "start_date": None, "end_date": None}
    values.update(changes)
    return SimpleNamespace(**values)


@pytest.mark.parametrize("module", [service_margin, service_margin_detail])
def test_default_retries_ten_natural_days_before_ctrl(conn, module):
    start, end, _ = module._resolve_margin_date_range(conn, module.CTRL_KEY, args(), today="20261008")
    assert (start, end) == ("20260921", "20261008")


@pytest.mark.parametrize("module", [service_margin, service_margin_detail])
def test_explicit_old_range_is_not_clamped_to_ctrl(conn, module):
    start, end, _ = module._resolve_margin_date_range(
        conn, module.CTRL_KEY, args(start_date="2026-09-01", end_date="2026-09-24"), today="20261008",
    )
    assert (start, end) == ("20260901", "20260924")


@pytest.mark.parametrize("module", [service_margin, service_margin_detail])
def test_explicit_old_single_day_is_not_clamped(conn, module):
    start, end, _ = module._resolve_margin_date_range(
        conn, module.CTRL_KEY, args(trade_date="20260918"), today="20261008",
    )
    assert (start, end) == ("20260918", "20260918")


def test_missing_ctrl_retains_full_default_start(conn):
    start, end, _ = service_margin._resolve_margin_date_range(conn, "missing", args(), today="20261008")
    assert (start, end) == ("20150101", "20261008")


def test_explicit_start_only_ends_today(conn):
    start, end, _ = service_margin._resolve_margin_date_range(
        conn, "cn_margin", args(start_date="20260901"), today="20261008",
    )
    assert (start, end) == ("20260901", "20261008")


def test_conflicting_single_day_and_range_rejected(conn):
    with pytest.raises(ValueError, match="不能与"):
        service_margin._resolve_margin_date_range(
            conn, "cn_margin", args(trade_date="20260918", end_date="20260924"), today="20261008",
        )


def test_reversed_explicit_range_rejected(conn):
    with pytest.raises(ValueError, match="起始日期不能晚于结束日期"):
        service_margin._resolve_margin_date_range(
            conn, "cn_margin", args(start_date="20260924", end_date="20260918"), today="20261008",
        )


@pytest.mark.parametrize("module", [service_margin, service_margin_detail])
@pytest.mark.parametrize("explicit", [False, True])
def test_service_forwards_retry_or_explicit_range_without_real_io(conn, module, explicit, monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 8, tzinfo=tz)

    monkeypatch.setattr(service_margin, "datetime", FixedDateTime)
    monkeypatch.setattr(module, "logger", Mock())
    if module is service_margin_detail:
        monkeypatch.setattr(module, "init_logger", Mock())
        from core import offline_db_client
        monkeypatch.setattr(offline_db_client, "init_db", Mock())
    down = Mock(conn_basic=conn)
    down.update_margin.return_value = down.update_margin_detail.return_value = 0
    monkeypatch.setattr(module, "CNDataDown", Mock(return_value=down))
    argv = ["service"] + (["--start-date", "20260918", "--end-date", "20260924"] if explicit else [])
    monkeypatch.setattr("sys.argv", argv)

    module.main()

    method = down.update_margin if module is service_margin else down.update_margin_detail
    expected = ("20260918", "20260924") if explicit else ("20260921", "20261008")
    method.assert_called_once_with(start_date=expected[0], end_date=expected[1])

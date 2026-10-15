"""Watchlist final snapshot must drain and expose persistence failures."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import core.logger as core_logger  # noqa: E402
import service.service_savedata_watchlist as watchlist_service  # noqa: E402
from service.service_savedata_watchlist import SavedataWatchlist  # noqa: E402
from service import savedata_loop  # noqa: E402


class FakeRedis:
    def __init__(self):
        self.meta = []

    def set_meta(self, key, value):
        self.meta.append((key, value))


def test_watchlist_final_flush_passes_drain_and_propagates_inserted_count():
    daemon = SavedataWatchlist.__new__(SavedataWatchlist)
    daemon.KIND = "watchlist"
    daemon.r = FakeRedis()
    daemon.log = logging.getLogger(__name__)
    daemon.round_idx = 0
    daemon._drain = True

    with mock.patch("service.service_savedata_watchlist.persist_kind", return_value=73) as persist:
        assert daemon._do_persist() == 73

    persist.assert_called_once()
    assert persist.call_args.kwargs["kind"] == "watchlist"
    assert persist.call_args.kwargs["drain"] is True


def test_watchlist_failure_records_error_and_raises_to_once_caller():
    daemon = SavedataWatchlist.__new__(SavedataWatchlist)
    daemon.KIND = "watchlist"
    daemon.r = FakeRedis()
    daemon.log = logging.getLogger(__name__)
    daemon.round_idx = 0
    daemon._drain = True

    with mock.patch("service.service_savedata_watchlist.persist_kind", side_effect=RuntimeError("db write failed")):
        try:
            daemon._do_persist()
        except RuntimeError as exc:
            assert str(exc) == "db write failed"
        else:
            raise AssertionError("watchlist persistence error was swallowed")

    assert daemon.r.meta and daemon.r.meta[0][0] == "watchlist_savedata_last_error"


def test_watchlist_cli_once_defaults_to_drain(monkeypatch):
    received = []
    monkeypatch.setattr(core_logger, "setup_logger", lambda name: logging.getLogger(name))
    monkeypatch.setattr(savedata_loop, "OnlineRedis", FakeRedis)
    monkeypatch.setattr(
        watchlist_service, "persist_kind",
        lambda redis, *, kind, trade_date, drain=False: received.append(
            (kind, trade_date, drain)
        ) or 0,
    )

    assert savedata_loop.main(SavedataWatchlist, extra_args=["--once"]) == 0
    assert received and received[0][0] == "watchlist" and received[0][2] is True

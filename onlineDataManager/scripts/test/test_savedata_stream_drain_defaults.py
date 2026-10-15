"""Verify ordinary daemon startup drains STREAM-backed families by default."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import core.logger as core_logger


def _quiet_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"test.{name}")
    logger.setLevel(logging.DEBUG)
    return logger


core_logger.setup_logger = _quiet_logger
from core import persist_client  # noqa: E402,F401
from service import savedata_loop  # noqa: E402
from service.service_savedata_anomaly import SavedataAnomaly  # noqa: E402
from service.service_savedata_break import SavedataBreak  # noqa: E402
from service.service_savedata_hot import SavedataHot  # noqa: E402
from service.service_savedata_limitperformance import SavedataLimitperformance  # noqa: E402
from service.service_savedata_zt import SavedataZt  # noqa: E402


class FakeOnlineRedis:
    pass


def test_savedata_main_uses_drain_without_cli_flag(monkeypatch):
    received = []
    monkeypatch.setattr(savedata_loop, "OnlineRedis", FakeOnlineRedis)
    monkeypatch.setattr(savedata_loop, "setup_logger", _quiet_logger)
    monkeypatch.setattr(
        savedata_loop,
        "persist_kind",
        lambda redis, *, kind, trade_date, drain=False: received.append(
            (kind, trade_date, drain)
        ) or 0,
    )

    for daemon_cls in (
        SavedataZt,
        SavedataBreak,
        SavedataAnomaly,
        SavedataHot,
        SavedataLimitperformance,
    ):
        # --once exercises the real CLI parser and _do_persist path; --drain is omitted.
        assert savedata_loop.main(daemon_cls, extra_args=["--once"]) == 0

    assert [kind for kind, _date, _drain in received] == [
        "zt", "break", "anomaly", "hot", "limitperformance",
    ]
    assert all(drain is True for _kind, _date, drain in received)
    assert all(len(trade_date) == 8 for _kind, trade_date, _drain in received)
    assert [cls.DEFAULT_INTERVAL for cls in (
        SavedataZt, SavedataBreak, SavedataAnomaly, SavedataHot,
        SavedataLimitperformance,
    )] == [1800.0, 1800.0, 1800.0, 1800.0, 900.0]

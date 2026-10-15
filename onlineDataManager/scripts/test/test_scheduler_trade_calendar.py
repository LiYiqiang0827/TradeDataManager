"""Mocked calendar gates for scheduler safety; no Redis/service is started."""
from __future__ import annotations

import logging
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import ModuleType
from unittest import mock

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))
redis_stub = ModuleType("core.redis_online")
redis_stub.OnlineRedis = object
sys.modules.setdefault("core.redis_online", redis_stub)
from scheduler import scheduler_onlineData as scheduler  # noqa: E402


class TradeCalendarGateTests(unittest.TestCase):
    def setUp(self):
        scheduler._CALENDAR_CACHE.clear()

    def test_database_calendar_is_authoritative_for_weekday_weekend_and_holiday(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "offline" / "db_cn_basic.db"
            db.parent.mkdir()
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE tbl_cn_tradecal(cal_date TEXT, exchange TEXT, is_open INTEGER)")
            conn.executemany("INSERT INTO tbl_cn_tradecal VALUES(?,?,?)", [
                ("20261015", "SSE", 1),  # Thursday
                ("20261017", "SSE", 0),  # Saturday
                ("20261019", "SSE", 0),  # Monday holiday
            ])
            conn.commit()
            conn.close()
            with mock.patch.dict("os.environ", {"TRADING_DATABASE_ROOT": tmp}):
                self.assertEqual(scheduler.get_phase(datetime(2026, 10, 15, 10, 0)), ("morning_writer", "morning_savedata"))
                self.assertEqual(scheduler.get_phase(datetime(2026, 10, 17, 10, 0)), ("market_closed", "market_closed"))
                self.assertEqual(scheduler.get_phase(datetime(2026, 10, 19, 10, 0)), ("market_closed", "market_closed"))

    def test_open_weekend_is_allowed_when_calendar_says_open(self):
        phase = scheduler.get_phase(datetime(2026, 10, 17, 10, 0), calendar_open=True)
        self.assertEqual(phase, ("morning_writer", "morning_savedata"))

    def test_missing_database_table_row_or_invalid_flag_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            off = Path(tmp) / "offline"
            off.mkdir()
            db = off / "db_cn_basic.db"
            with mock.patch.dict("os.environ", {"TRADING_DATABASE_ROOT": tmp}):
                self.assertIsNone(scheduler._trade_calendar_status(datetime(2026, 10, 15).date(), refresh=True))
                self.assertEqual(scheduler.get_phase(datetime(2026, 10, 15, 10, 0)), ("calendar_unavailable", "calendar_unavailable"))
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE tbl_cn_tradecal(cal_date TEXT, exchange TEXT, is_open INTEGER)")
            conn.execute("INSERT INTO tbl_cn_tradecal VALUES('20261015','SSE',NULL)")
            conn.commit()
            conn.close()
            scheduler._CALENDAR_CACHE.clear()
            with mock.patch.dict("os.environ", {"TRADING_DATABASE_ROOT": tmp}):
                self.assertIsNone(scheduler._trade_calendar_status(datetime(2026, 10, 15).date(), refresh=True))

    def test_refresh_bypasses_cached_closed_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "offline" / "db_cn_basic.db"
            db.parent.mkdir()
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE tbl_cn_tradecal(cal_date TEXT, exchange TEXT, is_open INTEGER)")
            conn.execute("INSERT INTO tbl_cn_tradecal VALUES('20261015','SSE',0)")
            conn.commit()
            conn.close()
            day = datetime(2026, 10, 15).date()
            with mock.patch.dict("os.environ", {"TRADING_DATABASE_ROOT": tmp}):
                self.assertIs(scheduler._trade_calendar_status(day), False)
                conn = sqlite3.connect(db)
                conn.execute("UPDATE tbl_cn_tradecal SET is_open=1 WHERE cal_date='20261015'")
                conn.commit()
                conn.close()
                self.assertIs(scheduler._trade_calendar_status(day), False)
                self.assertIs(scheduler._trade_calendar_status(day, refresh=True), True)

    def test_once_and_watchlist_are_blocked_without_open_calendar(self):
        log = logging.getLogger(__name__)
        when = datetime(2026, 10, 15, 9, 0)
        with mock.patch.object(scheduler, "_trade_calendar_status", return_value=None), \
             mock.patch.object(scheduler, "spawn_service") as spawn:
            scheduler.check_once_triggers(when, log)
            scheduler._reload_watchlist_if_in_trade_window(log)
        spawn.assert_not_called()

    def test_loop_missing_calendar_starts_no_redis_or_services(self):
        scheduler._should_exit = False
        def stop_after_one_scan(_):
            scheduler._should_exit = True
        with mock.patch.object(scheduler, "kill_all_daemons", return_value=True), \
             mock.patch.object(scheduler, "_trade_calendar_status", return_value=None), \
             mock.patch.object(scheduler, "OnlineRedis") as redis, \
             mock.patch.object(scheduler, "spawn_service") as spawn, \
             mock.patch.object(scheduler, "check_once_triggers") as once, \
             mock.patch.object(scheduler.time, "sleep", side_effect=stop_after_one_scan):
            self.assertTrue(scheduler._run_scheduler_loop_body(log=logging.getLogger(__name__), scan_interval_sec=0))
        redis.assert_not_called()
        spawn.assert_not_called()
        once.assert_not_called()


if __name__ == "__main__":
    unittest.main()

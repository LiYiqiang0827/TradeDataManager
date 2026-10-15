from __future__ import annotations

import logging
import sys
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))
from scheduler import scheduler_onlineData as scheduler  # noqa: E402


class SchedulerChildLivenessTests(unittest.TestCase):
    def setUp(self):
        scheduler._DAEMON_PIDS.clear()
        scheduler._DAEMON_RESTART_ATTEMPTS.clear()
        self.log = logging.getLogger("test_scheduler_child_liveness")
        self.services = {"active": [{"name": "service_writeredis_minute", "args": []}]}

    def tearDown(self):
        scheduler._DAEMON_PIDS.clear()
        scheduler._DAEMON_RESTART_ATTEMPTS.clear()

    def test_live_same_name_is_kept_without_duplicate_spawn(self):
        live = SimpleNamespace(pid=50001, poll=lambda: None, wait=lambda **_: None)
        scheduler._register_daemon("service_writeredis_minute", live)
        with mock.patch.object(scheduler, "spawn_service") as spawn, mock.patch.object(
            scheduler, "kill_daemon"
        ) as stop:
            result = scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log)
        self.assertTrue(result)
        spawn.assert_not_called()
        stop.assert_not_called()

    def test_dead_owned_leaf_restarts_only_after_group_stop_confirmation(self):
        dead = SimpleNamespace(pid=50002, poll=lambda: 17, wait=lambda **_: None)
        scheduler._register_daemon("service_writeredis_minute", dead)
        replacement = SimpleNamespace(pid=50003, poll=lambda: None, wait=lambda **_: None)

        def confirm_stop(name, _log):
            scheduler._DAEMON_PIDS.pop(name, None)
            return True

        def spawn(name, _args, *, log):
            scheduler._register_daemon(name, replacement)
            return 0

        with mock.patch.object(scheduler, "kill_daemon", side_effect=confirm_stop) as stop, mock.patch.object(
            scheduler, "spawn_service", side_effect=spawn
        ) as start:
            result = scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log)
        self.assertTrue(result)
        stop.assert_called_once_with("service_writeredis_minute", self.log)
        start.assert_called_once()
        self.assertIs(scheduler._DAEMON_PIDS["service_writeredis_minute"].proc, replacement)

    def test_actual_exited_popen_leaf_is_replaced_after_group_disappears(self):
        proc = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.exit(17)"],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.assertEqual(proc.wait(timeout=3), 17)
        scheduler._register_daemon("service_writeredis_minute", proc)
        replacement = SimpleNamespace(pid=50006, poll=lambda: None, wait=lambda **_: None)

        def spawn(name, _args, *, log):
            scheduler._register_daemon(name, replacement)
            return 0

        with mock.patch.object(scheduler, "spawn_service", side_effect=spawn):
            result = scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log)
        self.assertTrue(result)
        self.assertIs(scheduler._DAEMON_PIDS["service_writeredis_minute"].proc, replacement)

    def test_unknown_original_group_state_fails_closed_without_replacement(self):
        dead = SimpleNamespace(pid=50004, poll=lambda: 9, wait=lambda **_: None)
        scheduler._register_daemon("service_writeredis_minute", dead)
        with mock.patch.object(scheduler, "kill_daemon", return_value=False), mock.patch.object(
            scheduler, "spawn_service"
        ) as spawn:
            result = scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log)
        self.assertFalse(result)
        spawn.assert_not_called()

    def test_closed_phase_never_revives_a_service(self):
        dead = SimpleNamespace(pid=50005, poll=lambda: 1, wait=lambda **_: None)
        scheduler._register_daemon("service_writeredis_minute", dead)
        with mock.patch.object(scheduler, "spawn_service") as spawn, mock.patch.object(
            scheduler, "kill_daemon"
        ) as stop:
            result = scheduler.ensure_phase_services_alive("closed", self.services, "writer", self.log)
        self.assertTrue(result)
        spawn.assert_not_called()
        stop.assert_not_called()

    def test_restart_limit_stops_repeated_crash_loop(self):
        calls = []

        def confirm_stop(name, _log):
            scheduler._DAEMON_PIDS.pop(name, None)
            return True

        def spawn(name, _args, *, log):
            calls.append(name)
            dead = SimpleNamespace(pid=51000 + len(calls), poll=lambda: 1, wait=lambda **_: None)
            scheduler._register_daemon(name, dead)
            return 0

        with mock.patch.object(scheduler, "kill_daemon", side_effect=confirm_stop), mock.patch.object(
            scheduler, "spawn_service", side_effect=spawn
        ), mock.patch.object(scheduler.time, "monotonic", side_effect=[1.0, 2.0, 3.0, 4.0]):
            self.assertTrue(scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log))
            self.assertTrue(scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log))
            self.assertTrue(scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log))
            self.assertFalse(scheduler.ensure_phase_services_alive("active", self.services, "writer", self.log))
        self.assertEqual(len(calls), scheduler._DAEMON_RESTART_LIMIT)

    def test_loop_checks_liveness_each_scan_without_restarting_live_phase(self):
        scheduler._should_exit = False
        checks = []

        def check_alive(*_args):
            checks.append(True)
            if len(checks) == 4:  # writer + savedata checks on each of two scans
                scheduler._should_exit = True
            return True

        with mock.patch.object(scheduler, "kill_all_daemons", return_value=True), \
             mock.patch.object(scheduler, "_trade_calendar_status", return_value=True), \
             mock.patch.object(scheduler, "_reload_watchlist_if_in_trade_window"), \
             mock.patch.object(scheduler, "OnlineRedis") as redis, \
             mock.patch.object(scheduler, "check_once_triggers"), \
             mock.patch.object(scheduler, "get_phase", return_value=("morning_writer", "morning_savedata")), \
             mock.patch.object(scheduler, "run_phase_services", return_value=0) as transition, \
             mock.patch.object(scheduler, "ensure_phase_services_alive", side_effect=check_alive), \
             mock.patch.object(scheduler.time, "sleep"):
            result = scheduler._run_scheduler_loop_body(log=self.log, scan_interval_sec=0)
        self.assertTrue(result)
        self.assertEqual(transition.call_count, 2)  # only first scan: one writer, one savedata phase
        self.assertEqual(len(checks), 4)
        self.assertTrue(redis.called)


if __name__ == "__main__":
    unittest.main()

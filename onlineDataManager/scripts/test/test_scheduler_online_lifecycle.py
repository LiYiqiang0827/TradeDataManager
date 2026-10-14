"""Isolated lifecycle tests; never start the production scheduler/services."""
from __future__ import annotations

import io
import logging
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))
# Keep scheduler imports independent of Redis and production configuration.
redis_stub = ModuleType("core.redis_online")
redis_stub.OnlineRedis = object
sys.modules.setdefault("core.redis_online", redis_stub)
from scheduler import scheduler_onlineData as scheduler  # noqa: E402


class SchedulerLifecycleTests(unittest.TestCase):
    def setUp(self):
        scheduler._DAEMON_PIDS.clear()

    def tearDown(self):
        # Avoid leaving a real test child behind if an assertion fails.
        for name in list(scheduler._DAEMON_PIDS):
            scheduler.kill_daemon(name, logging.getLogger(__name__), timeout_sec=1, kill_timeout_sec=1)
        scheduler._DAEMON_PIDS.clear()

    def test_sigterm_stops_and_reaps_real_isolated_child(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            marker = Path(temp_dir) / "term-received"
            ready = Path(temp_dir) / "ready"
            code = (
                "import pathlib,signal,sys,time; "
                "p=pathlib.Path(sys.argv[1]); ready=pathlib.Path(sys.argv[2]); "
                "signal.signal(signal.SIGTERM,lambda *_:(p.write_text('term'),sys.exit(0))); "
                "ready.write_text('ready'); time.sleep(30)"
            )
            proc = subprocess.Popen([sys.executable, "-c", code, str(marker), str(ready)], start_new_session=True)
            deadline = time.monotonic() + 2
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists(), "fake child did not become ready")
            scheduler._register_daemon("service_writeredis_minute", proc)

            self.assertTrue(scheduler.kill_daemon("service_writeredis_minute", logging.getLogger(__name__), timeout_sec=2))
            self.assertEqual(proc.poll(), 0)
            self.assertEqual(marker.read_text(), "term")
            self.assertNotIn("service_writeredis_minute", scheduler._DAEMON_PIDS)

    def test_scheduler_finally_cleans_owned_child_after_exception(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            marker = Path(temp_dir) / "term-received"
            ready = Path(temp_dir) / "ready"
            code = (
                "import pathlib,signal,sys,time; "
                "p=pathlib.Path(sys.argv[1]); ready=pathlib.Path(sys.argv[2]); "
                "signal.signal(signal.SIGTERM,lambda *_:(p.write_text('term'),sys.exit(0))); "
                "ready.write_text('ready'); time.sleep(30)"
            )
            proc = subprocess.Popen([sys.executable, "-c", code, str(marker), str(ready)], start_new_session=True)
            deadline = time.monotonic() + 2
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists(), "fake child did not become ready")
            scheduler._register_daemon("service_savedata_minute", proc)

            with mock.patch.object(scheduler, "_run_scheduler_loop_body", side_effect=RuntimeError("test exit")):
                with self.assertRaisesRegex(RuntimeError, "test exit"):
                    scheduler.run_scheduler_loop(log=logging.getLogger(__name__))
            self.assertEqual(proc.poll(), 0)
            self.assertEqual(marker.read_text(), "term")
            self.assertNotIn("service_savedata_minute", scheduler._DAEMON_PIDS)

    def test_exited_owned_process_does_not_signal_reused_pid_or_unrelated_child(self):
        unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
        try:
            # poll() observed the owned child exit. Its numeric PID may now be reused.
            stale = SimpleNamespace(pid=unrelated.pid, poll=lambda: 0, wait=lambda **_: 0)
            scheduler._DAEMON_PIDS["service_savedata_minute"] = scheduler._OwnedDaemon(
                proc=stale, pid=unrelated.pid, pgid=unrelated.pid
            )
            signals = []
            def fake_killpg(pgid, sig):
                if sig == 0:
                    raise ProcessLookupError
                signals.append((pgid, sig))
            with mock.patch.object(scheduler.os, "killpg", side_effect=fake_killpg):
                self.assertTrue(scheduler.kill_daemon("service_savedata_minute", logging.getLogger(__name__)))
            self.assertEqual(signals, [])
            self.assertIsNone(unrelated.poll())
        finally:
            unrelated.terminate()
            unrelated.wait(timeout=3)

    def test_startup_cleanup_ignores_other_checkout_and_reused_pid(self):
        pid_other_checkout = 81231
        pid_reused = 81232
        exact = "python -u -m service.service_writeredis_minute"
        old_table = {
            pid_other_checkout: (pid_other_checkout, pid_other_checkout, "Tue Oct 14 23:00:00 2026", exact),
            pid_reused: (pid_reused, pid_reused, "Tue Oct 14 23:00:01 2026", exact),
        }
        # Before signal, reused PID now has a different command; other checkout is
        # rejected by cwd. Neither process may receive a signal.
        tables = [old_table, {pid_reused: (pid_reused, pid_reused, "Tue Oct 14 23:00:02 2026", "python -m unrelated.worker")}]
        calls = []
        with mock.patch.object(scheduler, "_read_process_table", side_effect=tables.pop), \
             mock.patch.object(scheduler, "_process_cwd", side_effect=lambda pid: "/other/checkout" if pid == pid_other_checkout else str(scheduler.SCRIPTS_DIR.resolve())), \
             mock.patch.object(scheduler.os, "killpg", side_effect=lambda *args: calls.append(args)):
            self.assertTrue(scheduler.kill_all_daemons(logging.getLogger(__name__)))
        self.assertEqual(calls, [])

    def test_startup_timeout_fails_closed_before_watchlist_or_phase_work(self):
        pid = 81235
        command = "python -u -m service.service_writeredis_minute"
        table = {pid: (pid, pid, "Tue Oct 14 23:00:03 2026", command)}
        # The same process-group member remains visible throughout the wait.
        with mock.patch.object(scheduler, "_read_process_table", side_effect=[table] * 7), \
             mock.patch.object(scheduler, "_process_cwd", return_value=str(scheduler.SCRIPTS_DIR.resolve())), \
             mock.patch.object(scheduler.os, "killpg") as killpg, \
             mock.patch.object(scheduler.time, "monotonic", side_effect=[0.0, 0.0, 1.0, 2.0, 3.0, 4.0, 6.0]), \
             mock.patch.object(scheduler.time, "sleep"):
            self.assertFalse(scheduler.kill_all_daemons(logging.getLogger(__name__)))
            killpg.assert_called_once_with(pid, signal.SIGTERM)

        with mock.patch.object(scheduler, "kill_all_daemons", return_value=False), \
             mock.patch.object(scheduler, "_reload_watchlist_if_in_trade_window") as watchlist, \
             mock.patch.object(scheduler, "run_phase_services") as phase_work:
            scheduler._run_scheduler_loop_body(log=logging.getLogger(__name__))
        watchlist.assert_not_called()
        phase_work.assert_not_called()

    def test_startup_inspection_failure_and_unverified_cwd_fail_closed(self):
        log = logging.getLogger(__name__)
        with mock.patch.object(scheduler, "_read_process_table", return_value=None), \
             mock.patch.object(scheduler.os, "killpg") as killpg:
            self.assertFalse(scheduler.kill_all_daemons(log))
            killpg.assert_not_called()

        pid = 81237
        table = {pid: (pid, pid, "Tue Oct 14 23:00:04 2026", "python -m service.service_writeredis_minute")}
        with mock.patch.object(scheduler, "_read_process_table", return_value=table), \
             mock.patch.object(scheduler, "_process_cwd", return_value=None), \
             mock.patch.object(scheduler.os, "killpg") as killpg:
            self.assertFalse(scheduler.kill_all_daemons(log))
            killpg.assert_not_called()

    def test_startup_term_waits_for_all_original_group_members(self):
        leader = 81238
        child = 81239
        command = "python -m service.service_writeredis_minute"
        leader_row = (leader, leader, "Tue Oct 14 23:00:05 2026", command)
        member_row = (leader, leader, "Tue Oct 14 23:00:06 2026", "python -c child-worker")
        with mock.patch.object(scheduler, "_read_process_table", side_effect=[
            {leader: leader_row}, {leader: leader_row}, {child: member_row}
        ]), mock.patch.object(scheduler, "_process_cwd", return_value=str(scheduler.SCRIPTS_DIR.resolve())), \
             mock.patch.object(scheduler.os, "killpg") as killpg, \
             mock.patch.object(scheduler.time, "monotonic", side_effect=[0.0, 0.0, 6.0]), \
             mock.patch.object(scheduler.time, "sleep"):
            self.assertFalse(scheduler.kill_all_daemons(logging.getLogger(__name__)))
        killpg.assert_called_once_with(leader, signal.SIGTERM)

    def test_phase_stop_failure_prevents_replacement_spawn(self):
        name = "service_writeredis_minute"
        stale = SimpleNamespace(pid=81240, poll=lambda: None, wait=lambda **_: None)
        scheduler._register_daemon(name, stale)
        try:
            with mock.patch.object(scheduler, "kill_daemon", return_value=False) as stop, \
                 mock.patch.object(scheduler, "spawn_service") as spawn:
                rc = scheduler.run_phase_services(
                    "morning_writer", {"morning_writer": [{"name": name, "args": []}]},
                    "writer", logging.getLogger(__name__)
                )
            self.assertEqual(rc, 1)
            stop.assert_called_once_with(name, mock.ANY)
            spawn.assert_not_called()
        finally:
            scheduler._DAEMON_PIDS.pop(name, None)

    def test_watchlist_close_stop_failure_fails_scheduler_body(self):
        scheduler._should_exit = False
        with mock.patch.object(scheduler, "kill_all_daemons", return_value=True), \
             mock.patch.object(scheduler, "_reload_watchlist_if_in_trade_window"), \
             mock.patch.object(scheduler, "OnlineRedis", side_effect=RuntimeError("isolated")), \
             mock.patch.object(scheduler, "get_phase", return_value=("writer_stopped", "closed")), \
             mock.patch.object(scheduler, "run_phase_services", return_value=0), \
             mock.patch.object(scheduler, "kill_daemon", return_value=False) as stop, \
             mock.patch.object(scheduler.time, "sleep") as sleep:
            result = scheduler._run_scheduler_loop_body(log=logging.getLogger(__name__))
        self.assertFalse(result)
        stop.assert_called_once_with("service_writeredis_watchlist", mock.ANY)
        sleep.assert_not_called()

    def test_watchlist_survives_phase_switch_and_stops_at_market_close(self):
        fake = SimpleNamespace(pid=81234, poll=lambda: None, wait=lambda **_: None)
        scheduler._register_daemon("service_writeredis_watchlist", fake)
        stopped = []
        def stop(name, log):
            stopped.append(name)
            scheduler._DAEMON_PIDS.pop(name, None)
            return True
        with mock.patch.object(scheduler, "kill_daemon", side_effect=stop):
            scheduler.run_phase_services("morning_writer", scheduler.WRITER_PHASE_SERVICES, "writer", logging.getLogger(__name__))
            self.assertEqual(stopped, [])
            scheduler.kill_daemon("service_writeredis_watchlist", logging.getLogger(__name__))
        self.assertEqual(stopped, ["service_writeredis_watchlist"])

    def test_stop_timeout_escalates_and_reports_unconfirmed_exit(self):
        class StuckProcess:
            pid = 81233

            def poll(self):
                return None

            def wait(self, timeout):
                raise subprocess.TimeoutExpired("test-child", timeout)

        proc = StuckProcess()
        scheduler._register_daemon("service_savedata_minute", proc)
        signals = []
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        logger = logging.getLogger("lifecycle-timeout-test")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        try:
            with mock.patch.object(scheduler.os, "getpgid", side_effect=lambda pid: pid), \
                 mock.patch.object(scheduler.os, "killpg", side_effect=lambda pgid, sig: signals.append((pgid, sig))):
                self.assertFalse(scheduler.kill_daemon(
                    "service_savedata_minute", logger, timeout_sec=0.01, kill_timeout_sec=0.02
                ))
        finally:
            logger.removeHandler(handler)
        self.assertEqual(signals, [(proc.pid, signal.SIGTERM), (proc.pid, signal.SIGKILL)])
        self.assertIn("仍超时", output.getvalue())
        self.assertIn("service_savedata_minute", scheduler._DAEMON_PIDS)


if __name__ == "__main__":
    unittest.main()

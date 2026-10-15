from scheduler import scheduler_updateData as scheduler
import pytest
import subprocess
import sys
from types import SimpleNamespace


def test_full_update_refreshes_trade_calendar_before_reading_target(monkeypatch):
    calls = []

    def fake_spawn(service_name, extra_args=None, sync=False, abort_on_failure=True):
        calls.append((service_name, extra_args, sync))
        return 0

    def fake_target_date():
        assert calls and calls[0][0] == "service_tradecal"
        return "19990101"

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(scheduler, "spawn_service", fake_spawn)
    monkeypatch.setattr(scheduler, "get_target_date", fake_target_date)
    monkeypatch.setattr(scheduler, "get_conn", lambda _name: FakeConn())
    monkeypatch.setattr(scheduler, "get_ctrl", lambda _conn, _key: "19990101")

    scheduler.task_full_update()

    assert calls == [
        (
            "service_tradecal",
            ["--trade-date", scheduler.datetime.now().strftime("%Y%m%d")],
            True,
        ),
        ("service_fifteenMinute", [], True),
        ("service_oneMinute", [], True),
    ]


def test_full_update_runs_daily_adj_week_month_in_dependency_order(monkeypatch):
    calls = []

    def fake_spawn(service_name, extra_args=None, sync=False, abort_on_failure=True):
        calls.append(service_name)
        return 0

    monkeypatch.setattr(scheduler, "spawn_service", fake_spawn)
    monkeypatch.setattr(
        scheduler,
        "get_target_date",
        lambda: scheduler.datetime.now().strftime("%Y%m%d"),
    )

    scheduler.task_full_update()

    dependency_chain = [
        service
        for service in calls
        if service in {
            "service_daily", "service_adj_factor", "service_fifteenMinute", "service_oneMinute",
            "service_week", "service_month",
        }
    ]
    assert dependency_chain == [
        "service_daily",
        "service_adj_factor",
        "service_fifteenMinute",
        "service_oneMinute",
        "service_week",
        "service_month",
    ]


def test_timeout_is_reported_without_uncaught_daemon_exception(monkeypatch, tmp_path):
    (tmp_path / "logs").mkdir()
    monkeypatch.setattr(scheduler, "PROJECT_ROOT", tmp_path)

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], 1800)

    monkeypatch.setattr(scheduler.subprocess, "run", timeout)
    with pytest.raises(RuntimeError, match="service_week.*超时"):
        scheduler.spawn_service("service_week", sync=True)
    assert scheduler.spawn_service("service_week", sync=True, abort_on_failure=False) == 124


def test_spawn_creates_log_directory_for_clean_checkout(monkeypatch, tmp_path):
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    monkeypatch.setattr(scheduler, "PROJECT_ROOT", tmp_path)
    calls = []

    def fake_run(cmd, **kwargs):
        output = kwargs["stdout"]
        assert not output.closed
        calls.append((cmd, output.name))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(scheduler.subprocess, "run", fake_run)

    assert not (tmp_path / "logs").exists()
    assert scheduler.spawn_service("service_news", sync=True) == 0
    assert calls == [
        ([sys.executable, "-m", "service.service_news"], str(tmp_path / "logs" / "service_news.log"))
    ]
    assert (tmp_path / "logs" / "service_news.log").is_file()


def test_failed_derived_tables_do_not_block_independent_source_updates(monkeypatch):
    calls = []

    def spawn(name, *args, **kwargs):
        calls.append(name)
        if name in {"service_week", "service_month"}:
            raise RuntimeError("derived update failed")
        return 0

    monkeypatch.setattr(scheduler, "spawn_service", spawn)
    monkeypatch.setattr(scheduler, "get_target_date", lambda: scheduler.datetime.now().strftime("%Y%m%d"))
    scheduler.task_full_update()
    assert "service_month" in calls
    assert "service_kpl_list" in calls
    assert "service_daily_basic" in calls
    assert "service_index_daily" in calls


@pytest.mark.parametrize("skip_startup_update, expected_calls", [(True, 0), (False, 1)])
def test_daemon_startup_update_is_explicitly_skippable(
    monkeypatch, skip_startup_update, expected_calls
):
    class StopLoop(Exception):
        pass

    class FakeJob:
        day = None
        minutes = None

        def __init__(self):
            self.day = self
            self.minutes = self

        def at(self, _time):
            return self

        def do(self, _function):
            return self

    fake_schedule = SimpleNamespace(
        every=lambda *_args: FakeJob(),
        run_pending=lambda: (_ for _ in ()).throw(StopLoop()),
    )
    monkeypatch.setitem(sys.modules, "schedule", fake_schedule)
    calls = []
    monkeypatch.setattr(scheduler, "task_full_update", lambda: calls.append("full"))

    with pytest.raises(StopLoop):
        scheduler.run_daemon(skip_startup_update=skip_startup_update)

    assert len(calls) == expected_calls

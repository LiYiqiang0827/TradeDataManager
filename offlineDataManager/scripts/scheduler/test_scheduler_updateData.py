from scheduler import scheduler_updateData as scheduler
import pytest
import subprocess


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

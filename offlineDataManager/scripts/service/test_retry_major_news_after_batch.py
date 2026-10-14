from __future__ import annotations

import json

from service.retry_major_news_after_batch import main


def test_completed_first_batch_needs_no_retry(tmp_path):
    supervisor = tmp_path / "supervisor.json"
    supervisor.write_text(json.dumps({"status": "complete"}))
    backfill = tmp_path / "backfill.json"
    backfill.write_text(json.dumps({"status": "complete", "failed": {}}))
    status = tmp_path / "retry.json"
    assert main(["--start-date", "20260101", "--end-date", "20260927",
                 "--supervisor-status", str(supervisor), "--backfill-status", str(backfill),
                 "--status-file", str(status), "--log-file", str(tmp_path / "retry.log")]) == 0
    assert json.loads(status.read_text())["status"] == "not_needed"


def test_source_failure_never_starts_model_retry(tmp_path):
    supervisor = tmp_path / "supervisor.json"
    supervisor.write_text(json.dumps({"status": "source_failed"}))
    backfill = tmp_path / "backfill.json"
    backfill.write_text(json.dumps({"status": "finished_with_failures", "failed": {"20260103": "x"}}))
    status = tmp_path / "retry.json"
    assert main(["--start-date", "20260101", "--end-date", "20260927",
                 "--supervisor-status", str(supervisor), "--backfill-status", str(backfill),
                 "--status-file", str(status), "--log-file", str(tmp_path / "retry.log")]) == 1
    assert json.loads(status.read_text())["status"] == "not_safe_to_retry"


def test_pipeline_failures_trigger_one_resume_pass(tmp_path, monkeypatch):
    supervisor = tmp_path / "backfill.json"
    supervisor.write_text(json.dumps({"status": "finished_with_failures",
                                      "failed": {"20260105": "模型漏项"}}))
    status = tmp_path / "retry.json"

    def fake_run(*args, **kwargs):
        supervisor.write_text(json.dumps({"status": "complete", "failed": {}}))
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr("service.retry_major_news_after_batch.subprocess.run", fake_run)
    assert main(["--start-date", "20260101", "--end-date", "20260927",
                 "--supervisor-status", str(supervisor), "--backfill-status", str(supervisor),
                 "--status-file", str(status), "--log-file", str(tmp_path / "retry.log")]) == 0
    assert json.loads(status.read_text())["status"] == "complete"

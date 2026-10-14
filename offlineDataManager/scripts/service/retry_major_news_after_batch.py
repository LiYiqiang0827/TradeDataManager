#!/usr/bin/env python3
"""等待长批次结束，再用现有断点补跑失败日期一次。"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from coreClient._paths import manager_root, offline_data_dir
ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()


def _state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    value["updated_at"] = datetime.now().isoformat(timespec="seconds")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--supervisor-status", type=Path, required=True)
    parser.add_argument("--backfill-status", type=Path, required=True)
    parser.add_argument("--status-file", type=Path, required=True)
    parser.add_argument("--log-file", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args(argv)
    progress = {"status": "waiting_for_first_batch"}
    _save(args.status_file, progress)
    while True:
        supervisor = _state(args.supervisor_status)
        if supervisor.get("status") in {"complete", "model_failed", "source_failed",
                                        "finished_with_failures", "stopped_after_prepare_failure"}:
            break
        time.sleep(args.poll_seconds)
    if supervisor["status"] == "complete":
        progress["status"] = "not_needed"
        _save(args.status_file, progress)
        return 0
    backfill = _state(args.backfill_status)
    if supervisor["status"] not in {"model_failed", "finished_with_failures"} or backfill.get("status") not in {
        "finished_with_failures", "stopped_after_consecutive_failures"
    }:
        progress["status"] = "not_safe_to_retry"
        progress["first_batch_status"] = supervisor["status"]
        _save(args.status_file, progress)
        return 1
    progress["status"] = "retrying"
    progress["initial_failed_dates"] = sorted(backfill.get("failed", {}))
    _save(args.status_file, progress)
    command = [sys.executable, "-m", "service.news_graph_tool", "backfill",
               "--start-date", args.start_date, "--end-date", args.end_date]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = f"{ROOT / 'offlineDataManager' / 'scripts'}:{ROOT}"
    args.log_file.parent.mkdir(parents=True, exist_ok=True)
    with args.log_file.open("a", encoding="utf-8") as output:
        result = subprocess.run(command, cwd=ROOT, env=environment,
                                stdout=output, stderr=subprocess.STDOUT, check=False)
    final = _state(args.backfill_status)
    progress["status"] = ("complete" if result.returncode == 0
                          and final.get("status") == "complete"
                          and not final.get("failed") else "retry_failed")
    progress["returncode"] = result.returncode
    progress["remaining_failed_dates"] = sorted(final.get("failed", {}))
    _save(args.status_file, progress)
    return 0 if progress["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())

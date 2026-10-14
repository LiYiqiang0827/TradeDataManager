#!/usr/bin/env python3
"""流水线回填：次日准备与当日模型提炼重叠，数据库操作仍串行。"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
import time

from service.news_graph_tool import (
    ARTIFACT_ROOT, MajorNewsGraphStore, _is_complete, _save_state,
    _source_count, _source_days,
)

from coreClient._paths import manager_root, offline_data_dir
ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()


def run_stage(day: str, stage: str, *, retries: int = 1) -> None:
    command = [sys.executable, "-m", "service.service_daily_major_news_analysis",
               "--trade-date", day, "--stage", stage]
    for attempt in range(retries + 1):
        result = subprocess.run(command, cwd=ROOT, check=False)
        if result.returncode == 0:
            return
        if attempt < retries:
            print(f"{day} {stage} 失败，重试 {attempt + 1}/{retries}", flush=True)
            time.sleep(10)
    raise RuntimeError(f"{day} {stage} 阶段失败，exit={result.returncode}")


def overlap_days(days: list[str], prepare, enrich, publish, on_result) -> None:
    """保持 prepare/publish 串行；最多一个日期占用 Qwen，另一个日期做向量化。"""
    if not days:
        return
    prepare(days[0])
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(enrich, days[0])
        for index, day in enumerate(days):
            next_day = days[index + 1] if index + 1 < len(days) else None
            prepare_error = None
            if next_day:
                try:
                    prepare(next_day)
                except Exception as exc:
                    prepare_error = exc
            try:
                future.result()
                publish(day)
                on_result(day, None)
            except Exception as exc:
                on_result(day, exc)
            if prepare_error:
                raise RuntimeError(f"{next_day} prepare 阶段失败") from prepare_error
            if next_day:
                future = pool.submit(enrich, next_day)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--status-file", type=Path)
    args = parser.parse_args(argv)
    days = _source_days(args.start_date, args.end_date)
    status_path = args.status_file or ARTIFACT_ROOT / f"backfill_status_{args.start_date}_{args.end_date}.json"
    state = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
    if state.get("start_date") != datetime.strptime(args.start_date, "%Y%m%d").date().isoformat():
        state = {"start_date": datetime.strptime(args.start_date, "%Y%m%d").date().isoformat(),
                 "end_date": datetime.strptime(args.end_date, "%Y%m%d").date().isoformat(),
                 "started_at": datetime.now().isoformat(timespec="seconds"),
                 "completed_dates": [], "failed": {}}
    completed = set(state.get("completed_dates", []))
    failed = dict(state.get("failed", {}))
    store = MajorNewsGraphStore()
    pending = []
    for day in days:
        if _is_complete(store, day, _source_count(day)):
            completed.add(day)
            failed.pop(day, None)
        else:
            completed.discard(day)
            pending.append(day)
    state.update({"source_days": len(days), "mode": "pipelined", "status": "running_pipeline",
                  "completed_dates": sorted(completed), "completed_count": len(completed),
                  "failed": failed, "failed_count": len(failed),
                  "remaining_count": len(days) - len(completed)})
    _save_state(status_path, state)
    started = time.perf_counter()
    processed = 0

    def on_result(day: str, error: Exception | None) -> None:
        nonlocal processed
        if error is None and _is_complete(store, day, _source_count(day)):
            completed.add(day)
            failed.pop(day, None)
            processed += 1
            print(f"{day} 完成；累计 {len(completed)}/{len(days)}", flush=True)
        else:
            failed[day] = str(error or "数据库/Obsidian 验收未通过")[:500]
            print(f"{day} 失败: {failed[day]}", flush=True)
        elapsed = time.perf_counter() - started
        state.update({"current_date": day, "completed_dates": sorted(completed),
                      "completed_count": len(completed), "failed": failed,
                      "failed_count": len(failed), "remaining_count": len(days) - len(completed),
                      "average_seconds_per_new_day": round(elapsed / processed, 1) if processed else None,
                      "estimated_remaining_hours": round((len(days) - len(completed)) * elapsed / processed / 3600, 1)
                      if processed else None})
        _save_state(status_path, state)

    try:
        overlap_days(pending, lambda day: run_stage(day, "prepare"),
                     lambda day: run_stage(day, "enrich"),
                     lambda day: run_stage(day, "publish"), on_result)
    except Exception as exc:
        state["status"] = "stopped_after_prepare_failure"
        state["error"] = str(exc)[:500]
        _save_state(status_path, state)
        raise
    state["status"] = "complete" if not failed else "finished_with_failures"
    _save_state(status_path, state)
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())

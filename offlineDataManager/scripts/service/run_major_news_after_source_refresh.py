#!/usr/bin/env python3
"""等原始 Major News 全量复核验收后，继续后台模型回填。"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

from coreClient._paths import manager_root, offline_data_dir
ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

from config.settings import DB_PATH_NEWS  # noqa: E402


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def audit_refresh(source_status: Path, database: Path, start: str, end: str) -> dict:
    state = json.loads(source_status.read_text(encoding="utf-8"))
    if state.get("status") != "complete":
        raise ValueError(f"源数据复核未完成: {state.get('status')}")
    first = datetime.strptime(start, "%Y%m%d").date()
    last = datetime.strptime(end, "%Y%m%d").date()
    expected = {(first + timedelta(days=offset)).isoformat()
                for offset in range((last - first).days + 1)}
    days = state.get("days", {})
    if set(days) != expected or any(item.get("status") != "complete" for item in days.values()):
        raise ValueError("源数据复核状态未覆盖全部自然日")
    if state.get("failed_dates") or state.get("remaining_missing"):
        raise ValueError("源数据复核仍有失败或空白日期")
    with sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) as conn:
        counts = dict(conn.execute("""SELECT substr(datetime,1,10), count(*)
            FROM tbl_major_news WHERE datetime>=? AND datetime<?
            GROUP BY 1""", (first.isoformat(), (last + timedelta(days=1)).isoformat())))
    if set(counts) != expected:
        raise ValueError("SQLite 原始库仍缺日期")
    # Tushare 的分页结果偶尔含有相同 (datetime, src, md5) 的重复行；
    # SQLite 以该主键去重，所以库内数量可以略小于 fetched。
    bad = [day for day in sorted(expected)
           if days[day].get("fetched", 0) <= 0
           or counts[day] != days[day]["source_count"]]
    if bad:
        raise ValueError(f"原始库与逐日抓取统计不一致: {bad[:10]}")
    return {"days": len(expected), "raw_articles": sum(counts.values()),
            "new_articles": sum(item.get("inserted", 0) for item in days.values())}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--source-status", type=Path, required=True)
    parser.add_argument("--status-file", type=Path, required=True)
    parser.add_argument("--model-log", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args(argv)
    status = {"status": "waiting_for_source", "source_status": str(args.source_status),
              "model_log": str(args.model_log)}
    _write(args.status_file, status)
    try:
        while True:
            if args.source_status.exists():
                source = json.loads(args.source_status.read_text(encoding="utf-8"))
                if source.get("status") != "running":
                    break
            time.sleep(args.poll_seconds)
        audit = audit_refresh(args.source_status, DB_PATH_NEWS, args.start_date, args.end_date)
        status.update({"status": "running_model", "source_audit": audit})
        _write(args.status_file, status)
        args.model_log.parent.mkdir(parents=True, exist_ok=True)
        command = [sys.executable, "-m", "service.news_graph_tool", "backfill",
                   "--start-date", args.start_date, "--end-date", args.end_date]
        environment = dict(os.environ)
        environment["PYTHONPATH"] = f"{ROOT / 'offlineDataManager' / 'scripts'}:{ROOT}"
        with args.model_log.open("a", encoding="utf-8") as output:
            result = subprocess.run(command, cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, check=False)
        status["status"] = "complete" if result.returncode == 0 else "model_failed"
        status["returncode"] = result.returncode
    except Exception as exc:
        status["status"] = "source_failed"
        status["error"] = str(exc)[:500]
    _write(args.status_file, status)
    print(json.dumps(status, ensure_ascii=False), flush=True)
    return 0 if status["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Major News 图数据库的更新、补录、查询与 Obsidian 投影入口。"""
from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

from coreClient._paths import manager_root, offline_data_dir
ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()
SCRIPTS = ROOT / "offlineDataManager" / "scripts"
sys.path.insert(0, str(SCRIPTS))

from config.settings import DB_PATH_NEWS, NEWS_EVENT_DB_PATH  # noqa: E402
from core.major_news_graph_store import MajorNewsGraphStore, _day  # noqa: E402


ARTIFACT_ROOT = ROOT / "outputs" / "major_news_analysis"


def _print(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def _run_module(module: str, args: list[str]) -> None:
    subprocess.run([sys.executable, "-m", module, *args], cwd=ROOT, check=True)


def _source_days(start: str, end: str) -> list[str]:
    conn = sqlite3.connect(f"file:{DB_PATH_NEWS.resolve()}?mode=ro", uri=True)
    try:
        return [row[0].replace("-", "") for row in conn.execute(
            """SELECT DISTINCT substr(datetime,1,10) FROM tbl_major_news
               WHERE datetime>=? AND datetime<? ORDER BY 1""",
            [_day(start) + " 00:00:00", _day(end) + " 23:59:59.999999"],
        )]
    finally:
        conn.close()


def _source_count(day: str) -> int:
    conn = sqlite3.connect(f"file:{DB_PATH_NEWS.resolve()}?mode=ro", uri=True)
    try:
        iso = _day(day)
        return int(conn.execute("""SELECT count(*) FROM tbl_major_news
            WHERE datetime>=? AND datetime<?""", [iso, iso + " 23:59:59.999999"]).fetchone()[0])
    finally:
        conn.close()


def _is_complete(store: MajorNewsGraphStore, day: str, raw_count: int) -> bool:
    note = Path.home() / "Documents" / "每日新闻分析" / "每日新闻" / f"{_day(day)}.md"
    if not note.exists() or not store.database.exists():
        return False
    rows = store.query_daily_summary(day, day)
    if len(rows) != 1:
        return False
    row = rows.iloc[0]
    return (int(row.raw_articles) == raw_count
            and int(row.selected_events) == int(row.bullish_events + row.bearish_events + row.neutral_events)
            and f"effective_events: {int(row.selected_events)}" in note.read_text(encoding="utf-8"))


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.now().isoformat(timespec="seconds")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def _backfill(store: MajorNewsGraphStore, args) -> int:
    days = _source_days(args.start_date, args.end_date)
    status_path = args.status_file or ARTIFACT_ROOT / f"backfill_status_{args.start_date.replace('-', '')}_{args.end_date.replace('-', '')}.json"
    state = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
    if state.get("start_date") != _day(args.start_date) or state.get("end_date") != _day(args.end_date):
        state = {"start_date": _day(args.start_date), "end_date": _day(args.end_date),
                 "started_at": datetime.now().isoformat(timespec="seconds"),
                 "completed_dates": [], "failed": {}}
    completed = set(state.get("completed_dates", []))
    failed = dict(state.get("failed", {}))
    state["source_days"] = len(days)
    started = time.perf_counter()
    processed_this_run = 0
    consecutive_failures = 0
    _save_state(status_path, state)
    for index, day in enumerate(days, 1):
        raw_count = _source_count(day)
        if not args.force and _is_complete(store, day, raw_count):
            completed.add(day)
            failed.pop(day, None)
            consecutive_failures = 0
            print(f"[{index}/{len(days)}] {day} 已验证，跳过", flush=True)
        else:
            completed.discard(day)
            print(f"[{index}/{len(days)}] {day} 开始；原始新闻 {raw_count} 条", flush=True)
            error = None
            for attempt in range(args.retry_count + 1):
                try:
                    _run_module("service.service_daily_major_news_analysis", ["--trade-date", day])
                    if not _is_complete(store, day, raw_count):
                        raise RuntimeError("处理进程返回成功，但数据库或 Obsidian 验收未通过")
                    error = None
                    break
                except (subprocess.CalledProcessError, RuntimeError) as exc:
                    error = str(exc)
                    print(f"[{index}/{len(days)}] {day} 第 {attempt + 1} 次失败: {error}", flush=True)
                    if attempt < args.retry_count:
                        time.sleep(args.retry_delay)
            if error is None:
                completed.add(day)
                failed.pop(day, None)
                processed_this_run += 1
                consecutive_failures = 0
            else:
                failed[day] = error
                consecutive_failures += 1
        state["completed_dates"] = sorted(completed)
        state["failed"] = failed
        state["current_date"] = day
        state["completed_count"] = len(completed.intersection(days))
        state["failed_count"] = len(failed)
        state["remaining_count"] = len(days) - state["completed_count"]
        elapsed = time.perf_counter() - started
        state["average_seconds_per_new_day"] = round(elapsed / processed_this_run, 1) if processed_this_run else None
        state["estimated_remaining_hours"] = (round(state["remaining_count"] * elapsed / processed_this_run / 3600, 1)
                                              if processed_this_run else None)
        state["status"] = "running"
        _save_state(status_path, state)
        if consecutive_failures >= args.max_consecutive_failures:
            state["status"] = "stopped_after_consecutive_failures"
            _save_state(status_path, state)
            return 1
    state["status"] = "complete" if not failed else "finished_with_failures"
    _save_state(status_path, state)
    print(json.dumps({"status_file": str(status_path), "source_days": len(days),
                      "completed": state["completed_count"], "failed": len(failed)}, ensure_ascii=False), flush=True)
    return 0 if not failed else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Major News 图数据库工具")
    sub = parser.add_subparsers(dest="command", required=True)
    update = sub.add_parser("update", aliases=["add"], help="新增或更新一个日期并写库、投影 Obsidian")
    update.add_argument("--date", required=True)
    update.add_argument("--force", action="store_true")
    backfill = sub.add_parser("backfill", help="按源库已有日期补录；完成日自动复用缓存")
    backfill.add_argument("--start-date", required=True)
    backfill.add_argument("--end-date", required=True)
    backfill.add_argument("--status-file", type=Path)
    backfill.add_argument("--force", action="store_true", help="重新处理已验证的日期")
    backfill.add_argument("--retry-count", type=int, default=1)
    backfill.add_argument("--retry-delay", type=int, default=15)
    backfill.add_argument("--max-consecutive-failures", type=int, default=3)
    ingest = sub.add_parser("import", help="仅将已有模型中间产物导入正式图数据库")
    ingest.add_argument("--date", required=True)
    ingest.add_argument("--artifact-root", type=Path, default=ARTIFACT_ROOT)
    query = sub.add_parser("query", help="查询入选新闻及评级")
    query.add_argument("--start-date", required=True)
    query.add_argument("--end-date", required=True)
    query.add_argument("--theme-id")
    query.add_argument("--theme-label")
    query.add_argument("--entity")
    query.add_argument("--keyword")
    query.add_argument("--impact", choices=["bullish", "bearish", "neutral"])
    query.add_argument("--min-importance", type=int, default=0)
    query.add_argument("--limit", type=int, default=30)
    event = sub.add_parser("event", help="查询一条新闻及原始报道、题材、实体边")
    event.add_argument("--id", required=True)
    event.add_argument("--as-of")
    status = sub.add_parser("status", help="读取每日入库数量及评级分布")
    status.add_argument("--start-date", required=True)
    status.add_argument("--end-date", required=True)
    project = sub.add_parser("project", help="从数据库重建某日 Obsidian 文档")
    project.add_argument("--date", required=True)
    args = parser.parse_args(argv)

    store = MajorNewsGraphStore(database=NEWS_EVENT_DB_PATH)
    if args.command in {"update", "add"}:
        call = ["--trade-date", args.date]
        if args.force:
            call.append("--force")
        _run_module("service.service_daily_major_news_analysis", call)
    elif args.command == "backfill":
        return _backfill(store, args)
    elif args.command == "import":
        lock_path = OFFLINE_DB_DIR / ".news_events.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("w") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            _print(store.import_day(args.date, args.artifact_root))
    elif args.command == "query":
        rows = store.query_analysis(args.start_date, args.end_date, theme_id=args.theme_id,
                                    theme_label=args.theme_label, entity=args.entity, keyword=args.keyword,
                                    market_impact=args.impact, min_importance=args.min_importance)
        _print({"total": len(rows), "items": rows.head(args.limit).to_dict("records")})
    elif args.command == "event":
        _print(store.query_event_graph(args.id, as_of=args.as_of))
    elif args.command == "status":
        _print(store.query_daily_summary(args.start_date, args.end_date).to_dict("records"))
    elif args.command == "project":
        _run_module("service.project_daily_news_to_obsidian", ["--trade-date", args.date, "--mode", "fast"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

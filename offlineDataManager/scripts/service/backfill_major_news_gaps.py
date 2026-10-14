#!/usr/bin/env python3
"""按自然日补抓或复核 tbl_major_news，不改写日常增量断点。"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from typing import Any

import pandas as pd

from coreClient._paths import manager_root, offline_data_dir
ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

from config.settings import DB_PATH_NEWS  # noqa: E402
from core.offline_db_client import upsert_df  # noqa: E402


def _date(value: str) -> date:
    return datetime.strptime(value.replace("-", "")[:8], "%Y%m%d").date()


def source_count(conn: sqlite3.Connection, day: date) -> int:
    start = day.isoformat()
    end = (day + timedelta(days=1)).isoformat()
    return int(conn.execute("""SELECT count(*) FROM tbl_major_news
        WHERE datetime>=? AND datetime<?""", (start, end)).fetchone()[0])


def missing_days(conn: sqlite3.Connection, start: date, end: date) -> list[date]:
    return [day for day in (start + timedelta(days=index) for index in range((end - start).days + 1))
            if source_count(conn, day) == 0]


def calendar_days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=index) for index in range((end - start).days + 1)]


def prepare_major_news_frame(frame, day: date):
    """沿用日常 service_major_news 的主键口径，并拒绝越日/截断数据。"""
    if frame is None or frame.empty:
        raise ValueError(f"{day} 接口返回空数据；该日继续保留为缺口")
    required = {"pub_time", "src", "title"}
    if not required.issubset(frame.columns):
        raise ValueError(f"{day} 缺少必要字段: {sorted(required - set(frame.columns))}")
    result = frame.copy()
    result["datetime"] = result["pub_time"].astype(str)
    if result["datetime"].isna().any() or not result["datetime"].str.startswith(day.isoformat()).all():
        raise ValueError(f"{day} 接口返回了越日或无效时间")
    result["md5"] = result["title"].apply(
        lambda value: hashlib.md5(str(value).encode("utf-8")).hexdigest() if value else "no_title")
    result["snap_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return result


def fetch_remote_day(day: date, host: str, *, page_size: int = 500, max_pages: int = 20) -> pd.DataFrame:
    """通过已配置 token 的 Mac mini 分页抓取；凭据留在远端。"""
    script = f"""
import os
import time
import pandas as pd
import tushare as ts

token = os.environ.get('TRADING_AGENT_TUSHARE_TOKEN')
if not token:
    raise RuntimeError('远端交互式 shell 未配置 Tushare token')
pro = ts.pro_api(token)
frames = []
for page in range({max_pages}):
    frame = pro.major_news(start_date='{day} 00:00:00', end_date='{day} 23:59:59',
                           limit={page_size}, offset=page * {page_size})
    if frame is None or frame.empty:
        break
    frames.append(frame)
    if len(frame) < {page_size}:
        break
    time.sleep(0.5)
else:
    raise RuntimeError('分页达到上限，可能截断')
result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
print(result.to_json(orient='records', force_ascii=False, date_format='iso'))
"""
    result = subprocess.run(
        ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
         host, "zsh -ic 'python3 -'"],
        input=script.encode("utf-8"), capture_output=True, timeout=300, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"远端抓取失败，exit={result.returncode}；凭据和原始错误输出未写入日志")
    try:
        rows = json.loads(result.stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("远端新闻输出不是完整 JSON") from exc
    if not isinstance(rows, list):
        raise RuntimeError("远端新闻输出应是 JSON 列表")
    return pd.DataFrame(rows)


def fetch_local_day(day: date, client, *, page_size: int = 500, max_pages: int = 20) -> pd.DataFrame:
    frames = []
    for page in range(max_pages):
        frame = client.major_news(start_date=f"{day} 00:00:00", end_date=f"{day} 23:59:59",
                                  limit=page_size, offset=page * page_size)
        if frame is None or frame.empty:
            break
        frames.append(frame)
        if len(frame) < page_size:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"{day} 分页达到上限，可能截断")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.now().isoformat(timespec="seconds")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tushare Major News 历史缺口逐日补抓")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--database", type=Path, default=DB_PATH_NEWS)
    parser.add_argument("--status-file", type=Path)
    parser.add_argument("--max-consecutive-failures", type=int, default=3)
    parser.add_argument("--ssh-host", help="在已配好 token 的远端主机上查询，凭据不传回本机")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--refresh-existing", action="store_true", help="逐日分页复核已有日期并补入遗漏报道")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.page_size < 1 or args.page_size > 1000:
        parser.error("page-size 必须在 1 到 1000 之间")
    start, end = _date(args.start_date), _date(args.end_date)
    if end < start:
        parser.error("end-date 不得早于 start-date")
    conn = sqlite3.connect(str(args.database), timeout=30)
    try:
        gaps = missing_days(conn, start, end)
        targets = calendar_days(start, end) if args.refresh_existing else gaps
        if args.dry_run:
            print(json.dumps({"missing_count": len(gaps), "target_count": len(targets),
                              "dates": [str(day) for day in targets]}, ensure_ascii=False))
            return 0
        if not targets:
            print("原始 Major News 已逐日覆盖指定日期范围")
            return 0
        client = None
        if not args.ssh_host:
            # 凭据只从本机环境读取，绝不写入状态或日志。
            from coreClient.tushare_config import TUSHARE_TOKEN
            if not TUSHARE_TOKEN:
                raise RuntimeError("未配置 TRADING_AGENT_TUSHARE_TOKEN；可用 --ssh-host macmini，勿在聊天中发送 token")
            from coreClient.tushare_client import TushareClient
            client = TushareClient()
        prefix = "source_full_refresh" if args.refresh_existing else "source_gap_backfill"
        status_path = args.status_file or ROOT / "outputs" / "major_news_analysis" / f"{prefix}_{start:%Y%m%d}_{end:%Y%m%d}.json"
        state: dict[str, Any] = {}
        if status_path.exists():
            state = json.loads(status_path.read_text(encoding="utf-8"))
        if (state.get("start_date") != str(start) or state.get("end_date") != str(end)
                or state.get("refresh_existing") != args.refresh_existing):
            state = {"start_date": str(start), "end_date": str(end),
                     "refresh_existing": args.refresh_existing,
                     "missing_initially": len(gaps), "target_days": len(targets),
                     "days": {}}
        state["status"] = "running"
        _write_state(status_path, state)
        consecutive_failures = 0
        for index, day in enumerate(targets, 1):
            if state["days"].get(str(day), {}).get("status") == "complete" and source_count(conn, day):
                print(f"[{index}/{len(targets)}] {day} 已完成，跳过", flush=True)
                continue
            if not args.refresh_existing and source_count(conn, day):
                state["days"][str(day)] = {"status": "already_present", "count": source_count(conn, day)}
                _write_state(status_path, state)
                continue
            try:
                frame = (fetch_remote_day(day, args.ssh_host, page_size=args.page_size)
                         if args.ssh_host else fetch_local_day(day, client, page_size=args.page_size))
                ready = prepare_major_news_frame(frame, day)
                inserted = upsert_df(conn, ready, "tbl_major_news", key_cols=["datetime", "src", "md5"])
                count = source_count(conn, day)
                if not count:
                    raise RuntimeError("写入后该日仍无原始新闻")
                state["days"][str(day)] = {"status": "complete", "fetched": len(ready),
                                           "inserted": inserted, "source_count": count}
                consecutive_failures = 0
                print(f"[{index}/{len(targets)}] {day} 补入 {inserted} 条，库内 {count} 条", flush=True)
            except Exception as exc:
                state["days"][str(day)] = {"status": "failed", "error": str(exc)[:500]}
                consecutive_failures += 1
                print(f"[{index}/{len(targets)}] {day} 失败: {exc}", flush=True)
            _write_state(status_path, state)
            if consecutive_failures >= args.max_consecutive_failures:
                state["status"] = "stopped_after_consecutive_failures"
                state["remaining_missing"] = [str(value) for value in missing_days(conn, start, end)]
                state["failed_dates"] = [value for value, item in state["days"].items()
                                         if item["status"] == "failed"]
                _write_state(status_path, state)
                return 1
        remaining = missing_days(conn, start, end)
        state["remaining_missing"] = [str(day) for day in remaining]
        failures = [day for day, item in state["days"].items() if item["status"] == "failed"]
        state["failed_dates"] = failures
        state["status"] = "complete" if not remaining and not failures else "incomplete"
        _write_state(status_path, state)
        print(json.dumps({"status_file": str(status_path), "remaining_missing": len(remaining),
                          "failed_days": len(failures)}, ensure_ascii=False))
        return 0 if not remaining and not failures else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

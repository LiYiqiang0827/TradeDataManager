"""Major News 增量清洗、过滤和规则事件聚类。"""
from __future__ import annotations

import argparse
import fcntl
from pathlib import Path
from coreClient._paths import manager_root, offline_data_dir
import sys

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from core.news_event_store import NewsEventStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Major News 去重、行情结果过滤和事件聚类")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args(argv)
    logger.add(str(PROJECT_ROOT / "logs" / "service_news_events.log"), rotation="50 MB", enqueue=True)
    lock_path = offline_data_dir() / ".news_events.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logger.info("[service_news_events] 已有实例运行，本轮跳过")
            return 0
        try:
            result = NewsEventStore().build(args.start_date, args.end_date, rebuild=args.rebuild)
            logger.info(f"[service_news_events] 完成: {result}")
            return 0
        except Exception as exc:
            logger.exception(f"[service_news_events] 失败: {exc}")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""生成单日 Major News 分析并投影到 Obsidian。

流程固定为规则事件层、BGE-M3 语义聚类、reranker/Qwen 快速级联和每日文档。
每一步都落盘，任务中断后再次运行会复用已完成结果。
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import fcntl
import json
from pathlib import Path
from coreClient._paths import manager_root, offline_data_dir
import subprocess
import sys
import time

from loguru import logger


PROJECT_ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()
REPO_ROOT = PROJECT_ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from core.news_event_store import NewsEventStore  # noqa: E402
from core.major_news_graph_store import MajorNewsGraphStore  # noqa: E402


def _run(command: list[str], env: dict[str, str] | None = None) -> None:
    logger.info("[daily_major_news] 运行: {}", " ".join(command))
    subprocess.run(command, cwd=REPO_ROOT, env=env, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="单日 Major News 清洗、提炼及 Obsidian 投影")
    parser.add_argument("--trade-date", help="YYYYMMDD，默认前一自然日")
    parser.add_argument("--force", action="store_true", help="重跑向量与模型批次")
    parser.add_argument("--skip-obsidian", action="store_true")
    parser.add_argument("--max-candidates", type=int, default=1000)
    parser.add_argument("--max-deep-events", type=int, default=60)
    parser.add_argument("--stage", choices=["all", "prepare", "enrich", "publish"], default="all",
                        help="流水线模式：准备、模型提炼和入库投影分阶段执行")
    args = parser.parse_args(argv)
    target = args.trade_date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    target = datetime.strptime(target.replace("-", "")[:8], "%Y%m%d").strftime("%Y%m%d")

    log_file = PROJECT_ROOT / "logs" / "service_daily_major_news_analysis.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    lock_path = OFFLINE_DB_DIR / ".news_events.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    python = sys.executable
    output_root = REPO_ROOT / "outputs" / "major_news_analysis"
    database = OFFLINE_DB_DIR / "db_major_news_events.duckdb"
    cluster = [
        python, str(PROJECT_ROOT / "scripts/experiments/news_event_cluster_pilot.py"),
        "--trade-date", target, "--output-root", str(output_root),
        "--database", str(database),
    ]
    fast = [
        python, str(PROJECT_ROOT / "scripts/experiments/news_event_fast_pilot.py"),
        "--trade-date", target, "--input-root", str(output_root),
        "--max-candidates", str(args.max_candidates),
        "--min-singleton-content", "0", "--stage1-batch-size", "100",
        "--max-deep-events", str(args.max_deep_events),
        "--stage2-batch-size", "20", "--workers", "4",
    ]
    if args.force:
        cluster.append("--force-embed")
        fast.extend(["--force", "--force-relevance"])

    def publish() -> dict:
        graph_result = MajorNewsGraphStore(database=database).import_day(target, output_root)
        if not args.skip_obsidian:
            _run([
                python, str(PROJECT_ROOT / "scripts/service/project_daily_news_to_obsidian.py"),
                "--trade-date", target, "--input-root", str(output_root), "--mode", "fast",
                "--database", str(database),
            ])
        return graph_result

    with lock_path.open("w") as lock:
        try:
            # 独立阶段需要等待数据库写锁；完整日常任务保留原有“已有实例则跳过”行为。
            if args.stage != "enrich":
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX if args.stage != "all"
                            else fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logger.info("[daily_major_news] 已有新闻分析或清洗实例运行，本轮跳过")
            return 0
        try:
            if args.stage == "enrich":
                # 模型阶段只使用日期独立的 CSV/JSON 产物，不持有 DuckDB 写锁。
                _run(fast)
                return 0
            if args.stage == "publish":
                publish()
                return 0
            cleaned = NewsEventStore().build(target, target)
            _run(cluster)
            if args.stage == "prepare":
                return 0
            _run(fast)
            graph_result = publish()
            summary_path = output_root / target / "fast_summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            logger.info("[daily_major_news] 完成: clean={} graph={} summary={} elapsed={:.1f}s", cleaned, graph_result, summary, time.perf_counter() - started)
            return 0
        except Exception as exc:
            logger.exception("[daily_major_news] 失败: {}", exc)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())

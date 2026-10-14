"""每日新闻事件 LLM 筛选与结构化提炼服务。"""
from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
from pathlib import Path
from coreClient._paths import manager_root, offline_data_dir
import sys

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from core.news_llm_refiner import NewsLLMRefiner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="每日新闻事件 LLM 筛选、压缩与提炼")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--provider", choices=["qwen-vllm", "minimax-hermes"], default="qwen-vllm")
    parser.add_argument("--model")
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--max-tokens", type=int, default=6000)
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--sample", action="store_true", help="配合--limit按事件ID确定性抽样")
    parser.add_argument("--sample-seed", default="news-refinement-v2")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    end_date = args.end_date or datetime.now().strftime("%Y%m%d")
    start_date = args.start_date or end_date
    logger.add(str(PROJECT_ROOT / "logs" / "service_news_llm.log"), rotation="50 MB", enqueue=True)
    # LLM表和事件表位于同一个DuckDB，必须与确定性清洗共用写锁；模型运行
    # 期间原始SQLite仍可更新，下一轮清洗会从断点补齐。
    lock_path = offline_data_dir() / ".news_events.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logger.info("[service_news_llm] 已有实例运行，本轮跳过")
            return 0
        try:
            result = NewsLLMRefiner().refine(
                start_date, end_date, provider=args.provider, model=args.model,
                batch_size=args.batch_size, force=args.force, timeout=args.timeout,
                max_tokens=args.max_tokens, limit=args.limit,
                sample=args.sample, sample_seed=args.sample_seed,
            )
            logger.info(f"[service_news_llm] 完成: {result}")
            return 0
        except Exception as exc:
            logger.exception(f"[service_news_llm] 失败: {exc}")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""开盘啦题材时序知识库的全量回填与每日增量服务。"""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
from pathlib import Path
import sys

logger = logging.getLogger(__name__)

OFFLINE_ROOT = Path(__file__).resolve().parents[2]
TRADING_AGENT_ROOT = OFFLINE_ROOT.parent
sys.path.insert(0, str(OFFLINE_ROOT / "scripts"))
sys.path.insert(0, str(TRADING_AGENT_ROOT))

from config.settings import DB_PATH_KPL, THEME_GRAPH_DB_PATH, THEME_VAULT_ROOT
from core.theme_graph_store import ThemeGraphStore, compact_date
from core.theme_vault import ThemeVaultRenderer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="构建 KPL 题材时序知识库")
    parser.add_argument("--mode", choices=["incremental", "backfill", "rebuild"], default="incremental")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--trade-date")
    parser.add_argument("--source-db", type=Path, default=DB_PATH_KPL)
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--vault", type=Path, default=THEME_VAULT_ROOT)
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--force", action="store_true", help="即使源哈希未变化也重建指定日期")
    parser.add_argument("--skip-vault", action="store_true")
    return parser


def run(args: argparse.Namespace) -> dict:
    database = Path(args.database).expanduser()
    if args.mode == "rebuild" and database.exists():
        database.unlink()
    store = ThemeGraphStore(database=database, source_db=args.source_db)
    if args.trade_date:
        start_date = end_date = compact_date(args.trade_date)
    elif args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise ValueError("start-date 与 end-date 必须同时提供")
        start_date, end_date = compact_date(args.start_date), compact_date(args.end_date)
    elif args.mode == "incremental":
        start_date, end_date = store.incremental_range(args.lookback_days)
    else:
        source_min, source_max = store.source_bounds()
        if not source_min or not source_max:
            raise RuntimeError("源库没有 KPL 数据")
        start_date, end_date = compact_date(source_min), compact_date(source_max)

    lock_path = database.with_suffix(database.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"已有题材知识库构建进程占用锁: {lock_path}") from exc
        result = store.build(start_date, end_date, mode=args.mode, force=args.force)
        if not args.skip_vault:
            renderer = ThemeVaultRenderer(database=database, vault_root=args.vault)
            result["vault"] = renderer.render()
    return result


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run(args)
    except Exception:
        logger.exception("题材知识库构建失败")
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

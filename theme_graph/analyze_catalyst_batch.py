"""按选择清单顺序、可恢复地执行题材催化分析。"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, research_root
ROOT = manager_root(__file__, 1)
RESEARCH_ROOT = research_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

from config.settings import DB_PATH_NEWS, THEME_GRAPH_DB_PATH
from core.theme_vault import ThemeVaultRenderer
from analyze_episode_catalysts import analyze
from select_catalyst_episodes import DEFAULT_EXCLUSIONS, load_exclusions


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_status(path: Path, status: dict) -> None:
    path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="批量运行题材催化分析；已有MANIFEST自动跳过")
    parser.add_argument("--manifest", type=Path,
                        default=RESEARCH_ROOT / "outputs" / "theme_catalyst_analysis" / "batch_v1.json")
    parser.add_argument("--priority", choices=["A", "B", "C", "all"], default="A")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--news-db", type=Path, default=DB_PATH_NEWS)
    parser.add_argument("--output-root", type=Path,
                        default=RESEARCH_ROOT / "outputs" / "theme_catalyst_analysis")
    parser.add_argument("--exclusions", type=Path, default=DEFAULT_EXCLUSIONS)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--episode-retries", type=int, default=2,
                        help="单个题材在整条链路失败后的重试次数")
    parser.add_argument("--episode-retry-backoff", type=float, default=15.0)
    args = parser.parse_args()
    payload = json.loads(args.manifest.read_text(encoding="utf-8"))
    exclusions = load_exclusions(args.exclusions)
    excluded_rows = [
        row for row in payload["rows"]
        if row.get("level1_name") in exclusions["exclude_level1_names"]
        or row.get("canonical_name") in exclusions["exclude_theme_names"]
    ]
    rows = [
        row for row in payload["rows"]
        if (args.priority == "all" or row["priority"] == args.priority)
        and row not in excluded_rows
    ]
    if args.limit is not None:
        rows = rows[:args.limit]
    status_path = args.output_root / "batch_status.json"
    status = {
        "started_at": _now(), "updated_at": _now(), "priority": args.priority,
        "total": len(rows), "current": None,
        "completed": [], "skipped": [], "failed": [],
        "excluded": [
            {"episode_id": row["episode_id"], "canonical_name": row["canonical_name"],
             "level1_name": row.get("level1_name")}
            for row in excluded_rows
            if args.priority == "all" or row["priority"] == args.priority
        ],
    }
    _write_status(status_path, status)
    for row in rows:
        target = args.output_root / row["episode_id"]
        if (target / "MANIFEST.json").exists() and not args.force:
            status["skipped"].append(row["episode_id"])
            status["updated_at"] = _now()
            _write_status(status_path, status)
            continue
        status["current"] = {"episode_id": row["episode_id"], "attempt": 1, "started_at": _now()}
        status["updated_at"] = _now()
        _write_status(status_path, status)
        final_error = None
        for attempt in range(args.episode_retries + 1):
            status["current"]["attempt"] = attempt + 1
            status["updated_at"] = _now()
            _write_status(status_path, status)
            try:
                analyze(row["episode_id"], args.database, args.news_db, target,
                        save=not args.no_save)
                status["completed"].append(row["episode_id"])
                final_error = None
                (target / "ERROR.json").unlink(missing_ok=True)
                break
            except Exception as exc:
                final_error = {
                    "episode_id": row["episode_id"], "attempt": attempt + 1,
                    "error": str(exc), "traceback": traceback.format_exc(), "failed_at": _now(),
                }
                target.mkdir(parents=True, exist_ok=True)
                (target / "ERROR.json").write_text(
                    json.dumps(final_error, ensure_ascii=False, indent=2), encoding="utf-8")
                if attempt < args.episode_retries:
                    delay = min(120.0, args.episode_retry_backoff * (2 ** attempt))
                    print(
                        f"{row['episode_id']} 失败，{delay:.0f}秒后重试 "
                        f"{attempt + 1}/{args.episode_retries}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    time.sleep(delay)
        if final_error is not None:
            status["failed"].append(final_error)
        status["current"] = None
        status["updated_at"] = _now()
        _write_status(status_path, status)
    if status["completed"] and not args.no_save:
        status["vault_render"] = ThemeVaultRenderer(args.database).render()
        status["updated_at"] = _now()
        _write_status(status_path, status)
    summary = {key: len(status[key]) for key in ("completed", "skipped", "failed")}
    if "vault_render" in status:
        summary["vault_render"] = status["vault_render"]
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if status["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

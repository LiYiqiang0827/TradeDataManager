"""为234个主归因题材选择第一轮最有研究价值的行情周期。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, research_root
ROOT = manager_root(__file__, 1)
RESEARCH_ROOT = research_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

import duckdb
from config.settings import THEME_GRAPH_DB_PATH

DEFAULT_EXCLUSIONS = Path(__file__).resolve().parent / "taxonomy" / "analysis_exclusions_v1.json"


def load_exclusions(path: Path = DEFAULT_EXCLUSIONS) -> dict:
    if not path.exists():
        return {"exclude_level1_names": [], "exclude_theme_names": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "exclude_level1_names": payload.get("exclude_level1_names", []),
        "exclude_theme_names": payload.get("exclude_theme_names", []),
    }


def select(database: Path, exclusions_path: Path = DEFAULT_EXCLUSIONS):
    conn = duckdb.connect(str(database), read_only=True)
    try:
        frame = conn.execute(
            """WITH primary_themes AS (
                 SELECT DISTINCT theme_id FROM rel_limit_theme WHERE attribution_role='primary'
               ), ranked AS (
                 SELECT e.*,t.canonical_name,COALESCE(x.level1_name,'待归类') AS level1_name,
                        e.peak_heat + e.peak_breadth*3 + LEAST(e.active_sessions,10) AS research_score,
                        ROW_NUMBER() OVER (
                          PARTITION BY e.theme_id ORDER BY
                          e.peak_heat + e.peak_breadth*3 + LEAST(e.active_sessions,10) DESC,
                          e.peak_breadth DESC,e.peak_date DESC
                        ) AS seq
                 FROM fact_theme_episode e JOIN primary_themes p USING(theme_id)
                 JOIN dim_theme t USING(theme_id)
                 LEFT JOIN dim_theme_taxonomy x USING(theme_id)
               )
               SELECT theme_id,canonical_name,level1_name,episode_id,start_date,last_active_date,
                      peak_date,peak_heat,peak_breadth,active_sessions,research_score,
                      CASE WHEN peak_breadth>=3 OR peak_heat>=40 THEN 'A'
                           WHEN peak_breadth>=1 OR peak_heat>=20 THEN 'B' ELSE 'C' END AS priority
               FROM ranked WHERE seq=1 ORDER BY priority,research_score DESC,canonical_name""").fetchdf()
    finally:
        conn.close()
    exclusions = load_exclusions(exclusions_path)
    return frame[
        ~frame["level1_name"].isin(exclusions["exclude_level1_names"])
        & ~frame["canonical_name"].isin(exclusions["exclude_theme_names"])
    ].reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="每个主归因题材选择一个首轮催化研究周期")
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--output", type=Path,
                        default=RESEARCH_ROOT / "outputs" / "theme_catalyst_analysis" / "batch_v1.json")
    parser.add_argument("--exclusions", type=Path, default=DEFAULT_EXCLUSIONS)
    args = parser.parse_args()
    frame = select(args.database, args.exclusions)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"version": "catalyst-batch-v1", "selection": "strongest_episode_per_primary_theme",
                                       "exclusions": load_exclusions(args.exclusions),
                                       "rows": frame.to_dict("records")}, ensure_ascii=False, indent=2),
                           encoding="utf-8")
    frame.to_csv(args.output.with_suffix(".csv"), index=False)
    print(json.dumps({"themes": len(frame), "priority": frame.groupby("priority").size().to_dict(),
                      "json": str(args.output), "csv": str(args.output.with_suffix('.csv'))},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

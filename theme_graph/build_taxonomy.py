"""生成并写入一级/二级题材映射。模型只生成草案，硬规则始终优先。"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
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
from core.theme_graph_store import SCHEMA_SQL
from model_runner import call_qwen, extract_json


BASE = Path(__file__).resolve().parent
CONFIG_PATH = BASE / "taxonomy" / "level1_v1.json"
MAPPING_PATH = BASE / "taxonomy" / "theme_mapping_v1.json"


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _prompt(config: dict, names: list[str]) -> str:
    categories = "\n".join(f"- {item['name']}: {item['definition']}" for item in config["categories"])
    return f"""你在为A股涨停题材建立两级分类。只能从允许的一级题材中选择。
每个原始题材保留为二级题材，不得改名或合并。给一个 primary_level1，并可给0到2个 secondary_level1。
分类按A股炒作语义，不是严格申万行业。电子布、MLCC、电阻电容按用户口径归半导体。
不确定时使用其他事件并降低confidence。只输出JSON对象：
{{"items":[{{"theme":"原名","primary_level1":"一级题材","secondary_level1":[],"confidence":0.0,"rationale":"20字内"}}]}}

允许的一级题材：
{categories}

待分类题材：
{json.dumps(names, ensure_ascii=False)}"""


def apply_curated_rules(items: list[dict], config: dict) -> list[dict]:
    aliases = {"区域政策": "政策与区域"}
    hard = config.get("hard_overrides", {})
    secondary_overrides = config.get("secondary_overrides", {})
    for item in items:
        item["primary_level1"] = aliases.get(item["primary_level1"], item["primary_level1"])
        item["secondary_level1"] = list(dict.fromkeys(
            aliases.get(value, value) for value in item.get("secondary_level1", [])
            if aliases.get(value, value) != item["primary_level1"]
        ))[:2]
        if item["level2_name"] in hard:
            item["primary_level1"] = hard[item["level2_name"]]
            item["confidence"] = 1.0
            item["classification_source"] = "user_or_curated_override"
            item["review_status"] = "confirmed"
            item["rationale"] = "用户口径或人工复核规则"
        elif item.get("confidence", 0) >= 0.75 and item["primary_level1"] != "其他事件":
            item["review_status"] = "reviewed_v1"
        if item["level2_name"] in secondary_overrides:
            item["secondary_level1"] = secondary_overrides[item["level2_name"]]
        item["secondary_level1"] = [value for value in item.get("secondary_level1", [])
                                    if value != item["primary_level1"]][:2]
    return items


def generate(database: Path, batch_size: int = 20) -> list[dict]:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    allowed = {item["name"] for item in config["categories"]}
    conn = duckdb.connect(str(database), read_only=True)
    try:
        rows = conn.execute(
            """SELECT t.theme_id,t.canonical_name FROM dim_theme t WHERE EXISTS(
                 SELECT 1 FROM rel_limit_theme r WHERE r.theme_id=t.theme_id
                 AND r.attribution_role='primary') ORDER BY t.canonical_name""").fetchall()
    finally:
        conn.close()
    by_name = {name: theme_id for theme_id, name in rows}
    results = []
    names = list(by_name)

    def classify_batch(batch: list[str]) -> list[dict]:
        try:
            raw, model = call_qwen(_prompt(config, batch), max_tokens=max(1800, len(batch) * 180))
            output = extract_json(raw)
            returned = {item.get("theme"): item for item in output.get("items", [])}
            if any(name not in returned for name in batch):
                raise ValueError("模型遗漏题材")
        except Exception:
            if len(batch) == 1:
                name = batch[0]
                return [{"theme_id": by_name[name], "level2_name": name,
                         "primary_level1": "其他事件", "secondary_level1": [],
                         "confidence": 0.0, "rationale": "模型输出失败，待人工复核",
                         "classification_source": "fallback", "review_status": "needs_review"}]
            middle = len(batch) // 2
            return classify_batch(batch[:middle]) + classify_batch(batch[middle:])
        batch_results = []
        for name in batch:
            item = returned[name]
            primary = item.get("primary_level1")
            if primary not in allowed:
                primary = "其他事件"
            secondary = [value for value in item.get("secondary_level1", [])
                         if value in allowed and value != primary][:2]
            batch_results.append({"theme_id": by_name[name], "level2_name": name,
                                  "primary_level1": primary, "secondary_level1": secondary,
                                  "confidence": float(item.get("confidence", 0.35)),
                                  "rationale": str(item.get("rationale", "模型初分")),
                                  "classification_source": f"qwen-vllm:{model}",
                                  "review_status": "needs_review"})
        return batch_results

    for start in range(0, len(names), batch_size):
        batch = names[start:start + batch_size]
        results.extend(classify_batch(batch))
        MAPPING_PATH.write_text(json.dumps({"version": config["version"], "items": results},
                                           ensure_ascii=False, indent=2), encoding="utf-8")
    results = apply_curated_rules(results, config)
    MAPPING_PATH.write_text(json.dumps({"version": config["version"], "items": results},
                                       ensure_ascii=False, indent=2), encoding="utf-8")
    return results


def apply(database: Path) -> int:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    mapping["items"] = apply_curated_rules(mapping["items"], config)
    MAPPING_PATH.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    categories = {item["name"]: item["id"] for item in config["categories"]}
    records = []
    for item in mapping["items"]:
        name = item["primary_level1"]
        records.append([item["theme_id"], categories[name], name, item["level2_name"],
                        json.dumps(item.get("secondary_level1", []), ensure_ascii=False),
                        item["classification_source"], float(item["confidence"]), item.get("rationale", ""),
                        mapping["version"], item.get("review_status", "needs_review"), _now()])
    conn = duckdb.connect(str(database))
    try:
        conn.execute(SCHEMA_SQL)
        conn.execute("BEGIN")
        conn.execute("DELETE FROM dim_theme_taxonomy WHERE taxonomy_version=?", [mapping["version"]])
        conn.executemany("INSERT INTO dim_theme_taxonomy VALUES (?,?,?,?,?,?,?,?,?,?,?)", records)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    return len(records)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--generate", action="store_true", help="调用本地Qwen重新生成分类草案")
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()
    if args.generate or not MAPPING_PATH.exists():
        generate(args.database, args.batch_size)
    count = apply(args.database)
    print(json.dumps({"mapping": str(MAPPING_PATH), "rows": count}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

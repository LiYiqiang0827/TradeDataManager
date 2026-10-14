"""把人工/联网核验的外部原始证据并入既有周期，再走完整模型审计链。"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, research_root
ROOT = manager_root(__file__, 1)
RESEARCH_ROOT = research_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

from config.settings import THEME_GRAPH_DB_PATH
from core.theme_vault import ThemeVaultRenderer
from analyze_episode_catalysts import _selected_evidence, _write
from model_runner import run_task


REQUIRED_EXTERNAL_FIELDS = {
    "url", "title", "source", "published_at", "observed_at", "matched_focus_date", "content",
}


def _load_external(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("evidence", []) if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not rows:
        raise ValueError("外部证据文件必须是非空数组，或包含非空 evidence 数组")
    normalized = []
    seen_ids, seen_urls = set(), set()
    for index, raw in enumerate(rows, 1):
        missing = REQUIRED_EXTERNAL_FIELDS - set(raw)
        if missing:
            raise ValueError(f"外部证据第{index}条缺字段: {sorted(missing)}")
        evidence_id = str(raw.get("evidence_id") or f"NW{index}")
        if not evidence_id.startswith("N"):
            raise ValueError(f"外部新闻 evidence_id 必须以 N 开头: {evidence_id}")
        if evidence_id in seen_ids or raw["url"] in seen_urls:
            raise ValueError(f"外部证据ID或URL重复: {evidence_id} / {raw['url']}")
        seen_ids.add(evidence_id)
        seen_urls.add(raw["url"])
        normalized.append({
            **raw,
            "evidence_id": evidence_id,
            "source_table": "external_web",
            "availability_basis": raw.get("availability_basis", "official_page_date_time_unknown"),
            "matched_terms": raw.get("matched_terms", []),
            "nearest_focus_date": raw["matched_focus_date"],
            "duplicate_sources": raw.get("duplicate_sources", [raw["source"]]),
            "candidate_score": float(raw.get("candidate_score", 50)),
            "evidence_kind_hint": raw.get("evidence_kind_hint", "source_event"),
            "has_independent_fact_marker": True,
            "market_recap_penalty": 0.0,
            "source_record_id": raw.get("source_record_id", raw["url"]),
        })
    return normalized


def reanalyze(episode_id: str, evidence_file: Path, output_dir: Path, database: Path,
              screening_provider: str = "qwen-vllm", synthesis_provider: str = "minimax-hermes",
              audit_provider: str = "qwen-vllm", save: bool = True) -> dict[str, Any]:
    base_path = output_dir / "05_extraction_packet.json"
    if not base_path.exists():
        raise FileNotFoundError(f"缺少既有分析包: {base_path}")
    base = json.loads(base_path.read_text(encoding="utf-8"))
    external = _load_external(evidence_file)
    screening_packet = {
        "entity_type": "theme_episode", "entity_id": episode_id,
        "episode_id": episode_id, "valid_at": base["valid_at"],
        "analysis_mode": "external_evidence_screening_with_stage_time_guard",
        "theme": base["theme"], "episode": base["episode"],
        "news_window": base["news_window"],
        "candidate_evidence": [
            {**item, "snippet": item["content"][:500]} for item in external
        ],
    }
    _write(output_dir / "10_external_screening_packet.json", screening_packet)
    # 外部证据筛选只是补丁子步骤，不覆盖知识库中“完整候选集筛选”的最新版本；
    # 最终合并后的 extraction/audit/summary 仍正常写入 DuckDB。
    screened = run_task("catalyst_screening", screening_packet, screening_provider,
                        database=database, save=False, timeout=300, max_tokens=3500)
    _write(output_dir / f"11_external_screening_{screening_provider}.json", screened)
    selected_external = _selected_evidence(screened["output"], external, limit=len(external))
    _write(output_dir / "12_external_selected.json", selected_external)

    merged = list(base.get("selected_evidence", [])) + selected_external
    merged = list({item["evidence_id"]: item for item in merged}.values())
    selected_decisions = [item.get("screening", {}) for item in merged if item.get("screening")]
    extraction_packet = {
        **{key: base[key] for key in (
            "entity_type", "entity_id", "episode_id", "valid_at", "analysis_mode",
            "theme", "episode", "news_window", "market_evidence", "stock_evidence",
        )},
        "analysis_mode": "retrospective_episode_with_external_verified_evidence",
        "selected_evidence": merged,
        "screening_output": {
            "episode_id": episode_id, "items": selected_decisions,
            "duplicate_clusters": [],
            "missing_information": screened["output"].get("missing_information", []),
            "confidence": screened["output"].get("confidence", 0), "needs_review": True,
        },
    }
    _write(output_dir / "13_external_extraction_packet.json", extraction_packet)
    synthesis = run_task("catalyst_extraction", extraction_packet, synthesis_provider,
                         database=database, save=save, timeout=360, max_tokens=6000)
    _write(output_dir / f"14_external_catalyst_{synthesis_provider}.json", synthesis)
    audit_packet = {
        "entity_type": "theme_episode", "entity_id": episode_id,
        "episode_id": episode_id, "valid_at": base["valid_at"], "theme": base["theme"],
        "episode": base["episode"], "market_evidence": base["market_evidence"],
        "stock_evidence": base["stock_evidence"], "selected_evidence": merged,
        "analysis_to_audit": synthesis["output"],
    }
    audit = run_task("catalyst_audit", audit_packet, audit_provider,
                     database=database, save=save, timeout=300, max_tokens=5000)
    _write(output_dir / f"15_external_audit_{audit_provider}.json", audit)
    summary_packet = {
        "entity_type": "theme_episode", "entity_id": episode_id,
        "episode_id": episode_id, "valid_at": base["valid_at"], "theme": base["theme"],
        "episode": base["episode"], "market_evidence": base["market_evidence"],
        "stock_evidence": base["stock_evidence"], "selected_evidence": merged,
        "catalyst_analysis": synthesis["output"], "catalyst_audit": audit["output"],
    }
    summary = run_task("episode_summary", summary_packet, synthesis_provider,
                       database=database, save=save, timeout=360, max_tokens=5000)
    _write(output_dir / f"16_external_episode_summary_{synthesis_provider}.json", summary)

    revised = audit["output"].get("revised_conclusion") or {}
    reason_status = revised.get("reason_status", synthesis["output"].get("reason_status"))
    causal_status = revised.get(
        "causal_status", synthesis["output"].get("dominant_reason", {}).get("causal_status")
    )
    result = {
        "episode_id": episode_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "external_evidence": len(external), "external_selected": len(selected_external),
        "merged_evidence": len(merged), "audit_verdict": audit["output"]["verdict"],
        "audited_reason_status": reason_status, "audited_causal_status": causal_status,
        "needs_web_research": reason_status == "no_reliable_reason" or causal_status in {"unknown", "background_only"},
        "needs_review": True, "database_saved": save,
    }
    _write(output_dir / "17_external_manifest.json", result)
    manifest_path = output_dir / "MANIFEST.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["external_research"] = result
        _write(manifest_path, manifest)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="合并联网/人工核验的外部证据并重跑题材归因")
    parser.add_argument("--episode-id", required=True)
    parser.add_argument("--evidence-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()
    output_dir = args.output_dir or RESEARCH_ROOT / "outputs" / "theme_catalyst_analysis" / args.episode_id
    result = reanalyze(args.episode_id, args.evidence_file, output_dir, args.database,
                       save=not args.no_save)
    if not args.no_save:
        result["vault_render"] = ThemeVaultRenderer(args.database).render()
        _write(output_dir / "17_external_manifest.json", result)
        manifest_path = output_dir / "MANIFEST.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["external_research"] = result
            _write(manifest_path, manifest)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

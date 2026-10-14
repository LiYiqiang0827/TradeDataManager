"""Qwen筛新闻、Hermes/MiniMax归纳原因，并生成可复核的题材周期草案。"""
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

from config.settings import DB_PATH_NEWS, THEME_GRAPH_DB_PATH
from core.theme_vault import ThemeVaultRenderer

from build_catalyst_packet import build_episode_packet
from model_runner import build_prompt, run_task, save_task_output, validate_output, validate_task_output


def _write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def _resume_result(path: Path, task: str, packet: dict, provider: str) -> dict | None:
    """读取已落盘且可再次通过校验的阶段结果；损坏文件自动回退到重算。"""
    if not path.exists():
        return None
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
        if result.get("provider") != provider or not isinstance(result.get("output"), dict):
            return None
        validate_task_output(task, packet, result["output"])
        return result
    except (OSError, json.JSONDecodeError, ValueError, KeyError, TypeError):
        return None


def _selected_evidence(screening: dict, full: list[dict], limit: int = 18) -> list[dict]:
    by_id = {item["evidence_id"]: item for item in full}
    decisions = {item["evidence_id"]: item for item in screening["items"]}
    selected = []
    for evidence in full:
        decision = decisions[evidence["evidence_id"]]
        if decision["relevance"] == "unrelated":
            continue
        selected.append({**evidence, "screening": decision})
    # 原候选顺序包含程序化相关度排序；反证完整保留，纯行情异动稿放到最后。
    # 异动稿仍可验证市场响应，但不能挤掉独立政策/公告/产业事件。
    counters = [item for item in selected if item["screening"]["relevance"] == "counter"]
    source_facts = [item for item in selected
                    if item["screening"]["relevance"] != "counter"
                    and item["screening"].get("evidence_nature") != "market_recap"]
    source_facts.sort(key=lambda item: (
        item["screening"].get("evidence_nature") != "trigger_fact",
        -float(item.get("candidate_score", 0)),
    ))
    recaps = [item for item in selected
              if item["screening"]["relevance"] != "counter"
              and item["screening"].get("evidence_nature") == "market_recap"]
    recaps.sort(key=lambda item: (
        item["screening"].get("candidate_causal_status") not in {"confirmed_trigger", "plausible_trigger"},
        not bool(item.get("has_independent_fact_marker")),
        -float(item.get("candidate_score", 0)),
    ))
    room = max(0, limit - len(counters))
    # 最多保留3篇含独立事件的异动稿用于追溯原始事件；纯异动稿不占精选名额。
    recap_candidates = [item for item in recaps if item.get("has_independent_fact_marker")]
    recap_limit = min(3, len(recap_candidates), room)
    source_room = max(0, room - recap_limit)
    chosen = source_facts[:source_room] + recap_candidates[:recap_limit] + counters
    return list({item["evidence_id"]: item for item in chosen}.values())[:limit]


def _screening_packet(packet: dict, candidates: list[dict], batch_index: int,
                      batch_count: int) -> dict:
    """新闻筛选只携带判别所需字段，避免把完整涨停明细重复送入每个请求。"""
    keep = (
        "entity_type", "entity_id", "episode_id", "valid_at", "analysis_mode",
        "theme", "episode", "news_window", "search_terms", "focus_dates",
    )
    return {
        **{key: packet[key] for key in keep if key in packet},
        "batch_context": {
            "batch_index": batch_index, "batch_count": batch_count,
            "instruction": "只筛选本批candidate_evidence；episode_id原样返回。",
        },
        "candidate_evidence": candidates,
    }


def _run_screening_batched(packet: dict, provider: str, database: Path, save: bool,
                           batch_size: int = 10) -> dict:
    """拆分长JSON筛选请求；合并后再作为一条完整分析入库。"""
    candidates = packet.get("candidate_evidence", [])
    batches = [candidates[index:index + batch_size]
               for index in range(0, len(candidates), batch_size)] or [[]]
    results = []
    for index, candidates_batch in enumerate(batches, start=1):
        compact = _screening_packet(packet, candidates_batch, index, len(batches))
        result = run_task(
            "catalyst_screening", compact, provider, database=database,
            save=False, timeout=300, max_tokens=2600,
        )
        results.append(result)

    items_by_id = {
        item["evidence_id"]: item
        for result in results
        for item in result["output"].get("items", [])
    }
    output = {
        "episode_id": packet["episode_id"],
        "items": [items_by_id[item["evidence_id"]] for item in candidates],
        "duplicate_clusters": [
            cluster
            for result in results
            for cluster in result["output"].get("duplicate_clusters", [])
        ],
        "missing_information": list(dict.fromkeys(
            item
            for result in results
            for item in result["output"].get("missing_information", [])
        )),
        "confidence": min(float(result["output"].get("confidence", 0)) for result in results),
        "needs_review": any(bool(result["output"].get("needs_review", True)) for result in results),
    }
    validate_output("catalyst_screening", output)
    expected = {item["evidence_id"] for item in candidates}
    returned = {item["evidence_id"] for item in output["items"]}
    if returned != expected:
        raise ValueError(
            f"分批筛选合并后未完整覆盖候选证据: missing={sorted(expected-returned)}, "
            f"extra={sorted(returned-expected)}")
    prompt_version = build_prompt("catalyst_screening", packet)[1]
    model = results[0]["model"]
    if save:
        save_task_output(
            "catalyst_screening", packet, provider, model, prompt_version, output, database,
        )
    return {
        "task": "catalyst_screening", "provider": provider, "model": model,
        "prompt_version": prompt_version, "batch_count": len(batches), "output": output,
    }


def analyze(episode_id: str, database: Path, news_db: Path, output_dir: Path,
            screening_provider: str = "qwen-vllm", synthesis_provider: str = "minimax-hermes",
            audit_provider: str = "qwen-vllm", compare_provider: str | None = None, save: bool = True,
            max_candidates: int = 40) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    packet, full = build_episode_packet(episode_id, database, news_db,
                                        max_candidates=max_candidates)
    _write(output_dir / "01_screening_packet.json", packet)
    _write(output_dir / "02_news_full.json", full)

    screen_path = output_dir / f"03_screening_{screening_provider}.json"
    screen_result = _resume_result(
        screen_path, "catalyst_screening", packet, screening_provider,
    )
    if screen_result is None:
        screen_result = _run_screening_batched(
            packet, screening_provider, database=database, save=save,
        )
        _write(screen_path, screen_result)
        print(f"{episode_id} 新闻筛选完成", flush=True)
    else:
        print(f"{episode_id} 复用已完成的新闻筛选", flush=True)
    screening = screen_result["output"]
    selected = _selected_evidence(screening, full)
    selected_ids = {item["evidence_id"] for item in selected}
    screening_selected = {
        **screening,
        "items": [item for item in screening["items"] if item["evidence_id"] in selected_ids],
        "duplicate_clusters": [
            {**cluster, "member_ids": [item for item in cluster.get("member_ids", []) if item in selected_ids]}
            for cluster in screening.get("duplicate_clusters", [])
            if cluster.get("representative_id") in selected_ids
        ],
    }
    _write(output_dir / "04_selected_news.json", selected)

    extraction_packet = {
        "entity_type": "theme_episode", "entity_id": episode_id,
        "episode_id": episode_id, "valid_at": packet["valid_at"],
        "analysis_mode": packet["analysis_mode"], "theme": packet["theme"],
        "episode": packet["episode"], "news_window": packet["news_window"],
        "market_evidence": packet["market_evidence"], "stock_evidence": packet["stock_evidence"],
        "selected_evidence": selected, "screening_output": screening_selected,
    }
    _write(output_dir / "05_extraction_packet.json", extraction_packet)
    synthesis_path = output_dir / f"06_catalyst_{synthesis_provider}.json"
    synthesis_result = _resume_result(
        synthesis_path, "catalyst_extraction", extraction_packet, synthesis_provider,
    )
    if synthesis_result is None:
        synthesis_result = run_task(
            "catalyst_extraction", extraction_packet, synthesis_provider,
            database=database, save=save, timeout=360, max_tokens=6000,
        )
        _write(synthesis_path, synthesis_result)
        print(f"{episode_id} 炒作归因完成", flush=True)
    else:
        print(f"{episode_id} 复用已完成的炒作归因", flush=True)

    comparison = None
    if compare_provider and compare_provider != synthesis_provider:
        comparison_path = output_dir / f"06_catalyst_{compare_provider}.json"
        comparison = _resume_result(
            comparison_path, "catalyst_extraction", extraction_packet, compare_provider,
        )
        if comparison is None:
            comparison = run_task(
                "catalyst_extraction", extraction_packet, compare_provider,
                database=database, save=save, timeout=360, max_tokens=6000,
            )
            _write(comparison_path, comparison)

    audit_packet = {
        "entity_type": "theme_episode", "entity_id": episode_id,
        "episode_id": episode_id, "valid_at": packet["valid_at"], "theme": packet["theme"],
        "episode": packet["episode"], "market_evidence": packet["market_evidence"],
        "stock_evidence": packet["stock_evidence"], "selected_evidence": selected,
        "analysis_to_audit": synthesis_result["output"],
    }
    audit_path = output_dir / f"07_audit_{audit_provider}.json"
    audit_result = _resume_result(audit_path, "catalyst_audit", audit_packet, audit_provider)
    if audit_result is None:
        audit_result = run_task(
            "catalyst_audit", audit_packet, audit_provider,
            database=database, save=save, timeout=300, max_tokens=5000,
        )
        _write(audit_path, audit_result)
        print(f"{episode_id} 归因审计完成", flush=True)
    else:
        print(f"{episode_id} 复用已完成的归因审计", flush=True)

    summary_packet = {
        "entity_type": "theme_episode", "entity_id": episode_id,
        "episode_id": episode_id, "valid_at": packet["valid_at"],
        "theme": packet["theme"], "episode": packet["episode"],
        "market_evidence": packet["market_evidence"], "stock_evidence": packet["stock_evidence"],
        "selected_evidence": selected, "catalyst_analysis": synthesis_result["output"],
        "catalyst_audit": audit_result["output"],
    }
    summary_path = output_dir / f"08_episode_summary_{synthesis_provider}.json"
    summary_result = _resume_result(
        summary_path, "episode_summary", summary_packet, synthesis_provider,
    )
    if summary_result is None:
        summary_result = run_task(
            "episode_summary", summary_packet, synthesis_provider,
            database=database, save=save, timeout=360, max_tokens=5000,
        )
        _write(summary_path, summary_result)
        print(f"{episode_id} 周期摘要完成", flush=True)
    else:
        print(f"{episode_id} 复用已完成的周期摘要", flush=True)
    revised = audit_result["output"].get("revised_conclusion") or {}
    audited_reason_status = revised.get("reason_status", synthesis_result["output"]["reason_status"])
    audited_causal_status = revised.get(
        "causal_status", synthesis_result["output"].get("dominant_reason", {}).get("causal_status", "unknown")
    )
    web_research_reasons = []
    if audited_reason_status == "no_reliable_reason":
        web_research_reasons.append("本地新闻未形成可靠归因")
    if audited_causal_status in {"background_only", "unknown"}:
        web_research_reasons.append(f"审计后的主因等级为 {audited_causal_status}")
    if audit_result["output"]["verdict"] == "reject":
        web_research_reasons.append("模型审计拒绝当前归因，需要补充独立证据后重做")
    needs_web_research = bool(web_research_reasons)
    if needs_web_research:
        _write(output_dir / "09_web_research_request.json", {
            "episode_id": episode_id,
            "theme": packet["theme"],
            "episode": packet["episode"],
            "search_window": packet["news_window"],
            "search_terms": packet["search_terms"],
            "focus_dates": packet["focus_dates"],
            "reasons": web_research_reasons,
            "required_evidence_fields": [
                "url", "title", "source", "published_at", "observed_at", "matched_focus_date", "content"
            ],
            "rules": [
                "优先原始公告、政府或机构发布、事件本身的报道",
                "个股异动和板块拉升是结果，不能单独作为原因",
                "搜索摘要不能作为证据，必须打开原文并核对发布时间",
                "补充证据写回后必须重新执行催化审计",
            ],
        })
    manifest = {
        "episode_id": episode_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "screening_provider": screening_provider,
        "synthesis_provider": synthesis_provider,
        "audit_provider": audit_provider,
        "compare_provider": compare_provider,
        "candidate_news": len(packet["candidate_evidence"]),
        "selected_news": len(selected),
        "market_days": len(packet["market_evidence"]),
        "stock_events": len(packet["stock_evidence"]),
        "reason_status": synthesis_result["output"]["reason_status"],
        "confidence": synthesis_result["output"]["confidence"],
        "audit_verdict": audit_result["output"]["verdict"],
        "audited_reason_status": audited_reason_status,
        "audited_causal_status": audited_causal_status,
        "needs_web_research": needs_web_research,
        "web_research_reasons": web_research_reasons,
        "needs_review": True,
        "database_saved": save,
    }
    _write(output_dir / "MANIFEST.json", manifest)
    return {"manifest": manifest, "screening": screen_result, "catalyst": synthesis_result,
            "comparison": comparison, "audit": audit_result, "summary": summary_result}


def main() -> int:
    parser = argparse.ArgumentParser(description="分析一次题材行情的炒作原因")
    parser.add_argument("--episode-id", required=True)
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--news-db", type=Path, default=DB_PATH_NEWS)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--screening-provider", choices=["qwen-vllm", "minimax-hermes"], default="qwen-vllm")
    parser.add_argument("--synthesis-provider", choices=["qwen-vllm", "minimax-hermes"], default="minimax-hermes")
    parser.add_argument("--audit-provider", choices=["qwen-vllm", "minimax-hermes"], default="qwen-vllm")
    parser.add_argument("--compare-provider", choices=["qwen-vllm", "minimax-hermes"])
    parser.add_argument("--max-candidates", type=int, default=40)
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()
    target = args.output_dir or RESEARCH_ROOT / "outputs" / "theme_catalyst_analysis" / args.episode_id
    result = analyze(args.episode_id, args.database, args.news_db, target,
                     args.screening_provider, args.synthesis_provider, args.audit_provider, args.compare_provider,
                     not args.no_save, args.max_candidates)
    if not args.no_save:
        result["manifest"]["vault_render"] = ThemeVaultRenderer(args.database).render()
        _write(target / "MANIFEST.json", result["manifest"])
    print(json.dumps(result["manifest"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

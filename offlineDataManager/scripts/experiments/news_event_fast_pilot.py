#!/usr/bin/env python3
"""单日财经新闻快速级联试验。

先按传播度筛掉低信息密度孤立事件，并用本地重排模型救回高价值独家新闻；
候选按大块送入 LLM 轻筛，只对最高分的少量事件做深提炼。
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any

import pandas as pd

from news_event_cascade_pilot import (
    CATEGORIES,
    DEFAULT_INPUT_ROOT,
    STAGE1_PROMPT,
    STAGE2_PROMPT,
    _compact_payload,
    _invoke_json,
    _save_batches,
    _validate_stage2,
)

FAST_STAGE1_PROMPT = STAGE1_PROMPT.replace(
    "只返回一个JSON对象，不要解释：",
    "每个输入块最多只保留最重要的50项；不足50项不要凑数。只返回一个JSON对象，不要解释：",
)

IMPACT_PROMPT = """你是A股财经新闻编辑。只根据输入标题和正文，判断事实对A股整体或最直接相关行业的边际影响。
只能返回 bullish（利好）、bearish（利空）、neutral（中性）。正负影响并存、影响对象不明确、只是活动或表态且没有明确措施时选 neutral。不能依据股价结果反推新闻利好利空，也不能编造标题以外的细节。
每个ID都要返回，原因用不超过40字指出影响对象和依据；neutral也需说明不确定之处。只输出严格JSON：
{"ratings":[["事件ID","bullish|bearish|neutral","原因"]]}
输入事件：
"""


def validate_impacts(value: dict[str, Any], allowed: set[str]) -> list[dict[str, str]]:
    ratings = value.get("ratings")
    if not isinstance(ratings, list):
        raise ValueError("影响评级缺少 ratings 数组")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    aliases = {"利好": "bullish", "利空": "bearish", "中性": "neutral"}
    for item in ratings:
        if not isinstance(item, list) or len(item) < 3:
            raise ValueError(f"评级条目格式错误: {item!r}")
        event_id = str(item[0])
        rating = aliases.get(str(item[1]).strip().lower(), str(item[1]).strip().lower())
        reason = str(item[2]).strip()[:100]
        if event_id not in allowed or event_id in seen or rating not in {"bullish", "bearish", "neutral"} or not reason:
            raise ValueError(f"评级条目非法: {item!r}")
        result.append({"semantic_event_id": event_id, "market_impact": rating, "impact_reason": reason})
        seen.add(event_id)
    if seen != allowed:
        raise ValueError(f"评级未完整覆盖输入: missing={sorted(allowed - seen)[:10]}")
    return result


def validate_stage1_fast(value: dict[str, Any], allowed: set[str]) -> list[dict[str, Any]]:
    """大批量轻筛允许跳过单条格式瑕疵，避免整块重跑。"""
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value.get("selected", []):
        if not isinstance(item, list) or len(item) < 3:
            continue
        event_id = str(item[0])
        if event_id not in allowed or event_id in seen:
            continue
        try:
            score = int(float(item[1]))
        except (TypeError, ValueError):
            continue
        if score < 40:
            continue
        category = str(item[2])
        if category not in CATEGORIES:
            category = "other"
        result.append({
            "semantic_event_id": event_id,
            "importance_score": min(100, score),
            "category": category,
        })
        seen.add(event_id)
    return result


def invoke_complete_batch(
    prompt_prefix: str,
    payload: list[dict[str, Any]],
    validator,
    *,
    timeout: int,
    max_tokens: int,
) -> tuple[list[dict[str, Any]], str]:
    """模型漏掉必答 ID 时逐级拆小重试，绝不把缺失项伪装成已完成。"""
    allowed = {str(item["id"]) for item in payload}
    prompt = (prompt_prefix
              + f"\n本批共 {len(payload)} 个ID，必须逐一返回，不得遗漏。\n"
              + json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    try:
        value, model = _invoke_json(prompt, timeout=timeout, max_tokens=max_tokens)
        return validator(value, allowed), model
    except ValueError:
        if len(payload) <= 1:
            raise
        midpoint = len(payload) // 2
        left, left_model = invoke_complete_batch(
            prompt_prefix, payload[:midpoint], validator,
            timeout=timeout, max_tokens=max_tokens,
        )
        right, right_model = invoke_complete_batch(
            prompt_prefix, payload[midpoint:], validator,
            timeout=timeout, max_tokens=max_tokens,
        )
        return left + right, left_model if left_model == right_model else f"{left_model},{right_model}"


def rerank_relevance(
    rows: list[dict[str, Any]], *, out: Path, ssh_host: str, base_url: str,
    model: str, batch_size: int, timeout: int, force: bool,
) -> tuple[dict[str, float], float, bool]:
    texts = []
    for row in rows:
        item = _compact_payload(row)
        texts.append(f"标题：{item['title']}\n正文：{item['text']}")
    text_hash = hashlib.sha256("\n".join(
        f"{row['semantic_event_id']}\t{text}" for row, text in zip(rows, texts)
    ).encode("utf-8")).hexdigest()
    score_path = out / "fast_relevance_scores.csv"
    meta_path = out / "fast_relevance_meta.json"
    if not force and score_path.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("text_hash") == text_hash and meta.get("model") == model:
            frame = pd.read_csv(score_path)
            return dict(zip(frame.semantic_event_id, frame.relevance_score)), 0.0, True

    query = (
        "对A股下一交易日研判有价值的新增事实新闻，包括重大政策、宏观数据、行业供需、"
        "商品价格、科技产业、公司基本面、地缘政治和海外风险；排除个股涨跌、指数异动、"
        "泛泛评论、软文和日常噪声。"
    )
    scores: list[float] = []
    started = time.perf_counter()
    for offset in range(0, len(texts), batch_size):
        documents = texts[offset:offset + batch_size]
        payload = {
            "model": model, "text_1": query, "text_2": documents,
            "use_activation": True, "truncate_prompt_tokens": 768,
        }
        command = [
            "ssh", "-T", "-o", "BatchMode=yes", ssh_host,
            "curl", "-sS", "--fail-with-body", "--max-time", str(timeout),
            "-H", "Content-Type:application/json", "--data-binary", "@-",
            base_url.rstrip("/") + "/score",
        ]
        run = subprocess.run(
            command, input=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            capture_output=True, timeout=timeout + 30, check=True,
        )
        value = json.loads(run.stdout.decode("utf-8"))
        scores.extend(float(item["score"]) for item in sorted(value["data"], key=lambda item: item["index"]))
    elapsed = time.perf_counter() - started
    frame = pd.DataFrame({
        "semantic_event_id": [str(row["semantic_event_id"]) for row in rows],
        "relevance_score": scores,
    })
    frame.to_csv(score_path, index=False)
    meta_path.write_text(json.dumps({
        "text_hash": text_hash, "model": model, "rows": len(rows),
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return dict(zip(frame.semantic_event_id, frame.relevance_score)), elapsed, False


def percentile_ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: values[index])
    result = [0.0] * len(values)
    denominator = max(1, len(values) - 1)
    for rank, index in enumerate(order):
        result[index] = rank / denominator
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default="20260924")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--max-candidates", type=int, default=1000)
    parser.add_argument("--min-singleton-content", type=int, default=0)
    parser.add_argument("--stage1-batch-size", type=int, default=100)
    parser.add_argument("--max-deep-events", type=int, default=60)
    parser.add_argument("--stage2-batch-size", type=int, default=20)
    parser.add_argument("--impact-batch-size", type=int, default=50)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--reranker-ssh-host", default="llm-server")
    parser.add_argument("--reranker-base-url", default="http://127.0.0.1:8004")
    parser.add_argument("--reranker-model", default="bge-reranker-v2-m3")
    parser.add_argument("--reranker-batch-size", type=int, default=128)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--force-relevance", action="store_true")
    args = parser.parse_args()

    os.environ.setdefault("THEME_VLLM_TRANSPORT", "ssh")
    os.environ.setdefault("THEME_VLLM_SSH_HOST", "llm-server")
    os.environ.setdefault("THEME_VLLM_REMOTE_BASE_URL", "http://127.0.0.1:8002/v1")
    day = datetime.strptime(re.sub(r"\D", "", args.trade_date)[:8], "%Y%m%d").strftime("%Y%m%d")
    out = args.input_root / day
    rows = pd.read_csv(out / "semantic_events.csv").fillna("").to_dict("records")
    by_id = {str(row["semantic_event_id"]): row for row in rows}
    started = time.perf_counter()

    relevance, relevance_seconds, relevance_cached = rerank_relevance(
        rows, out=out, ssh_host=args.reranker_ssh_host,
        base_url=args.reranker_base_url, model=args.reranker_model,
        batch_size=args.reranker_batch_size, timeout=args.timeout,
        force=args.force_relevance,
    )
    rel_values = [relevance[str(row["semantic_event_id"])] for row in rows]
    rel_percentiles = percentile_ranks(rel_values)
    lengths = [len(str(row.get("content") or "")) for row in rows]
    len_percentiles = percentile_ranks([float(value) for value in lengths])
    candidates: list[dict[str, Any]] = []
    for row, rel_pct, len_pct, content_len in zip(rows, rel_percentiles, len_percentiles, lengths):
        propagated = (
            int(row.get("member_event_count") or 0) >= 2
            or int(row.get("raw_article_count") or 0) >= 2
            or int(row.get("source_count") or 0) >= 2
        )
        if not propagated and content_len < args.min_singleton_content:
            continue
        propagation = (
            math.log1p(int(row.get("raw_article_count") or 0))
            + 0.6 * math.log1p(int(row.get("source_count") or 0))
            + 0.8 * math.log1p(int(row.get("member_event_count") or 0))
        )
        item = dict(row)
        item["prefilter_score"] = propagation + 1.5 * rel_pct + len_pct
        item["relevance_score"] = relevance[str(row["semantic_event_id"])]
        item["propagated"] = propagated
        candidates.append(item)
    candidates.sort(key=lambda row: float(row["prefilter_score"]), reverse=True)
    candidates = candidates[:max(1, args.max_candidates)]
    pd.DataFrame(candidates).to_csv(out / "fast_prefilter_candidates.csv", index=False)

    stage1_path = out / "fast_stage1_batches.json"
    stage1_meta_path = out / "fast_stage1_meta.json"
    candidate_signature = hashlib.sha256("\n".join(
        f"{row['semantic_event_id']}\t{row.get('title','')}\t{row.get('content','')}\t{row['prefilter_score']}"
        for row in candidates
    ).encode("utf-8")).hexdigest()
    old_stage1_meta = (
        json.loads(stage1_meta_path.read_text(encoding="utf-8"))
        if stage1_meta_path.exists() else {}
    )
    existing_stage1 = json.loads(stage1_path.read_text(encoding="utf-8")) if stage1_path.exists() else {}
    legacy_stage1_ids = {
        str(event_id) for batch in existing_stage1.values()
        for event_id in (batch.get("input_ids") or [])
    }
    current_candidate_ids = {str(row["semantic_event_id"]) for row in candidates}
    stage1_cache_valid = (
        old_stage1_meta.get("candidate_signature") == candidate_signature
        or (not old_stage1_meta and legacy_stage1_ids == current_candidate_ids)
    )
    stage1_batches = {} if args.force or not stage1_cache_valid else existing_stage1
    stage1_meta_path.write_text(json.dumps({
        "candidate_signature": candidate_signature,
        "candidate_count": len(candidates),
        "stage1_batch_size": args.stage1_batch_size,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    stage1_started = time.perf_counter()
    completed_candidate_ids: set[str] = set()
    for key, batch in stage1_batches.items():
        input_ids = batch.get("input_ids")
        if isinstance(input_ids, list):
            completed_candidate_ids.update(str(item) for item in input_ids)
        elif key.isdigit():
            offset = int(key)
            size = int(batch.get("input_count") or 0)
            completed_candidate_ids.update(
                str(item["semantic_event_id"]) for item in candidates[offset:offset + size]
            )
    remaining_candidates = [
        item for item in candidates
        if str(item["semantic_event_id"]) not in completed_candidate_ids
    ]
    jobs = []
    for offset in range(0, len(remaining_candidates), args.stage1_batch_size):
        batch = remaining_candidates[offset:offset + args.stage1_batch_size]
        key = "ids-" + str(batch[0]["semantic_event_id"])
        payload = [_compact_payload(row) for row in batch]
        allowed = {item["id"] for item in payload}
        prompt = FAST_STAGE1_PROMPT + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        jobs.append((key, len(batch), sorted(allowed), allowed, prompt))

    def run_stage1(job):
        key, size, input_ids, allowed, prompt = job
        value, model = _invoke_json(prompt, timeout=args.timeout, max_tokens=3000)
        return key, size, input_ids, validate_stage1_fast(value, allowed), model

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        future_map = [pool.submit(run_stage1, job) for job in jobs]
        for future in as_completed(future_map):
            key, size, input_ids, selected, model = future.result()
            stage1_batches[key] = {
                "input_count": size, "input_ids": input_ids,
                "selected": selected, "model": model,
            }
            _save_batches(stage1_path, dict(sorted(stage1_batches.items())))
            print(f"fast stage1 batch={key} input={size} kept={len(selected)}", flush=True)
    stage1_seconds = time.perf_counter() - stage1_started
    selected = [item for batch in stage1_batches.values() for item in batch.get("selected", [])]
    selected_by_id = {item["semantic_event_id"]: item for item in selected if item["semantic_event_id"] in by_id}
    selected = sorted(selected_by_id.values(), key=lambda item: (-item["importance_score"], item["semantic_event_id"]))
    pd.DataFrame(selected).to_csv(out / "fast_selected_events.csv", index=False)

    deep = [item for item in selected if item["importance_score"] >= 60][:args.max_deep_events]
    stage2_path = out / "fast_stage2_batches.json"
    stage2_meta_path = out / "fast_stage2_meta.json"
    deep_signature = hashlib.sha256((STAGE2_PROMPT + "\n" + "\n".join(
        f"{item['semantic_event_id']}\t{item['importance_score']}\t{item['category']}\t"
        f"{by_id[item['semantic_event_id']].get('title', '')}\t"
        f"{by_id[item['semantic_event_id']].get('content', '')}"
        for item in deep
    )).encode("utf-8")).hexdigest()
    old_stage2_meta = (
        json.loads(stage2_meta_path.read_text(encoding="utf-8"))
        if stage2_meta_path.exists() else {}
    )
    existing_stage2 = json.loads(stage2_path.read_text(encoding="utf-8")) if stage2_path.exists() else {}
    legacy_stage2_ids = {
        str(item.get("semantic_event_id")) for batch in existing_stage2.values()
        for item in batch.get("items", []) if item.get("semantic_event_id")
    }
    current_deep_ids = {str(item["semantic_event_id"]) for item in deep}
    stage2_cache_valid = (
        old_stage2_meta.get("deep_signature") == deep_signature
        or (not old_stage2_meta and legacy_stage2_ids == current_deep_ids)
    )
    stage2_batches = {} if args.force or not stage2_cache_valid else existing_stage2
    stage2_meta_path.write_text(json.dumps({
        "deep_signature": deep_signature,
        "deep_count": len(deep),
        "stage2_batch_size": args.stage2_batch_size,
        "prompt_hash": hashlib.sha256(STAGE2_PROMPT.encode("utf-8")).hexdigest(),
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    completed_ids = {
        item["semantic_event_id"] for batch in stage2_batches.values()
        for item in batch.get("items", []) if item.get("semantic_event_id")
    }
    remaining = [item for item in deep if item["semantic_event_id"] not in completed_ids]
    stage2_started = time.perf_counter()
    jobs = []
    for offset in range(0, len(remaining), args.stage2_batch_size):
        batch = remaining[offset:offset + args.stage2_batch_size]
        payload = []
        for meta in batch:
            item = _compact_payload(by_id[meta["semantic_event_id"]])
            item.update({"score": meta["importance_score"], "category": meta["category"]})
            payload.append(item)
        jobs.append(("ids-" + batch[0]["semantic_event_id"], len(batch), payload))

    def run_stage2(job):
        key, size, payload = job
        items, model = invoke_complete_batch(
            STAGE2_PROMPT, payload, _validate_stage2,
            timeout=args.timeout, max_tokens=6500,
        )
        return key, size, items, model

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        future_map = [pool.submit(run_stage2, job) for job in jobs]
        for future in as_completed(future_map):
            key, size, items, model = future.result()
            stage2_batches[key] = {"input_count": size, "items": items, "model": model}
            _save_batches(stage2_path, dict(sorted(stage2_batches.items())))
            print(f"fast stage2 input={size} completed={sum(len(v.get('items', [])) for v in stage2_batches.values())}/{len(deep)}", flush=True)
    stage2_seconds = time.perf_counter() - stage2_started

    detailed = {
        item["semantic_event_id"]: item for batch in stage2_batches.values()
        for item in batch.get("items", []) if item.get("semantic_event_id") in by_id
    }
    final = []
    # 文档只写深提炼的前N条；完整轻筛表另存CSV用于审计。
    for meta in deep:
        event_id = meta["semantic_event_id"]
        final.append({**_compact_payload(by_id[event_id]), **meta, **detailed.get(event_id, {})})
    (out / "fast_final_events.json").write_text(
        json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    pd.DataFrame(final).to_csv(out / "fast_final_events.csv", index=False)

    # 深提炼之外的有效新闻也需要方向评级。该轻量步骤仅输出评级和依据，
    # 保留每批结果以支持断点续跑；深提炼事件复用已有评级。
    detailed_ids = {item["semantic_event_id"] for item in final}
    impact_targets = [item for item in selected if item["semantic_event_id"] not in detailed_ids]
    impact_signature = hashlib.sha256((IMPACT_PROMPT + "\n" + "\n".join(
        f"{item['semantic_event_id']}\t{item['importance_score']}\t"
        f"{by_id[item['semantic_event_id']].get('title', '')}\t"
        f"{by_id[item['semantic_event_id']].get('content', '')}"
        for item in impact_targets
    )).encode("utf-8")).hexdigest()
    impact_path = out / "fast_impact_batches.json"
    impact_meta_path = out / "fast_impact_meta.json"
    old_impact_meta = json.loads(impact_meta_path.read_text(encoding="utf-8")) if impact_meta_path.exists() else {}
    impact_batches = (
        json.loads(impact_path.read_text(encoding="utf-8"))
        if impact_path.exists() and not args.force and old_impact_meta.get("signature") == impact_signature
        else {}
    )
    impact_meta_path.write_text(json.dumps({
        "signature": impact_signature, "target_count": len(impact_targets),
        "prompt_hash": hashlib.sha256(IMPACT_PROMPT.encode("utf-8")).hexdigest(),
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    completed_impact_ids = {
        item["semantic_event_id"] for batch in impact_batches.values()
        for item in batch.get("ratings", [])
    }
    remaining_impacts = [item for item in impact_targets if item["semantic_event_id"] not in completed_impact_ids]
    impact_started = time.perf_counter()
    impact_jobs = []
    for offset in range(0, len(remaining_impacts), args.impact_batch_size):
        batch = remaining_impacts[offset:offset + args.impact_batch_size]
        payload = []
        for meta in batch:
            item = _compact_payload(by_id[meta["semantic_event_id"]])
            payload.append({"id": item["id"], "title": item["title"], "text": item["text"], "category": meta["category"]})
        impact_jobs.append(("ids-" + batch[0]["semantic_event_id"], len(batch), payload))

    def run_impact(job):
        key, size, payload = job
        ratings, model = invoke_complete_batch(
            IMPACT_PROMPT, payload, validate_impacts,
            timeout=args.timeout, max_tokens=6500,
        )
        return key, size, ratings, model

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(run_impact, job) for job in impact_jobs]
        for future in as_completed(futures):
            key, size, ratings, model = future.result()
            impact_batches[key] = {"input_count": size, "ratings": ratings, "model": model}
            _save_batches(impact_path, dict(sorted(impact_batches.items())))
            print(f"fast impact input={size} completed={sum(len(v.get('ratings', [])) for v in impact_batches.values())}/{len(impact_targets)}", flush=True)
    impact_seconds = time.perf_counter() - impact_started
    impact_by_id = {
        item["semantic_event_id"]: item for batch in impact_batches.values()
        for item in batch.get("ratings", [])
    }
    detailed_by_id = {item["semantic_event_id"]: item for item in final}
    rated_selected = []
    for meta in selected:
        event_id = meta["semantic_event_id"]
        rating = detailed_by_id.get(event_id) or impact_by_id.get(event_id)
        if not rating or not rating.get("market_impact") or not rating.get("impact_reason"):
            raise ValueError(f"有效新闻缺少评级: {event_id}")
        rated_selected.append({**meta, "market_impact": rating["market_impact"], "impact_reason": rating["impact_reason"]})
    (out / "fast_rated_selected_events.json").write_text(json.dumps(rated_selected, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(rated_selected).to_csv(out / "fast_selected_events.csv", index=False)
    summary = {
        "trade_date": day,
        "semantic_input_events": len(rows),
        "prefilter_candidates": len(candidates),
        "prefilter_reduction_rate": round(1 - len(candidates) / len(rows), 4),
        "stage1_kept_events": len(selected),
        "stage2_detailed_events": len(final),
        "rated_events": len(rated_selected),
        "settings": {
            "max_candidates": args.max_candidates,
            "min_singleton_content": args.min_singleton_content,
            "stage1_batch_size": args.stage1_batch_size,
            "max_deep_events": args.max_deep_events,
            "stage2_batch_size": args.stage2_batch_size,
            "impact_batch_size": args.impact_batch_size,
            "workers": args.workers,
        },
        "timings": {
            "relevance_seconds": round(relevance_seconds, 3),
            "relevance_cached": relevance_cached,
            "stage1_seconds": round(stage1_seconds, 3),
            "stage2_seconds": round(stage2_seconds, 3),
            "impact_seconds": round(impact_seconds, 3),
            "total_seconds": round(time.perf_counter() - started, 3),
        },
    }
    (out / "fast_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

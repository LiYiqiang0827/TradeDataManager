#!/usr/bin/env python3
"""单日财经新闻“语义事件 -> 轻筛 -> 深提炼”级联试验。

输入是 ``news_event_cluster_pilot.py`` 生成的语义事件，不写正式数据库。
第一阶段让大模型只返回值得保留的事件编号、重要性和类别；第二阶段只对
高重要性事件做摘要、事实、实体和题材提取。每批结果落盘，可中断续跑。
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
MODEL_RUNNER_DIR = ROOT / "policyStudy" / "policy" / "题材涨停研究" / "theme_graph"
if str(MODEL_RUNNER_DIR) not in sys.path:
    sys.path.insert(0, str(MODEL_RUNNER_DIR))

from model_runner import call_qwen, extract_json  # noqa: E402


DEFAULT_INPUT_ROOT = ROOT / "outputs" / "major_news_analysis"
CATEGORIES = {
    "policy", "macro", "industry", "company", "commodity", "technology",
    "geopolitics", "overseas_market", "data_release", "other",
}
HORIZONS = {"immediate", "short", "medium", "long"}
NOVELTIES = {"new", "follow_up", "background"}
MARKET_IMPACTS = {"bullish", "bearish", "neutral"}
HORIZON_ALIASES = {
    "即时": "immediate", "短期": "short", "中期": "medium", "长期": "long",
    "near_term": "short", "near-term": "short", "short_term": "short",
    "short-term": "short", "mid_term": "medium", "mid-term": "medium",
    "medium_term": "medium", "medium-term": "medium", "long_term": "long",
    "long-term": "long",
}
NOVELTY_ALIASES = {
    "新增": "new", "新事件": "new", "进展": "follow_up", "跟进": "follow_up",
    "follow-up": "follow_up", "followup": "follow_up", "背景": "background",
}
MARKET_IMPACT_ALIASES = {
    "利好": "bullish", "正面": "bullish", "positive": "bullish", "beneficial": "bullish",
    "利空": "bearish", "负面": "bearish", "negative": "bearish", "adverse": "bearish",
    "中性": "neutral", "中立": "neutral", "mixed": "neutral", "unclear": "neutral",
}


STAGE1_PROMPT = """你是A股盘前研究的财经新闻编辑。输入已经过规则去重和语义聚类，每项代表一个新闻事件。
目标是保留会改变政策预期、宏观预期、行业供需、商品价格、技术产业趋势、公司基本面或全球风险偏好的事实性事件。

评分：
- 80-100：重大政策、重大宏观/地缘变化、强行业催化、重大公司事件；
- 60-79：会直接影响行业或上市公司的新增事实；
- 40-59：有跟踪价值的客观数据、价格、供需和海外市场事实；
- 0-39：日常噪声，不输出。

必须剔除：A股个股/指数突然拉升下跌、涨停跌停、成交异动等行情结果；没有新事实的评论；重复背景；宣传软文；生活娱乐体育。
市场价格或指数数据只有在客观且有跨市场/宏观参考价值时才保留。不要因为来源多就自动提高分数。

category只能是 policy,macro,industry,company,commodity,technology,geopolitics,overseas_market,data_release,other。
只返回一个JSON对象，不要解释：
{"selected":[["事件ID",整数分数,"category"]]}
没有可保留项时返回 {"selected":[]}。
输入事件：
"""


STAGE2_PROMPT = """你是A股主题研究的数据编辑。请对输入的重要财经事件做结构化提炼。禁止补充输入中没有的事实。
market_impact表示该事件对A股整体或主要相关行业的边际方向，只能是bullish、bearish、neutral。利好利空并存、影响对象不清楚、只有事实但无法判断方向时必须选neutral。评级不是交易建议。
每个输入ID必须返回一次。输出一个JSON对象：
{"items":[{"id":"事件ID","title":"不超过28字的事实标题","summary":"不超过90字","key_facts":["最多3条"],"themes":["最多4个A股相关题材"],"entities":["最多6个实体"],"horizon":"immediate|short|medium|long","novelty":"new|follow_up|background","market_impact":"bullish|bearish|neutral","impact_reason":"不超过60字，指出影响对象和评级依据"}]}
只输出JSON，不要Markdown。输入事件：
"""


def _fallback_title(row: dict[str, Any]) -> str:
    title = str(row.get("title") or "").strip()
    if title:
        return title
    content = str(row.get("content") or "").strip()
    return re.split(r"[。！？；;]", content, maxsplit=1)[0][:180]


def _compact_payload(row: dict[str, Any]) -> dict[str, Any]:
    title = _fallback_title(row)
    content = str(row.get("content") or "").strip()
    if title and content.startswith(title):
        content = content[len(title):].lstrip(" :：,，。")
    return {
        "id": str(row["semantic_event_id"]),
        "time": str(row.get("first_published_at") or "")[:19],
        "title": title[:180],
        "text": content[:240],
        "articles": int(row.get("raw_article_count") or 0),
        "sources": int(row.get("source_count") or 0),
    }


def _invoke_json(prompt: str, *, timeout: int, max_tokens: int) -> tuple[dict[str, Any], str]:
    last_error: Exception | None = None
    current = prompt
    for _ in range(3):
        raw, model = call_qwen(current, timeout=timeout, max_tokens=max_tokens)
        try:
            return extract_json(raw), model
        except Exception as exc:  # 模型偶发Markdown或截断时给一次定向纠错机会。
            last_error = exc
            current = prompt + "\n上次输出格式错误。请重新输出完整、严格JSON，不能使用Markdown。"
    assert last_error is not None
    raise last_error


def _load_batches(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def _save_batches(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def _validate_stage1(value: dict[str, Any], allowed: set[str]) -> list[dict[str, Any]]:
    selected = value.get("selected")
    if not isinstance(selected, list):
        raise ValueError("第一阶段缺少 selected 数组")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in selected:
        if not isinstance(item, list) or len(item) < 3:
            raise ValueError(f"第一阶段条目格式错误: {item!r}")
        event_id, score, category = str(item[0]), item[1], str(item[2])
        if event_id not in allowed or event_id in seen:
            raise ValueError(f"第一阶段ID非法或重复: {event_id}")
        if not isinstance(score, (int, float)) or not 40 <= float(score) <= 100:
            raise ValueError(f"第一阶段分数非法: {score}")
        if category not in CATEGORIES:
            raise ValueError(f"第一阶段类别非法: {category}")
        seen.add(event_id)
        result.append({"semantic_event_id": event_id, "importance_score": int(score), "category": category})
    return result


def _validate_stage2(value: dict[str, Any], allowed: set[str]) -> list[dict[str, Any]]:
    items = value.get("items")
    if not isinstance(items, list):
        raise ValueError("第二阶段缺少 items 数组")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("第二阶段条目必须是对象")
        event_id = str(item.get("id") or "")
        if event_id not in allowed or event_id in seen:
            raise ValueError(f"第二阶段ID非法或重复: {event_id}")
        horizon_raw = str(item.get("horizon") or "").strip().lower()
        novelty_raw = str(item.get("novelty") or "").strip().lower()
        item["horizon"] = HORIZON_ALIASES.get(horizon_raw, horizon_raw)
        item["novelty"] = NOVELTY_ALIASES.get(novelty_raw, novelty_raw)
        impact_raw = str(item.get("market_impact") or item.get("impact") or "").strip().lower()
        item["market_impact"] = MARKET_IMPACT_ALIASES.get(impact_raw, impact_raw)
        # 模型偶发返回自然语言枚举。该字段只是展示元数据，不应让已经完成的
        # 新闻事实提炼整批失败；未知值使用保守默认值并继续保留可审计原文。
        if item["horizon"] not in HORIZONS:
            item["horizon"] = "short"
        if item["novelty"] not in NOVELTIES:
            item["novelty"] = "new"
        if item["market_impact"] not in MARKET_IMPACTS:
            item["market_impact"] = "neutral"
        for key, maximum in (("key_facts", 3), ("themes", 4), ("entities", 6)):
            if not isinstance(item.get(key), list):
                raise ValueError(f"第二阶段 {event_id}.{key} 必须是数组")
            item[key] = item[key][:maximum]
        item["title"] = str(item.get("title") or "")[:60]
        item["summary"] = str(item.get("summary") or "")[:180]
        item["impact_reason"] = str(item.get("impact_reason") or "")[:120]
        if not item["impact_reason"] and item["market_impact"] == "neutral":
            item["impact_reason"] = "影响方向或主要受影响对象不明确"
        item["semantic_event_id"] = event_id
        item.pop("id", None)
        result.append(item)
        seen.add(event_id)
    if seen != allowed:
        raise ValueError(f"第二阶段未完整覆盖输入: missing={sorted(allowed-seen)[:10]}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default="20260924")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--stage1-batch-size", type=int, default=120)
    parser.add_argument("--stage2-batch-size", type=int, default=35)
    parser.add_argument("--deep-threshold", type=int, default=60)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    os.environ.setdefault("THEME_VLLM_TRANSPORT", "ssh")
    os.environ.setdefault("THEME_VLLM_SSH_HOST", "llm-server")
    os.environ.setdefault("THEME_VLLM_REMOTE_BASE_URL", "http://127.0.0.1:8002/v1")

    day = datetime.strptime(re.sub(r"\D", "", args.trade_date)[:8], "%Y%m%d").strftime("%Y%m%d")
    out = args.input_root / day
    source_path = out / "semantic_events.csv"
    if not source_path.exists():
        raise SystemExit(f"缺少语义事件文件: {source_path}")
    rows = pd.read_csv(source_path).fillna("").to_dict("records")
    by_id = {str(row["semantic_event_id"]): row for row in rows}
    started = time.perf_counter()
    timings: dict[str, float] = {}
    models: set[str] = set()

    stage1_path = out / "cascade_stage1_batches.json"
    stage1_batches = {} if args.force else _load_batches(stage1_path)
    stage1_started = time.perf_counter()
    stage1_jobs = []
    for offset in range(0, len(rows), max(1, args.stage1_batch_size)):
        key = f"{offset:06d}"
        if key in stage1_batches:
            continue
        batch = rows[offset:offset + max(1, args.stage1_batch_size)]
        payload = [_compact_payload(row) for row in batch]
        allowed = {item["id"] for item in payload}
        prompt = STAGE1_PROMPT + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        stage1_jobs.append((key, offset, len(batch), allowed, prompt))

    def run_stage1(job):
        key, offset, size, allowed, prompt = job
        value, model = _invoke_json(prompt, timeout=args.timeout, max_tokens=5000)
        return key, offset, size, _validate_stage1(value, allowed), model

    completed_stage1 = sum(int(item.get("input_count") or 0) for item in stage1_batches.values())
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(run_stage1, job) for job in stage1_jobs]
        for future in as_completed(futures):
            key, offset, size, selected, model = future.result()
            stage1_batches[key] = {"input_count": size, "selected": selected, "model": model}
            completed_stage1 += size
            models.add(model)
            _save_batches(stage1_path, dict(sorted(stage1_batches.items())))
            print(f"stage1 {completed_stage1}/{len(rows)} kept_in_batch={len(selected)}", flush=True)
    timings["stage1_seconds"] = time.perf_counter() - stage1_started

    selected_rows = [item for batch in stage1_batches.values() for item in batch.get("selected", [])]
    # 若续跑文件来自不同输入，过滤不存在的ID并按ID保留一次。
    selected_by_id = {
        item["semantic_event_id"]: item for item in selected_rows
        if item.get("semantic_event_id") in by_id
    }
    selected_rows = sorted(selected_by_id.values(), key=lambda item: (-item["importance_score"], item["semantic_event_id"]))
    pd.DataFrame(selected_rows).to_csv(out / "cascade_selected_events.csv", index=False)

    deep_candidates = [item for item in selected_rows if item["importance_score"] >= args.deep_threshold]
    stage2_path = out / "cascade_stage2_batches.json"
    stage2_batches = {} if args.force else _load_batches(stage2_path)
    stage2_started = time.perf_counter()
    # 以已写入的事件ID作为断点，允许修小 batch_size 后安全续跑，不依赖旧偏移量。
    completed_stage2_ids = {
        str(item.get("semantic_event_id"))
        for batch in stage2_batches.values()
        for item in batch.get("items", [])
        if item.get("semantic_event_id")
    }
    remaining_deep_candidates = [
        item for item in deep_candidates
        if item["semantic_event_id"] not in completed_stage2_ids
    ]
    stage2_jobs = []
    for offset in range(0, len(remaining_deep_candidates), max(1, args.stage2_batch_size)):
        batch_meta = remaining_deep_candidates[offset:offset + max(1, args.stage2_batch_size)]
        key = "ids-" + batch_meta[0]["semantic_event_id"]
        payload = []
        for meta in batch_meta:
            value = _compact_payload(by_id[meta["semantic_event_id"]])
            value.update({"score": meta["importance_score"], "category": meta["category"]})
            payload.append(value)
        allowed = {item["id"] for item in payload}
        prompt = STAGE2_PROMPT + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        stage2_jobs.append((key, offset, len(batch_meta), allowed, prompt))

    def run_stage2(job):
        key, offset, size, allowed, prompt = job
        value, model = _invoke_json(prompt, timeout=args.timeout, max_tokens=7000)
        return key, offset, size, _validate_stage2(value, allowed), model

    completed_stage2 = len(completed_stage2_ids)
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(run_stage2, job) for job in stage2_jobs]
        for future in as_completed(futures):
            key, offset, size, items, model = future.result()
            stage2_batches[key] = {"input_count": size, "items": items, "model": model}
            completed_stage2 += size
            models.add(model)
            _save_batches(stage2_path, dict(sorted(stage2_batches.items())))
            print(f"stage2 {completed_stage2}/{len(deep_candidates)}", flush=True)
    timings["stage2_seconds"] = time.perf_counter() - stage2_started

    detailed = [item for batch in stage2_batches.values() for item in batch.get("items", [])]
    detailed_by_id = {item["semantic_event_id"]: item for item in detailed if item.get("semantic_event_id") in by_id}
    final_rows = []
    for meta in selected_rows:
        event_id = meta["semantic_event_id"]
        base = _compact_payload(by_id[event_id])
        row = {**base, **meta, **detailed_by_id.get(event_id, {})}
        final_rows.append(row)
    pd.DataFrame(final_rows).to_csv(out / "cascade_final_events.csv", index=False)
    (out / "cascade_final_events.json").write_text(
        json.dumps(final_rows, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    timings["total_seconds"] = time.perf_counter() - started
    summary = {
        "trade_date": day,
        "semantic_input_events": len(rows),
        "stage1_kept_events": len(selected_rows),
        "stage1_keep_rate": round(len(selected_rows) / len(rows), 4) if rows else 0,
        "stage2_detailed_events": len(deep_candidates),
        "effective_compression_from_rule_events": None,
        "models": sorted(models | {str(batch.get("model")) for batch in stage1_batches.values() if batch.get("model")}),
        "timings": {key: round(value, 3) for key, value in timings.items()},
    }
    rule_summary_path = out / "summary.json"
    if rule_summary_path.exists():
        rule_summary = json.loads(rule_summary_path.read_text(encoding="utf-8"))
        rule_events = int(rule_summary.get("input_rule_events") or 0)
        if rule_events:
            summary["effective_compression_from_rule_events"] = round(1 - len(selected_rows) / rule_events, 4)
    (out / "cascade_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

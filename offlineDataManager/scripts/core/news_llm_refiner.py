"""对去重新闻事件做批量 LLM 筛选、压缩和结构化提炼。"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
from typing import Any
import uuid

import duckdb
import pandas as pd

try:
    from config.settings import NEWS_EVENT_DB_PATH
    from core.news_event_store import NEWS_LLM_PROMPT_VERSION, NewsEventStore, _iso_date, _text
except ModuleNotFoundError:  # pragma: no cover
    from offlineDataManager.scripts.config.settings import NEWS_EVENT_DB_PATH
    from offlineDataManager.scripts.core.news_event_store import (
        NEWS_LLM_PROMPT_VERSION, NewsEventStore, _iso_date, _text,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[3]
MODEL_RUNNER_DIR = PROJECT_ROOT / "policyStudy" / "policy" / "题材涨停研究" / "theme_graph"
if str(MODEL_RUNNER_DIR) not in sys.path:
    sys.path.insert(0, str(MODEL_RUNNER_DIR))

from model_runner import call_hermes, call_qwen, extract_json  # noqa: E402


PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "news_refinement_v2.md"
PROMPT_VERSION = NEWS_LLM_PROMPT_VERSION
CATEGORIES = {
    "policy", "macro", "industry", "company", "commodity", "technology",
    "geopolitics", "overseas_market", "data_release", "other",
}
HORIZONS = {"immediate", "short", "medium", "long"}
NOVELTIES = {"new", "follow_up", "background"}


def _event_hash(row: dict[str, Any]) -> str:
    raw = "\n".join([
        str(row.get("event_id", "")), _text(row.get("title")), _text(row.get("content")),
        str(row.get("processor_version", "")), PROMPT_VERSION,
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _payload_item(row: dict[str, Any]) -> dict[str, Any]:
    title = _text(row.get("title"))
    content = _text(row.get("content"))
    if title and content.startswith(title):
        content = content[len(title):].lstrip(" :：,，。")
    return {
        "event_id": str(row["event_id"]),
        "published_at": str(row["first_published_at"])[:19],
        "source": _text(row.get("representative_src")),
        "title": title[:180],
        "content_excerpt": content[:260],
        "source_count": int(row.get("source_count") or 0),
    }


def _validate(output: dict[str, Any], allowed_ids: set[str]) -> list[dict[str, Any]]:
    selected = output.get("selected")
    if not isinstance(selected, list):
        raise ValueError("selected 必须是数组")
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for index, item in enumerate(selected):
        if not isinstance(item, dict):
            raise ValueError(f"selected[{index}] 必须是对象")
        event_id = item.get("event_id")
        if event_id not in allowed_ids or event_id in seen:
            raise ValueError(f"selected[{index}].event_id 不属于输入或重复: {event_id}")
        score = item.get("importance_score")
        if not isinstance(score, (int, float)) or not 35 <= score <= 100:
            raise ValueError(f"selected[{index}].importance_score 必须在35到100之间")
        if item.get("category") not in CATEGORIES:
            raise ValueError(f"selected[{index}].category 非法")
        if item.get("horizon") not in HORIZONS:
            raise ValueError(f"selected[{index}].horizon 非法")
        if item.get("novelty") not in NOVELTIES:
            raise ValueError(f"selected[{index}].novelty 非法")
        for key, maximum in (("key_facts", 3), ("themes", 5), ("entities", 8)):
            value = item.get(key)
            if not isinstance(value, list):
                raise ValueError(f"selected[{index}].{key} 必须是数组")
            # 多给一两个事实不影响证据身份，确定性截断比重新调用模型更稳健。
            item[key] = value[:maximum]
        seen.add(event_id)
        result.append(item)
    return result


class NewsLLMRefiner:
    def __init__(self, database: Path = NEWS_EVENT_DB_PATH):
        self.database = Path(database)
        NewsEventStore(database=self.database)  # 建表或执行向前兼容的 schema 补充。
        self.prompt = PROMPT_PATH.read_text(encoding="utf-8")

    def _connect(self):
        return duckdb.connect(str(self.database))

    def _invoke(self, prompt: str, provider: str, model: str | None,
                timeout: int, max_tokens: int,
                allowed_ids: set[str] | None = None) -> tuple[dict[str, Any], str]:
        error: Exception | None = None
        current = prompt
        for attempt in range(3):
            if provider == "qwen-vllm":
                raw, actual_model = call_qwen(
                    current, model=model, timeout=timeout, max_tokens=max_tokens,
                )
            elif provider == "minimax-hermes":
                raw, actual_model = call_hermes(
                    current, model=model or "MiniMax-M3", timeout=timeout,
                )
            else:
                raise ValueError("provider 必须是 qwen-vllm 或 minimax-hermes")
            try:
                output = extract_json(raw)
                if allowed_ids is not None:
                    _validate(output, allowed_ids)
                return output, actual_model
            except (ValueError, json.JSONDecodeError) as exc:
                error = exc
                current = (
                    prompt + "\n上次输出不是合法JSON。请严格按格式重答，不要Markdown。\n"
                    + raw[:3000]
                )
        assert error is not None
        raise error

    def refine(
        self,
        start_date: str,
        end_date: str,
        *,
        provider: str = "qwen-vllm",
        model: str | None = None,
        batch_size: int = 40,
        force: bool = False,
        timeout: int = 240,
        max_tokens: int = 6000,
        limit: int | None = None,
        sample: bool = False,
        sample_seed: str = "news-refinement-v2",
    ) -> dict[str, Any]:
        started = datetime.now()
        run_id = str(uuid.uuid4())
        conn = self._connect()
        processed = kept = batches = 0
        try:
            rows = conn.execute(
                """SELECT e.* FROM fact_news_event e
                   WHERE e.event_date BETWEEN ? AND ? AND NOT e.excluded
                   ORDER BY e.first_published_at,e.event_id""",
                [_iso_date(start_date), _iso_date(end_date)],
            ).fetchdf().to_dict("records")
            hashes = {str(row["event_id"]): _event_hash(row) for row in rows}
            if not force and rows:
                existing = {
                    event_id: input_hash for event_id, input_hash in conn.execute(
                        """SELECT event_id,input_hash FROM fact_news_insight
                           WHERE event_date BETWEEN ? AND ? AND prompt_version=?""",
                        [_iso_date(start_date), _iso_date(end_date), PROMPT_VERSION],
                    ).fetchall()
                }
                rows = [row for row in rows if existing.get(str(row["event_id"])) != hashes[str(row["event_id"])]]
            if sample:
                rows.sort(key=lambda row: hashlib.sha256(
                    f"{sample_seed}:{row['event_id']}".encode("utf-8")
                ).hexdigest())
            if limit is not None:
                rows = rows[:int(limit)]
            conn.execute(
                "INSERT INTO news_llm_run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [run_id, start_date, end_date, provider, PROMPT_VERSION, len(rows), 0, 0, 0,
                 "running", "", started, None],
            )
            for offset in range(0, len(rows), max(1, int(batch_size))):
                batch = rows[offset:offset + max(1, int(batch_size))]
                payload = [_payload_item(row) for row in batch]
                prompt = self.prompt + "\n输入事件：\n" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                allowed_ids = {item["event_id"] for item in payload}
                output, actual_model = self._invoke(
                    prompt, provider, model, timeout, max_tokens, allowed_ids,
                )
                selected = _validate(output, allowed_ids)
                selected_by_id = {item["event_id"]: item for item in selected}
                now = datetime.now()
                insight_rows = []
                for row in batch:
                    event_id = str(row["event_id"])
                    item = selected_by_id.get(event_id)
                    is_kept = item is not None
                    insight_rows.append([
                        event_id, str(row["event_date"]), is_kept,
                        int(item["importance_score"]) if item else 0,
                        item["category"] if item else "noise",
                        _text(item.get("summary"))[:120] if item else "",
                        json.dumps(item.get("key_facts", []), ensure_ascii=False) if item else "[]",
                        json.dumps(item.get("themes", []), ensure_ascii=False) if item else "[]",
                        json.dumps(item.get("entities", []), ensure_ascii=False) if item else "[]",
                        item["horizon"] if item else "short",
                        item["novelty"] if item else "background",
                        _text(item.get("reason"))[:100] if item else "llm_not_selected",
                        provider, actual_model, PROMPT_VERSION, hashes[event_id], now, run_id,
                    ])
                conn.executemany(
                    """INSERT OR REPLACE INTO fact_news_insight(
                           event_id,event_date,llm_keep,importance_score,category,summary,
                           key_facts_json,themes_json,entities_json,horizon,novelty,reason,
                           provider,model,prompt_version,input_hash,processed_at,run_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    insight_rows,
                )
                processed += len(batch)
                kept += len(selected)
                batches += 1
                conn.execute(
                    """UPDATE news_llm_run SET processed_events=?,kept_events=?,batch_count=?
                       WHERE run_id=?""", [processed, kept, batches, run_id],
                )
            conn.execute(
                """UPDATE news_llm_run SET status='ok',finished_at=CURRENT_TIMESTAMP,
                          processed_events=?,kept_events=?,batch_count=? WHERE run_id=?""",
                [processed, kept, batches, run_id],
            )
            return {
                "run_id": run_id, "status": "ok", "candidate_events": len(rows),
                "processed_events": processed, "kept_events": kept,
                "keep_rate": round(kept / processed, 4) if processed else 0.0,
                "batch_count": batches, "provider": provider,
                "prompt_version": PROMPT_VERSION,
            }
        except Exception as exc:
            conn.execute(
                """UPDATE news_llm_run SET status='failed',error=?,finished_at=CURRENT_TIMESTAMP,
                          processed_events=?,kept_events=?,batch_count=? WHERE run_id=?""",
                [f"{type(exc).__name__}: {exc}"[:2000], processed, kept, batches, run_id],
            )
            raise
        finally:
            conn.close()


__all__ = ["NewsLLMRefiner", "PROMPT_VERSION"]

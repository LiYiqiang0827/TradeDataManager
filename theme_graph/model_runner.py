"""题材知识库的可替换本地模型运行器与审计缓存。"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any
from http.client import RemoteDisconnected
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, research_root
ROOT = manager_root(__file__, 1)
RESEARCH_ROOT = research_root()
OFFLINE_SCRIPTS = ROOT / "offlineDataManager" / "scripts"
sys.path.insert(0, str(OFFLINE_SCRIPTS))

import duckdb
from config.settings import THEME_GRAPH_DB_PATH
from core.theme_graph_store import SCHEMA_SQL


PROMPT_DIR = Path(__file__).resolve().parent / "prompts"


def _discover_tasks() -> dict[str, Path]:
    """每个任务自动选择数字版本最高的提示词。"""
    selected: dict[str, tuple[int, Path]] = {}
    for path in PROMPT_DIR.glob("*_v*.md"):
        if path.name.startswith("_"):
            continue
        match = re.match(r"^(?P<task>.+)_v(?P<version>\d+)$", path.stem)
        if not match:
            continue
        task, version = match.group("task"), int(match.group("version"))
        if task not in selected or version > selected[task][0]:
            selected[task] = (version, path)
    return {task: value[1] for task, value in selected.items()}


TASKS = _discover_tasks()
REQUIRED_KEYS = {
    "stock_exposure": {"market_role", "business_relation", "commercial_stage", "industrial_core",
                       "conclusion", "positive_evidence_ids", "negative_evidence_ids", "confidence", "needs_review"},
    "theme_normalization": {"relation", "preferred_name", "parent_name", "reason", "evidence_ids", "confidence", "needs_review"},
    "catalyst_screening": {"episode_id", "items", "duplicate_clusters", "missing_information", "confidence", "needs_review"},
    "catalyst_extraction": {"episode_id", "reason_status", "dominant_reason", "catalysts",
                            "narrative_timeline", "alternative_explanations", "unresolved",
                            "confidence", "needs_review"},
    "catalyst_audit": {"episode_id", "verdict", "citation_errors", "temporal_errors",
                       "causal_overstatements", "classification_errors", "revised_conclusion",
                       "unresolved", "confidence", "needs_review"},
    "limit_attribution": {"attribution", "market_narrative", "fundamental_link", "evidence_ids", "counter_evidence_ids", "confidence", "needs_review"},
    "episode_summary": {"episode_id", "phase_summary", "key_dates", "market_cores", "dominant_reason",
                        "catalysts", "reason_market_alignment", "unresolved", "confidence", "needs_review"},
}


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def build_prompt(task: str, packet: dict[str, Any]) -> tuple[str, str]:
    if task not in TASKS:
        raise ValueError(f"未知任务 {task}; 可选: {sorted(TASKS)}")
    task_path = TASKS[task]
    match = re.search(r"_v(\d+)$", task_path.stem)
    prompt_version = f"v{match.group(1)}" if match else "v1"
    common_path = PROMPT_DIR / f"_common_{prompt_version}.md"
    if not common_path.exists():
        common_path = PROMPT_DIR / "_common_v1.md"
    common = common_path.read_text(encoding="utf-8")
    body = task_path.read_text(encoding="utf-8")
    prompt = common + "\n" + body + "\n输入证据包：\n" + json.dumps(packet, ensure_ascii=False, indent=2)
    return prompt, prompt_version


def extract_json(text: str) -> dict[str, Any]:
    clean = text.strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", clean, flags=re.S)
    try:
        value = json.loads(clean)
    except json.JSONDecodeError:
        start, end = clean.find("{"), clean.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("模型输出中没有 JSON 对象")
        value = json.loads(clean[start:end + 1])
    if not isinstance(value, dict):
        raise ValueError("模型输出必须是 JSON 对象")
    return value


def validate_output(task: str, value: dict[str, Any]) -> None:
    missing = REQUIRED_KEYS[task] - set(value)
    if missing:
        raise ValueError(f"模型输出缺少字段: {sorted(missing)}")
    confidence = value.get("confidence")
    if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        raise ValueError("confidence 必须在 0 到 1 之间")
    if task == "catalyst_screening":
        if not isinstance(value.get("items"), list):
            raise ValueError("items 必须是数组")
        ids = [item.get("evidence_id") for item in value["items"]]
        if not all(isinstance(item, str) and item for item in ids) or len(ids) != len(set(ids)):
            raise ValueError("筛选结果 evidence_id 必须非空且唯一")
        for index, item in enumerate(value["items"]):
            if item.get("evidence_nature") not in {
                "trigger_fact", "market_recap", "background_fact", "counter_fact", "unrelated"
            }:
                raise ValueError(f"items[{index}].evidence_nature 枚举非法")
            if item.get("relevance") not in {"direct", "supporting", "counter", "unrelated"}:
                raise ValueError(f"items[{index}].relevance 枚举非法")
            if item.get("timing_role") not in {"pre_start", "start_day", "during_episode",
                                               "late_explanation", "post_episode"}:
                raise ValueError(f"items[{index}].timing_role 枚举非法")
    if task == "catalyst_extraction" and value.get("reason_status") not in {
        "identified", "multiple_competing", "no_reliable_reason"
    }:
        raise ValueError("reason_status 枚举非法")
    if task == "catalyst_extraction":
        dominant = value.get("dominant_reason") or {}
        if dominant.get("causal_status") != "unknown" and not dominant.get("evidence_ids"):
            raise ValueError("dominant_reason 非 unknown 时必须引用新闻证据")
        for index, item in enumerate(value.get("catalysts", [])):
            if not item.get("evidence_ids"):
                raise ValueError(f"catalysts[{index}] 缺少 evidence_ids")
            if item.get("catalyst_type") not in {
                "policy", "industry_event", "company_event", "price_change", "technology",
                "geopolitics", "earnings", "rumor", "denial", "unknown"
            }:
                raise ValueError(f"catalysts[{index}].catalyst_type 枚举非法")
            if item.get("role") not in {"initial_trigger", "reinforcement", "branch_rotation",
                                        "late_explanation", "counter"}:
                raise ValueError(f"catalysts[{index}].role 枚举非法")
        for index, item in enumerate(value.get("narrative_timeline", [])):
            if not item.get("evidence_ids"):
                raise ValueError(f"narrative_timeline[{index}] 缺少 evidence_ids")
    if task == "catalyst_audit" and value.get("verdict") not in {"accept", "revise", "reject"}:
        raise ValueError("catalyst_audit verdict 枚举非法")
    if task == "episode_summary":
        for field in ("key_dates", "market_cores"):
            for index, item in enumerate(value.get(field, [])):
                if not item.get("evidence_ids"):
                    raise ValueError(f"{field}[{index}] 缺少 evidence_ids")
        for index, item in enumerate(value.get("catalysts", [])):
            if item.get("role") not in {"initial_trigger", "reinforcement", "branch_rotation",
                                        "late_explanation", "counter"}:
                raise ValueError(f"catalysts[{index}].role 枚举非法")


def _input_evidence_ids(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "evidence_id" and isinstance(child, str):
                found.add(child)
            found.update(_input_evidence_ids(child))
    elif isinstance(value, list):
        for child in value:
            found.update(_input_evidence_ids(child))
    return found


def _output_evidence_ids(value: Any, parent_key: str = "") -> set[str]:
    found: set[str] = set()
    citation_keys = {"evidence_id", "evidence_ids", "market_evidence_ids", "counter_evidence_ids",
                     "positive_evidence_ids", "negative_evidence_ids", "member_ids", "representative_id"}
    if isinstance(value, dict):
        for key, child in value.items():
            if key in citation_keys:
                values = child if isinstance(child, list) else [child]
                found.update(item for item in values if isinstance(item, str) and item)
            found.update(_output_evidence_ids(child, key))
    elif isinstance(value, list):
        for child in value:
            found.update(_output_evidence_ids(child, parent_key))
    return found


def validate_task_output(task: str, packet: dict[str, Any], output: dict[str, Any]) -> None:
    """同时校验JSON结构、证据引用和需要完整覆盖的任务约束。"""
    validate_output(task, output)
    available_ids = _input_evidence_ids(packet)
    unknown_ids = sorted(_output_evidence_ids(output) - available_ids)
    if unknown_ids:
        raise ValueError(f"模型引用了输入中不存在的 evidence_id: {unknown_ids}")
    if task == "catalyst_screening":
        expected = {item["evidence_id"] for item in packet.get("candidate_evidence", [])}
        returned = {item["evidence_id"] for item in output["items"]}
        if returned != expected:
            raise ValueError(
                f"筛选结果未完整覆盖候选证据: missing={sorted(expected-returned)}, "
                f"extra={sorted(returned-expected)}")


def call_qwen(prompt: str, base_url: str | None = None, model: str | None = None,
              timeout: int = 180, max_tokens: int = 1200, retries: int = 5,
              retry_backoff: float = 5.0) -> tuple[str, str]:
    base = (base_url or os.environ.get("THEME_VLLM_BASE_URL") or "http://localhost:8002/v1").rstrip("/")
    chosen = model or os.environ.get("THEME_VLLM_MODEL")
    if not chosen:
        with urlopen(base + "/models", timeout=10) as response:
            chosen = json.loads(response.read().decode("utf-8"))["data"][0]["id"]
    payload = {
        "model": chosen,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "max_tokens": max_tokens,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    data = json.dumps(payload).encode("utf-8")
    transport = os.environ.get("THEME_VLLM_TRANSPORT", "http").strip().lower()
    if transport not in {"http", "ssh"}:
        raise ValueError("THEME_VLLM_TRANSPORT 必须是 http 或 ssh")
    ssh_host = os.environ.get("THEME_VLLM_SSH_HOST", "llm-server")
    remote_base = os.environ.get(
        "THEME_VLLM_REMOTE_BASE_URL", "http://127.0.0.1:8002/v1"
    ).rstrip("/")

    def post() -> dict[str, Any]:
        if transport == "http":
            request = Request(base + "/chat/completions", data=data,
                              headers={"Content-Type": "application/json", "Connection": "close"})
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        command = [
            "ssh", "-T", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=20",
            "-o", "ServerAliveCountMax=30", ssh_host,
            "curl", "-sS", "--fail-with-body", "--max-time", str(timeout),
            "-H", "Content-Type:application/json", "--data-binary", "@-",
            remote_base + "/chat/completions",
        ]
        run = subprocess.run(
            command, input=data, capture_output=True, timeout=timeout + 45, check=True,
        )
        return json.loads(run.stdout.decode("utf-8"))

    retryable = (RemoteDisconnected, URLError, TimeoutError, ConnectionResetError,
                 BrokenPipeError, subprocess.SubprocessError)
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            result = post()
            return result["choices"][0]["message"]["content"], chosen
        except HTTPError as exc:
            # 429和服务器端错误多为共享GPU繁忙；参数/权限错误应立即暴露。
            if exc.code != 429 and not 500 <= exc.code < 600:
                raise
            last_error = exc
        except retryable as exc:
            last_error = exc
        if attempt >= retries:
            break
        delay = min(60.0, retry_backoff * (2 ** attempt))
        print(
            f"Qwen调用失败，{delay:.0f}秒后重试 {attempt + 1}/{retries}: "
            f"{type(last_error).__name__}: {last_error}",
            file=sys.stderr,
            flush=True,
        )
        time.sleep(delay)
    assert last_error is not None
    raise last_error


def call_hermes(prompt: str, provider: str = "minimax-cn", model: str = "MiniMax-M3",
                timeout: int = 180) -> tuple[str, str]:
    hermes = os.environ.get("HERMES_BIN", str(Path.home() / ".local" / "bin" / "hermes"))
    with tempfile.NamedTemporaryFile("w", suffix=".txt", encoding="utf-8", delete=False) as handle:
        handle.write(prompt)
        query_file = Path(handle.name)
    try:
        command = [hermes, "chat", "--query-file", str(query_file), "--oneshot", "-Q",
                   "--provider", provider, "-m", model, "--ignore-rules", "--max-turns", "1", "--source", "tool"]
        run = subprocess.run(command, text=True, capture_output=True, timeout=timeout, check=True)
        return run.stdout.strip(), model
    finally:
        query_file.unlink(missing_ok=True)


def save_task_output(task: str, packet: dict[str, Any], provider: str,
                     actual_model: str, prompt_version: str, output: dict[str, Any],
                     database: str | Path = THEME_GRAPH_DB_PATH) -> None:
    """保存已经完成机器校验的任务结果，供分批推理后的合并结果复用。"""
    validate_task_output(task, packet, output)
    entity_type = str(packet.get("entity_type", "unknown"))
    entity_id = str(packet.get("entity_id", "unknown"))
    valid_at = packet.get("valid_at")
    input_hash = hashlib.sha256(
        json.dumps(packet, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    evidence = packet.get("evidence")
    if evidence is None:
        evidence = {key: packet[key] for key in (
            "candidate_evidence", "selected_evidence", "market_evidence", "stock_evidence"
        ) if key in packet}
    db = Path(database).expanduser()
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db))
    try:
        conn.execute(SCHEMA_SQL)
        conn.execute(
            """INSERT OR IGNORE INTO llm_analysis VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [str(uuid.uuid4()), entity_type, entity_id, task, valid_at, provider, actual_model,
             prompt_version, input_hash, json.dumps(output, ensure_ascii=False),
             json.dumps(evidence, ensure_ascii=False), float(output.get("confidence", 0)),
             "needs_review" if output.get("needs_review", True) else "machine_pass", _now()],
        )
    finally:
        conn.close()


def run_task(task: str, packet: dict[str, Any], provider: str, model: str | None = None,
             database: str | Path = THEME_GRAPH_DB_PATH, save: bool = True,
             timeout: int = 180, max_tokens: int = 1200) -> dict[str, Any]:
    prompt, prompt_version = build_prompt(task, packet)
    def invoke(current_prompt: str) -> tuple[str, str]:
        if provider == "qwen-vllm":
            return call_qwen(current_prompt, model=model, timeout=timeout, max_tokens=max_tokens)
        if provider == "minimax-hermes":
            return call_hermes(current_prompt, model=model or "MiniMax-M3", timeout=timeout)
        raise ValueError("provider 必须是 qwen-vllm 或 minimax-hermes")

    validation_error = None
    for attempt in range(3):
        raw, actual_model = invoke(prompt)
        try:
            output = extract_json(raw)
            validate_task_output(task, packet, output)
            validation_error = None
            break
        except (ValueError, KeyError, TypeError) as exc:
            validation_error = exc
            if attempt < 2:
                enum_hint = ""
                if task in {"catalyst_extraction", "episode_summary"}:
                    enum_hint = (
                        "\n所有catalysts[].role只能是 initial_trigger、reinforcement、"
                        "branch_rotation、late_explanation、counter 之一。"
                    )
                prompt = (prompt + "\n\n上一次输出未通过机器校验。请只修正JSON，不增加输入外事实。"
                          f"{enum_hint}\n校验错误：{exc}\n上一次输出：\n{raw}")
    if validation_error is not None:
        raise validation_error
    if save:
        save_task_output(task, packet, provider, actual_model, prompt_version, output, database)
    return {"task": task, "provider": provider, "model": actual_model, "output": output}


def main() -> int:
    parser = argparse.ArgumentParser(description="运行题材知识库本地模型任务")
    parser.add_argument("--task", required=True, choices=sorted(TASKS))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--provider", choices=["qwen-vllm", "minimax-hermes"], default="qwen-vllm")
    parser.add_argument("--model")
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--max-tokens", type=int, default=1200)
    args = parser.parse_args()
    packet = json.loads(args.input.read_text(encoding="utf-8"))
    result = run_task(args.task, packet, args.provider, args.model, args.database,
                      not args.no_save, args.timeout, args.max_tokens)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

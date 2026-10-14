"""为本地新闻无法归因的题材周期批量联网补证并重跑审计链。

联网阶段使用 Hermes Agent 的 web 工具打开原文，只接受带 URL、
发布时间和原文事实摘要的证据。合格证据会交给现有的
``reanalyze_with_external_evidence`` 链路，再次执行 Qwen 筛选、MiniMax
归因、Qwen 审计和 MiniMax 周期摘要。

脚本是可续跑的：已有 ``10_web_evidence.json`` 时复用证据，已有
``17_external_manifest.json`` 时跳过整个周期，除非显式使用 ``--force``。
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, research_root
ROOT = manager_root(__file__, 1)
RESEARCH_ROOT = research_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

from config.settings import THEME_GRAPH_DB_PATH  # noqa: E402
from core.theme_vault import ThemeVaultRenderer  # noqa: E402
from ima_evidence_source import prompt_section, search_ima_leads  # noqa: E402
from reanalyze_with_external_evidence import reanalyze  # noqa: E402


OUTPUT_ROOT = RESEARCH_ROOT / "outputs" / "theme_catalyst_analysis"
STATUS_PATH = OUTPUT_ROOT / "web_research_batch_status.json"
EVIDENCE_NAME = "10_web_evidence.json"
RAW_NAME = "10_web_evidence_raw.txt"
PROMPT_NAME = "10_web_research_prompt.txt"
EXTERNAL_MANIFEST_NAME = "17_external_manifest.json"
REQUIRED_FIELDS = {
    "url", "title", "source", "published_at", "observed_at",
    "matched_focus_date", "content",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_status(status: dict[str, Any]) -> None:
    status["updated_at"] = _now()
    _write_json(STATUS_PATH, status)


def _extract_json(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "evidence" in value:
            return value
    raise ValueError("Hermes 返回中未找到包含 evidence 的 JSON 对象")


def _date_part(value: str) -> str:
    digits = "".join(char for char in str(value)[:10] if char.isdigit())
    return digits[:8]


def _normalize_evidence(payload: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("evidence")
    if not isinstance(rows, list):
        raise ValueError("evidence 必须是数组")
    episode_id = request["episode_id"]
    focus_dates = sorted({_date_part(value) for value in request.get("focus_dates", []) if _date_part(value)})
    window_start = _date_part(request["search_window"]["start"])
    window_end = _date_part(request["search_window"]["end"])
    normalized: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for index, raw in enumerate(rows, 1):
        if not isinstance(raw, dict):
            rejections.append({"row": index, "reason": "证据不是对象"})
            continue
        missing = REQUIRED_FIELDS - set(raw)
        if missing:
            rejections.append({"row": index, "reason": f"缺字段: {sorted(missing)}"})
            continue
        url = str(raw["url"]).strip()
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            rejections.append({"row": index, "reason": f"URL无效: {url}"})
            continue
        if any(name in parsed.netloc.lower() for name in (
            "google.com", "bing.com", "baidu.com", "so.com", "sogou.com",
        )):
            rejections.append({"row": index, "reason": f"搜索结果页不是原文: {url}"})
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        published_date = _date_part(raw["published_at"])
        matched_date = _date_part(raw["matched_focus_date"])
        if not published_date or not (window_start <= published_date <= window_end):
            rejections.append({
                "row": index,
                "reason": f"发布日 {published_date or '-'} 超出检索窗口 {window_start}-{window_end}",
            })
            continue
        original_matched_date = matched_date
        if matched_date not in focus_dates or published_date > matched_date:
            eligible = [value for value in focus_dates if value >= published_date]
            if not eligible:
                rejections.append({
                    "row": index,
                    "reason": f"发布日 {published_date} 之后没有可匹配的关键日",
                    "original_matched_focus_date": original_matched_date,
                })
                continue
            matched_date = eligible[0]
            warnings.append({
                "row": index,
                "reason": "matched_focus_date按历史可见时点重映射",
                "original_matched_focus_date": original_matched_date,
                "matched_focus_date": matched_date,
            })
        title = str(raw["title"]).strip()
        source = str(raw["source"]).strip()
        content = " ".join(str(raw["content"]).split())
        if not title or not source or len(content) < 40:
            rejections.append({"row": index, "reason": "标题、来源或原文事实摘要不完整"})
            continue
        normalized.append({
            **raw,
            "evidence_id": f"NW{len(normalized) + 1}",
            "url": url,
            "title": title,
            "source": source,
            "published_at": str(raw["published_at"]).strip(),
            "observed_at": str(raw["observed_at"]).strip(),
            "matched_focus_date": matched_date,
            "original_matched_focus_date": original_matched_date,
            "content": content[:2000],
            "availability_basis": raw.get("availability_basis", "publisher_page_date"),
            "matched_terms": raw.get("matched_terms", []),
            "evidence_kind_hint": raw.get("evidence_kind_hint", "source_event"),
        })
    research_note = str(payload.get("research_note", "")).strip()
    if warnings or rejections:
        research_note = (research_note + " " if research_note else "") + (
            f"标准化阶段重映射{len(warnings)}条、剔除{len(rejections)}条。"
        )
    return {
        "episode_id": episode_id,
        "theme": request["theme"]["canonical_name"],
        "research_note": research_note,
        "evidence": normalized,
        "normalization_warnings": warnings,
        "rejected_evidence": rejections,
        "researched_at": _now(),
        "research_method": "Hermes MiniMax-M3 with web search and page fetch",
    }


def _build_prompt(request: dict[str, Any], ima_result: dict[str, Any] | None = None) -> str:
    observed_at = datetime.now().astimezone().isoformat()
    return f"""你是A股题材周期的联网证据研究员。必须使用Web搜索和网页抓取工具，不能依靠记忆。

研究对象：
{json.dumps(request, ensure_ascii=False, indent=2)}

IMA研报线索：
{prompt_section(ima_result or {"status": "disabled", "candidates": []})}

任务：
1. 围绕题材启动前5个自然日至最后活跃日，搜索可能引爆或强化行情的政策、产业事件、产品发布、价格与供给变化、权威会议或公司公告。
2. 优先打开政府、监管部门、交易所、公司公告、行业协会、会议主办方和权威媒体的原文。市场涨停、个股异动、板块拉升稿是行情结果，不能作为原因证据。
3. 每条证据必须真正打开原文页面，核对网页标题、来源和发布时间。搜索结果摘要不能作为证据。
4. 只保留发布日在 search_window 内且不晚于 matched_focus_date 的页面。晚于行情发生的解释不能倒置为启动原因。
5. 找到1至5条最有价值且互不重复的证据。如果没有合格证据，返回空 evidence，并在research_note说明搜索过的方向，禁止编造。
6. content用100至500字中文准确概括原文事实、历史可见时点及与题材的关系，不要复制长段原文，不要把“可能相关”写成“已证实引爆”。
7. observed_at统一填写“{observed_at}”。
8. 输出严格JSON，不要Markdown代码围栏，不要任何额外文字。

JSON格式：
{{
  "episode_id": "{request['episode_id']}",
  "theme": "{request['theme']['canonical_name']}",
  "research_note": "简述检索范围、是否找到启动或强化证据以及仍有何缺口",
  "evidence": [
    {{
      "evidence_id": "NW1",
      "url": "已打开原文的完整URL",
      "title": "原文标题",
      "source": "发布机构或媒体",
      "published_at": "YYYY-MM-DD HH:MM:SS；只有日期则YYYY-MM-DD",
      "observed_at": "上述固定时间",
      "matched_focus_date": "YYYYMMDD",
      "content": "原文事实的准确摘要及其与题材周期的关系",
      "availability_basis": "official_page_timestamp或publisher_page_date",
      "matched_terms": ["题材词", "事件词"],
      "evidence_kind_hint": "policy或industry_event或company_announcement或price_supply或conference"
    }}
  ]
}}
""".strip()


def _hermes_path(value: str | None) -> str:
    if value:
        return value
    found = shutil.which("hermes")
    if found:
        return found
    fallback = Path.home() / ".local" / "bin" / "hermes"
    if fallback.exists():
        return str(fallback)
    raise FileNotFoundError("未找到 Hermes CLI")


def _research(request: dict[str, Any], output_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    prompt_path = output_dir / PROMPT_NAME
    raw_path = output_dir / RAW_NAME
    if raw_path.exists() and args.reuse_raw and not args.force:
        try:
            return _normalize_evidence(_extract_json(raw_path.read_text(encoding="utf-8")), request)
        except Exception:
            pass
    ima_result = {"status": "disabled", "candidates": []}
    if not args.no_ima:
        ima_result = search_ima_leads(
            request, output_dir, force=args.refresh_ima,
            max_candidates=args.ima_max_candidates,
            knowledge_base_id=args.ima_kb_id,
        )
    prompt_path.write_text(_build_prompt(request, ima_result) + "\n", encoding="utf-8")
    command = [
        _hermes_path(args.hermes), "chat", "--query-file", str(prompt_path),
        "--oneshot", "-Q", "--provider", args.provider, "-m", args.model,
        "--ignore-rules", "--max-turns", str(args.max_turns),
        "--run-budget", str(args.run_budget), "--source", "tool", "-t", "web",
    ]
    result = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True,
        timeout=args.timeout,
    )
    combined = result.stdout
    if result.stderr:
        combined += "\n[stderr]\n" + result.stderr
    raw_path.write_text(combined, encoding="utf-8")
    if result.returncode != 0:
        raise RuntimeError(f"Hermes 返回码 {result.returncode}: {result.stderr[-1000:]}")
    return _normalize_evidence(_extract_json(result.stdout), request)


def _load_jobs(output_root: Path) -> list[tuple[dict[str, Any], Path]]:
    jobs = []
    for request_path in output_root.glob("EP-*/09_web_research_request.json"):
        request = json.loads(request_path.read_text(encoding="utf-8"))
        jobs.append((request, request_path.parent))
    return sorted(
        jobs,
        key=lambda item: (
            -float(item[0].get("episode", {}).get("peak_heat") or 0),
            item[0]["episode_id"],
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="批量联网补齐题材周期催化证据")
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--episode-id", action="append", default=[])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--provider", default="minimax-cn")
    parser.add_argument("--model", default="MiniMax-M3")
    parser.add_argument("--hermes")
    parser.add_argument("--max-turns", type=int, default=16)
    parser.add_argument("--run-budget", type=int, default=240)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--retry-backoff", type=float, default=15.0)
    parser.add_argument("--research-only", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-reuse-raw", action="store_false", dest="reuse_raw")
    parser.set_defaults(reuse_raw=True)
    parser.add_argument("--no-ima", action="store_true")
    parser.add_argument("--refresh-ima", action="store_true")
    parser.add_argument("--ima-kb-id")
    parser.add_argument("--ima-max-candidates", type=int, default=12)
    parser.add_argument("--render-every", type=int, default=10)
    args = parser.parse_args()

    jobs = _load_jobs(args.output_root)
    selected = set(args.episode_id)
    if selected:
        jobs = [item for item in jobs if item[0]["episode_id"] in selected]
    if args.limit is not None:
        jobs = jobs[: args.limit]

    status: dict[str, Any] = {
        "started_at": _now(), "updated_at": _now(), "total": len(jobs),
        "current": None, "completed": [], "resolved": [], "still_needs_web": [],
        "no_evidence": [], "failed": [], "skipped": [],
    }
    _write_status(status)
    since_render = 0
    for request, output_dir in jobs:
        episode_id = request["episode_id"]
        evidence_path = output_dir / EVIDENCE_NAME
        external_manifest_path = output_dir / EXTERNAL_MANIFEST_NAME
        if external_manifest_path.exists() and not args.force and not args.research_only:
            result = json.loads(external_manifest_path.read_text(encoding="utf-8"))
            status["skipped"].append(episode_id)
            target = "still_needs_web" if result.get("needs_web_research") else "resolved"
            status[target].append(episode_id)
            _write_status(status)
            continue
        status["current"] = {"episode_id": episode_id, "theme": request["theme"]["canonical_name"],
                             "stage": "web_research", "attempt": 1, "started_at": _now()}
        _write_status(status)
        final_error = None
        evidence = None
        for attempt in range(args.retries + 1):
            status["current"]["attempt"] = attempt + 1
            _write_status(status)
            try:
                if evidence_path.exists() and not args.force:
                    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                else:
                    evidence = _research(request, output_dir, args)
                    _write_json(evidence_path, evidence)
                final_error = None
                break
            except Exception as exc:  # 错误要记录后续跑，不让单题阻断批次
                final_error = {
                    "episode_id": episode_id, "stage": "web_research",
                    "attempt": attempt + 1, "error": str(exc), "failed_at": _now(),
                }
                _write_json(output_dir / "WEB_RESEARCH_ERROR.json", final_error)
                if attempt < args.retries:
                    time.sleep(min(120.0, args.retry_backoff * (2 ** attempt)))
        if final_error is not None or evidence is None:
            status["failed"].append(final_error)
            status["current"] = None
            _write_status(status)
            continue
        (output_dir / "WEB_RESEARCH_ERROR.json").unlink(missing_ok=True)
        if not evidence.get("evidence"):
            status["no_evidence"].append(episode_id)
            status["completed"].append(episode_id)
            status["current"] = None
            _write_status(status)
            continue
        if args.research_only:
            status["completed"].append(episode_id)
            status["current"] = None
            _write_status(status)
            continue

        status["current"]["stage"] = "model_reanalysis"
        _write_status(status)
        try:
            result = reanalyze(
                episode_id, evidence_path, output_dir, args.database, save=True,
            )
            _write_json(external_manifest_path, result)
            manifest_path = output_dir / "MANIFEST.json"
            if manifest_path.exists():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["external_research"] = result
                _write_json(manifest_path, manifest)
            status["completed"].append(episode_id)
            target = "still_needs_web" if result.get("needs_web_research") else "resolved"
            status[target].append(episode_id)
            since_render += 1
            if args.render_every > 0 and since_render >= args.render_every:
                status["vault_render"] = ThemeVaultRenderer(args.database).render()
                since_render = 0
        except Exception as exc:
            error = {
                "episode_id": episode_id, "stage": "model_reanalysis",
                "error": str(exc), "failed_at": _now(),
            }
            _write_json(output_dir / "WEB_RESEARCH_ERROR.json", error)
            status["failed"].append(error)
        status["current"] = None
        _write_status(status)

    if not args.research_only and (since_render or status["completed"]):
        status["vault_render"] = ThemeVaultRenderer(args.database).render()
    status["current"] = None
    status["finished_at"] = _now()
    _write_status(status)
    summary = {
        key: len(status[key])
        for key in ("completed", "resolved", "still_needs_web", "no_evidence", "failed", "skipped")
    }
    if "vault_render" in status:
        summary["vault_render"] = status["vault_render"]
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if status["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

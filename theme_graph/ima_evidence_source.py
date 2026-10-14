"""从 IMA 研报知识库提取题材催化的候选线索。

IMA 搜索结果只用于发现可能的事件和研报标题，不能直接作为因果证据。
后续必须由联网研究阶段打开可访问的原始网页并核对发布时间。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any


DEFAULT_NODE = Path("/Users/nickzhang/.local/bin/node")
DEFAULT_HELPER = Path("/Users/nickzhang/.local/share/ima-mcp-server/call-tool.mjs")
DEFAULT_KB_ID = "g3BN8HlkBkJEMmXSkZ1XkM3vCGCi-inCrTwk-DlauiA="
DEFAULT_KB_NAME = "前瞻研报团队（每日更新）"
CACHE_NAME = "09_ima_research_leads.json"

RESULT_REPORTING_TERMS = (
    "龙虎榜", "涨停", "异动", "板块拉升", "公告全知道", "风口研报",
    "复盘", "盘面", "收评", "早盘", "午评",
)
SOURCE_EVENT_TERMS = (
    "政策", "通知", "意见", "规划", "会议", "发布会", "白皮书", "标准",
    "产业", "行业", "供给", "需求", "价格", "涨价", "降价", "产能",
    "订单", "中标", "签约", "获批", "上市", "量产", "技术", "产品",
)


class ImaUnavailable(RuntimeError):
    """IMA 当前不可用，但不应阻断联网研究。"""


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _parse_outer_result(stdout: str) -> Any:
    outer = json.loads(stdout)
    if isinstance(outer, dict) and outer.get("isError"):
        messages = [
            str(item.get("text", "")) for item in outer.get("content", [])
            if isinstance(item, dict)
        ]
        raise ImaUnavailable(" ".join(messages).strip() or "IMA MCP 返回错误")
    if isinstance(outer, dict) and isinstance(outer.get("content"), list):
        for item in outer["content"]:
            if not isinstance(item, dict) or item.get("type") != "text":
                continue
            text = str(item.get("text", "")).strip()
            if not text:
                continue
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"text": text}
    return outer


def _call_ima(tool: str, args: dict[str, Any], timeout: int = 60) -> Any:
    node = Path(os.environ.get("THEME_IMA_NODE", str(DEFAULT_NODE)))
    helper = Path(os.environ.get("THEME_IMA_HELPER", str(DEFAULT_HELPER)))
    if not node.exists() or not helper.exists():
        raise ImaUnavailable(f"IMA MCP 本地入口不存在: node={node}, helper={helper}")
    result = subprocess.run(
        [str(node), str(helper), tool, json.dumps(args, ensure_ascii=False)],
        text=True, capture_output=True, timeout=timeout,
    )
    if result.returncode != 0:
        detail = result.stdout.strip() or result.stderr.strip()
        try:
            _parse_outer_result(result.stdout)
        except Exception as exc:
            detail = str(exc)
        raise ImaUnavailable(detail[-1000:] or f"IMA MCP 返回码 {result.returncode}")
    return _parse_outer_result(result.stdout)


def _valid_date(year: int, month: int, day: int) -> str | None:
    try:
        return date(year, month, day).strftime("%Y%m%d")
    except ValueError:
        return None


def extract_title_date(title: str) -> str | None:
    """从研报标题中提取 YYYYMMDD，兼容常见中文和分隔符格式。"""
    value = str(title)
    patterns = (
        r"(?<!\d)(20\d{2})[年./_-]?(\d{1,2})[月./_-]?(\d{1,2})(?:日)?(?!\d)",
        r"(?<!\d)(\d{2})[年./_-](\d{1,2})[月./_-](\d{1,2})(?:日)?(?!\d)",
        r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)",
    )
    for index, pattern in enumerate(patterns):
        match = re.search(pattern, value)
        if not match:
            continue
        year, month, day = map(int, match.groups())
        if index > 0:
            year += 2000
        parsed = _valid_date(year, month, day)
        if parsed:
            return parsed
    return None


def _walk_candidates(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, dict):
        title = value.get("title") or value.get("name")
        media_id = value.get("media_id") or value.get("id")
        if title and media_id and not str(media_id).startswith("folder_"):
            found.append(value)
        for child in value.values():
            found.extend(_walk_candidates(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_candidates(child))
    return found


def _score_candidate(title: str, title_date: str, focus_dates: list[str]) -> float:
    score = 50.0
    if any(term in title for term in SOURCE_EVENT_TERMS):
        score += 15.0
    if any(term in title for term in RESULT_REPORTING_TERMS):
        score -= 35.0
    if focus_dates:
        candidate_day = datetime.strptime(title_date, "%Y%m%d").date()
        distance = min(abs((candidate_day - datetime.strptime(item, "%Y%m%d").date()).days)
                       for item in focus_dates)
        score += max(0.0, 20.0 - distance * 2.0)
        if any(title_date <= item for item in focus_dates):
            score += 5.0
    return score


def normalize_ima_candidates(payload: Any, request: dict[str, Any],
                             max_candidates: int = 12) -> list[dict[str, Any]]:
    start = "".join(char for char in str(request["search_window"]["start"]) if char.isdigit())[:8]
    end = "".join(char for char in str(request["search_window"]["end"]) if char.isdigit())[:8]
    focus_dates = sorted(str(item) for item in request.get("focus_dates", []))
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in _walk_candidates(payload):
        media_id = str(raw.get("media_id") or raw.get("id") or "").strip()
        title = " ".join(str(raw.get("title") or raw.get("name") or "").split())
        if not media_id or not title or media_id in seen:
            continue
        title_date = extract_title_date(title)
        if not title_date or not (start <= title_date <= end):
            continue
        seen.add(media_id)
        highlight = raw.get("highlight_content") or raw.get("summary") or raw.get("snippet") or ""
        normalized.append({
            "media_id": media_id,
            "title": title,
            "title_date": title_date,
            "highlight": " ".join(str(highlight).split())[:500],
            "media_type": raw.get("media_type") or raw.get("type"),
            "parent_folder_id": raw.get("parent_folder_id"),
            "candidate_score": _score_candidate(title, title_date, focus_dates),
            "source_role": "discovery_lead",
            "evidence_usable": False,
        })
    normalized.sort(key=lambda item: (-item["candidate_score"], item["title_date"], item["title"]))
    return normalized[:max_candidates]


def _quota_retry_date() -> str:
    return (datetime.now().astimezone().date() + timedelta(days=1)).isoformat()


def search_ima_leads(request: dict[str, Any], output_dir: Path, *, force: bool = False,
                     max_candidates: int = 12, knowledge_base_id: str | None = None) -> dict[str, Any]:
    """搜索并缓存 IMA 候选线索；失败时返回状态而不抛出。"""
    cache_path = output_dir / CACHE_NAME
    if cache_path.exists() and not force:
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
        if cached.get("status") == "ok":
            return cached
        if (cached.get("status") == "quota_exhausted"
                and datetime.now().astimezone().date().isoformat() < cached.get("retry_after_date", "")):
            return cached

    kb_id = knowledge_base_id or os.environ.get("THEME_IMA_KB_ID") or DEFAULT_KB_ID
    query = request["theme"]["canonical_name"]
    base = {
        "episode_id": request["episode_id"],
        "theme": query,
        "knowledge_base_id": kb_id,
        "knowledge_base_name": DEFAULT_KB_NAME,
        "query": query,
        "searched_at": _now(),
        "source_policy": "discovery_only_requires_web_verification",
    }
    try:
        payload = _call_ima("search_knowledge", {
            "query": query,
            "knowledge_base_id": kb_id,
            "cursor": "",
        })
        leads = normalize_ima_candidates(payload, request, max_candidates=max_candidates)
        result = {**base, "status": "ok", "candidate_count": len(leads), "candidates": leads}
    except Exception as exc:
        message = str(exc)
        quota = "220021" in message or "次数已达上限" in message
        result = {
            **base,
            "status": "quota_exhausted" if quota else "unavailable",
            "candidate_count": 0,
            "candidates": [],
            "error": message[-1000:],
        }
        if quota:
            result["retry_after_date"] = _quota_retry_date()
    _write_json(cache_path, result)
    return result


def prompt_section(result: dict[str, Any]) -> str:
    candidates = result.get("candidates") or []
    if not candidates:
        return (
            f"IMA知识库状态：{result.get('status', 'unavailable')}。"
            "本次继续使用Web检索，不得因IMA不可用而降低证据标准。"
        )
    compact = [
        {
            "title": item["title"],
            "title_date": item["title_date"],
            "candidate_score": item["candidate_score"],
        }
        for item in candidates
    ]
    return (
        "以下内容来自IMA知识库‘前瞻研报团队（每日更新）’，只作事件发现线索。"
        "标题和摘要不是因果证据，不得直接写入最终evidence。请依据标题中的事件词继续Web搜索，"
        "打开政府、公司、交易所、行业协会或权威媒体原文核验；最终仍须返回可访问的HTTP原文URL。\n"
        + json.dumps(compact, ensure_ascii=False, indent=2)
    )

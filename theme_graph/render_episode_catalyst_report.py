"""把一次题材催化分析的JSON产物渲染为可直接阅读的Markdown。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _cell(value) -> str:
    if isinstance(value, list):
        value = "、".join(str(item) for item in value)
    return str(value or "-").replace("|", "\\|").replace("\n", " ")


def _table(headers, rows) -> str:
    result = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    result.extend("| " + " | ".join(_cell(item) for item in row) + " |" for row in rows)
    return "\n".join(result)


def render(folder: Path) -> Path:
    packet = _load(folder / "01_screening_packet.json")
    external_packet = folder / "13_external_extraction_packet.json"
    selected = (_load(external_packet)["selected_evidence"] if external_packet.exists()
                else _load(folder / "05_extraction_packet.json")["selected_evidence"])
    external_audits = sorted(folder.glob("15_external_audit_*.json"))
    external_summaries = sorted(folder.glob("16_external_episode_summary_*.json"))
    audit_file = external_audits[-1] if external_audits else sorted(folder.glob("07_audit_*.json"))[-1]
    summary_file = (external_summaries[-1] if external_summaries
                    else sorted(folder.glob("08_episode_summary_*.json"))[-1])
    audit = _load(audit_file)["output"]
    summary = _load(summary_file)["output"]
    manifest = _load(folder / "MANIFEST.json")
    theme, episode = packet["theme"], packet["episode"]
    conclusion = audit["revised_conclusion"]

    key_rows = [[item["date"], item["role"], item["evidence_ids"]] for item in summary["key_dates"]]
    catalyst_rows = [[item.get("date"), item.get("role"), item.get("summary"), item.get("evidence_ids", [])]
                     for item in summary["catalysts"]]
    core_rows = [[item["name"], item["ts_code"], item["role"], item["evidence_ids"]]
                 for item in summary["market_cores"]]
    evidence_rows = []
    for item in selected:
        decision = item.get("screening", {})
        title = item["title"] or item.get("content", "")[:60]
        if item.get("url"):
            title = f"[{title}]({item['url']})"
        evidence_rows.append([item["evidence_id"], item["published_at"], item["source"],
                              title, decision.get("relevance"),
                              decision.get("candidate_causal_status")])
    error_rows = []
    for category in ("citation_errors", "temporal_errors", "causal_overstatements", "classification_errors"):
        for item in audit.get(category, []):
            error_rows.append([category, item.get("field"), item.get("issue"), item.get("evidence_ids", [])])

    lines = [
        "---", "type: theme-catalyst-analysis", f"theme: {theme['canonical_name']}",
        f"episode_id: {episode['episode_id']}", f"start_date: {episode['start_date']}",
        f"last_active_date: {episode['last_active_date']}", f"audit_verdict: {audit['verdict']}",
        f"confidence: {audit['confidence']}", "needs_review: true", "---", "",
        f"# {theme['canonical_name']}炒作原因分析：{episode['start_date']}—{episode['last_active_date']}", "",
        "> 本页是经过Qwen筛选、Hermes/MiniMax综合、Qwen审计后的研究草案。"
        " `needs_review=true` 表示仍需人工确认，不能把推测当成事实。", "",
        "## 审计后结论", "", f"- 原因状态：`{conclusion['reason_status']}`",
        f"- 因果等级：`{conclusion['causal_status']}`", f"- 置信度：{audit['confidence']:.2f}",
        f"- 结论：{conclusion['summary']}",
        f"- 新闻证据：{_cell(conclusion.get('evidence_ids', []))}",
        f"- 行情证据：{_cell(conclusion.get('market_evidence_ids', []))}", "",
        "## 行情阶段", "", summary["phase_summary"], "",
        _table(["日期", "阶段", "行情证据"], key_rows), "", "## 催化与叙事时间线", "",
        _table(["日期", "角色", "事件或解释", "证据"], catalyst_rows), "",
        "## 原因与行情是否同步", "", summary["reason_market_alignment"], "",
        "## 市场核心候选", "", _table(["股票", "代码", "市场角色", "证据"], core_rows), "",
        "## Qwen审计发现", "",
        _table(["问题类型", "位置", "问题", "证据"], error_rows) if error_rows else "未发现结构性问题。", "",
        "## 仍未解决", "",
    ]
    lines.extend(f"- {item}" for item in summary["unresolved"])
    lines += ["", "## 已选新闻证据", "", _table(
        ["ID", "发布时间", "来源", "标题/摘要", "相关性", "候选因果等级"], evidence_rows), "",
        "## 运行信息", "", f"- Qwen候选新闻：{manifest['candidate_news']}条",
        f"- 综合使用新闻：{len(selected)}条", f"- 行情日：{manifest['market_days']}个",
        f"- 涨停事件：{manifest['stock_events']}条", f"- 审计结果：`{manifest.get('audit_verdict', audit['verdict'])}`",
        f"- 新闻全文：[{(folder / '02_news_full.json').name}]({(folder / '02_news_full.json').name})",
        f"- 结构化摘要：[{summary_file.name}]({summary_file.name})", ""]
    target = folder / "REPORT.md"
    target.write_text("\n".join(lines), encoding="utf-8")
    return target


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("folders", type=Path, nargs="+")
    args = parser.parse_args()
    for folder in args.folders:
        print(render(folder))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

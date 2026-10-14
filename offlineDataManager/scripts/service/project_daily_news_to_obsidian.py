#!/usr/bin/env python3
"""把单日新闻级联分析结果投影为 Obsidian 每日文档。"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any

import duckdb


from coreClient._paths import manager_root, offline_data_dir, research_root
ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()
DEFAULT_INPUT_ROOT = research_root() / "outputs" / "major_news_analysis"
DEFAULT_DB = OFFLINE_DB_DIR / "db_major_news_events.duckdb"
DEFAULT_VAULT = Path.home() / "Documents" / "每日新闻分析"

CATEGORY_NAMES = {
    "policy": "政策",
    "macro": "宏观",
    "industry": "行业",
    "company": "公司",
    "commodity": "商品与供需",
    "technology": "科技",
    "geopolitics": "地缘政治",
    "overseas_market": "海外市场",
    "data_release": "经济数据",
    "other": "其他",
}
HORIZON_NAMES = {
    "immediate": "即时",
    "short": "短期",
    "medium": "中期",
    "long": "长期",
}
NOVELTY_NAMES = {"new": "新增", "follow_up": "进展", "background": "背景"}
MARKET_IMPACT_NAMES = {
    "bullish": "🔴 利好",
    "bearish": "🟢 利空",
    "neutral": "⚪ 中性",
}


def _one_line(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_one_line(item) for item in value if _one_line(item)]
    if isinstance(value, str) and value.strip().startswith("["):
        try:
            return _list(json.loads(value))
        except json.JSONDecodeError:
            pass
    return []


def _database_counts(database: Path, day: str) -> dict[str, int]:
    if not database.exists():
        return {}
    conn = duckdb.connect(str(database), read_only=True)
    try:
        row = conn.execute(
            """SELECT count(*) AS rule_events,
                      count(*) FILTER (WHERE excluded) AS excluded_events
               FROM fact_news_event WHERE event_date=?""", [day],
        ).fetchone()
        articles = conn.execute("""SELECT count(*),count(*) FILTER (WHERE excluded)
               FROM rel_news_event_member WHERE event_date=?""", [day]).fetchone()
        return {
            "rule_events": int(row[0]), "raw_articles": int(articles[0]),
            "excluded_events": int(row[1]), "excluded_articles": int(articles[1]),
        }
    finally:
        conn.close()


def _database_analysis(database: Path, day: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """从正式归档读取文档内容；JSON/CSV仅作流水线中间产物。"""
    conn = duckdb.connect(str(database), read_only=True)
    try:
        cursor = conn.execute("""SELECT a.semantic_event_id,a.importance_score,a.category,
                   a.market_impact,a.impact_reason,a.refined_title,a.summary,
                   a.key_facts_json,a.themes_json,a.entities_json,a.horizon,a.novelty,
                   s.first_published_at,s.title AS source_title,s.content
            FROM fact_major_news_analysis a
            JOIN fact_major_news_semantic_event s USING(semantic_event_id)
            WHERE a.event_date=? ORDER BY a.importance_score DESC,s.first_published_at""", [day])
        columns = [item[0] for item in cursor.description]
        rows = [dict(zip(columns, values)) for values in cursor.fetchall()]
        summary_row = conn.execute("""SELECT semantic_events,selected_events,detailed_events,
                   bullish_events,bearish_events,neutral_events
            FROM fact_major_news_daily_analysis WHERE event_date=?""", [day]).fetchone()
    finally:
        conn.close()
    if summary_row is None:
        raise ValueError(f"{day} 的 Major News 分析尚未写入数据库")
    events = []
    for row in rows:
        events.append({
            "semantic_event_id": row["semantic_event_id"],
            "importance_score": int(row["importance_score"]),
            "category": row["category"], "market_impact": row["market_impact"],
            "impact_reason": row["impact_reason"],
            "title": row["refined_title"] or row["source_title"],
            "summary": row["summary"] or "",
            "key_facts": json.loads(row["key_facts_json"] or "[]"),
            "themes": json.loads(row["themes_json"] or "[]"),
            "entities": json.loads(row["entities_json"] or "[]"),
            "horizon": row["horizon"], "novelty": row["novelty"],
            "time": str(row["first_published_at"])[:19],
            "text": row["content"] or "",
        })
    if len(events) != int(summary_row[1]):
        raise ValueError("数据库日度统计与分析记录数不一致")
    return events, {
        "semantic_input_events": int(summary_row[0]),
        "stage1_kept_events": int(summary_row[1]),
        "stage2_detailed_events": int(summary_row[2]),
        "bullish_events": int(summary_row[3]),
        "bearish_events": int(summary_row[4]),
        "neutral_events": int(summary_row[5]),
    }


def render(day: str, events: list[dict[str, Any]], cascade: dict[str, Any],
           semantic: dict[str, Any], database_counts: dict[str, int]) -> str:
    detailed = [item for item in events if _one_line(item.get("summary"))]
    detailed.sort(key=lambda item: (-int(item.get("importance_score") or 0), _one_line(item.get("time"))))
    tracked = [item for item in events if not _one_line(item.get("summary"))]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in detailed:
        grouped[str(item.get("category") or "other")].append(item)
    category_counts = Counter(str(item.get("category") or "other") for item in events)
    impact_counts = Counter(str(item.get("market_impact") or "neutral") for item in events)
    effective_count = int(cascade.get("stage1_kept_events") or len(events))

    lines = [
        "---",
        f"date: {day}",
        "type: 每日新闻分析",
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"effective_events: {effective_count}",
        f"detailed_events: {len(detailed)}",
        f"bullish_events: {impact_counts.get('bullish', 0)}",
        f"bearish_events: {impact_counts.get('bearish', 0)}",
        f"neutral_events: {impact_counts.get('neutral', 0)}",
        "tags: [每日新闻, 财经新闻, A股研究]",
        "---",
        "",
        f"# {day} 每日新闻分析",
        "",
        "> 本页只使用 Major News，由规则清洗、跨源去重、语义事件聚类、重要性轻筛和高分事件深度提炼生成。新闻事实用于研究线索，不代表交易结论。",
        "",
        "## 处理概览",
        "",
        "| 层级 | 数量 |",
        "|---|---:|",
        f"| 原始 Major News | {database_counts.get('raw_articles', 0):,} |",
        f"| 规则事件 | {database_counts.get('rule_events', 0):,} |",
        f"| 规则排除事件 | {database_counts.get('excluded_events', 0):,} |",
        f"| 语义事件 | {int(cascade.get('semantic_input_events') or semantic.get('semantic_events') or 0):,} |",
        f"| 有效事件（40分以上） | {effective_count:,} |",
        f"| 深度提炼（60分以上） | {len(detailed):,} |",
        "",
        "### 有效事件分类",
        "",
        "| 分类 | 数量 |",
        "|---|---:|",
    ]
    for category, count in category_counts.most_common():
        lines.append(f"| {CATEGORY_NAMES.get(category, category)} | {count} |")

    lines += [
        "", "### 有效新闻评级分布", "",
        "| 评级 | 数量 |", "|---|---:|",
        f"| {MARKET_IMPACT_NAMES['bullish']} | {impact_counts.get('bullish', 0)} |",
        f"| {MARKET_IMPACT_NAMES['bearish']} | {impact_counts.get('bearish', 0)} |",
        f"| {MARKET_IMPACT_NAMES['neutral']} | {impact_counts.get('neutral', 0)} |",
        "",
        "> 评级表示新闻对A股整体或主要相关行业的边际方向。影响对象不清楚或利好利空并存时按中性处理，不构成交易建议。",
    ]

    major = [item for item in detailed if int(item.get("importance_score") or 0) >= 80]
    lines += ["", "## 今日重大事件（80分以上）", ""]
    if major:
        for item in major:
            title = _one_line(item.get("title")) or _one_line(item.get("summary"))[:40]
            impact = MARKET_IMPACT_NAMES.get(str(item.get("market_impact")), MARKET_IMPACT_NAMES["neutral"])
            lines.append(
                f"- **{int(item.get('importance_score') or 0)}｜{impact}｜{CATEGORY_NAMES.get(str(item.get('category')), str(item.get('category')))}｜{title}**："
                f"{_one_line(item.get('summary'))}"
            )
    else:
        lines.append("- 今日没有80分以上事件。")

    lines += ["", "## 深度提炼事件（60分以上）", ""]
    ordered_categories = sorted(grouped, key=lambda key: (-max(int(x.get("importance_score") or 0) for x in grouped[key]), CATEGORY_NAMES.get(key, key)))
    for category in ordered_categories:
        items = grouped[category]
        lines += [f"### {CATEGORY_NAMES.get(category, category)}（{len(items)}）", ""]
        for item in items:
            score = int(item.get("importance_score") or 0)
            title = _one_line(item.get("title")) or _one_line(item.get("summary"))[:40]
            impact = MARKET_IMPACT_NAMES.get(str(item.get("market_impact")), MARKET_IMPACT_NAMES["neutral"])
            lines += [
                f"#### {score}｜{impact}｜{title}",
                "",
                f"- **时间**：{_one_line(item.get('time'))}",
                f"- **影响评级**：{impact}",
                f"- **评级依据**：{_one_line(item.get('impact_reason')) or '影响方向或主要受影响对象不明确'}",
                f"- **影响周期**：{HORIZON_NAMES.get(str(item.get('horizon')), str(item.get('horizon') or ''))}",
                f"- **信息状态**：{NOVELTY_NAMES.get(str(item.get('novelty')), str(item.get('novelty') or ''))}",
                f"- **摘要**：{_one_line(item.get('summary'))}",
            ]
            facts = _list(item.get("key_facts"))
            if facts:
                lines.append("- **关键事实**：")
                lines.extend(f"  - {fact}" for fact in facts)
            themes = _list(item.get("themes"))
            if themes:
                lines.append(f"- **关联题材**：{'、'.join(themes)}")
            entities = _list(item.get("entities"))
            if entities:
                lines.append(f"- **相关实体**：{'、'.join(entities)}")
            lines += [f"- **事件ID**：`{_one_line(item.get('semantic_event_id'))}`", ""]

    lines += ["## 其余入选事件（未深度提炼）", ""]
    if tracked:
        lines += ["| 分数 | 评级 | 分类 | 时间 | 事件 | 评级依据 |", "|---:|---|---|---|---|---|"]
        for item in sorted(tracked, key=lambda x: (-int(x.get("importance_score") or 0), _one_line(x.get("time")))):
            title = _one_line(item.get("title")) or _one_line(item.get("text"))[:80]
            title = title.replace("|", "｜")
            reason = _one_line(item.get("impact_reason")).replace("|", "｜")
            impact = MARKET_IMPACT_NAMES.get(str(item.get("market_impact")), MARKET_IMPACT_NAMES["neutral"])
            lines.append(
                f"| {int(item.get('importance_score') or 0)} | "
                f"{impact} | "
                f"{CATEGORY_NAMES.get(str(item.get('category')), str(item.get('category')))} | "
                f"{_one_line(item.get('time'))} | {title} | {reason} |"
            )
    else:
        lines.append("- 无。")
    lines += ["", "---", "", "数据来源：TradingAgent 本地 `tbl_major_news`；本页可由同一日期结果幂等覆盖。", ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--vault", type=Path, default=DEFAULT_VAULT)
    parser.add_argument("--mode", choices=["auto", "fast", "cascade"], default="auto")
    args = parser.parse_args()
    day = datetime.strptime(re.sub(r"\D", "", args.trade_date)[:8], "%Y%m%d").date().isoformat()
    out = args.input_root / day.replace("-", "")
    if args.mode == "cascade":
        final_path = out / "cascade_final_events.json"
        if not final_path.exists():
            raise SystemExit(f"缺少深度提炼结果: {final_path}")
        events = json.loads(final_path.read_text(encoding="utf-8"))
        cascade = json.loads((out / "cascade_summary.json").read_text(encoding="utf-8"))
        semantic = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    else:
        events, cascade = _database_analysis(args.database, day)
        semantic = {"semantic_events": cascade["semantic_input_events"]}
    counts = _database_counts(args.database, day)

    vault = args.vault.expanduser().resolve()
    daily_dir = vault / "每日新闻"
    (vault / ".obsidian").mkdir(parents=True, exist_ok=True)
    daily_dir.mkdir(parents=True, exist_ok=True)
    app_config = vault / ".obsidian" / "app.json"
    if not app_config.exists():
        app_config.write_text(json.dumps({
            "newFileLocation": "folder", "newFileFolderPath": "每日新闻",
            "showLineNumber": True,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    target = daily_dir / f"{day}.md"
    target.write_text(render(day, events, cascade, semantic, counts), encoding="utf-8")
    home = vault / "首页.md"
    links = sorted(
        (f"- [[每日新闻/{path.stem}|{path.stem}]]" for path in daily_dir.glob("*.md")),
        reverse=True,
    )
    home.write_text("# 每日新闻分析\n\n" + "\n".join(links) + "\n", encoding="utf-8")
    print(json.dumps({
        "vault": str(vault), "daily_note": str(target),
        "effective_events": int(cascade.get("stage1_kept_events") or len(events)),
        "detailed_events": sum(bool(_one_line(item.get("summary"))) for item in events),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

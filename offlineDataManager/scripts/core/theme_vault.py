"""把题材 DuckDB 投影为可阅读、可人工补充的 Obsidian 知识库。"""
from __future__ import annotations

from dataclasses import dataclass, asdict
import hashlib
import json
from pathlib import Path
import re

import duckdb
import pandas as pd

from config.settings import THEME_GRAPH_DB_PATH, THEME_VAULT_ROOT
from core.theme_daily_review import build_market_theme_reviews


def _safe_name(value: str) -> str:
    value = re.sub(r"[\\/:*?\"<>|#^\[\]]+", "_", str(value)).strip(" .")
    return value[:90] or "未命名"


def _fmt(value, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _value(value, fallback: str = "-") -> str:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return fallback
    return str(value)


def _table(headers: list[str], rows: list[list[object]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(item).replace("|", "\\|") for item in row) + " |")
    return "\n".join(lines)


def _json_object(value) -> dict:
    """把 DuckDB JSON 文本安全解析为字典，坏数据只影响当前分析块。"""
    if isinstance(value, dict):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _one_line(value, limit: int | None = None) -> str:
    text = re.sub(r"\s+", " ", _value(value, "")).strip()
    if limit and len(text) > limit:
        return text[: max(1, limit - 1)].rstrip() + "…"
    return text or "-"


def _ids(value) -> str:
    if not value:
        return "-"
    if isinstance(value, str):
        return value
    return "、".join(str(item) for item in value)


@dataclass
class VaultStats:
    files_written: int = 0
    files_unchanged: int = 0
    manual_notes_created: int = 0
    theme_dashboards: int = 0
    stock_dashboards: int = 0
    episode_notes: int = 0
    daily_notes: int = 0


class ThemeVaultRenderer:
    def __init__(self, database: str | Path = THEME_GRAPH_DB_PATH,
                 vault_root: str | Path = THEME_VAULT_ROOT):
        self.database = Path(database).expanduser()
        self.root = Path(vault_root).expanduser()
        self.stats = VaultStats()

    def render(self) -> dict:
        if not self.database.exists():
            raise FileNotFoundError(f"题材图谱库不存在: {self.database}")
        for folder in ["00_首页", "01_题材", "04_股票", "06_每日复盘",
                       "07_证据", "09_待确认", "_generated/themes", "_generated/stocks",
                       "_generated/daily"]:
            (self.root / folder).mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(self.database), read_only=True)
        try:
            # Obsidian 默认只展示曾作为 lu_desc 主归因的题材。辅助标签仍保留
            # 在 DuckDB，可查询但不批量生成数百个低价值入口页。
            themes = conn.execute(
                """SELECT t.*,COALESCE(x.level1_id,'L1-PENDING') AS level1_id,
                          COALESCE(x.level1_name,'待归类') AS level1_name,
                          COALESCE(x.secondary_level1_json,'[]') AS secondary_level1_json,
                          COALESCE(x.review_status,'needs_review') AS taxonomy_review_status
                   FROM dim_theme t LEFT JOIN dim_theme_taxonomy x USING(theme_id) WHERE EXISTS (
                     SELECT 1 FROM rel_limit_theme r WHERE r.theme_id=t.theme_id
                       AND r.attribution_role='primary')
                   ORDER BY t.canonical_name,t.theme_id""").df()
            daily = conn.execute(
                """SELECT d.*,t.canonical_name,
                          COALESCE(x.level1_name,'待归类') AS level1_name
                   FROM fact_theme_daily d
                   JOIN dim_theme t USING(theme_id)
                   LEFT JOIN dim_theme_taxonomy x USING(theme_id) WHERE EXISTS (
                     SELECT 1 FROM rel_limit_theme r WHERE r.theme_id=d.theme_id
                       AND r.attribution_role='primary')
                   ORDER BY d.trade_date,d.heat_score DESC,t.canonical_name,d.theme_id""").df()
            episodes = conn.execute(
                """SELECT e.*, t.canonical_name FROM fact_theme_episode e
                   JOIN dim_theme t USING(theme_id) WHERE EXISTS (
                     SELECT 1 FROM rel_limit_theme r WHERE r.theme_id=e.theme_id
                       AND r.attribution_role='primary')
                   ORDER BY e.start_date,e.peak_heat DESC""").df()
            aliases = conn.execute("SELECT * FROM theme_alias ORDER BY alias").df()
            stock_events = conn.execute(
                """SELECT e.trade_date,e.ts_code,e.name,e.tag,e.status_raw,e.board_height,
                          e.lu_time,e.limit_order,e.lu_limit_order,e.bid_amount,e.amount,e.free_float,
                          r.theme_id,t.canonical_name,r.attribution_role
                   FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
                   JOIN dim_theme t USING(theme_id)""").df()
            analyses = conn.execute(
                """WITH ranked AS (
                     SELECT a.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY a.entity_id,a.analysis_type
                              ORDER BY a.created_at DESC,a.analysis_id DESC
                            ) AS rn
                     FROM llm_analysis a
                     WHERE a.entity_type='theme_episode'
                       AND a.analysis_type IN ('catalyst_audit','episode_summary')
                   )
                   SELECT r.*,e.theme_id,e.start_date,e.last_active_date,e.peak_date
                   FROM ranked r JOIN fact_theme_episode e ON e.episode_id=r.entity_id
                   WHERE r.rn=1
                   ORDER BY e.start_date DESC,r.analysis_type""").df()
        finally:
            conn.close()

        theme_files = {row.theme_id: f"{_safe_name(row.level1_name)}/{_safe_name(row.canonical_name)}__{_safe_name(row.theme_id)}"
                       for row in themes.itertuples(index=False)}
        daily_reviews = build_market_theme_reviews(daily, stock_events, top_n=10, leader_count=3)
        self._render_home(themes, daily, episodes, theme_files)
        for level1_name, part in themes.groupby("level1_name", sort=True):
            folder = self.root / "01_题材" / _safe_name(level1_name)
            folder.mkdir(parents=True, exist_ok=True)
            self._render_level1(str(level1_name), part, daily, theme_files)
        for row in themes.itertuples(index=False):
            self._render_theme(row, aliases, daily, episodes, stock_events, analyses, theme_files)
        for date, part in daily.groupby("trade_date", sort=True):
            self._render_daily(str(date), part, theme_files, daily_reviews.get(str(date), {}))
        return asdict(self.stats)

    def _render_level1(self, level1_name: str, themes: pd.DataFrame, daily: pd.DataFrame,
                       theme_files: dict[str, str]) -> None:
        rows = []
        for theme in themes.itertuples(index=False):
            t_daily = daily[daily["theme_id"] == theme.theme_id]
            latest = t_daily.sort_values("trade_date", ascending=False).head(1)
            if latest.empty:
                date, heat, state = "-", "-", "-"
            else:
                record = latest.iloc[0]
                date, heat, state = record["trade_date"], record["heat_score"], record["lifecycle_state"]
            rows.append([f"[[01_题材/{theme_files[theme.theme_id]}|{theme.canonical_name}]]",
                         date, heat, state, theme.taxonomy_review_status])
        content = f"""---
type: level1-theme-index
level1_name: {level1_name}
theme_count: {len(themes)}
---
# {level1_name}

本页是一级题材目录；开盘啦原始主归因题材作为二级题材保留。

{_table(['二级题材', '最近活跃', '最近热度', '阶段', '归类状态'], rows)}
"""
        path = self.root / "01_题材" / _safe_name(level1_name) / f"00_{_safe_name(level1_name)}总览.md"
        self._write(path, content)

    def _write(self, path: Path, content: str, manual: bool = False) -> None:
        content = content.rstrip() + "\n"
        if path.exists():
            if manual:
                self.stats.files_unchanged += 1
                return
            if path.read_text(encoding="utf-8") == content:
                self.stats.files_unchanged += 1
                return
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
        self.stats.files_written += 1
        if manual:
            self.stats.manual_notes_created += 1

    def _render_home(self, themes: pd.DataFrame, daily: pd.DataFrame, episodes: pd.DataFrame,
                     theme_files: dict[str, str]) -> None:
        latest = daily["trade_date"].max() if not daily.empty else "-"
        leaders = daily[daily["trade_date"] == latest].head(20) if latest != "-" else pd.DataFrame()
        rows = []
        for item in leaders.itertuples(index=False):
            rows.append([f"[[01_题材/{theme_files[item.theme_id]}|{item.canonical_name}]]", item.heat_score,
                         item.limit_up_count, _value(item.max_board_height), item.lifecycle_state])
        level_rows = []
        for level1_name, part in themes.groupby("level1_name", sort=True):
            safe = _safe_name(level1_name)
            level_rows.append([f"[[01_题材/{safe}/00_{safe}总览|{level1_name}]]", len(part)])
        content = f"""---
type: theme-knowledge-home
generated_from: db_theme_graph.duckdb
latest_trade_date: {latest}
---
# A股题材时序知识库

事实主库是 DuckDB；本目录是自动投影与人工研究笔记。`_generated` 中的文件会被更新，
`01_题材`、`04_股票` 中可在“人工研究”段落持续补充，不会被自动覆盖。

- 题材数：{len(themes)}
- 炒作周期数：{len(episodes)}
- 最新数据日：{latest}

## 一级题材目录

{_table(['一级题材', '二级题材数'], level_rows)}

## 最新热度前 20

{_table(['题材', '热度', '涨停数', '高度', '阶段'], rows) if rows else '暂无数据'}
"""
        self._write(self.root / "00_首页" / "题材知识库.md", content)

    def _render_theme(self, theme, aliases, daily, episodes, stock_events, analyses, theme_files) -> None:
        stem = f"{_safe_name(theme.canonical_name)}__{_safe_name(theme.theme_id)}"
        generated_path = self.root / "_generated" / "themes" / f"{stem}__dashboard.md"
        manual_path = self.root / "01_题材" / f"{theme_files[theme.theme_id]}.md"
        self._migrate_theme_note(manual_path, stem, str(theme.level1_name))
        t_aliases = aliases[aliases["theme_id"] == theme.theme_id]
        t_daily = daily[daily["theme_id"] == theme.theme_id].sort_values("trade_date", ascending=False)
        t_episodes = episodes[episodes["theme_id"] == theme.theme_id].sort_values("start_date", ascending=False)
        t_analyses = analyses[analyses["theme_id"] == theme.theme_id]
        t_events = stock_events[(stock_events["theme_id"] == theme.theme_id) &
                                (stock_events["attribution_role"] == "primary")]
        event_stats = (t_events.groupby(["ts_code", "name"], dropna=False)
                       .agg(limit_days=("trade_date", "nunique"), max_height=("board_height", "max"),
                            last_limit=("trade_date", "max")).reset_index()
                       .sort_values(["limit_days", "max_height", "last_limit", "ts_code"],
                                    ascending=[False, False, False, True]).head(30))
        core_rows = []
        for item in event_stats.itertuples(index=False):
            core_rows.append([_value(item.name, item.ts_code), item.ts_code,
                              item.limit_days, _fmt(item.max_height), item.last_limit])
        daily_rows = [[r.trade_date, r.heat_score, r.limit_up_count, r.break_count,
                       _value(r.max_board_height), r.lifecycle_state]
                      for r in t_daily.head(30).itertuples(index=False)]
        ep_rows = []
        for episode in t_episodes.itertuples(index=False):
            attribution, attribution_status = self._episode_attribution(
                t_analyses[t_analyses["entity_id"] == episode.episode_id]
            )
            ep_rows.append([episode.episode_id, episode.start_date, episode.last_active_date,
                            episode.peak_heat, episode.peak_breadth, episode.status,
                            attribution, attribution_status])
        analysis_section = self._render_episode_analyses(t_episodes, t_analyses)
        alias_text = "、".join(sorted(set(t_aliases["alias"].astype(str)))) or theme.canonical_name
        generated = f"""---
type: generated-theme-dashboard
theme_id: {theme.theme_id}
canonical_name: {theme.canonical_name}
level1_name: {theme.level1_name}
first_seen: {theme.first_seen_date}
last_seen: {theme.last_seen_date}
---
> [!warning] 自动生成
> 此页来自 DuckDB，重新构建时会更新。人工内容请写在对应题材主页。

## 身份与别名

- 稳定 ID：`{theme.theme_id}`
- 一级题材：[[01_题材/{_safe_name(theme.level1_name)}/00_{_safe_name(theme.level1_name)}总览|{theme.level1_name}]]
- 二级题材：{theme.canonical_name}
- 交叉归属：{theme.secondary_level1_json}
- 归类状态：{theme.taxonomy_review_status}
- 类型：{theme.theme_level}
- 首次/最近出现：{theme.first_seen_date} / {theme.last_seen_date}
- 别名：{alias_text}

## 历史涨停核心候选（按主归因次数，最多 30）

{_table(['股票', '代码', '涨停日数', '最高板', '最近涨停'], core_rows) if core_rows else '暂无主归因涨停'}

## 炒作周期

{_table(['周期', '开始', '最后活跃', '峰值热度', '峰值宽度', '状态', '大模型归因', '归因复核'], ep_rows) if ep_rows else '暂无周期'}

{analysis_section}

## 最近 30 个活跃日

{_table(['日期', '热度', '涨停', '炸板', '高度', '阶段'], daily_rows) if daily_rows else '暂无每日数据'}
"""
        manual = f"""---
type: theme
theme_id: {theme.theme_id}
level1_name: {theme.level1_name}
level2_name: {theme.canonical_name}
aliases: [{theme.canonical_name}]
tags: [A股题材]
---
# {theme.canonical_name}

![[{stem}__dashboard]]

## 人工研究

### 定义与边界

### 产业链与分支

### 催化时间线

### 核心受益公司判断

### 待确认问题
"""
        self._write(generated_path, generated)
        self._write(manual_path, manual, manual=True)
        self.stats.theme_dashboards += 1

    @staticmethod
    def _analysis_row(rows: pd.DataFrame, analysis_type: str):
        selected = rows[rows["analysis_type"] == analysis_type]
        return None if selected.empty else selected.sort_values("created_at", ascending=False).iloc[0]

    def _episode_attribution(self, rows: pd.DataFrame) -> tuple[str, str]:
        if rows.empty:
            return "待分析", "未分析"
        audit_row = self._analysis_row(rows, "catalyst_audit")
        summary_row = self._analysis_row(rows, "episode_summary")
        audit = _json_object(audit_row["output_json"]) if audit_row is not None else {}
        summary = _json_object(summary_row["output_json"]) if summary_row is not None else {}
        revised = audit.get("revised_conclusion") or {}
        dominant = summary.get("dominant_reason") or {}
        raw_attribution = revised.get("summary") or dominant.get("summary")
        attribution = raw_attribution or "本地证据尚未形成可靠归因，需要联网补证"
        reason_status = revised.get("reason_status", "")
        verdict = audit.get("verdict") or "未审计"
        confidence = audit.get("confidence", summary.get("confidence"))
        needs_review = bool(audit.get("needs_review", summary.get("needs_review", True)))
        confirmed = (not needs_review and str(verdict).lower() in {"pass", "confirmed"}
                     and all(str(item).lower() == "confirmed" for item in rows["review_status"]))
        needs_web = not raw_attribution or reason_status == "no_reliable_reason"
        review_text = "需要联网补证" if needs_web else ("已复核" if confirmed else "待人工复核")
        confidence_text = f"{float(confidence):.2f}" if confidence is not None else "-"
        return _one_line(attribution, 220), f"{review_text}；审计={verdict}；置信={confidence_text}"

    def _render_episode_analyses(self, episodes: pd.DataFrame, analyses: pd.DataFrame) -> str:
        """把模型归因与证据投影到题材页；不改写事实表或人工笔记。"""
        if analyses.empty:
            return """## 已沉淀的炒作原因分析

暂无已完成的周期归因。后续模型分析写入 `llm_analysis` 后会自动显示在这里。"""

        blocks = ["""## 已沉淀的炒作原因分析

> [!info] 后续研究入口
> 先复用这里的归因、证据与未解决项。只有出现新周期、源数据修订或明确证据缺口时，才重新检索新闻。标记为“待人工复核”的内容是经模型审计的研究底稿，不等同于确认事实。"""]
        role_names = {
            "initial_trigger": "初始触发", "reinforcement": "强化", "acceleration": "加速",
            "branch_rotation": "分支轮动", "counter": "反证", "late_explanation": "盘后解释",
            "background_only": "背景", "start": "启动", "climax": "高潮",
            "divergence": "分歧", "retreat": "退潮", "repair": "修复",
        }
        core_roles = {
            "height_core": "高度核心", "first_mover": "先启动核心",
            "capacity_core": "容量核心", "turnover_core": "换手核心",
            "supplement": "补涨候选",
        }
        for episode in episodes.itertuples(index=False):
            rows = analyses[analyses["entity_id"] == episode.episode_id]
            if rows.empty:
                continue
            audit_row = self._analysis_row(rows, "catalyst_audit")
            summary_row = self._analysis_row(rows, "episode_summary")
            audit = _json_object(audit_row["output_json"]) if audit_row is not None else {}
            summary = _json_object(summary_row["output_json"]) if summary_row is not None else {}
            attribution, attribution_status = self._episode_attribution(rows)
            revised = audit.get("revised_conclusion") or {}
            dominant = summary.get("dominant_reason") or {}
            reason_status = revised.get("reason_status", "-")
            causal_status = revised.get("causal_status", dominant.get("causal_status", "-"))
            phase = _one_line(summary.get("phase_summary"))
            alignment = _one_line(summary.get("reason_market_alignment"))

            catalyst_rows = []
            for item in summary.get("catalysts", []) or []:
                catalyst_rows.append([
                    _value(item.get("date")), role_names.get(item.get("role"), _value(item.get("role"))),
                    _one_line(item.get("summary")), _ids(item.get("evidence_ids")),
                ])
            key_rows = []
            for item in summary.get("key_dates", []) or []:
                key_rows.append([
                    _value(item.get("date")), role_names.get(item.get("role"), _value(item.get("role"))),
                    _ids(item.get("evidence_ids")),
                ])
            core_rows = []
            for item in summary.get("market_cores", []) or []:
                core_rows.append([
                    _value(item.get("name"), _value(item.get("ts_code"))), _value(item.get("ts_code")),
                    core_roles.get(item.get("role"), _value(item.get("role"))), _ids(item.get("evidence_ids")),
                ])

            evidence_container = {}
            if audit_row is not None:
                evidence_container = _json_object(audit_row["evidence_json"])
            if not evidence_container and summary_row is not None:
                evidence_container = _json_object(summary_row["evidence_json"])
            news_rows = []
            for item in evidence_container.get("selected_evidence", []) or []:
                screening = item.get("screening") if isinstance(item.get("screening"), dict) else {}
                title = item.get("title") or item.get("content") or "-"
                title_text = _one_line(title, 120)
                if item.get("url"):
                    title_text = f"[{title_text}]({item['url']})"
                news_rows.append([
                    _value(item.get("evidence_id")), _value(item.get("published_at")),
                    _value(item.get("source")), title_text,
                    _value(screening.get("relevance")), _value(screening.get("candidate_causal_status")),
                ])
            market_rows = []
            for item in evidence_container.get("market_evidence", []) or []:
                market_rows.append([
                    _value(item.get("evidence_id")), _value(item.get("trade_date", item.get("date"))),
                    _value(item.get("lifecycle_state", item.get("state"))),
                    _value(item.get("limit_up_count")), _value(item.get("max_board_height")),
                    _value(item.get("heat_score")),
                ])

            unresolved = audit.get("unresolved") or summary.get("unresolved") or []
            unresolved_text = "\n".join(f"- {_one_line(item)}" for item in unresolved) or "- 无"
            audit_provider = "-" if audit_row is None else (
                f"{audit_row['provider']} / {audit_row['model']} / prompt {audit_row['prompt_version']} / {audit_row['created_at']}"
            )
            summary_provider = "-" if summary_row is None else (
                f"{summary_row['provider']} / {summary_row['model']} / prompt {summary_row['prompt_version']} / {summary_row['created_at']}"
            )
            blocks.append(f"""### {episode.start_date}—{episode.last_active_date}（`{episode.episode_id}`）

> [!warning] {attribution_status}
> 归因类别：`{reason_status}`；因果等级：`{causal_status}`。

**大模型归因：** {attribution}

**周期阶段：** {phase}

**原因与行情对应：** {alignment}

#### 催化时间线

{_table(['日期', '角色', '事件摘要', '证据'], catalyst_rows) if catalyst_rows else '暂无结构化催化时间线'}

#### 关键行情日期

{_table(['日期', '阶段', '证据'], key_rows) if key_rows else '暂无关键日期'}

#### 市场核心股

{_table(['股票', '代码', '角色', '证据'], core_rows) if core_rows else '暂无核心股判断'}

#### 新闻证据索引

{_table(['证据', '发布时间', '来源', '标题/摘要', '相关性', '因果等级'], news_rows) if news_rows else '暂无新闻证据索引'}

#### 行情证据索引

{_table(['证据', '日期', '阶段', '涨停数', '最高板', '热度'], market_rows) if market_rows else '行情证据保存在 DuckDB，可通过统一接口读取'}

#### 尚未解决

{unresolved_text}

#### 模型与版本

- 审计：{audit_provider}
- 周期摘要：{summary_provider}
""")
        return "\n\n".join(blocks)

    def _migrate_theme_note(self, manual_path: Path, stem: str, level1_name: str) -> None:
        """题材改归属时移动人工页，避免旧目录残留或人工内容丢失。"""
        theme_root = self.root / "01_题材"
        candidates = [path for path in theme_root.rglob(f"{stem}.md") if path != manual_path]
        if not candidates:
            return
        manual_path.parent.mkdir(parents=True, exist_ok=True)

        def with_current_level1(text: str) -> str:
            return re.sub(r"(?m)^level1_name:.*$", f"level1_name: {level1_name}", text, count=1)

        def has_research(text: str) -> bool:
            # 默认骨架之外的文本视为人工研究；迁移时它的优先级最高。
            body = re.sub(r"(?s)^---.*?---", "", text)
            meaningful = []
            for line in body.splitlines():
                value = line.strip()
                if not value or value.startswith("#") or value.startswith("![["):
                    continue
                meaningful.append(value)
            return bool(meaningful)

        existing = manual_path.read_text(encoding="utf-8") if manual_path.exists() else ""
        for old_path in sorted(candidates):
            old = with_current_level1(old_path.read_text(encoding="utf-8"))
            if not existing or (has_research(old) and not has_research(existing)):
                existing = old
            elif has_research(old) and old.strip() != existing.strip():
                existing = (existing.rstrip() + "\n\n## 从旧一级题材目录迁移的人工内容\n\n" +
                            old.rstrip() + "\n")
            old_path.unlink()
        if existing:
            manual_path.write_text(with_current_level1(existing).rstrip() + "\n", encoding="utf-8")

    def _render_stock(self, stock, events, theme_files, stock_files) -> None:
        stem = stock_files[stock.ts_code]
        generated_path = self.root / "_generated" / "stocks" / f"{stem}__dashboard.md"
        manual_path = self.root / "04_股票" / f"{stem}.md"
        selected = events[events["ts_code"] == stock.ts_code].sort_values("trade_date", ascending=False)
        rows = []
        for item in selected.head(80).itertuples(index=False):
            tlink = theme_files.get(item.theme_id, _safe_name(item.theme_id))
            rows.append([item.trade_date, item.tag, _value(item.status_raw), _value(item.board_height),
                         f"[[01_题材/{tlink}|{item.canonical_name}]]", item.attribution_role])
        generated = f"""---
type: generated-stock-dashboard
ts_code: {stock.ts_code}
name: {_value(stock.name, stock.ts_code)}
---
> [!warning] 自动生成
> 每日题材归因是当时市场叙事，不等于公司基本面收入或利润暴露。

## 涨停与题材时间线（最近 80 条关系）

{_table(['日期', '状态', '板型', '高度', '题材', '归因角色'], rows) if rows else '暂无数据'}
"""
        manual = f"""---
type: stock
ts_code: {stock.ts_code}
aliases: [{_value(stock.name, stock.ts_code)}]
tags: [A股个股]
---
# {_value(stock.name, stock.ts_code)}（{stock.ts_code}）

![[{stem}__dashboard]]

## 人工研究

### 主营业务与题材关系

### 收益阶段与证据

### 市场地位变化

### 待确认问题
"""
        self._write(generated_path, generated)
        self._write(manual_path, manual, manual=True)
        self.stats.stock_dashboards += 1

    def _render_daily(self, trade_date: str, part: pd.DataFrame, theme_files: dict[str, str],
                      review: dict | None = None) -> None:
        part = part.sort_values(["heat_score", "canonical_name", "theme_id"],
                                ascending=[False, True, True])
        full_rows = []
        for item in part.head(30).itertuples(index=False):
            link = theme_files.get(item.theme_id, _safe_name(item.theme_id))
            full_rows.append([f"[[01_题材/{link}|{item.canonical_name}]]", item.heat_score,
                              item.limit_up_count, item.break_count, _value(item.max_board_height), item.lifecycle_state])
        review = review or {}
        structure = review.get("structure") or {}
        hot_rows: list[list[object]] = []
        leader_sections: list[str] = []
        for theme in review.get("hot_themes", []):
            link = theme_files.get(theme["theme_id"], _safe_name(theme["theme_id"]))
            leaders = theme.get("leaders") or []
            leader_names = [item.get("name") or item.get("ts_code") or "-" for item in leaders]
            leader_names += ["-"] * (3 - len(leader_names))
            change = theme.get("rank_change")
            rank_change = "新进" if theme.get("previous_rank") is None else (f"+{change}" if change > 0 else str(change))
            hot_rows.append([
                theme["rank"], f"[[01_题材/{link}|{theme['theme']}]]", _fmt(theme.get("heat_score"), 2),
                theme.get("limit_up_count", 0), theme.get("break_count", 0),
                _value(theme.get("max_board_height")), _fmt(theme.get("seal_rate"), 2),
                theme.get("lifecycle_state", "-"), rank_change,
                leader_names[0], leader_names[1], leader_names[2],
            ])
            if leaders:
                detail_rows = []
                for leader in leaders:
                    detail_rows.append([
                        leader.get("title", "-"), f"{leader.get('name', '-')}（{leader.get('ts_code', '-')}）",
                        _fmt(leader.get("leader_score"), 1), leader.get("board_height", "-"),
                        leader.get("limit_days_20", "-"), leader.get("limit_time") or "-",
                        "、".join(leader.get("roles") or []) or "-",
                    ])
                leader_sections.append(
                    f"### {theme['rank']}. {theme['theme']}\n\n"
                    + _table(["龙位", "股票", "龙头分", "高度", "近20日涨停", "首次封板", "角色"], detail_rows)
                )
        score = review.get("theme_sentiment_score", "-")
        level = review.get("theme_sentiment_level", "-")
        composite = review.get("top3_heat_composite", "-")
        history_window = review.get("history_window_sessions", "-")
        structure_label = structure.get("label", "待判断")
        structure_confidence = structure.get("confidence", "-")
        summary = structure.get("summary", "暂无结构判断。")
        content = f"""---
type: generated-daily-theme-review
trade_date: {trade_date}
theme_sentiment_score: {score}
theme_sentiment_level: {level}
theme_structure: {structure.get('code', 'unknown')}
method_version: {review.get('method_version', 'unknown')}
---
# {trade_date} 题材复盘

## 今日题材情绪

- **题材情绪：{score}/100（{level}）**
- **市场结构：{structure_label}**（判断置信度：{structure_confidence}）
- 前三题材原始热度合成：{composite}；历史标尺：截至当日最近 {history_window} 个交易日
- 结构结论：{summary}

## 热门题材排名

{_table(['排名', '题材', '热度', '涨停', '炸板', '高度', '封板率', '阶段', '较前日', '龙一', '龙二', '龙三'], hot_rows) if hot_rows else '暂无可用题材复盘。'}

## 热门题材龙头梯队

{chr(10).join(leader_sections) if leader_sections else '暂无可用龙头候选。'}

## 完整热度表

{_table(['题材', '热度', '涨停', '炸板', '高度', '阶段'], full_rows)}

> 情绪分由前三题材热度按 50%/30%/20% 合成后，计算其在截至当日最近120个交易日中的历史百分位。龙头先按板高分层，同板高内再比较近20日涨停频次、封板时间、封单/流通盘和成交额；全程只使用当日及以前事实。ST板块、ST摘帽和次新股不参与统计。程序结果是候选排序，仍需结合催化与题材逻辑复核。
"""
        self._write(self.root / "_generated" / "daily" / f"{trade_date}__dashboard.md", content)
        manual_path = self.root / "06_每日复盘" / f"{trade_date}.md"
        manual = f"# {trade_date} 每日复盘\n\n![[{trade_date}__dashboard]]\n\n## 人工结论\n"
        self._write(manual_path, manual, manual=True)
        self.stats.daily_notes += 1

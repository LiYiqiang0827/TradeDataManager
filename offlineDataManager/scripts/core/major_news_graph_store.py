"""把每日 Major News 分析归档为可追溯的时序关系图。

新闻图与 KPL 题材图分库：只有经过日期有效性与唯一性校验的题材名才引用
KPL theme_id；未匹配标签保留原名和待映射状态，不能伪造 KPL 身份。
"""
from __future__ import annotations

import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import unicodedata
from typing import Any

import duckdb
import pandas as pd

try:
    from config.settings import NEWS_EVENT_DB_PATH, THEME_GRAPH_DB_PATH
except ModuleNotFoundError:  # pragma: no cover
    from offlineDataManager.scripts.config.settings import NEWS_EVENT_DB_PATH, THEME_GRAPH_DB_PATH


def _day(value: str) -> str:
    digits = re.sub(r"\D", "", str(value))[:8]
    return datetime.strptime(digits, "%Y%m%d").strftime("%Y-%m-%d")


def _name(value: Any) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(value or ""))).strip()


def _id(prefix: str, label: str) -> str:
    return prefix + hashlib.sha256(label.encode("utf-8")).hexdigest()[:20]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_major_news_semantic_event(
  semantic_event_id VARCHAR PRIMARY KEY, event_date VARCHAR NOT NULL,
  first_published_at TIMESTAMP, last_published_at TIMESTAMP,
  representative_event_id VARCHAR, representative_src VARCHAR,
  title VARCHAR, content VARCHAR, member_event_count INTEGER,
  raw_article_count INTEGER, source_count INTEGER, sources_json VARCHAR,
  pipeline_version VARCHAR, imported_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_major_semantic_date ON fact_major_news_semantic_event(event_date);
CREATE TABLE IF NOT EXISTS rel_major_news_semantic_member(
  semantic_event_id VARCHAR, event_id VARCHAR, event_date VARCHAR,
  is_representative BOOLEAN, similarity DOUBLE,
  source_name VARCHAR, source_title VARCHAR, source_content VARCHAR,
  raw_article_count INTEGER,
  PRIMARY KEY(semantic_event_id,event_id)
);
CREATE TABLE IF NOT EXISTS fact_major_news_analysis(
  semantic_event_id VARCHAR PRIMARY KEY, event_date VARCHAR NOT NULL,
  importance_score INTEGER, category VARCHAR, market_impact VARCHAR,
  impact_reason VARCHAR, detailed BOOLEAN, refined_title VARCHAR,
  summary VARCHAR, key_facts_json VARCHAR, themes_json VARCHAR,
  entities_json VARCHAR, horizon VARCHAR, novelty VARCHAR,
  provider VARCHAR, model VARCHAR, prompt_hash VARCHAR,
  input_hash VARCHAR, review_status VARCHAR, imported_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_major_analysis_date ON fact_major_news_analysis(event_date);
CREATE TABLE IF NOT EXISTS dim_major_news_theme_tag(
  tag_id VARCHAR PRIMARY KEY, label VARCHAR NOT NULL, created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS rel_major_news_theme(
  semantic_event_id VARCHAR, event_date VARCHAR, tag_id VARCHAR,
  raw_label VARCHAR, kpl_theme_id VARCHAR, match_status VARCHAR,
  relation_type VARCHAR, evidence_status VARCHAR,
  first_published_at TIMESTAMP, analysis_prompt_hash VARCHAR,
  PRIMARY KEY(semantic_event_id,tag_id)
);
CREATE INDEX IF NOT EXISTS idx_major_theme_ref ON rel_major_news_theme(kpl_theme_id,event_date);
CREATE TABLE IF NOT EXISTS dim_major_news_entity(
  entity_id VARCHAR PRIMARY KEY, label VARCHAR NOT NULL, created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS rel_major_news_entity(
  semantic_event_id VARCHAR, event_date VARCHAR, entity_id VARCHAR,
  first_published_at TIMESTAMP, analysis_prompt_hash VARCHAR,
  PRIMARY KEY(semantic_event_id,entity_id)
);
CREATE TABLE IF NOT EXISTS fact_major_news_daily_analysis(
  event_date VARCHAR PRIMARY KEY, raw_articles INTEGER, rule_events INTEGER,
  semantic_events INTEGER, selected_events INTEGER, detailed_events INTEGER,
  bullish_events INTEGER, bearish_events INTEGER, neutral_events INTEGER,
  prompt_hash VARCHAR, artifact_hash VARCHAR, theme_mapping_as_of VARCHAR,
  imported_at TIMESTAMP
);
"""


class MajorNewsGraphStore:
    def __init__(self, database: Path = NEWS_EVENT_DB_PATH, theme_database: Path = THEME_GRAPH_DB_PATH):
        self.database = Path(database)
        self.theme_database = Path(theme_database)

    def _theme_lookup(self, day: str) -> dict[str, set[str]]:
        if not self.theme_database.exists():
            return {}
        as_of = day.replace("-", "")
        conn = duckdb.connect(str(self.theme_database), read_only=True)
        try:
            rows = conn.execute("""
                SELECT a.alias AS label,a.theme_id
                FROM theme_alias a JOIN dim_theme t USING(theme_id)
                WHERE a.valid_from<=? AND (a.valid_to IS NULL OR a.valid_to>=?)
                  AND t.first_seen_date<=?
            """, [as_of, as_of, as_of]).fetchall()
        finally:
            conn.close()
        lookup: dict[str, set[str]] = {}
        for label, theme_id in rows:
            lookup.setdefault(_name(label), set()).add(str(theme_id))
        return lookup

    @staticmethod
    def _create_schema(conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(SCHEMA)
        # 旧版图数据库有同名五列表，保留其他日期归档并逐列迁移。
        for name, dtype in (("source_name", "VARCHAR"), ("source_title", "VARCHAR"),
                            ("source_content", "VARCHAR"), ("raw_article_count", "INTEGER")):
            conn.execute(f"ALTER TABLE rel_major_news_semantic_member ADD COLUMN IF NOT EXISTS {name} {dtype}")

    def import_day(self, trade_date: str, artifact_root: Path) -> dict[str, Any]:
        day = _day(trade_date)
        folder = Path(artifact_root) / day.replace("-", "")
        semantic_path = folder / "semantic_events.csv"
        member_path = folder / "semantic_event_members.csv"
        selected_path = folder / "fast_rated_selected_events.json"
        detailed_path = folder / "fast_final_events.json"
        summary_path = folder / "fast_summary.json"
        for path in (semantic_path, member_path, selected_path, detailed_path, summary_path):
            if not path.exists():
                raise FileNotFoundError(path)
        semantic = _load_csv(semantic_path)
        members = _load_csv(member_path)
        selected = json.loads(selected_path.read_text(encoding="utf-8"))
        detailed = json.loads(detailed_path.read_text(encoding="utf-8"))
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if not isinstance(selected, list) or not isinstance(detailed, list):
            raise ValueError("模型分析结果必须是列表")
        semantic_ids = {row["semantic_event_id"] for row in semantic}
        selected_ids = {str(row["semantic_event_id"]) for row in selected}
        detailed_ids = {str(row["semantic_event_id"]) for row in detailed}
        if len(semantic_ids) != len(semantic) or len(selected_ids) != len(selected) or len(detailed_ids) != len(detailed):
            raise ValueError("语义事件或入选事件存在重复ID")
        if not selected_ids <= semantic_ids or not detailed_ids <= selected_ids:
            raise ValueError("分析事件ID无法追溯到语义事件")
        if len(semantic) != int(summary["semantic_input_events"]) or len(selected) != int(summary["stage1_kept_events"]) or len(detailed) != int(summary["stage2_detailed_events"]):
            raise ValueError("分析结果与日度统计数量不一致")
        if any(row.get("market_impact") not in {"bullish", "bearish", "neutral"} or not row.get("impact_reason") for row in selected):
            raise ValueError("存在未评级的入选新闻")
        if any(row["semantic_event_id"] not in semantic_ids for row in members):
            raise ValueError("语义成员指向不存在的事件")
        detail_by_id = {str(row["semantic_event_id"]): row for row in detailed}
        semantic_by_id = {row["semantic_event_id"]: row for row in semantic}
        theme_lookup = self._theme_lookup(day)
        prompt_meta = json.loads((folder / "fast_stage2_meta.json").read_text(encoding="utf-8"))
        impact_meta_path = folder / "fast_impact_meta.json"
        impact_meta = json.loads(impact_meta_path.read_text(encoding="utf-8")) if impact_meta_path.exists() else {}
        detail_prompt_hash = str(prompt_meta.get("prompt_hash") or "")
        impact_prompt_hash = str(impact_meta.get("prompt_hash") or "")
        model_by_id: dict[str, str] = {}
        for filename, key in (("fast_stage2_batches.json", "items"), ("fast_impact_batches.json", "ratings")):
            path = folder / filename
            if not path.exists():
                continue
            batches = json.loads(path.read_text(encoding="utf-8"))
            for batch in batches.values():
                for item in batch.get(key, []):
                    model_by_id[str(item["semantic_event_id"])] = str(batch.get("model") or "unknown")
        prompt_hash = hashlib.sha256(f"{detail_prompt_hash}:{impact_prompt_hash}".encode("utf-8")).hexdigest()
        artifact_hash = hashlib.sha256(b"".join(p.read_bytes() for p in (semantic_path, member_path, selected_path, detailed_path, summary_path))).hexdigest()

        conn = duckdb.connect(str(self.database))
        try:
            self._create_schema(conn)
            raw_articles = conn.execute("""SELECT count(*) FROM rel_news_event_member
                WHERE event_date=?""", [day]).fetchone()[0]
            rule_events = conn.execute("""SELECT count(*) FROM fact_news_event
                WHERE event_date=?""", [day]).fetchone()[0]
            now = datetime.now()
            conn.execute("BEGIN TRANSACTION")
            try:
                # 本日整体替换，消除模型或语义聚类重跑后遗留的旧边。
                for table in ("rel_major_news_theme", "rel_major_news_entity", "fact_major_news_analysis",
                              "rel_major_news_semantic_member", "fact_major_news_semantic_event",
                              "fact_major_news_daily_analysis"):
                    conn.execute(f"DELETE FROM {table} WHERE event_date=?", [day])
                for row in semantic:
                    conn.execute("""INSERT INTO fact_major_news_semantic_event VALUES
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", [
                        row["semantic_event_id"], day, row["first_published_at"], row["last_published_at"],
                        row["representative_event_id"], row["representative_src"], row["title"],
                        row["content"], int(row["member_event_count"]), int(row["raw_article_count"]),
                        int(row["source_count"]), row["sources_json"], "major-news-semantic-v1", now,
                    ])
                for row in members:
                    conn.execute("""INSERT INTO rel_major_news_semantic_member
                        (semantic_event_id,event_id,event_date,is_representative,similarity,
                         source_name,source_title,source_content,raw_article_count)
                        VALUES (?,?,?,?,?,?,?,?,?)""", [
                        row["semantic_event_id"], row["event_id"], day,
                        row["is_representative"].lower() == "true", float(row["similarity_to_representative"]),
                        row.get("source", ""), row.get("title", ""), row.get("content", ""),
                        int(row.get("item_count") or 0),
                    ])
                theme_edges = entity_edges = matched = unresolved = 0
                for row in selected:
                    event_id = str(row["semantic_event_id"])
                    detail = detail_by_id.get(event_id, {})
                    source = semantic_by_id[event_id]
                    model = model_by_id.get(event_id, "unknown")
                    analysis_prompt_hash = detail_prompt_hash if detail else impact_prompt_hash
                    input_hash = hashlib.sha256(_json({"source": source, "score": row["importance_score"]}).encode("utf-8")).hexdigest()
                    conn.execute("""INSERT INTO fact_major_news_analysis VALUES
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", [
                        event_id, day, int(row["importance_score"]), row["category"],
                        row["market_impact"], row["impact_reason"], bool(detail), detail.get("title"),
                        detail.get("summary"), _json(detail.get("key_facts", [])), _json(detail.get("themes", [])),
                        _json(detail.get("entities", [])), detail.get("horizon"), detail.get("novelty"),
                        "qwen-vllm", model, analysis_prompt_hash, input_hash, "unreviewed", now,
                    ])
                    seen_tags: set[str] = set()
                    for label_raw in detail.get("themes", []):
                        label = _name(label_raw)
                        if not label or label in seen_tags:
                            continue
                        seen_tags.add(label)
                        tag_id = _id("NT-", label)
                        conn.execute("INSERT OR IGNORE INTO dim_major_news_theme_tag VALUES (?,?,?)", [tag_id, label, now])
                        matches = theme_lookup.get(label, set())
                        excluded = bool(re.search(r"(?:^|\s)(?:ST|次新)", label, re.I))
                        status = "excluded" if excluded else ("matched" if len(matches) == 1 else "ambiguous" if matches else "unmatched")
                        theme_id = next(iter(matches)) if status == "matched" else None
                        conn.execute("""INSERT INTO rel_major_news_theme VALUES
                            (?,?,?,?,?,?,?,?,?,?)""", [event_id, day, tag_id, label, theme_id,
                            status, "related_to", "model_unreviewed", source["first_published_at"], analysis_prompt_hash])
                        theme_edges += 1
                        matched += status == "matched"
                        unresolved += status in {"unmatched", "ambiguous"}
                    seen_entities: set[str] = set()
                    for label_raw in detail.get("entities", []):
                        label = _name(label_raw)
                        if not label or label in seen_entities:
                            continue
                        seen_entities.add(label)
                        entity_id = _id("EN-", label)
                        conn.execute("INSERT OR IGNORE INTO dim_major_news_entity VALUES (?,?,?)", [entity_id, label, now])
                        conn.execute("INSERT OR IGNORE INTO rel_major_news_entity VALUES (?,?,?,?,?)", [
                            event_id, day, entity_id, source["first_published_at"], analysis_prompt_hash,
                        ])
                        entity_edges += 1
                counts = {key: sum(row["market_impact"] == key for row in selected) for key in ("bullish", "bearish", "neutral")}
                conn.execute("""INSERT INTO fact_major_news_daily_analysis VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?)""", [day, int(raw_articles), int(rule_events),
                    len(semantic), len(selected), len(detailed), counts["bullish"], counts["bearish"],
                    counts["neutral"], prompt_hash, artifact_hash, day, now])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            return {"event_date": day, "semantic_events": len(semantic), "selected_events": len(selected),
                    "detailed_events": len(detailed), "theme_edges": theme_edges, "matched_theme_edges": matched,
                    "unresolved_theme_edges": unresolved, "entity_edges": entity_edges,
                    "ratings": counts, "artifact_hash": artifact_hash}
        finally:
            conn.close()

    def query_analysis(self, start_date: str, end_date: str, *, theme_id: str | None = None,
                       theme_label: str | None = None, entity: str | None = None,
                       keyword: str | None = None, market_impact: str | None = None,
                       min_importance: int = 0) -> pd.DataFrame:
        conn = duckdb.connect(str(self.database), read_only=True)
        try:
            sql = """SELECT a.*,s.first_published_at,s.representative_src,s.title AS source_title,
                     s.content AS source_content,s.raw_article_count,s.source_count
                     FROM fact_major_news_analysis a
                     JOIN fact_major_news_semantic_event s USING(semantic_event_id)"""
            conditions = ["a.event_date BETWEEN ? AND ?", "a.importance_score>=?"]
            params: list[Any] = [_day(start_date), _day(end_date), min_importance]
            if theme_id:
                conditions.append("EXISTS (SELECT 1 FROM rel_major_news_theme t WHERE t.semantic_event_id=a.semantic_event_id AND t.kpl_theme_id=? AND t.match_status='matched')")
                params.append(theme_id)
            if theme_label:
                conditions.append("EXISTS (SELECT 1 FROM rel_major_news_theme t WHERE t.semantic_event_id=a.semantic_event_id AND contains(lower(t.raw_label),lower(?)))")
                params.append(theme_label)
            if entity:
                conditions.append("EXISTS (SELECT 1 FROM rel_major_news_entity r JOIN dim_major_news_entity e USING(entity_id) WHERE r.semantic_event_id=a.semantic_event_id AND contains(lower(e.label),lower(?)))")
                params.append(entity)
            if keyword:
                conditions.append("contains(lower(coalesce(a.refined_title,'') || ' ' || coalesce(s.title,'') || ' ' || coalesce(s.content,'') || ' ' || coalesce(a.summary,'')),lower(?))")
                params.append(keyword)
            if market_impact:
                conditions.append("a.market_impact=?")
                params.append(market_impact)
            return conn.execute(sql + " WHERE " + " AND ".join(conditions) +
                                " ORDER BY a.event_date,a.importance_score DESC,s.first_published_at", params).fetchdf()
        finally:
            conn.close()

    def query_theme_edges(self, start_date: str, end_date: str, *, theme_id: str | None = None) -> pd.DataFrame:
        conn = duckdb.connect(str(self.database), read_only=True)
        try:
            where = "event_date BETWEEN ? AND ?"
            params: list[Any] = [_day(start_date), _day(end_date)]
            if theme_id:
                where += " AND kpl_theme_id=? AND match_status='matched'"
                params.append(theme_id)
            return conn.execute("SELECT * FROM rel_major_news_theme WHERE " + where +
                                " ORDER BY event_date,first_published_at,semantic_event_id", params).fetchdf()
        finally:
            conn.close()

    def query_daily_summary(self, start_date: str, end_date: str) -> pd.DataFrame:
        conn = duckdb.connect(str(self.database), read_only=True)
        try:
            return conn.execute("""SELECT * FROM fact_major_news_daily_analysis
                WHERE event_date BETWEEN ? AND ? ORDER BY event_date""",
                [_day(start_date), _day(end_date)]).fetchdf()
        finally:
            conn.close()

    def query_event_graph(self, semantic_event_id: str, *, as_of: str | None = None) -> dict[str, Any]:
        """返回语义事件及其原始报道、题材、实体边；按最早发布时间截断。"""
        conn = duckdb.connect(str(self.database), read_only=True)
        try:
            cutoff = pd.Timestamp(_day(as_of)) + pd.Timedelta(days=1) if as_of else None
            event = conn.execute("""SELECT s.*,a.importance_score,a.category,a.market_impact,
                a.impact_reason,a.summary,a.review_status
                FROM fact_major_news_semantic_event s
                LEFT JOIN fact_major_news_analysis a USING(semantic_event_id)
                WHERE s.semantic_event_id=?""", [semantic_event_id]).fetchdf()
            if event.empty or (cutoff is not None and pd.Timestamp(event.iloc[0]["first_published_at"]) >= cutoff):
                return {}
            member_rows = conn.execute("""SELECT m.*
                FROM rel_major_news_semantic_member m
                WHERE m.semantic_event_id=?
                ORDER BY m.event_id""", [semantic_event_id]).fetchdf()
            raw_rows = conn.execute("""SELECT r.item_id,r.event_id,r.published_at,r.src,r.title,
                r.content,r.source_md5,r.duplicate_kind,r.excluded
                FROM rel_news_event_member r
                JOIN rel_major_news_semantic_member m USING(event_id)
                WHERE m.semantic_event_id=? ORDER BY r.published_at,r.item_id""", [semantic_event_id]).fetchdf()
            theme_rows = conn.execute("""SELECT r.*,t.label AS tag_label FROM rel_major_news_theme r
                JOIN dim_major_news_theme_tag t USING(tag_id)
                WHERE r.semantic_event_id=? ORDER BY r.raw_label""", [semantic_event_id]).fetchdf()
            entity_rows = conn.execute("""SELECT r.*,e.label FROM rel_major_news_entity r
                JOIN dim_major_news_entity e USING(entity_id)
                WHERE r.semantic_event_id=? ORDER BY e.label""", [semantic_event_id]).fetchdf()
            if cutoff is not None:
                raw_rows = raw_rows[pd.to_datetime(raw_rows["published_at"]) < cutoff]
                theme_rows = theme_rows[pd.to_datetime(theme_rows["first_published_at"]) < cutoff]
                entity_rows = entity_rows[pd.to_datetime(entity_rows["first_published_at"]) < cutoff]
            return {
                "event": event.to_dict("records")[0],
                "rule_members": member_rows.to_dict("records"),
                "raw_articles": raw_rows.to_dict("records"),
                "themes": theme_rows.to_dict("records"),
                "entities": entity_rows.to_dict("records"),
            }
        finally:
            conn.close()

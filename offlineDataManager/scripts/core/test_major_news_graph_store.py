from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb

from core.major_news_graph_store import MajorNewsGraphStore
from core.news_event_store import NewsEventStore


def _csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_import_graph_is_idempotent_and_preserves_point_in_time_theme_mapping(tmp_path: Path):
    news_db = tmp_path / "news.duckdb"
    theme_db = tmp_path / "theme.duckdb"
    NewsEventStore(database=news_db)
    theme = duckdb.connect(str(theme_db))
    theme.execute("CREATE TABLE dim_theme(theme_id VARCHAR,canonical_name VARCHAR,first_seen_date VARCHAR)")
    theme.execute("CREATE TABLE theme_alias(theme_id VARCHAR,alias VARCHAR,valid_from VARCHAR,valid_to VARCHAR)")
    theme.execute("INSERT INTO dim_theme VALUES ('T-COPPER','铜矿','20260101'),('T-FUTURE','机器人','20261001')")
    theme.execute("INSERT INTO theme_alias VALUES ('T-COPPER','铜矿','20260101',NULL),('T-COPPER','有色金属','20260101',NULL)")
    theme.close()
    conn = duckdb.connect(str(news_db))
    conn.execute("""INSERT INTO fact_news_event(event_id,event_date,first_published_at,last_published_at,
        representative_item_id,title,item_count,source_count,excluded)
        VALUES ('NE-1','2026-09-24','2026-09-24 09:00:00','2026-09-25 09:00:00','I-1','铜矿停产',2,1,FALSE)""")
    conn.execute("""INSERT INTO rel_news_event_member(item_id,event_id,published_at,event_date,src,title)
        VALUES ('I-1','NE-1','2026-09-24 09:00:00','2026-09-24','同花顺','铜矿停产')""")
    conn.execute("""INSERT INTO rel_news_event_member(item_id,event_id,published_at,event_date,src,title)
        VALUES ('I-2','NE-1','2026-09-25 09:00:00','2026-09-25','同花顺','铜矿后续')""")
    conn.close()
    folder = tmp_path / "artifacts" / "20260924"
    folder.mkdir(parents=True)
    _csv(folder / "semantic_events.csv", [{
        "semantic_event_id": "SE-1", "event_date": "2026-09-24",
        "first_published_at": "2026-09-24 09:00:00", "last_published_at": "2026-09-24 09:00:00",
        "representative_event_id": "NE-1", "representative_src": "同花顺", "title": "铜矿停产",
        "content": "", "member_event_count": "1", "raw_article_count": "1",
        "source_count": "1", "sources_json": '["同花顺"]',
    }])
    _csv(folder / "semantic_event_members.csv", [{
        "semantic_event_id": "SE-1", "event_id": "NE-1", "is_representative": "True",
        "similarity_to_representative": "1.0", "first_published_at": "2026-09-24 09:00:00",
        "source": "同花顺", "title": "铜矿停产", "content": "首日公告",
        "item_count": "1", "source_count": "1",
    }])
    selected = [{"semantic_event_id": "SE-1", "importance_score": 85, "category": "commodity",
                 "market_impact": "bullish", "impact_reason": "铜矿供应减少利好铜矿板块"}]
    detailed = [{**selected[0], "title": "铜矿停产", "summary": "铜矿停产",
                 "key_facts": ["铜矿停产"], "themes": ["铜矿", "机器人", "未定义题材"],
                 "entities": ["某铜矿"], "horizon": "short", "novelty": "new"}]
    (folder / "fast_rated_selected_events.json").write_text(json.dumps(selected, ensure_ascii=False), encoding="utf-8")
    (folder / "fast_final_events.json").write_text(json.dumps(detailed, ensure_ascii=False), encoding="utf-8")
    (folder / "fast_summary.json").write_text(json.dumps({"semantic_input_events": 1,
        "stage1_kept_events": 1, "stage2_detailed_events": 1}), encoding="utf-8")
    (folder / "fast_stage2_meta.json").write_text('{"prompt_hash":"prompt-v1"}', encoding="utf-8")

    store = MajorNewsGraphStore(news_db, theme_db)
    result = store.import_day("20260924", tmp_path / "artifacts")
    assert result["selected_events"] == 1
    assert result["matched_theme_edges"] == 1
    assert result["unresolved_theme_edges"] == 2
    assert int(store.query_daily_summary("20260924", "20260924").iloc[0].raw_articles) == 1
    assert len(store.query_analysis("20260924", "20260924", theme_id="T-COPPER")) == 1
    assert len(store.query_analysis("20260924", "20260924", theme_label="铜矿", entity="某铜矿", keyword="停产")) == 1
    assert store.query_analysis("20260924", "20260924", keyword="不存在的词").empty
    edges = store.query_theme_edges("20260924", "20260924")
    assert dict(zip(edges.raw_label, edges.match_status)) == {
        "铜矿": "matched", "机器人": "unmatched", "未定义题材": "unmatched",
    }
    graph = store.query_event_graph("SE-1", as_of="20260924")
    assert len(graph["raw_articles"]) == 1
    assert graph["rule_members"][0]["source_title"] == "铜矿停产"
    assert graph["rule_members"][0]["source_content"] == "首日公告"
    assert len(graph["themes"]) == 3
    assert graph["event"]["market_impact"] == "bullish"
    assert store.query_event_graph("SE-1", as_of="20260923") == {}
    store.import_day("20260924", tmp_path / "artifacts")
    assert len(store.query_theme_edges("20260924", "20260924")) == 3
    assert len(store.query_analysis("20260924", "20260924")) == 1

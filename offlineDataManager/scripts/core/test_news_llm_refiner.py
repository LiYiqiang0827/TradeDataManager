from datetime import datetime

import duckdb
import pytest

from core.news_event_store import NewsEventStore
from core.news_llm_refiner import NewsLLMRefiner, _validate


def test_validate_rejects_unknown_event():
    with pytest.raises(ValueError, match="不属于输入"):
        _validate({"selected": [{
            "event_id": "unknown", "importance_score": 70, "category": "policy",
            "summary": "摘要", "key_facts": [], "themes": [], "entities": [],
            "horizon": "short", "novelty": "new", "reason": "政策事实",
        }]}, {"known"})


def test_refiner_saves_kept_and_dropped_events(tmp_path, monkeypatch):
    database = tmp_path / "news.duckdb"
    NewsEventStore(database=database)
    conn = duckdb.connect(str(database))
    now = datetime(2026, 9, 1, 9, 0)
    for event_id, title in (("E1", "某部委发布产业政策"), ("E2", "普通背景信息")):
        conn.execute(
            """INSERT INTO fact_news_event(
                   event_id,event_date,first_published_at,last_published_at,
                   representative_item_id,title,content,excluded,item_count,source_count,
                   processor_version,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [event_id, "2026-09-01", now, now, event_id + "-item", title, title,
             False, 1, 1, "test", now],
        )
    conn.close()

    refiner = NewsLLMRefiner(database=database)
    monkeypatch.setattr(refiner, "_invoke", lambda *args, **kwargs: ({"selected": [{
        "event_id": "E1", "importance_score": 82, "category": "policy",
        "summary": "某部委发布产业政策", "key_facts": ["发布产业政策"],
        "themes": ["产业政策"], "entities": ["某部委"],
        "horizon": "medium", "novelty": "new", "reason": "影响行业预期",
    }]}, "test-model"))
    result = refiner.refine("20260901", "20260901", batch_size=10)
    assert result["processed_events"] == 2
    assert result["kept_events"] == 1

    conn = duckdb.connect(str(database), read_only=True)
    rows = conn.execute(
        "SELECT event_id,llm_keep,importance_score,category FROM fact_news_insight ORDER BY event_id"
    ).fetchall()
    conn.close()
    assert rows == [("E1", True, 82, "policy"), ("E2", False, 0, "noise")]

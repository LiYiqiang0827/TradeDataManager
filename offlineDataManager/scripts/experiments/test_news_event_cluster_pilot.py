from __future__ import annotations

from pathlib import Path

import duckdb

from core.news_event_store import NewsEventStore
from experiments.news_event_cluster_pilot import load_day_snapshot


def test_day_snapshot_does_not_include_next_day_followup(tmp_path: Path):
    database = tmp_path / "news.duckdb"
    NewsEventStore(database=database)
    conn = duckdb.connect(str(database))
    conn.execute("""INSERT INTO fact_news_event(event_id,event_date,first_published_at,last_published_at,
        representative_item_id,title,content,item_count,source_count,excluded)
        VALUES ('NE-1','2026-09-24','2026-09-24 09:00:00','2026-09-25 09:00:00',
        'I-2','后续信息','第二天才有的正文',2,2,FALSE)""")
    conn.execute("""INSERT INTO rel_news_event_member(item_id,event_id,published_at,event_date,src,title,content,excluded,representative_score)
        VALUES ('I-0','NE-1','2026-09-23 09:00:00','2026-09-23','sina','前日旧闻','前日正文',FALSE,200),
               ('I-1','NE-1','2026-09-24 09:00:00','2026-09-24','cls','首日事实','首日正文',FALSE,5),
               ('I-2','NE-1','2026-09-25 09:00:00','2026-09-25','sina','后续信息','第二天才有的正文',FALSE,100)""")
    frame = load_day_snapshot(conn, "2026-09-24")
    conn.close()
    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["title"] == "首日事实"
    assert row["item_count"] == 1
    assert row["source_count"] == 1
    assert row["sources_json"] == '["cls"]'
    assert str(row["last_published_at"]).startswith("2026-09-24")

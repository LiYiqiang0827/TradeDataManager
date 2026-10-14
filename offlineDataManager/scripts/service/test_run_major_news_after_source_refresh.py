from __future__ import annotations

import json
import sqlite3

import pytest

from service.run_major_news_after_source_refresh import audit_refresh


def test_audit_refresh_requires_every_day_and_matching_raw_counts(tmp_path):
    database = tmp_path / "news.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE tbl_major_news(datetime TEXT)")
        conn.executemany("INSERT INTO tbl_major_news VALUES(?)", [
            ("2026-06-01 09:00:00",), ("2026-06-01 15:00:00",),
            ("2026-06-02 09:00:00",),
        ])
    status = tmp_path / "status.json"
    payload = {"status": "complete", "failed_dates": [], "remaining_missing": [],
               "days": {
                   "2026-06-01": {"status": "complete", "fetched": 2, "inserted": 1, "source_count": 2},
                   "2026-06-02": {"status": "complete", "fetched": 1, "inserted": 1, "source_count": 1},
               }}
    status.write_text(json.dumps(payload))
    assert audit_refresh(status, database, "20260601", "20260602") == {
        "days": 2, "raw_articles": 3, "new_articles": 2}
    payload["days"]["2026-06-02"]["source_count"] = 2
    status.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="不一致"):
        audit_refresh(status, database, "20260601", "20260602")

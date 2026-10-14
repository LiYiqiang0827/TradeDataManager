from __future__ import annotations

from datetime import date
import sqlite3

import pandas as pd
import pytest

from service.backfill_major_news_gaps import fetch_local_day, main, missing_days, prepare_major_news_frame


def test_missing_days_and_major_news_frame_preserve_daily_source_contract(tmp_path):
    conn = sqlite3.connect(tmp_path / "news.db")
    conn.execute("CREATE TABLE tbl_major_news(datetime TEXT,src TEXT,title TEXT,content TEXT,md5 TEXT)")
    conn.execute("INSERT INTO tbl_major_news(datetime,src,title) VALUES('2026-06-01 12:00:00','cls','事实')")
    assert missing_days(conn, date(2026, 6, 1), date(2026, 6, 3)) == [date(2026, 6, 2), date(2026, 6, 3)]
    frame = pd.DataFrame([{"pub_time": "2026-06-02 09:00:00", "src": "cls", "title": "铜矿减产", "content": "减产10万吨"}])
    result = prepare_major_news_frame(frame, date(2026, 6, 2))
    assert result.iloc[0]["datetime"] == "2026-06-02 09:00:00"
    assert len(result.iloc[0]["md5"]) == 32
    with pytest.raises(ValueError, match="越日"):
        prepare_major_news_frame(frame, date(2026, 6, 3))
    with pytest.raises(ValueError, match="空数据"):
        prepare_major_news_frame(frame.iloc[0:0], date(2026, 6, 2))
    conn.close()


def test_fetch_day_uses_offset_until_short_page(monkeypatch):
    monkeypatch.setattr("service.backfill_major_news_gaps.time.sleep", lambda _: None)

    class Client:
        def __init__(self):
            self.offsets = []

        def major_news(self, **kwargs):
            self.offsets.append(kwargs["offset"])
            count = 500 if kwargs["offset"] == 0 else 100
            return pd.DataFrame({"pub_time": ["2026-06-01 09:00:00"] * count,
                                 "src": ["cls"] * count, "title": ["事实"] * count})

    client = Client()
    frame = fetch_local_day(date(2026, 6, 1), client)
    assert len(frame) == 600
    assert client.offsets == [0, 500]


def test_refresh_existing_rechecks_present_days_and_resumes(tmp_path, monkeypatch):
    database = tmp_path / "news.db"
    with sqlite3.connect(database) as conn:
        conn.execute("""CREATE TABLE tbl_major_news (
            datetime TEXT NOT NULL, src TEXT NOT NULL, title TEXT,
            content TEXT, md5 TEXT, snap_ts TEXT,
            PRIMARY KEY(datetime, src, md5))""")
        conn.execute("""INSERT INTO tbl_major_news(datetime,src,title,md5)
                        VALUES('2026-06-01 09:00:00','cls','旧报道','old')""")
    calls = []

    def fetch(day, host, **kwargs):
        calls.append(str(day))
        return pd.DataFrame([{"pub_time": f"{day} 15:00:00", "src": "cls",
                              "title": f"{day} 新报道", "content": "事实"}])

    monkeypatch.setattr("service.backfill_major_news_gaps.fetch_remote_day", fetch)
    arguments = ["--start-date", "20260601", "--end-date", "20260602",
                 "--database", str(database), "--status-file", str(tmp_path / "status.json"),
                 "--ssh-host", "macmini", "--refresh-existing"]
    assert main(arguments) == 0
    assert calls == ["2026-06-01", "2026-06-02"]
    assert main(arguments) == 0
    assert calls == ["2026-06-01", "2026-06-02"]
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT count(*) FROM tbl_major_news").fetchone()[0] == 3

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pytest

from core.news_event_store import (
    NewsEventStore,
    StockNameMatcher,
    classify_news,
    repair_cctv_news,
    stable_cctv_md5,
)


def _news_source(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tbl_major_news(
          datetime TEXT NOT NULL,src TEXT NOT NULL,title TEXT,content TEXT,channels TEXT,
          score REAL,md5 TEXT,snap_ts TEXT,PRIMARY KEY(datetime,src,md5)
        );
        CREATE TABLE tbl_cctv_news(
          datetime TEXT NOT NULL,title TEXT,content TEXT,src TEXT,md5 TEXT,snap_ts TEXT,
          PRIMARY KEY(datetime,md5)
        );
        """
    )
    rows = [
        ("2026-09-01 09:00:00", "sina", "", "【智利铜矿宣布减产】该矿全年产量下调10万吨。", "a"),
        ("2026-09-01 09:01:00", "sina", "", "【智利铜矿宣布减产】该矿全年产量下调10万吨。", "b"),
        ("2026-09-01 09:02:00", "cls", "智利铜矿宣布减产", "智利大型铜矿宣布减产，全年产量下调10万吨。", "c"),
        ("2026-09-01 09:03:00", "eastmoney", "智利大型铜矿下调全年产量", "智利大型铜矿宣布减产，全年产量下调10万吨。", "d"),
        ("2026-09-01 10:00:00", "sina", "沪指快速拉升翻红", "沪指快速拉升翻红。", "e"),
        ("2026-09-01 10:01:00", "sina", "测试股份快速下跌", "测试股份盘中快速下跌5%。", "f"),
        ("2026-09-01 10:02:00", "cls", "测试股份中标10亿元项目 股价涨停", "公司公告中标10亿元项目。", "g"),
        ("2026-09-01 10:03:00", "cls", "LME铜价涨超2%", "LME铜价涨超2%，报每吨12000美元。", "h"),
        ("2026-09-01 10:04:00", "cls", "LME铜价涨超3%", "LME铜价涨超3%，报每吨12100美元。", "i"),
        ("2026-09-01 10:05:00", "yicai", "美股三大指数收涨", "纳指涨1.2%，标普涨0.8%。", "j"),
    ]
    conn.executemany(
        "INSERT INTO tbl_major_news(datetime,src,title,content,md5,snap_ts) VALUES(?,?,?,?,?, 's')", rows
    )
    conn.commit()
    conn.close()


def _basic(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE tbl_cn_basic(ts_code TEXT,name TEXT)")
    conn.execute("INSERT INTO tbl_cn_basic VALUES('000001.SZ','测试股份')")
    conn.commit()
    conn.close()


def test_market_result_filter_preserves_independent_and_global_facts():
    matcher = StockNameMatcher(["测试股份"])
    assert classify_news("沪指快速拉升翻红", "", matcher)["excluded"] is True
    assert classify_news("测试股份快速下跌5%", "", matcher)["excluded"] is True
    assert classify_news("测试股份中标项目 股价涨停", "公司公告中标10亿元项目", matcher)["excluded"] is False
    assert classify_news("固态电池板块拉升", "消息面上，国际标准正式立项", matcher)["excluded"] is True
    assert classify_news("沪深两市成交额突破1万亿元", "较昨日放量200亿元", matcher)["excluded"] is True
    assert classify_news("农业板块拉升", "农产品价格上涨13%", matcher)["excluded"] is True
    assert classify_news("农产品价格上涨13% 农业板块拉升", "", matcher)["excluded"] is False
    assert classify_news("国债期货午盘全线上涨", "", matcher)["excluded"] is False
    assert classify_news("美股收评:三大指数涨跌不一", "", matcher)["excluded"] is False
    assert classify_news("暑运刚过机票价格大跳水", "", matcher)["excluded"] is False
    assert classify_news("韩国银行业爆发抗议活动", "", matcher)["excluded"] is False
    assert classify_news("LME铜价涨超2%", "报每吨12000美元", matcher)["filter_class"] == "protected_market_fact"
    assert classify_news("美股三大指数收涨", "纳指涨1.2%", matcher)["excluded"] is False


def test_build_deduplicates_and_is_incremental(tmp_path: Path):
    source, basic, database = tmp_path / "news.db", tmp_path / "basic.db", tmp_path / "events.duckdb"
    _news_source(source)
    _basic(basic)
    store = NewsEventStore(database, source, basic)
    result = store.build("20260901", "20260901", rebuild=True)
    assert result["raw_rows"] == 10
    assert result["same_source_exact"] == 1
    assert result["cross_source_exact"] >= 1
    assert result["excluded_items"] == 2
    events = store.query_events("2026-09-01", "2026-09-01")
    assert not events.empty
    assert not events["title"].fillna("").str.contains("沪指快速拉升").any()
    assert events["title"].fillna("").str.contains("LME铜价涨超2%").any()
    assert events["title"].fillna("").str.contains("LME铜价涨超3%").any()
    assert events["title"].fillna("").str.contains("中标10亿元").any()
    # 数值更新不能被近重复压成同一条。
    assert len(events[events["title"].fillna("").str.contains("LME铜价")]) == 2
    second = store.build("20260901", "20260901")
    assert second["processed_items"] == 0
    assert second["skipped_existing"] == 10
    assert result["source_kind"] == "major_news"
    conn = duckdb.connect(str(database), read_only=True)
    state = dict(conn.execute("SELECT key,value FROM pipeline_state").fetchall())
    conn.close()
    assert state["source_kind"] == "major_news"
    assert state["major_news_max_datetime"] == "2026-09-01 10:05:00"


def test_historical_backfill_does_not_regress_incremental_checkpoint(tmp_path: Path):
    source, basic, database = tmp_path / "news.db", tmp_path / "basic.db", tmp_path / "events.duckdb"
    _news_source(source)
    _basic(basic)
    store = NewsEventStore(database, source, basic)
    store.build("20260901", "20260901")
    store.build("20260831", "20260831")
    conn = duckdb.connect(str(database), read_only=True)
    checkpoint = conn.execute(
        "SELECT value FROM pipeline_state WHERE key='major_news_max_datetime'"
    ).fetchone()[0]
    conn.close()
    assert checkpoint == "2026-09-01 10:05:00"
    assert store.build()["start_date"] == "2026-08-31"


def test_source_kind_guard_prevents_mixed_derivative_database(tmp_path: Path):
    source, basic, database = tmp_path / "news.db", tmp_path / "basic.db", tmp_path / "events.duckdb"
    _news_source(source)
    conn = sqlite3.connect(source)
    conn.execute("CREATE TABLE tbl_news AS SELECT * FROM tbl_major_news")
    conn.close()
    _basic(basic)
    NewsEventStore(database, source, basic).build("20260901", "20260901", rebuild=True)
    with pytest.raises(RuntimeError, match="数据源"):
        NewsEventStore(database, source, basic, source_table="tbl_news").build("20260901", "20260901")


def test_repair_cctv_news_fills_md5_and_removes_duplicates(tmp_path: Path):
    source = tmp_path / "news.db"
    _news_source(source)
    conn = sqlite3.connect(source)
    conn.executemany(
        "INSERT INTO tbl_cctv_news VALUES(?,?,?,?,?,?)",
        [
            ("20260901", "政策发布", "正文", None, None, "s1"),
            ("20260901", "政策发布", "正文", None, None, "s2"),
            ("20260901", "另一条", "另一正文", "cctv", None, "s3"),
        ],
    )
    conn.commit(); conn.close()
    result = repair_cctv_news(source)
    assert result == {"before": 3, "after": 2, "removed": 1}
    conn = sqlite3.connect(source)
    rows = conn.execute("SELECT datetime,title,content,src,md5 FROM tbl_cctv_news ORDER BY title").fetchall()
    conn.close()
    assert all(row[3] == "cctv" for row in rows)
    assert all(row[4] == stable_cctv_md5(row[0], row[1], row[2]) for row in rows)
    assert len({row[4] for row in rows}) == 2

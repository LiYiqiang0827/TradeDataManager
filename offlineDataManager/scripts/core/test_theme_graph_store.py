from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from core.theme_graph_store import ThemeGraphStore, name_theme_id
from core.theme_daily_review import _classify_structure, _leader_rows
from core.theme_vault import ThemeVaultRenderer


def _source(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tbl_cn_kpl_list (
          ts_code TEXT,name TEXT,trade_date TEXT,lu_time TEXT,ld_time TEXT,open_time TEXT,last_time TEXT,
          lu_desc TEXT,tag TEXT,theme TEXT,status TEXT,limit_order REAL,lu_limit_order REAL,bid_amount REAL,
          amount REAL,turnover_rate REAL,free_float REAL,snap_ts TEXT
        );
        CREATE TABLE tbl_cn_kpl_concept_cons (
          ts_code TEXT,name TEXT,con_name TEXT,con_code TEXT,trade_date TEXT,desc TEXT,hot_num INTEGER,snap_ts TEXT
        );
        """
    )
    members = [
        ("000001.KP", "测试题材", "甲公司", "000001.SZ", "20260105", "直接受益", 1, "s1"),
        ("000001.KP", "测试题材", "乙公司", "000002.SZ", "20260105", "供应商", 2, "s1"),
        ("000001.KP", "测试题材", "甲公司", "000001.SZ", "20260106", "直接受益", 1, "s2"),
        ("000001.KP", "测试题材", "甲公司", "000001.SZ", "20260107", "直接受益", 1, "s3"),
        ("000001.KP", "测试题材", "乙公司", "000002.SZ", "20260107", "供应商", 2, "s3"),
    ]
    conn.executemany("INSERT INTO tbl_cn_kpl_concept_cons VALUES (?,?,?,?,?,?,?,?)", members)
    events = [
        ("000001.SZ", "甲公司", "20260105", "09:35:00", "", "", "", "测试题材", "涨停", "测试题材、机器人", "首板", 1, 1, 1, 10, 2, 5, "s1"),
        ("000001.SZ", "甲公司", "20260106", "10:30:00", "", "", "", "测试题材", "涨停", "测试题材", "2连板", 1, 1, 1, 10, 2, 5, "s2"),
        ("000003.SZ", "丙公司", "20260106", "14:00:00", "", "", "", "", "炸板", "测试题材", "", 1, 1, 1, 10, 2, 5, "s2"),
        ("000002.SZ", "乙公司", "20260107", "09:40:00", "", "", "", "测试题材", "涨停", "测试题材", "3天2板", 1, 1, 1, 10, 2, 5, "s3"),
    ]
    conn.executemany("INSERT INTO tbl_cn_kpl_list VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", events)
    conn.commit()
    conn.close()


def test_build_is_idempotent_and_temporal(tmp_path: Path):
    source, graph = tmp_path / "kpl.db", tmp_path / "graph.duckdb"
    _source(source)
    store = ThemeGraphStore(graph, source)
    first = store.build("20260105", "20260107")
    assert first["changed_list_dates"] == 3
    theme_id = name_theme_id("测试题材")
    assert store.query_theme_profile(theme_id=theme_id)["canonical_name"] == "测试题材"
    assert store.query_theme_profile(theme_id=theme_id)["last_seen_date"] == "20260107"
    historical_profile = store.query_theme_profile(theme_id=theme_id, as_of="20260106")
    assert max(item["last_active_date"] for item in historical_profile["episodes"]) <= "20260106"
    assert set(store.query_theme_members(theme_id, as_of="20260106")["ts_code"]) == {"000001.SZ"}
    history = store.query_theme_members(theme_id, historical=True)
    assert len(history[history["ts_code"] == "000002.SZ"]) == 1
    conn = duckdb.connect(str(graph), read_only=True)
    assert conn.execute("SELECT COUNT(*) FROM fact_membership_snapshot").fetchone()[0] == 0
    conn.close()
    daily = store.query_theme_daily(theme_id)
    assert daily["trade_date"].tolist() == ["20260105", "20260106", "20260107"]
    assert daily.iloc[1]["break_count"] == 1
    review = store.query_market_theme_review("20260106")
    assert review["point_in_time"] is True
    assert review["trade_date"] == "20260106"
    assert 0 <= review["theme_sentiment_score"] <= 100
    assert review["hot_themes"][0]["theme"] == "测试题材"
    assert review["hot_themes"][0]["leaders"][0]["title"] == "龙一"
    assert review["hot_themes"][0]["leaders"][0]["ts_code"] == "000001.SZ"
    review_series = store.query_market_theme_review_series("20260105", "20260107")
    assert [item["trade_date"] for item in review_series] == ["20260105", "20260106", "20260107"]
    assert review_series[-1]["market_breadth"]["limit_up_count"] == 1
    cycle = store.query_theme_cycle_data(
        name="测试题材", start_date="20260105", end_date="20260107", as_of="20260107"
    )
    assert cycle["mode"] == "as_of"
    assert cycle["daily"]["trade_date"].tolist() == ["20260105", "20260106", "20260107"]
    assert set(cycle["events"]["ts_code"]) == {"000001.SZ", "000002.SZ", "000003.SZ"}
    assert cycle["leaders"].iloc[0]["title"] == "龙一"
    assert cycle["analyses"].empty
    second = store.build("20260105", "20260107")
    assert second["changed_list_dates"] == 0
    conn = duckdb.connect(str(graph), read_only=True)
    assert conn.execute("SELECT COUNT(*) FROM fact_limit_event").fetchone()[0] == 4
    conn.close()


def test_source_revision_replaces_day(tmp_path: Path):
    source, graph = tmp_path / "kpl.db", tmp_path / "graph.duckdb"
    _source(source)
    store = ThemeGraphStore(graph, source)
    store.build("20260105", "20260107")
    with sqlite3.connect(source) as conn:
        conn.execute(
            "INSERT INTO tbl_cn_kpl_list VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("000004.SZ", "丁公司", "20260106", "09:50:00", "", "", "", "测试题材", "涨停",
             "测试题材", "首板", 1, 1, 1, 10, 2, 5, "revision"),
        )
    result = store.build("20260106", "20260106")
    assert result["changed_list_dates"] == 1
    conn = duckdb.connect(str(graph), read_only=True)
    assert conn.execute("SELECT COUNT(*) FROM fact_limit_event WHERE trade_date='20260106'").fetchone()[0] == 3
    conn.close()


def test_vault_preserves_manual_notes(tmp_path: Path):
    source, graph, vault = tmp_path / "kpl.db", tmp_path / "graph.duckdb", tmp_path / "vault"
    _source(source)
    ThemeGraphStore(graph, source).build("20260105", "20260107")
    renderer = ThemeVaultRenderer(graph, vault)
    renderer.render()
    daily_dashboard = vault / "_generated" / "daily" / "20260106__dashboard.md"
    daily_text = daily_dashboard.read_text(encoding="utf-8")
    assert "今日题材情绪" in daily_text
    assert "热门题材排名" in daily_text
    assert "龙一" in daily_text
    note = next((vault / "01_题材").rglob("测试题材*.md"))
    assert not list((vault / "04_股票").glob("*.md"))
    assert not list((vault / "03_炒作周期").glob("*.md"))
    note.write_text(note.read_text(encoding="utf-8") + "\n人工内容保留\n", encoding="utf-8")
    ThemeVaultRenderer(graph, vault).render()
    assert "人工内容保留" in note.read_text(encoding="utf-8")
    theme_id = name_theme_id("测试题材")
    conn = duckdb.connect(str(graph))
    conn.execute(
        """INSERT OR REPLACE INTO dim_theme_taxonomy VALUES
           (?, 'L1-TEST', '测试一级', '测试题材', '[]', 'test', 1.0,
            '测试归类', 'level1-v1', 'confirmed', CURRENT_TIMESTAMP)""",
        [theme_id],
    )
    conn.close()
    ThemeVaultRenderer(graph, vault).render()
    moved = next((vault / "01_题材" / "测试一级").glob("测试题材*.md"))
    assert "人工内容保留" in moved.read_text(encoding="utf-8")
    assert not note.exists()
    taxonomy = ThemeGraphStore(graph, source).query_theme_taxonomy("测试一级")
    assert taxonomy["level2_name"].tolist() == ["测试题材"]
    assert ThemeVaultRenderer(graph, vault).render()["files_written"] == 0


def test_market_theme_review_excludes_st_and_recent_listing_themes(tmp_path: Path):
    source, graph = tmp_path / "kpl.db", tmp_path / "graph.duckdb"
    _source(source)
    with sqlite3.connect(source) as conn:
        conn.executemany(
            "INSERT INTO tbl_cn_kpl_list VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("000004.SZ", "ST样本", "20260107", "09:31:00", "", "", "", "ST板块", "涨停",
                 "ST板块", "4连板", 1, 1, 1, 10, 2, 5, "s3"),
                ("000005.SZ", "次新样本", "20260107", "09:32:00", "", "", "", "次新股", "涨停",
                 "次新股", "5连板", 1, 1, 1, 10, 2, 5, "s3"),
            ],
        )
    store = ThemeGraphStore(graph, source)
    store.build("20260105", "20260107")
    review = store.query_market_theme_review("20260107")
    names = {item["theme"] for item in review["hot_themes"]}
    assert "ST板块" not in names
    assert "次新股" not in names


def test_leader_ranking_never_lets_lower_board_override_higher_board():
    events = pd.DataFrame([
        {
            "trade_date": "20260109", "ts_code": "000001.SZ", "name": "二板样本",
            "board_height": 2, "amount": 10, "limit_order": 1, "lu_limit_order": 1,
            "bid_amount": 1, "free_float": 100, "lu_time": "14:50:00",
        },
        {
            "trade_date": "20260109", "ts_code": "000002.SZ", "name": "首板人气样本",
            "board_height": 1, "amount": 1000, "limit_order": 100, "lu_limit_order": 100,
            "bid_amount": 100, "free_float": 100, "lu_time": "09:30:00",
        },
    ])
    leaders = _leader_rows(
        events,
        {"000001.SZ": [4], "000002.SZ": [0, 1, 2, 3, 4]},
        {"20260105": 0, "20260106": 1, "20260107": 2, "20260108": 3, "20260109": 4},
        2,
    )
    assert leaders[0]["ts_code"] == "000001.SZ"
    assert leaders[0]["board_height"] == 2


def test_same_level1_rotation_is_not_called_mainline_replacement():
    current = pd.DataFrame([
        {"theme_id": "smart-grid", "canonical_name": "智能电网", "level1_name": "电力电网",
         "heat_score": 85.75, "limit_up_count": 28, "break_count": 0,
         "max_board_height": 3, "seal_rate": 1.0, "persistence_5": 1.0},
        {"theme_id": "green-power", "canonical_name": "绿色电力", "level1_name": "电力电网",
         "heat_score": 78.15, "limit_up_count": 8, "break_count": 3,
         "max_board_height": 6, "seal_rate": 8 / 11, "persistence_5": 0.4},
        {"theme_id": "chemical", "canonical_name": "化工", "level1_name": "化工与新材料",
         "heat_score": 65.24, "limit_up_count": 9, "break_count": 2,
         "max_board_height": 4, "seal_rate": 9 / 11, "persistence_5": 0.8},
    ])
    previous = pd.DataFrame([
        {"theme_id": "power", "canonical_name": "电力", "level1_name": "电力电网",
         "heat_score": 90.0, "limit_up_count": 20, "break_count": 1,
         "max_board_height": 6, "seal_rate": 20 / 21, "persistence_5": 1.0},
    ])
    result = _classify_structure(current, previous, 72.5)
    assert result["code"] == "clear_single_mainline"
    assert result["evidence"]["same_level1_as_previous_top"] is True


def test_multi_mainline_is_aggregated_by_level1_theme():
    current = pd.DataFrame([
        {"theme_id": "chip", "canonical_name": "芯片", "level1_name": "半导体",
         "heat_score": 87.12, "limit_up_count": 14, "break_count": 1,
         "max_board_height": 4, "seal_rate": 14 / 15, "persistence_5": 1.0},
        {"theme_id": "component", "canonical_name": "元器件", "level1_name": "半导体",
         "heat_score": 82.79, "limit_up_count": 14, "break_count": 0,
         "max_board_height": 3, "seal_rate": 1.0, "persistence_5": 0.8},
        {"theme_id": "communication", "canonical_name": "通信", "level1_name": "算力与通信",
         "heat_score": 79.5, "limit_up_count": 28, "break_count": 4,
         "max_board_height": 2, "seal_rate": 0.875, "persistence_5": 1.0},
        {"theme_id": "compute", "canonical_name": "算力", "level1_name": "算力与通信",
         "heat_score": 56.33, "limit_up_count": 6, "break_count": 2,
         "max_board_height": 4, "seal_rate": 0.75, "persistence_5": 1.0},
    ])
    previous = current.copy()
    result = _classify_structure(current, previous, 87.8)
    assert result["code"] == "multiple_mainlines"
    assert result["evidence"]["broad_level1_themes"][:2] == ["半导体", "算力与通信"]


def test_episode_attribution_is_queryable_and_rendered_in_cycle_table(tmp_path: Path):
    source, graph, vault = tmp_path / "kpl.db", tmp_path / "graph.duckdb", tmp_path / "vault"
    _source(source)
    store = ThemeGraphStore(graph, source)
    store.build("20260105", "20260107")
    theme_id = name_theme_id("测试题材")
    conn = duckdb.connect(str(graph))
    episode_id = conn.execute(
        "SELECT episode_id FROM fact_theme_episode WHERE theme_id=?", [theme_id]
    ).fetchone()[0]
    evidence = {
        "selected_evidence": [{
            "evidence_id": "N1", "published_at": "2026-01-05 09:20:00",
            "source": "test", "title": "测试政策发布",
            "screening": {"relevance": "direct", "candidate_causal_status": "plausible_trigger"},
        }],
        "market_evidence": [{
            "evidence_id": "M1", "trade_date": "20260105", "lifecycle_state": "启动",
            "limit_up_count": 1, "max_board_height": 1, "heat_score": 20.0,
        }],
        "stock_evidence": [],
    }
    audit = {
        "episode_id": episode_id, "verdict": "revise", "citation_errors": [],
        "temporal_errors": [], "causal_overstatements": [], "classification_errors": [],
        "revised_conclusion": {
            "reason_status": "single_dominant", "summary": "测试政策是本轮启动的主要可能催化。",
            "causal_status": "plausible_trigger", "evidence_ids": ["N1"],
            "market_evidence_ids": ["M1"], "counter_evidence_ids": [],
        },
        "unresolved": ["仍需核验原始公告"], "confidence": 0.7, "needs_review": True,
    }
    summary = {
        "episode_id": episode_id, "phase_summary": "1月5日启动，随后延续。",
        "key_dates": [{"date": "20260105", "role": "start", "evidence_ids": ["M1"]}],
        "market_cores": [{"ts_code": "000001.SZ", "name": "甲公司",
                          "role": "first_mover", "evidence_ids": ["S1"]}],
        "dominant_reason": {"summary": "测试政策驱动。", "causal_status": "plausible_trigger",
                            "evidence_ids": ["N1"]},
        "catalysts": [{"date": "20260105", "role": "initial_trigger",
                       "summary": "测试政策发布。", "evidence_ids": ["N1"]}],
        "reason_market_alignment": "政策发布时间早于涨停。", "unresolved": [],
        "confidence": 0.7, "needs_review": True,
    }
    rows = [
        ("A-AUDIT", "catalyst_audit", "qwen-vllm", "qwen-test", audit),
        ("A-SUMMARY", "episode_summary", "minimax-hermes", "minimax-test", summary),
    ]
    for analysis_id, analysis_type, provider, model, output in rows:
        conn.execute(
            """INSERT INTO llm_analysis VALUES
               (?, 'theme_episode', ?, ?, '20260107', ?, ?, 'v2', ?, ?, ?, 0.7,
                'needs_review', CURRENT_TIMESTAMP)""",
            [analysis_id, episode_id, analysis_type, provider, model, analysis_id,
             json.dumps(output, ensure_ascii=False), json.dumps(evidence, ensure_ascii=False)],
        )
    conn.close()

    queried = store.query_theme_analyses(theme_id=theme_id)
    assert set(queried["analysis_type"]) == {"catalyst_audit", "episode_summary"}
    assert queried.loc[queried["analysis_type"] == "catalyst_audit", "analysis"].iloc[0][
        "revised_conclusion"
    ]["summary"] == "测试政策是本轮启动的主要可能催化。"

    ThemeVaultRenderer(graph, vault).render()
    dashboard = next((vault / "_generated" / "themes").glob("测试题材*.md"))
    text = dashboard.read_text(encoding="utf-8")
    assert "大模型归因" in text
    assert "测试政策是本轮启动的主要可能催化" in text
    assert "待人工复核；审计=revise；置信=0.70" in text
    assert "测试政策发布" in text
    assert "仍需核验原始公告" in text

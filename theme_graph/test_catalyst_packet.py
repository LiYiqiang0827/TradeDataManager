from __future__ import annotations

from pathlib import Path
import sys


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from build_catalyst_packet import _market_recap_features, _rank_candidates
from analyze_episode_catalysts import _selected_evidence
from reanalyze_with_external_evidence import _load_external


def test_market_recap_is_kept_but_ranked_below_independent_event():
    rows = [
        {
            "datetime": "2026-01-05 09:10:00", "src": "official",
            "title": "测试题材专项政策正式印发", "content": "主管部门印发专项政策。",
            "channels": "", "md5": "event", "snap_ts": "2026-01-05 09:11:00",
            "source_table": "tbl_news",
        },
        {
            "datetime": "2026-01-05 09:20:00", "src": "market-wire",
            "title": "测试题材异动拉升 甲公司触及涨停", "content": "测试题材走强，多股涨超5%。",
            "channels": "", "md5": "recap", "snap_ts": "2026-01-05 09:21:00",
            "source_table": "tbl_news",
        },
    ]
    candidates, _ = _rank_candidates(
        rows, ["测试题材"], "测试题材", "20260105", ["20260105"], 10,
    )
    assert candidates[0]["source_record_id"] == "event"
    recap = next(item for item in candidates if item["source_record_id"] == "recap")
    assert recap["evidence_kind_hint"] == "market_recap"
    assert recap["market_recap_penalty"] == 13.0


def test_recap_with_embedded_event_is_still_recap_but_penalty_is_smaller():
    is_recap, has_fact, penalty = _market_recap_features(
        "商业航天板块拉升", "消息面上，运载火箭于12时发射成功。",
    )
    assert is_recap is True
    assert has_fact is True
    assert penalty == 7.0


def test_selected_evidence_reserves_only_recaps_with_independent_facts():
    full = [
        {"evidence_id": "N1", "candidate_score": 20, "has_independent_fact_marker": True},
        {"evidence_id": "N2", "candidate_score": 30, "has_independent_fact_marker": True},
        {"evidence_id": "N3", "candidate_score": 40, "has_independent_fact_marker": False},
    ]
    screening = {"items": [
        {"evidence_id": "N1", "relevance": "direct", "evidence_nature": "trigger_fact",
         "candidate_causal_status": "plausible_trigger"},
        {"evidence_id": "N2", "relevance": "direct", "evidence_nature": "market_recap",
         "candidate_causal_status": "plausible_trigger"},
        {"evidence_id": "N3", "relevance": "direct", "evidence_nature": "market_recap",
         "candidate_causal_status": "background_only"},
    ]}
    selected = _selected_evidence(screening, full, limit=3)
    assert [item["evidence_id"] for item in selected] == ["N1", "N2"]


def test_external_evidence_keeps_url_and_marks_official_source(tmp_path: Path):
    path = tmp_path / "external.json"
    path.write_text(
        '{"evidence":[{"url":"https://example.com/source","title":"官方事件",'
        '"source":"官方来源","published_at":"2026-01-05 time_unknown",'
        '"observed_at":"2026-01-06T00:00:00+08:00","matched_focus_date":"20260105",'
        '"content":"发布独立事件。"}]}',
        encoding="utf-8",
    )
    rows = _load_external(path)
    assert rows[0]["evidence_id"] == "NW1"
    assert rows[0]["url"] == "https://example.com/source"
    assert rows[0]["source_table"] == "external_web"
    assert rows[0]["availability_basis"] == "official_page_date_time_unknown"

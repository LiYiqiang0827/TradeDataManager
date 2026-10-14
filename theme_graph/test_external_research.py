from __future__ import annotations

from pathlib import Path
import sys


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from ima_evidence_source import extract_title_date, normalize_ima_candidates, prompt_section
from research_external_evidence_batch import _extract_json, _normalize_evidence


def _request() -> dict:
    return {
        "episode_id": "EP-TEST",
        "theme": {"canonical_name": "商业航天"},
        "search_window": {"start": "2026-01-01", "end": "2026-01-31"},
        "focus_dates": ["20260105", "20260112", "20260129"],
    }


def _evidence(**overrides) -> dict:
    value = {
        "url": "https://example.com/source",
        "title": "主管部门发布产业政策",
        "source": "主管部门",
        "published_at": "2026-01-10 09:00:00",
        "observed_at": "2026-09-28T00:00:00+08:00",
        "matched_focus_date": "20260112",
        "content": "主管部门正式发布产业政策，明确建设目标、实施范围和后续工作安排，可作为题材强化事件的原始事实依据。",
        "availability_basis": "official_page_timestamp",
        "matched_terms": ["商业航天", "政策"],
        "evidence_kind_hint": "policy",
    }
    value.update(overrides)
    return value


def test_extract_title_date_supports_common_report_formats():
    assert extract_title_date("20260105-商业航天日报") == "20260105"
    assert extract_title_date("2026-01-05 商业航天政策跟踪") == "20260105"
    assert extract_title_date("【260105】商业航天产业研究") == "20260105"


def test_ima_candidates_are_date_filtered_and_result_recaps_are_penalized():
    payload = {"data": {"list": [
        {"media_id": "report-1", "title": "20260104 商业航天产业政策与供给研究",
         "highlight_content": "政策线索", "media_type": "pdf"},
        {"media_id": "report-2", "title": "20260105 商业航天涨停复盘",
         "highlight_content": "盘面结果", "media_type": "pdf"},
        {"media_id": "report-3", "title": "20251201 商业航天白皮书",
         "highlight_content": "窗口外", "media_type": "pdf"},
    ]}}
    rows = normalize_ima_candidates(payload, _request())
    assert [row["media_id"] for row in rows] == ["report-1", "report-2"]
    assert rows[0]["candidate_score"] > rows[1]["candidate_score"]
    assert all(row["source_role"] == "discovery_lead" for row in rows)
    assert all(row["evidence_usable"] is False for row in rows)


def test_ima_prompt_explicitly_forbids_using_titles_as_evidence():
    section = prompt_section({
        "status": "ok",
        "candidates": [{"title": "20260104 商业航天产业政策", "title_date": "20260104",
                        "candidate_score": 90}],
    })
    assert "只作事件发现线索" in section
    assert "标题和摘要不是因果证据" in section
    assert "原文URL" in section


def test_normalizer_remaps_to_first_focus_date_after_publication():
    result = _normalize_evidence({
        "research_note": "找到政策原文",
        "evidence": [_evidence(published_at="2026-01-29", matched_focus_date="20260128")],
    }, _request())
    assert result["evidence"][0]["matched_focus_date"] == "20260129"
    assert result["evidence"][0]["original_matched_focus_date"] == "20260128"
    assert len(result["normalization_warnings"]) == 1


def test_normalizer_drops_bad_row_without_discarding_valid_rows():
    result = _normalize_evidence({
        "evidence": [
            _evidence(url="https://example.com/good"),
            _evidence(url="https://example.com/late", published_at="2026-01-31",
                      matched_focus_date="20260112"),
        ],
    }, _request())
    assert [row["url"] for row in result["evidence"]] == ["https://example.com/good"]
    assert result["evidence"][0]["evidence_id"] == "NW1"
    assert result["rejected_evidence"][0]["reason"].startswith("发布日 20260131 之后")


def test_extract_json_finds_valid_payload_after_model_prose():
    value = _extract_json('说明文字\n```json\n{"episode_id":"EP-TEST","evidence":[]}\n```')
    assert value["episode_id"] == "EP-TEST"

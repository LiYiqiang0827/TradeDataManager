from news_event_cascade_pilot import _validate_stage2


def test_validate_stage2_normalizes_market_impact():
    value = {"items": [{
        "id": "E1", "title": "政策发布", "summary": "政策正式发布",
        "key_facts": ["正式发布"], "themes": ["产业政策"], "entities": ["主管部门"],
        "horizon": "中期", "novelty": "新增", "market_impact": "利好",
        "impact_reason": "改善相关行业政策预期",
    }]}
    result = _validate_stage2(value, {"E1"})
    assert result[0]["market_impact"] == "bullish"
    assert result[0]["impact_reason"] == "改善相关行业政策预期"


def test_validate_stage2_defaults_unknown_impact_to_neutral():
    value = {"items": [{
        "id": "E1", "title": "数据公布", "summary": "数据公布",
        "key_facts": [], "themes": [], "entities": [],
        "horizon": "short", "novelty": "new", "market_impact": "方向复杂",
        "impact_reason": "不同板块影响方向不一",
    }]}
    result = _validate_stage2(value, {"E1"})
    assert result[0]["market_impact"] == "neutral"

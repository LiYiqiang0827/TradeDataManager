import json

import pytest

import news_event_fast_pilot as fast
from news_event_cascade_pilot import _validate_stage2


@pytest.mark.parametrize("validator,output_key", [
    (fast.validate_impacts, "ratings"), (_validate_stage2, "items"),
])
def test_invoke_complete_batch_splits_when_model_omits_an_id(monkeypatch, validator, output_key):
    calls = []

    def invoke(prompt, **kwargs):
        payload = json.loads(prompt.rsplit("\n", 1)[-1])
        calls.append(len(payload))
        present = payload[:-1] if len(payload) == 4 else payload
        if output_key == "ratings":
            value = {"ratings": [[item["id"], "neutral", "影响方向不明确"] for item in present]}
        else:
            value = {"items": [{"id": item["id"], "key_facts": [], "themes": [],
                                "entities": [], "market_impact": "neutral"}
                               for item in present]}
        return value, "qwen"

    monkeypatch.setattr(fast, "_invoke_json", invoke)
    payload = [{"id": f"E{i}"} for i in range(4)]
    result, model = fast.invoke_complete_batch(
        "输入事件：\n", payload, validator, timeout=10, max_tokens=500,
    )
    assert calls == [4, 2, 2]
    assert {item["semantic_event_id"] for item in result} == {item["id"] for item in payload}
    assert model == "qwen"


def test_invoke_complete_batch_rejects_uncovered_single_id(monkeypatch):
    monkeypatch.setattr(fast, "_invoke_json", lambda *args, **kwargs: ({"ratings": []}, "qwen"))
    with pytest.raises(ValueError, match="未完整覆盖"):
        fast.invoke_complete_batch("输入事件：\n", [{"id": "E1"}], fast.validate_impacts,
                                   timeout=10, max_tokens=500)

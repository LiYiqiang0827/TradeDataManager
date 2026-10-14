from __future__ import annotations

from threading import Event

from service.pipeline_major_news_backfill import overlap_days


def test_next_day_prepare_overlaps_current_day_enrich():
    enriching = Event()
    prepared_next = Event()
    events = []

    def prepare(day):
        events.append(("prepare", day))
        if day == "d2":
            assert enriching.wait(2)
            prepared_next.set()

    def enrich(day):
        events.append(("enrich", day))
        if day == "d1":
            enriching.set()
            assert prepared_next.wait(2)

    overlap_days(["d1", "d2"], prepare, enrich,
                 lambda day: events.append(("publish", day)),
                 lambda day, error: events.append(("result", day, error)))
    assert events.index(("prepare", "d2")) < events.index(("publish", "d1"))
    assert events.index(("publish", "d1")) < events.index(("enrich", "d2"))
    assert [(item[0], item[1]) for item in events if item[0] == "publish"] == [
        ("publish", "d1"), ("publish", "d2")]


def test_failed_enrich_does_not_publish_but_next_day_continues():
    events = []

    def enrich(day):
        if day == "d1":
            raise ValueError("模型漏项")

    overlap_days(["d1", "d2"], lambda day: events.append(("prepare", day)),
                 enrich, lambda day: events.append(("publish", day)),
                 lambda day, error: events.append(("result", day, error)))
    assert ("publish", "d1") not in events
    assert ("publish", "d2") in events
    assert isinstance(events[2][2], ValueError)

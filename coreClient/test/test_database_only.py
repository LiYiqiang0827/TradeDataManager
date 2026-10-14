"""Strict offline routing must not silently acquire later data."""
import pandas as pd
import pytest
from coreClient.data_provider import _route


@pytest.mark.parametrize("frame", [None, pd.DataFrame(), pd.DataFrame({"x": [1]})])
def test_strict_offline_never_calls_network(frame):
    def forbidden():
        raise AssertionError("Network fallback was called")
    result = _route("database_only", lambda: frame, forbidden)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == (0 if frame is None else len(frame))


def test_existing_database_fallback_preserved():
    result = _route("database", lambda: pd.DataFrame(), lambda: pd.DataFrame({"x": [3]}))
    assert result.x.tolist() == [3]


def test_local_error_not_hidden_as_missing():
    def broken():
        raise RuntimeError("schema mismatch")
    with pytest.raises(RuntimeError, match="schema mismatch"):
        _route("database_only", broken, lambda: pd.DataFrame())


def test_news_exact_cutoff_and_limit_forwarded(monkeypatch):
    from coreClient import data_provider
    from offlineDataManager.scripts.core import offline_db_client
    seen = {}
    def fake(**kwargs):
        seen.update(kwargs)
        return pd.DataFrame()
    monkeypatch.setattr(offline_db_client, "get_news", fake)
    data_provider.get_news(source="database_only", start_datetime="2026-01-05 00:00:00",
                           end_datetime="2026-01-05 10:30:00", limit=None)
    assert seen["end_datetime"] == "2026-01-05 10:30:00"
    assert seen["limit"] is None


def test_news_extended_options_cannot_silently_fallback():
    from coreClient import data_provider
    with pytest.raises(ValueError, match="仅支持本地"):
        data_provider.get_news(source="online", end_datetime="2026-01-05 10:30:00")

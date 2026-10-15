import json
import subprocess

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from coreClient import remote_data_provider as remote


def _arrow_response(frame, *, provider="get_day", source="database_only"):
    table = pa.Table.from_pandas(frame, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[b"trading-data-manager"] = json.dumps({
        "protocol": 1, "provider": provider, "source": source,
        "rows": len(frame), "empty": frame.empty,
    }).encode()
    table = table.replace_schema_metadata(metadata)
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def test_allowlist_rejects_unknown_source_and_path_params():
    with pytest.raises(ValueError, match="not remotely allowed"):
        remote._validate_query("get_arbitrary", {})
    with pytest.raises(ValueError, match="cannot be overridden"):
        remote._validate_query("get_day", {"source": "online"})
    with pytest.raises(ValueError, match="unsupported provider parameters"):
        remote._validate_query("get_oneMin", {"data_root": "/tmp/other"})
    with pytest.raises(ValueError, match="flat lists"):
        remote._validate_query("get_day", {"ts_codes": [["600000.SH"]]})


def test_remote_query_forces_database_only_and_returns_arrow(monkeypatch, capsys):
    from coreClient import data_provider

    seen = {}
    def fake_get_day(ts_code=None, source="database"):
        seen.update({"ts_code": ts_code, "source": source})
        print("provider diagnostic must not enter IPC")
        return pd.DataFrame({"trade_date": pd.to_datetime(["2026-10-14"]),
                             "close": pd.Series([1.25], dtype="Float64"),
                             "volume": pd.Series([None], dtype="Int64")})
    fake_get_day.__module__ = data_provider.__name__
    monkeypatch.setattr(data_provider, "get_day", fake_get_day)
    payload, info = remote._run_remote_query({
        "protocol": 1, "provider": "get_day", "params": {"ts_code": "600000.SH"},
    })
    assert seen == {"ts_code": "600000.SH", "source": "database_only"}
    assert info == {"provider": "get_day", "source": "database_only", "rows": 1}
    assert capsys.readouterr().out == ""
    table = ipc.open_stream(pa.py_buffer(payload)).read_all()
    frame = table.to_pandas()
    assert frame["close"].dtype.name == "Float64"
    assert frame["volume"].isna().all()
    assert frame["trade_date"].dtype.kind == "M"
    meta = json.loads(table.schema.metadata[b"trading-data-manager"])
    assert meta["source"] == "database_only" and meta["rows"] == 1 and not meta["empty"]


def test_database_only_minute_provider_rejects_arbitrary_data_root(monkeypatch):
    with pytest.raises(ValueError, match="unsupported provider parameters"):
        remote._run_remote_query({"protocol": 1, "provider": "get_oneMin",
                                  "params": {"trade_date": "20260922", "data_root": "/tmp"}})


def test_only_dataframe_results_are_supported(monkeypatch):
    from coreClient import data_provider
    def fake_get_day(ts_code=None, source="database"):
        return {"rows": []}
    fake_get_day.__module__ = data_provider.__name__
    monkeypatch.setattr(data_provider, "get_day", fake_get_day)
    with pytest.raises(TypeError, match="DataFrame results only"):
        remote._run_remote_query({"protocol": 1, "provider": "get_day", "params": {}})


def test_client_uses_quoted_fixed_remote_command_and_marks_empty(monkeypatch):
    seen = {}
    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["stdin"] = json.loads(kwargs["input"])
        return subprocess.CompletedProcess(argv, 0, _arrow_response(pd.DataFrame(columns=["trade_date"])), b"")
    monkeypatch.setattr(remote.subprocess, "run", fake_run)
    client = remote.RemoteProvider("nick@mini.local", "/opt/trading py/bin/python",
                                   "/Users/nickzhang/TradingDataManager;safe")
    frame = client.query("get_day", {"trade_date": "20260922"})
    command = seen["argv"][-1]
    assert "'/Users/nickzhang/TradingDataManager;safe'" in command
    assert "'/opt/trading py/bin/python'" in command
    assert seen["stdin"]["provider"] == "get_day"
    assert frame.empty and frame.attrs["remote_empty"] is True
    assert frame.attrs["remote_source"] == "database_only"


def test_empty_result_can_be_required_explicitly(monkeypatch):
    monkeypatch.setattr(remote.subprocess, "run", lambda argv, **kwargs:
                        subprocess.CompletedProcess(argv, 0, _arrow_response(pd.DataFrame()), b""))
    client = remote.RemoteProvider("mini.local", "/usr/bin/python3", "/opt/TradingDataManager")
    with pytest.raises(remote.EmptyRemoteResult):
        client.query("get_day", require_nonempty=True)


def test_ssh_error_has_no_local_fallback(monkeypatch):
    calls = []
    def failed(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 1, b"", b"remote query failed: RuntimeError")
    monkeypatch.setattr(remote.subprocess, "run", failed)
    client = remote.RemoteProvider("mini.local", "/usr/bin/python3", "/opt/TradingDataManager")
    with pytest.raises(remote.RemoteProviderError, match="SSH provider failed"):
        client.query("get_day", {"trade_date": "20260922"})
    assert len(calls) == 1

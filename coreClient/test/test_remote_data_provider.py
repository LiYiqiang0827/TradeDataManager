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


def test_remote_minute_quality_summary_survives_arrow_metadata(monkeypatch):
    from coreClient import data_provider

    def fake_get_one_min(trade_date=None):
        frame = pd.DataFrame({"ts_code": ["920000.BJ"], "vol": [0]})
        frame.attrs["quality_summary"] = {
            "trade_date": "20260922", "soft_quality_warning_rows": 1,
            "soft_quality_warnings": {"amount_nonzero_with_zero_volume": 1},
            "query": "get_oneMin_quality(trade_date=..., ts_code=...)",
        }
        return frame

    fake_get_one_min.__module__ = data_provider.__name__
    monkeypatch.setattr(data_provider, "get_oneMin", fake_get_one_min)
    payload, _ = remote._run_remote_query({
        "protocol": 1, "provider": "get_oneMin", "params": {"trade_date": "20260922"},
    })
    table = ipc.open_stream(pa.py_buffer(payload)).read_all()
    metadata = json.loads(table.schema.metadata[b"trading-data-manager"])
    assert metadata["quality_summary"]["soft_quality_warning_rows"] == 1
    assert metadata["quality_summary"]["soft_quality_warnings"]["amount_nonzero_with_zero_volume"] == 1


def test_remote_roundtrip_preserves_nullable_datetime_index(monkeypatch):
    from coreClient import data_provider

    index = pd.DatetimeIndex(["2026-10-14 09:31:00", None], name="source_time")

    def fake_get_day(ts_code=None, source="database"):
        assert source == "database_only"
        return pd.DataFrame({"nullable": pd.array([7, None], dtype="Int64")}, index=index)

    fake_get_day.__module__ = data_provider.__name__
    monkeypatch.setattr(data_provider, "get_day", fake_get_day)

    def fake_run(argv, **kwargs):
        request = json.loads(kwargs["input"])
        payload, _info = remote._run_remote_query(request)
        return subprocess.CompletedProcess(argv, 0, payload, b"")

    monkeypatch.setattr(remote.subprocess, "run", fake_run)
    frame = remote.RemoteProvider("mini.local", "/usr/bin/python3", "/opt/TradingDataManager").query(
        "get_day", {"ts_code": "600000.SH"}
    )

    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame.index.name == "source_time"
    assert frame.index[0] == pd.Timestamp("2026-10-14 09:31:00")
    assert pd.isna(frame.index[1])
    assert frame["nullable"].dtype.name == "Int64"
    assert frame["nullable"].isna().tolist() == [False, True]


def test_parquet_cli_exports_preserve_datetime_index(monkeypatch, tmp_path):
    from coreClient import cli as provider_cli

    frame = pd.DataFrame(
        {"nullable": pd.array([7, None], dtype="Int64")},
        index=pd.DatetimeIndex(["2026-10-14 09:31:00", None], name="source_time"),
    )
    provider_path = tmp_path / "provider.parquet"
    provider_cli._write_result(frame, provider_path, "parquet")
    provider_roundtrip = pd.read_parquet(provider_path)
    assert provider_roundtrip.index.name == "source_time"
    assert pd.isna(provider_roundtrip.index[1])

    class FakeRemoteProvider:
        def __init__(self, *args, **kwargs):
            pass

        def query(self, provider, params, *, require_nonempty=False):
            return frame

    monkeypatch.setattr(remote, "RemoteProvider", FakeRemoteProvider)
    remote_path = tmp_path / "remote.parquet"
    assert remote._cli([
        "get_day", "--host", "mini.local", "--remote-python", "/usr/bin/python3",
        "--remote-root", "/opt/TradingDataManager", "--output", str(remote_path),
    ]) == 0
    remote_roundtrip = pd.read_parquet(remote_path)
    assert remote_roundtrip.index.name == "source_time"
    assert pd.isna(remote_roundtrip.index[1])


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


@pytest.mark.parametrize(
    "provider,params",
    [
        ("get_week", {
            "ts_codes": ["000001.SZ", "600000.SH"],
            "start_date": "20260101", "end_date": "20260922", "qfq": False,
        }),
        ("get_month", {
            "ts_code": "000001.SZ", "start_date": "20250101",
            "end_date": "20260922", "qfq": True,
        }),
        ("get_index_daily", {"ts_code": "000001.SH", "trade_date": "20260922"}),
        ("get_ticks", {"ts_code": "000001.SZ", "trade_date": "20260922"}),
    ],
)
def test_added_dataframe_providers_force_database_only(monkeypatch, provider, params):
    from coreClient import data_provider

    seen = {}

    def fake_provider(ts_code=None, ts_codes=None, start_date=None, end_date=None,
                      trade_date=None, qfq=True, source="database"):
        seen.update({
            "ts_code": ts_code, "ts_codes": ts_codes, "start_date": start_date,
            "end_date": end_date, "trade_date": trade_date, "qfq": qfq,
            "source": source,
        })
        return pd.DataFrame({"trade_date": ["20260922"], "value": [1]})

    fake_provider.__module__ = data_provider.__name__
    monkeypatch.setattr(data_provider, provider, fake_provider)
    payload, info = remote._run_remote_query({
        "protocol": 1, "provider": provider, "params": params,
    })
    assert seen["source"] == "database_only"
    assert all(seen[key] == value for key, value in params.items())
    assert info == {"provider": provider, "source": "database_only", "rows": 1}
    table = ipc.open_stream(pa.py_buffer(payload)).read_all()
    metadata = json.loads(table.schema.metadata[b"trading-data-manager"])
    assert metadata["provider"] == provider and metadata["source"] == "database_only"
    assert table.num_rows == 1


def test_added_source_providers_reject_source_override():
    for provider in ("get_week", "get_month", "get_index_daily", "get_ticks"):
        with pytest.raises(ValueError, match="cannot be overridden"):
            remote._validate_query(provider, {"source": "online"})


def test_added_provider_allowlists_match_real_database_signatures():
    import inspect

    from coreClient import data_provider

    for provider in ("get_week", "get_month", "get_index_daily", "get_ticks"):
        function = getattr(data_provider, provider)
        parameters = inspect.signature(function).parameters
        assert "source" in parameters
        assert remote.PROVIDER_PARAMS[provider] <= set(parameters)


def test_remote_cli_lists_added_database_dataframe_providers(capsys):
    with pytest.raises(SystemExit) as exc:
        remote._cli(["--help"])
    assert exc.value.code == 0
    output = capsys.readouterr().out
    for provider in ("get_week", "get_month", "get_index_daily", "get_ticks"):
        assert provider in output


def test_legacy_ticks_is_separate_allowlisted_provider_without_path_parameter(monkeypatch):
    from coreClient import data_provider

    def fake_legacy(ts_code=None, ts_codes=None, trade_date=None, start_date=None, end_date=None):
        return pd.DataFrame({"ts_code": [ts_code], "seqId": [17], "datetime": ["09:31"],
                             "buyorsell": [9]})

    fake_legacy.__module__ = data_provider.__name__
    monkeypatch.setattr(data_provider, "get_ticks_legacy", fake_legacy)
    with pytest.raises(ValueError, match="unsupported provider parameters"):
        remote._validate_query("get_ticks_legacy", {"ts_code": "000001.SZ", "path": "/tmp/other.db"})

    payload, info = remote._run_remote_query({
        "protocol": 1, "provider": "get_ticks_legacy",
        "params": {"ts_code": "000001.SZ", "trade_date": "20260922"},
    })
    assert info == {"provider": "get_ticks_legacy", "source": "database_only", "rows": 1}
    table = ipc.open_stream(pa.py_buffer(payload)).read_all()
    frame = table.to_pandas()
    assert frame.loc[0, "seqId"] == 17
    assert frame.loc[0, "buyorsell"] == 9
    metadata = json.loads(table.schema.metadata[b"trading-data-manager"])
    assert metadata["provider"] == "get_ticks_legacy"
    assert metadata["source"] == "database_only"
    assert metadata["dataset_semantics"] == {
        "table_generation": "legacy_policy_ticks_v1",
        "timestamp_precision": "minute",
        "seqId_semantics": "not_validated_as_exchange_sequence",
        "buyorsell_semantics": "raw_stored_codes",
        "historical_integrity_status": "legacy_archive_preserved; semantic_audit_pending",
    }


def test_legacy_ticks_allowlist_matches_real_provider_signature_and_cli():
    import inspect

    from coreClient import data_provider

    function = getattr(data_provider, "get_ticks_legacy")
    parameters = inspect.signature(function).parameters
    assert "source" not in parameters
    assert remote.PROVIDER_PARAMS["get_ticks_legacy"] <= set(parameters)
    with pytest.raises(SystemExit) as exc:
        remote._cli(["--help"])
    assert exc.value.code == 0


def test_legacy_dataset_semantics_survive_the_remote_client_roundtrip(monkeypatch):
    from coreClient import data_provider

    def fake_legacy(ts_code=None, trade_date=None):
        return pd.DataFrame({"ts_code": [ts_code], "trade_date": [trade_date],
                             "seqId": [3], "buyorsell": [9]})

    fake_legacy.__module__ = data_provider.__name__
    monkeypatch.setattr(data_provider, "get_ticks_legacy", fake_legacy)

    def fake_run(argv, **kwargs):
        request = json.loads(kwargs["input"])
        payload, _info = remote._run_remote_query(request)
        return subprocess.CompletedProcess(argv, 0, payload, b"")

    monkeypatch.setattr(remote.subprocess, "run", fake_run)
    result = remote.RemoteProvider("macmini", "/usr/bin/python3", "/opt/TradingDataManager").query(
        "get_ticks_legacy", {"ts_code": "600000.SH", "trade_date": "2026-10-14"},
    )
    assert result["seqId"].tolist() == [3]
    assert result["buyorsell"].tolist() == [9]
    assert result.attrs["remote_provider"] == "get_ticks_legacy"
    assert result.attrs["remote_dataset_semantics"]["timestamp_precision"] == "minute"

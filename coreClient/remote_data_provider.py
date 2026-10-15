"""Read-only SSH bridge to a remote TradingDataManager database provider.

The client accepts only a fixed provider/parameter allowlist and receives Arrow
IPC. The remote dispatcher rechecks the same allowlist and forces
``source='database_only'`` wherever the provider exposes a source parameter.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from dataclasses import dataclass
import importlib
import inspect
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
from typing import Any


SOURCE_PROVIDERS = {
    "get_day": {"ts_code", "ts_codes", "start_date", "end_date", "trade_date", "qfq"},
    "get_minute": {"ts_code", "ts_codes", "trade_date", "start_date", "end_date"},
    "get_week": {"ts_code", "ts_codes", "start_date", "end_date", "qfq"},
    "get_month": {"ts_code", "ts_codes", "start_date", "end_date", "qfq"},
    "get_index_daily": {"ts_code", "trade_date", "start_date", "end_date"},
    "get_ticks": {"ts_code", "ts_codes", "trade_date", "start_date", "end_date"},
}
DATABASE_ONLY_PROVIDERS = {
    "get_oneMin": {"ts_code", "ts_codes", "trade_date", "start_date", "end_date", "columns"},
    "get_oneMin_quality": {"trade_date", "ts_code", "ts_codes"},
    "get_fifteenMin": {"ts_code", "ts_codes", "trade_date", "start_date", "end_date", "columns"},
    # This is deliberately separate from get_ticks (the active v2 store).
    # Its database path is fixed inside the provider and cannot be supplied by
    # the caller; returned legacy semantics are attached to Arrow metadata.
    "get_ticks_legacy": {"ts_code", "ts_codes", "trade_date", "start_date", "end_date"},
}
PROVIDER_PARAMS = {**SOURCE_PROVIDERS, **DATABASE_ONLY_PROVIDERS}
_HOST_RE = re.compile(r"^(?:[A-Za-z0-9._-]+@)?[A-Za-z0-9][A-Za-z0-9.-]*$")
_PROTOCOL_VERSION = 1
_MAX_REQUEST_BYTES = 64 * 1024


class RemoteProviderError(RuntimeError):
    """The remote query failed or returned an invalid result."""


class EmptyRemoteResult(RemoteProviderError):
    """A remote database query succeeded but returned no rows."""


def _json_value(value: Any) -> bool:
    if value is None or isinstance(value, (str, bool, int, float)):
        return True
    if isinstance(value, list):
        return all(item is None or isinstance(item, (str, bool, int, float)) for item in value)
    return False


def _validate_query(provider: str, params: dict[str, Any]) -> None:
    allowed = PROVIDER_PARAMS.get(provider)
    if allowed is None:
        raise ValueError(f"provider is not remotely allowed: {provider}")
    if not isinstance(params, dict) or any(not isinstance(key, str) for key in params):
        raise ValueError("provider parameters must be a JSON object with string keys")
    if "source" in params:
        raise ValueError("source is selected by the remote bridge and cannot be overridden")
    unknown = set(params) - allowed
    if unknown:
        raise ValueError(f"unsupported provider parameters: {', '.join(sorted(unknown))}")
    if not all(_json_value(value) for value in params.values()):
        raise ValueError("provider values must be JSON scalars or flat lists of scalars")
    try:
        encoded = json.dumps(params, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError):
        raise ValueError("provider parameters must be finite JSON values") from None
    if len(encoded) > _MAX_REQUEST_BYTES:
        raise ValueError("provider parameters exceed the request size limit")


def _provider_function(name: str):
    module = importlib.import_module("coreClient.data_provider")
    function = getattr(module, name, None)
    if not inspect.isfunction(function) or function.__module__ != module.__name__:
        raise ValueError("provider is not available")
    return function


def _run_remote_query(request: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
    if set(request) != {"protocol", "provider", "params"} or request.get("protocol") != _PROTOCOL_VERSION:
        raise ValueError("unsupported remote query request")
    provider = request.get("provider")
    params = request.get("params")
    _validate_query(provider, params)
    function = _provider_function(provider)
    signature = inspect.signature(function)
    kwargs = dict(params)
    if provider in SOURCE_PROVIDERS:
        if "source" not in signature.parameters:
            raise ValueError("provider no longer supports an explicit database_only source")
        kwargs["source"] = "database_only"
    elif "source" in signature.parameters:
        raise ValueError("provider requires explicit source routing and is not in the source allowlist")
    signature.bind(**kwargs)

    # Provider stdout is never allowed to corrupt the Arrow IPC response stream.
    with redirect_stdout(sys.stderr):
        result = function(**kwargs)
    import pandas as pd
    import pyarrow as pa
    import pyarrow.ipc as ipc

    if not isinstance(result, pd.DataFrame):
        raise TypeError("remote bridge supports pandas DataFrame results only")
    rows = len(result)
    source = "database_only"
    # Preserve the pandas index; some providers use a DatetimeIndex or MultiIndex.
    table = pa.Table.from_pandas(result, preserve_index=True)
    metadata = dict(table.schema.metadata or {})
    contract = {"protocol": _PROTOCOL_VERSION, "provider": provider, "source": source,
                "rows": rows, "empty": rows == 0,
                "quality_summary": result.attrs.get("quality_summary")}
    if provider == "get_ticks_legacy":
        contract["dataset_semantics"] = {
            "table_generation": "legacy_policy_ticks_v1",
            "timestamp_precision": "minute",
            "seqId_semantics": "not_validated_as_exchange_sequence",
            "buyorsell_semantics": "raw_stored_codes",
            "historical_integrity_status": "legacy_archive_preserved; semantic_audit_pending",
        }
    metadata[b"trading-data-manager"] = json.dumps(
        contract,
        separators=(",", ":"),
    ).encode("utf-8")
    table = table.replace_schema_metadata(metadata)
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes(), {"provider": provider, "source": source, "rows": rows}


def _dispatch() -> int:
    raw = sys.stdin.buffer.read(_MAX_REQUEST_BYTES + 1)
    if len(raw) > _MAX_REQUEST_BYTES:
        print("remote query failed: request too large", file=sys.stderr)
        return 2
    try:
        request = json.loads(raw.decode("utf-8"))
        if not isinstance(request, dict):
            raise ValueError("request must be an object")
        payload, _info = _run_remote_query(request)
    except Exception as exc:
        # Keep diagnostics short and never echo request parameters or exception text.
        print(f"remote query failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()
    return 0


def _validate_remote_config(host: str, remote_python: str, remote_root: str) -> None:
    if not isinstance(host, str) or not _HOST_RE.fullmatch(host):
        raise ValueError("remote host must be a hostname or user@hostname")
    if not Path(remote_python).is_absolute() or not Path(remote_root).is_absolute():
        raise ValueError("remote Python and repository paths must be absolute")


@dataclass(frozen=True)
class RemoteProvider:
    """A configured, read-only SSH client for the TradingDataManager provider."""

    host: str
    remote_python: str
    remote_root: str
    timeout: float = 120.0

    def query(self, provider: str, params: dict[str, Any] | None = None, *,
              require_nonempty: bool = False):
        """Return a DataFrame with remote provenance recorded in ``df.attrs``.

        Empty results are returned as empty frames with ``remote_empty=True``;
        ``require_nonempty=True`` turns that condition into ``EmptyRemoteResult``.
        """
        _validate_remote_config(self.host, self.remote_python, self.remote_root)
        params = {} if params is None else params
        _validate_query(provider, params)
        request = json.dumps(
            {"protocol": _PROTOCOL_VERSION, "provider": provider, "params": params},
            ensure_ascii=False, allow_nan=False, separators=(",", ":"),
        ).encode("utf-8")
        if len(request) > _MAX_REQUEST_BYTES:
            raise ValueError("provider request exceeds the request size limit")
        command = "cd " + shlex.quote(self.remote_root) + " && exec " + shlex.quote(self.remote_python)
        command += " -m coreClient.remote_data_provider --dispatch"
        completed = subprocess.run(
            ["ssh", "-T", "-o", "BatchMode=yes", "--", self.host, command],
            input=request, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=self.timeout, check=False,
        )
        if completed.returncode:
            raise RemoteProviderError(f"SSH provider failed ({completed.returncode})")
        import pyarrow as pa
        import pyarrow.ipc as ipc
        try:
            table = ipc.open_stream(pa.py_buffer(completed.stdout)).read_all()
            raw_meta = (table.schema.metadata or {}).get(b"trading-data-manager")
            meta = json.loads(raw_meta.decode("utf-8")) if raw_meta else None
        except Exception as exc:
            raise RemoteProviderError(f"remote response is not valid Arrow IPC: {type(exc).__name__}") from None
        if (not isinstance(meta, dict) or meta.get("protocol") != _PROTOCOL_VERSION
                or meta.get("provider") != provider or meta.get("source") != "database_only"
                or meta.get("rows") != table.num_rows or meta.get("empty") != (table.num_rows == 0)):
            raise RemoteProviderError("remote response provenance or row count is invalid")
        frame = table.to_pandas()
        frame.attrs.update({
            "remote_provider": provider,
            "remote_source": "database_only",
            "remote_empty": table.num_rows == 0,
        })
        if isinstance(meta.get("quality_summary"), dict):
            frame.attrs["quality_summary"] = meta["quality_summary"]
            frame.attrs["quality_query"] = meta["quality_summary"].get("query")
        if isinstance(meta.get("dataset_semantics"), dict):
            frame.attrs["remote_dataset_semantics"] = meta["dataset_semantics"]
        if require_nonempty and table.num_rows == 0:
            raise EmptyRemoteResult(f"remote {provider} query returned zero rows")
        return frame


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only SSH query to a TradingDataManager database provider")
    parser.add_argument("provider", choices=sorted(PROVIDER_PARAMS))
    parser.add_argument("--host", required=True, help="SSH host or user@host")
    parser.add_argument("--remote-python", required=True, help="absolute Python executable on the remote host")
    parser.add_argument("--remote-root", required=True, help="absolute TradingDataManager checkout on the remote host")
    parser.add_argument("--params-json", default="{}", help="provider parameters as a JSON object")
    parser.add_argument("--output", required=True, help="local output file (.parquet, .csv, or .json)")
    parser.add_argument("--require-nonempty", action="store_true", help="fail if the query returns no rows")
    args = parser.parse_args(argv)
    try:
        params = json.loads(args.params_json)
        if not isinstance(params, dict):
            raise ValueError("parameters must be a JSON object")
        result = RemoteProvider(args.host, args.remote_python, args.remote_root).query(
            args.provider, params, require_nonempty=args.require_nonempty,
        )
        output = Path(args.output).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.suffix.lower() == ".parquet":
            result.to_parquet(output)
        elif output.suffix.lower() == ".csv":
            result.to_csv(output, index=False)
        elif output.suffix.lower() == ".json":
            output.write_text(result.to_json(orient="records", date_format="iso", force_ascii=False), encoding="utf-8")
        else:
            raise ValueError("output extension must be .parquet, .csv, or .json")
        print(json.dumps({"provider": args.provider, "source": "database_only", "rows": len(result),
                          "empty": bool(result.attrs.get("remote_empty")), "output": str(output)}, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"remote query failed: {type(exc).__name__}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv == ["--dispatch"]:
        return _dispatch()
    return _cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())

"""Command-line access to the stable data-provider API.

Examples:
  trading-data providers
  trading-data query get_day --params-json '{"ts_code":"600000.SH","trade_date":"2026-10-14"}' --output /tmp/day.parquet
"""
from __future__ import annotations

import argparse
import importlib
import inspect
import json
from pathlib import Path
import sys
from typing import Any


def _provider_functions() -> dict[str, Any]:
    module = importlib.import_module("coreClient.data_provider")
    return {
        name: value
        for name, value in inspect.getmembers(module, inspect.isfunction)
        if name.startswith("get_") and value.__module__ == module.__name__
    }


def _load_params(args: argparse.Namespace) -> dict[str, Any]:
    raw = Path(args.params_file).expanduser().read_text(encoding="utf-8") if args.params_file else args.params_json
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        raise ValueError("provider parameters must be a JSON object") from None
    if not isinstance(value, dict):
        raise ValueError("provider parameters must be a JSON object")
    return value


def _write_result(result: Any, path: Path, format_name: str) -> int:
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(result, pd.DataFrame):
        rows = len(result)
        if format_name == "parquet":
            result.to_parquet(path, index=False)
        elif format_name == "csv":
            result.to_csv(path, index=False)
        else:
            result.to_json(path, orient="records", force_ascii=False, date_format="iso")
        return rows
    if format_name != "json":
        raise ValueError("CSV and Parquet output require a pandas DataFrame")
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return len(result) if isinstance(result, (list, tuple, dict)) else int(result is not None)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect and call TradingDataManager providers")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("providers", help="list public get_* provider functions")

    query = subparsers.add_parser("query", help="call one provider and save its result")
    query.add_argument("provider", help="public get_* function name")
    params = query.add_mutually_exclusive_group()
    params.add_argument("--params-json", default="{}", help="provider keyword arguments as a JSON object")
    params.add_argument("--params-file", help="UTF-8 file containing provider keyword arguments as a JSON object")
    query.add_argument("--source", choices=("database_only", "database", "online"), default="database_only",
                       help="source passed when this provider exposes a source parameter")
    query.add_argument("--output", required=True, help="result file path")
    query.add_argument("--format", choices=("parquet", "csv", "json"), help="default: infer from extension, otherwise json")
    args = parser.parse_args(argv)

    try:
        providers = _provider_functions()
        if args.command == "providers":
            print(f"{len(providers)} public providers")
            for name, function in sorted(providers.items()):
                print(f"{name}{inspect.signature(function)}")
            return 0

        function = providers.get(args.provider)
        if function is None:
            print(f"Unknown provider. Run `trading-data providers` to list available functions.", file=sys.stderr)
            return 2
        kwargs = _load_params(args)
        signature = inspect.signature(function)
        if "source" in signature.parameters:
            if "source" in kwargs:
                raise ValueError("pass source with --source, not inside the parameter JSON")
            kwargs["source"] = args.source
        output = Path(args.output).expanduser()
        format_name = args.format or (output.suffix.lstrip(".").lower() if output.suffix else "json")
        if format_name not in {"parquet", "csv", "json"}:
            raise ValueError("output extension must be .parquet, .csv, or .json")
        result = function(**kwargs)
        rows = _write_result(result, output, format_name)
        print(json.dumps({"provider": args.provider, "rows": rows, "output": str(output),
                          "source": args.source if "source" in signature.parameters else "provider-default"},
                         ensure_ascii=False))
        return 0
    except Exception as exc:
        # Exception messages from HTTP clients can contain request details; only report the type.
        print(f"Provider command failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Read-only runner for the frozen 18-case migration API fingerprint manifest."""
from __future__ import annotations
import argparse, hashlib, json, os, sys, time
from pathlib import Path

import numpy as np
import pandas as pd


def normalize_scalar(x):
    if x is None or x is pd.NA or x is pd.NaT:
        return None
    if isinstance(x, np.generic):
        x = x.item()
    if isinstance(x, float) and np.isnan(x):
        return None
    if hasattr(x, "isoformat"):
        try:
            return {"__datetime__": x.isoformat()}
        except Exception:
            pass
    if isinstance(x, (str, int, float, bool)):
        return x
    return repr(x)


def fingerprint(value):
    meta = {"type": type(value).__name__}
    if isinstance(value, pd.DataFrame):
        meta.update({
            "shape": list(value.shape),
            "columns": [str(c) for c in value.columns],
            "dtypes": [str(d) for d in value.dtypes],
            "index_name": [str(n) if n is not None else None for n in value.index.names],
            "index_dtype": [str(value.index.get_level_values(i).dtype) for i in range(value.index.nlevels)],
        })
        # Include index and all cells. JSON encoding retains nulls and scalar types.
        rows = []
        for idx, row in zip(value.index.tolist(), value.itertuples(index=False, name=None)):
            if not isinstance(idx, tuple):
                idx = (idx,)
            rows.append([*[normalize_scalar(x) for x in idx], *[normalize_scalar(x) for x in row]])
        blob = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        meta["row_count"] = len(rows)
        meta["sha256_ordered_rows"] = hashlib.sha256(blob).hexdigest()
        return meta
    if isinstance(value, (dict, list, tuple)):
        blob = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=normalize_scalar, allow_nan=False).encode()
        meta["sha256_json"] = hashlib.sha256(blob).hexdigest()
        meta["value"] = value
        return meta
    meta["value"] = normalize_scalar(value)
    return meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default="/private/tmp/trading_restructure_20261014/api_fingerprint_manifest.json")
    ap.add_argument("--label", required=True, help="frozen-source or migrated-target")
    ap.add_argument("--output", required=True)
    ap.add_argument("--repo-root", default="/Users/nickzhang/TradingDataManager", help="checkout containing the active provider implementation")
    ap.add_argument("--database-root", help="explicit TRADING_DATABASE_ROOT for frozen source layout or migrated target")
    args = ap.parse_args()
    root = Path(args.repo_root).expanduser().resolve()
    sys.path.insert(0, str(root))
    if args.database_root:
        database_root = Path(args.database_root).expanduser()
        if not database_root.is_absolute() or not database_root.is_dir():
            raise SystemExit("--database-root must be an existing absolute directory; refusing to create an empty database root")
        os.environ["TRADING_DATABASE_ROOT"] = str(database_root.resolve())
    manifest = json.loads(Path(args.manifest).read_text())
    from coreClient import data_provider
    cases = manifest["query_cases"]
    results = []
    for case in cases:
        started = time.monotonic()
        try:
            fn = getattr(data_provider, case["api"])
            result = fn(**case["kwargs"])
            record = {"id": case["id"], "api": case["api"], "status": "ok", "elapsed_seconds": round(time.monotonic()-started, 3), "fingerprint": fingerprint(result)}
        except Exception as exc:
            record = {"id": case["id"], "api": case["api"], "status": "error", "elapsed_seconds": round(time.monotonic()-started, 3), "error_type": type(exc).__name__, "error": str(exc)}
        results.append(record)
        print(json.dumps({k:v for k,v in record.items() if k != "fingerprint"}, ensure_ascii=False), flush=True)
    report = {"manifest_version": manifest["manifest_version"], "api_count": manifest["api_count"], "case_count": len(cases), "label": args.label, "generated_at_epoch": time.time(), "results": results}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=normalize_scalar) + "\n")
    errors = sum(r["status"] != "ok" for r in results)
    return 1 if errors else 0

if __name__ == "__main__":
    raise SystemExit(main())

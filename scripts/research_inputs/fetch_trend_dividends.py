"""Fetch original Tushare implemented dividend facts for the frozen watchlist.

No credentials are printed or persisted. This fetch records facts, not trading
signals; corporate-action entitlements must be calculated separately.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, research_root

ROOT = manager_root(__file__, 2)
FIELDS = ("ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
          "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
          "imp_ann_date,base_date,base_share")
DATES = ("record_date", "ex_date", "pay_date", "div_listdate")


def _error_category(exc):
    value = str(exc).lower()
    if any(x in value for x in ("权限", "积分", "permission", "token", "unauthorized")):
        return "api_permission_or_authentication"
    if any(x in value for x in ("connection", "network", "proxy", "resolution", "timeout", "timed out", "网络")):
        return "network_transport"
    return "api_or_response_error"


def select_implemented(frame, start, end):
    """Include implemented events with any accounting date in the study."""
    if frame.empty:
        return pd.DataFrame(columns=FIELDS.split(","))
    frame = frame.copy()
    for column in FIELDS.split(","):
        if column not in frame:
            frame[column] = None
    relevant = pd.Series(False, index=frame.index)
    for column in DATES:
        values = frame[column].fillna("").astype(str).str.replace("-", "", regex=False)
        relevant |= values.between(start, end)
    return frame.loc[frame.div_proc.eq("实施") & relevant].drop_duplicates().reset_index(drop=True)


def run(args):
    out = Path(args.output)
    inputs = out / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)
    rawdir = inputs / "dividend_raw"
    rawdir.mkdir(exist_ok=True)
    codes = sorted(pd.read_parquet(inputs / "daily.parquet").ts_code.astype(str).unique())
    from loguru import logger
    logger.disable("coreClient.tushare_client")
    client = None
    initialization_error = None
    try:
        if not os.environ.get("TRADING_AGENT_TUSHARE_TOKEN"):
            raise PermissionError("Missing TRADING_AGENT_TUSHARE_TOKEN")
        from coreClient.tushare_client import TushareClient
        client = TushareClient()
    except Exception as exc:
        initialization_error = exc
    frames, status = [], []
    for code in codes:
        cache = rawdir / f"{code}.parquet"
        try:
            if cache.exists():
                original = pd.read_parquet(cache)
                reused = True
            else:
                if initialization_error is not None:
                    raise initialization_error
                original = client.call("dividend", ts_code=code, fields=FIELDS)
                if original is None:
                    raise ValueError("API returned None rather than a factual table")
                original = original.copy()
                original.to_parquet(cache, index=False)
                reused = False
            selected = select_implemented(original, args.start_date, args.end_date)
            frames.append(selected)
            status.append(dict(ts_code=code, status="ok", raw_rows=len(original),
                               implemented_period_rows=len(selected), reused=reused,
                               dates=selected[list(DATES)].fillna("").to_dict("records"),
                               sha256=hashlib.sha256(cache.read_bytes()).hexdigest()))
            print(json.dumps(dict(ts_code=code, raw_rows=len(original),
                                  implemented_period_rows=len(selected), reused=reused), ensure_ascii=False), flush=True)
        except Exception as exc:
            status.append(dict(ts_code=code, status="failed", error_type=type(exc).__name__,
                               error_category=_error_category(exc)))
            print(json.dumps(status[-1], ensure_ascii=False), flush=True)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=FIELDS.split(","))
    combined = combined.drop_duplicates().sort_values(["ts_code", "ex_date"], na_position="last")
    path = inputs / "dividend.parquet"
    combined.to_parquet(path, index=False)
    manifest = dict(source="Tushare pro.dividend via coreClient.TushareClient.call",
                    documentation="https://tushare.pro/document/2?doc_id=103",
                    retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
                    start_date=args.start_date, end_date=args.end_date, fields=FIELDS.split(","),
                    selection="div_proc=实施 and record/ex/pay/div_list date intersects study range",
                    codes=codes, requested=len(codes), succeeded=sum(r["status"] == "ok" for r in status),
                    failed=sum(r["status"] == "failed" for r in status), event_rows=len(combined),
                    complete=all(r["status"] == "ok" for r in status), per_code=status,
                    parquet_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    caveats=["Original facts do not infer cash or share distributions from adjustment factors.",
                             "Entitlement uses record-date closing holdings; cash becomes available on pay_date.",
                             "Bonus shares become tradable on div_listdate; stk_div already totals bonus plus conversion.",
                             "Investor-specific dividend tax and fractional-share settlement are separate accounting inputs.",
                             "Retrieved source revisions are not a point-in-time corporate-action announcement archive."])
    (out / "DIVIDEND_SOURCE.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in ("requested", "succeeded", "failed", "event_rows", "complete")}, ensure_ascii=False), flush=True)
    return 0 if manifest["complete"] else 2


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(research_root() / "outputs/trend_pullback_multiscale_v1"))
    parser.add_argument("--start-date", default="20250701")
    parser.add_argument("--end-date", default="20260930")
    raise SystemExit(run(parser.parse_args()))

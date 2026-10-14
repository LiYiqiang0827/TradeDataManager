"""
~/TradingAgent/scripts/tdx_matrix_validation_v1.py
TDX ticks 矩阵验收(2026-09-18 新增)

目的:
- 用新接口 fetch_history_ticks_with_meta 跑 2 股票 × 2 日期 × 6 服务器 = 24 次
- 验证:
  1. 新接口在生产 TDX 上能正常返回
  2. 各服务器是否真存在 A/B 二分裂(单股单日 vs 多日多股)
  3. buyorsell=5/8 出现在哪些服务器 × 哪些股票 × 哪些日期
  4. raw_rows vs returned_rows 是否一致(seen 去重影响)
  5. pagination_complete 在每台机器上是否 100% True

输出:
- reports/local/tdx_matrix_validation_v1.json(完整原始数据)
- 控制台总结表

只读,不写 db,不修改任何文件,无副作用。
"""
from __future__ import annotations

import json
import argparse
import os
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from coreClient.tdx_client import TdxClient, TDX_IP_POOL
from coreClient.tdx_ticks_meta import fetch_history_ticks_with_meta

# 矩阵参数
MATRIX = [
    # (ts_code, trade_date)
    ("000001.SZ", 20260827),
    ("000001.SZ", 20260917),
    ("000006.SZ", 20260827),
    ("000006.SZ", 20260917),
]

DEFAULT_AUDIT_ROOT = Path(
    os.environ.get("TRADING_DATA_AUDIT_ROOT", str(Path(tempfile.gettempdir()) / "trading_data_audit"))
).expanduser()


def run_one(host: str, port: int, ts_code: str, trade_date: int) -> dict:
    """跑一个 cell,返回 dict(含 ticks 摘要 + meta + 失败原因)"""
    client = None
    try:
        client = TdxClient(ip=host, port=port)
        ticks, meta = fetch_history_ticks_with_meta(
            client, ts_code, trade_date, max_pages=5,
        )
        # 计算额外统计
        vol0 = sum(1 for r in ticks if not r.get("vol"))
        pre_open = sum(1 for r in ticks if r.get("time", "") < "09:30")
        post_close = sum(1 for r in ticks if r.get("time", "") > "15:00")
        total_vol = sum(r.get("vol", 0) or 0 for r in ticks)
        return {
            "status": "ok",
            "ticks_count": len(ticks),
            "vol_zero_count": vol0,
            "pre_open_count": pre_open,
            "post_close_count": post_close,
            "total_vol": total_vol,
            "meta": meta.to_dict(),
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "error_message": str(e)[:300],
        }
    finally:
        if client is not None:
            try:
                client.disconnect()
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run 24 read-only TDX history-tick matrix requests and write a JSON audit report."
    )
    parser.add_argument(
        "--output", type=Path,
        default=DEFAULT_AUDIT_ROOT / "tdx_matrix_validation_v1.json",
        help="report path outside the code tree (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    output_path = args.output.expanduser()
    cells = []
    for host, port in TDX_IP_POOL:
        for ts_code, trade_date in MATRIX:
            print(f"[{host}] {ts_code} / {trade_date} ...", end=" ", flush=True)
            result = run_one(host, port, ts_code, trade_date)
            cells.append({
                "host": host,
                "port": port,
                "ts_code": ts_code,
                "trade_date": trade_date,
                "result": result,
            })
            # 控制台一行摘要
            if result["status"] == "ok":
                m = result["meta"]
                bs = m["buyorsell_values"]
                bs_str = ",".join(str(b) for b in bs) if bs else "-"
                print(
                    f"rows={result['ticks_count']:>4}  raw={m['raw_rows']:>4}  "
                    f"removed={m['rows_removed_by_seen']:>2}  "
                    f"vol0={result['vol_zero_count']:>2}  pre/post={result['pre_open_count']}/{result['post_close_count']}  "
                    f"bs=[{bs_str}]  complete={m['pagination_complete']}  "
                    f"term={m['termination']}"
                )
            else:
                print(f"ERROR {result['error_type']}: {result['error_message']}")

    # 写 reports/local
    payload = {
        "record_version": "tdx-matrix-validation-v1",
        "matrix": MATRIX,
        "ip_pool": [list(p) for p in TDX_IP_POOL],
        "cells": cells,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    # 总结表
    print()
    print("=" * 78)
    print("总结(按 host 聚合)")
    print("=" * 78)
    by_host: dict[str, list[dict]] = {}
    for c in cells:
        by_host.setdefault(c["host"], []).append(c)

    print(f"{'host':<18} {'ok/total':<10} {'rows范围':<14} {'raw_rows范围':<16} {'bs=8出现次数':<12}")
    print("-" * 78)
    for host, cs in by_host.items():
        ok = sum(1 for c in cs if c["result"]["status"] == "ok")
        if ok == 0:
            print(f"{host:<18} {ok}/{len(cs)}    (全失败)")
            continue
        rows = [c["result"]["ticks_count"] for c in cs if c["result"]["status"] == "ok"]
        raws = [c["result"]["meta"]["raw_rows"] for c in cs if c["result"]["status"] == "ok"]
        bs8 = sum(
            1 for c in cs
            if c["result"]["status"] == "ok" and 8 in c["result"]["meta"]["buyorsell_values"]
        )
        print(
            f"{host:<18} {ok}/{len(cs):<8} "
            f"{min(rows)}-{max(rows):<10} "
            f"{min(raws)}-{max(raws):<14} "
            f"{bs8}/{ok}"
        )

    # 完整性总结
    print()
    print("=" * 78)
    print("pagination_complete 汇总")
    print("=" * 78)
    complete_count = sum(
        1 for c in cells
        if c["result"]["status"] == "ok" and c["result"]["meta"]["pagination_complete"]
    )
    total_ok = sum(1 for c in cells if c["result"]["status"] == "ok")
    print(f"  complete: {complete_count}/{total_ok}")

    # termination 分布
    term_dist: dict[str, int] = {}
    for c in cells:
        if c["result"]["status"] == "ok":
            t = c["result"]["meta"]["termination"]
            term_dist[t] = term_dist.get(t, 0) + 1
    print(f"  termination 分布: {term_dist}")

    print(f"\n原始数据: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

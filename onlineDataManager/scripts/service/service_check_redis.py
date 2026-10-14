"""
service/service_check_redis.py
================================

Redis 状态检查 CLI 工具(2026-09-15 v6.7 新增)。

目的:
    给日常 debug / Cron / AI Agent 提供"快速检查 Redis online: 数据状态"的统一入口。
    不重写业务逻辑 — 全部委托给 core.check_redis 中的现有函数。

覆盖 kind(对应 core.check_redis 公开 API):
    - watchlist    check_watchlist() + check_watchlist_three()(v6.7)
    - snapshot     check_snapshot() / check_snapshot_for_stock(ts_code)
    - orderbook    check_orderbook() / check_orderbook_for_stock(ts_code)
    - minute       check_minute() / check_minute_for_stock(ts_code)
    - auction      check_auction() / check_auction_for_stock(ts_code)
    - zt           check_zt()
    - break        check_break()
    - anomaly      check_anomaly()
    - hot          check_hot()
    - limitperf    check_limitperformance()
    - meta         check_meta()
    - cursor       check_cursor(stream_key)
    - all_keys     check_all_keys(pattern)
    - overview     check_overview()
    - all          全跑以上所有(等价 --kind watchlist,snapshot,...)

用法:
    # 默认:一键总览
    python3 -m service.service_check_redis

    # 跑单个 kind
    python3 -m service.service_check_redis --kind watchlist
    python3 -m service.service_check_redis --kind zt --format json

    # 多个 kind
    python3 -m service.service_check_redis --kind watchlist,zt,break

    # 单股细化(只 snapshot / orderbook / minute / auction 支持)
    python3 -m service.service_check_redis --kind snapshot --ts-code 000001.SZ

    # cursor 检查
    python3 -m service.service_check_redis --kind cursor --stream-key online:auction:stream

    # 全 key 列表(SCAN online:*)
    python3 -m service.service_check_redis --kind all_keys --pattern "online:auction:*"

设计原则:
    1. service 层薄 — argparse + 调用 core.check_redis,无业务逻辑
    2. 错误隔离 — 单 kind 异常不影响其他 kind
    3. pretty 输出人读;json 输出供脚本/AI 调用
    4. exit code:脚本本身崩溃才非 0;业务问题靠 --format json 让 caller 自己解析
"""


from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from core.check_redis import (                # noqa: E402
    check_watchlist,
    check_watchlist_three,
    check_snapshot,
    check_snapshot_for_stock,
    check_orderbook,
    check_orderbook_for_stock,
    check_minute,
    check_minute_for_stock,
    check_auction,
    check_auction_for_stock,
    check_zt,
    check_limitperformance,
    check_break,
    check_anomaly,
    check_hot,
    check_meta,
    check_cursor,
    check_all_keys,
    check_overview,
)
from core.logger import setup_logger          # noqa: E402


# 所有支持的单股 kind(snapshot / orderbook / minute / auction)
_FOR_STOCK_KINDS = {"snapshot", "orderbook", "minute", "auction"}

# overview 等价清单(走单一函数,避免重复调用)
_OVERVIEW_KINDS = [
    "watchlist", "snapshot", "orderbook", "minute",
    "auction", "zt", "break", "anomaly", "hot", "limitperf", "meta",
]


def _run_kind(kind: str, *, ts_code: str = "", stream_key: str = "",
              pattern: str = "online:*") -> dict:
    """单个 kind 检查 — 委托给 core.check_redis 中对应函数"""
    k = kind.lower()
    try:
        if k == "watchlist":
            # 同时返回基础 + 三件套细分
            base = check_watchlist()
            three = check_watchlist_three()
            return {"_kind": k, "**base**": base, "**three**": three}
        if k == "watchlist_three":
            return {"_kind": k, **check_watchlist_three()}
        if k == "snapshot":
            if ts_code:
                return {"_kind": k, "ts_code": ts_code,
                        **check_snapshot_for_stock(ts_code)}
            return {"_kind": k, **check_snapshot()}
        if k == "orderbook":
            if ts_code:
                return {"_kind": k, "ts_code": ts_code,
                        **check_orderbook_for_stock(ts_code)}
            return {"_kind": k, **check_orderbook()}
        if k == "minute":
            if ts_code:
                return {"_kind": k, "ts_code": ts_code,
                        **check_minute_for_stock(ts_code)}
            return {"_kind": k, **check_minute()}
        if k == "auction":
            if ts_code:
                return {"_kind": k, "ts_code": ts_code,
                        **check_auction_for_stock(ts_code)}
            return {"_kind": k, **check_auction()}
        if k == "zt":
            return {"_kind": k, **check_zt()}
        if k == "limitperf":
            return {"_kind": k, **check_limitperformance()}
        if k == "break":
            return {"_kind": k, **check_break()}
        if k == "anomaly":
            return {"_kind": k, **check_anomaly()}
        if k == "hot":
            return {"_kind": k, **check_hot()}
        if k == "meta":
            return {"_kind": k, **check_meta()}
        if k == "cursor":
            if not stream_key:
                return {"_kind": k, "_error": "cursor 需要 --stream-key 参数"}
            return {"_kind": k, "stream_key": stream_key, **check_cursor(stream_key)}
        if k == "all_keys":
            return {"_kind": k, "pattern": pattern,
                    "count": 0, "keys": check_all_keys(pattern)}
        if k == "overview":
            return {"_kind": k, **check_overview()}
        return {"_kind": k, "_error": f"未知 kind: {kind}"}
    except Exception as e:
        return {"_kind": k, "_error": f"{type(e).__name__}: {e}"}


def _run_kinds(kinds: list[str], **kwargs) -> dict:
    """多 kind 检查 — 单 kind 异常不影响其他"""
    results = {}
    for k in kinds:
        results[k] = _run_kind(k, **kwargs)
    return {"check_time": datetime.now().strftime("%Y-%m-%d %H:%M:%f"),
            "kinds": results}


def _format_pretty(results: dict) -> str:
    """人读输出(简易表格)"""
    lines = []
    lines.append(f"=== Redis 检查结果({results.get('check_time', '')})===")
    kinds = results.get("kinds", {})
    for kind, r in kinds.items():
        lines.append("")
        lines.append(f"--- {kind.upper()} ---")
        if "_error" in r and "_kind" in r:
            lines.append(f"  ERROR: {r['_error']}")
            continue
        # watchlist 三件套特殊展示
        if kind == "watchlist" and "**base**" in r and "**three**" in r:
            base = r["**base**"]
            three = r["**three**"]
            lines.append(f"  [基础]")
            for kk, vv in base.items():
                lines.append(f"    {kk}: {vv}")
            lines.append(f"  [三件套(stream/timeline/archive)]")
            for kk, vv in three.items():
                if kk == "archive_keys" and isinstance(vv, list):
                    lines.append(f"    {kk}:")
                    for ak in vv:
                        lines.append(f"      - {ak}")
                else:
                    lines.append(f"    {kk}: {vv}")
            continue
        for kk, vv in r.items():
            if kk == "_kind":
                continue
            if isinstance(vv, list) and len(str(vv)) > 80:
                lines.append(f"    {kk}: ({len(vv)} 项)")
            else:
                lines.append(f"    {kk}: {vv}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Redis 状态检查(2026-09-15 v6.7)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例:
  %(prog)s                                          # 默认 --kind all
  %(prog)s --kind watchlist --format json
  %(prog)s --kind watchlist,zt,break
  %(prog)s --kind snapshot --ts-code 000001.SZ
  %(prog)s --kind cursor --stream-key online:auction:stream
  %(prog)s --kind all_keys --pattern 'online:auction:*'
  %(prog)s --kind overview""",
    )
    parser.add_argument(
        "--kind",
        type=str,
        default="all",
        help=(
            "要检查的 kind(逗号分隔多选,默认 all)。可选:"
            "watchlist, snapshot, orderbook, minute, auction, zt, break, "
            "anomaly, hot, limitperf, meta, cursor, all_keys, overview, all"
        ),
    )
    parser.add_argument(
        "--ts-code",
        type=str,
        default="",
        help="单股细化检查(仅 snapshot/orderbook/minute/auction 支持)",
    )
    parser.add_argument(
        "--stream-key",
        type=str,
        default="",
        help="cursor 专用: stream key,比如 online:auction:stream",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="online:*",
        help="all_keys 专用: SCAN pattern(默认 online:*)",
    )
    parser.add_argument(
        "--format",
        choices=["pretty", "json"],
        default="pretty",
        help="输出格式(默认 pretty 人读,json 机器读)",
    )
    args = parser.parse_args()

    # 解析 kind 列表
    if args.kind.lower() == "all":
        kinds = _OVERVIEW_KINDS
    else:
        kinds = [k.strip() for k in args.kind.split(",") if k.strip()]
        # 标准化 limitperformance → limitperf(对外别名)
        kinds = ["limitperf" if k == "limitperformance" else k for k in kinds]

    # 运行
    results = _run_kinds(
        kinds,
        ts_code=args.ts_code,
        stream_key=args.stream_key,
        pattern=args.pattern,
    )

    # 输出
    if args.format == "json":
        print(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    else:
        print(_format_pretty(results))

    return 0


if __name__ == "__main__":
    sys.exit(main())

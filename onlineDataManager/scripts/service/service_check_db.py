"""
service/service_check_db.py
=============================

落盘数据库(SQLite)状态检查 CLI 工具(2026-09-15 v6.7 新增)。

目的:
    给日常 debug / Cron / AI Agent 提供"快速检查 SQLite 落盘数据"的统一入口。
    不重写业务逻辑 — 全部委托给 core.check_db 中的现有函数。

覆盖 kind(对应 core.check_db 公开 API):
    - 8 个落盘 kind  snapshot / orderbook / minute / auction / zt / break / anomaly / hot
    - limitperf      check_limitperformance()
    - watchlist      check_watchlist()(单独,query 而非 _check_kind)
    - cursor         check_cursor(stream_key, year_month)
    - cursors        check_cursors(year_month)— 所有游标
    - db_list        check_db_list()
    - table_list     check_table_list(year_month)
    - table_info     check_table_info(year_month, table_name)
    - overview       check_overview(trade_date, year_month)— 一键总览
    - all            等价 --kind snapshot,orderbook,...+watchlist+cursors

设计原则:
    1. service 层薄 — argparse + 调用 core.check_db,无业务逻辑
    2. --trade-date 必填(除 db_list 外)— 所有 kind 检查都需要日期
    3. --year-month 可选,不传则从 trade_date 推(YYYYMMDD → YYYYMM)
    4. 错误隔离 — 单 kind 异常不影响其他 kind
    5. pretty 输出人读;json 输出供脚本/AI 调用
    6. exit code:脚本本身崩溃才非 0;业务问题靠 --format json 让 caller 自己解析

用法:
    # 默认(需 --trade-date)
    python3 -m service.service_check_db --trade-date 20260915

    # 跑单个 kind
    python3 -m service.service_check_db --kind zt --trade-date 20260915
    python3 -m service.service_check_db --kind watchlist --trade-date 20260915 --format json

    # 多个 kind
    python3 -m service.service_check_db --kind snapshot,orderbook --trade-date 20260915

    # 单游标
    python3 -m service.service_check_db --kind cursor --stream-key online:auction:stream --trade-date 20260915

    # 全游标 + 落盘
    python3 -m service.service_check_db --kind cursors,watchlist --trade-date 20260915

    # 表信息(需 --table-name)
    python3 -m service.service_check_db --kind table_info --year-month 202609 --table-name watchlist_20260915

    # DB 文件列表(无需 trade_date)
    python3 -m service.service_check_db --kind db_list
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

from core.check_db import (                        # noqa: E402
    check_snapshot,
    check_orderbook,
    check_minute,
    check_auction,
    check_zt,
    check_limitperformance,
    check_break,
    check_anomaly,
    check_hot,
    check_watchlist,
    check_cursor,
    check_cursors,
    check_db_list,
    check_table_list,
    check_table_info,
    check_overview,
)
from core.logger import setup_logger                # noqa: E402


# overview 等价清单
_OVERVIEW_KINDS = [
    "snapshot", "orderbook", "minute", "auction",
    "zt", "break", "anomaly", "hot",
]


def _run_kind(kind: str, *, trade_date: str, year_month: str,
              stream_key: str = "", table_name: str = "") -> dict:
    """单个 kind 检查 — 委托给 core.check_db 中对应函数"""
    k = kind.lower()
    try:
        if k == "snapshot":
            return {"_kind": k, **check_snapshot(trade_date, year_month)}
        if k == "orderbook":
            return {"_kind": k, **check_orderbook(trade_date, year_month)}
        if k == "minute":
            return {"_kind": k, **check_minute(trade_date, year_month)}
        if k == "auction":
            return {"_kind": k, **check_auction(trade_date, year_month)}
        if k == "zt":
            return {"_kind": k, **check_zt(trade_date, year_month)}
        if k == "limitperf":
            return {"_kind": k, **check_limitperformance(trade_date, year_month)}
        if k == "break":
            return {"_kind": k, **check_break(trade_date, year_month)}
        if k == "anomaly":
            return {"_kind": k, **check_anomaly(trade_date, year_month)}
        if k == "hot":
            return {"_kind": k, **check_hot(trade_date, year_month)}
        if k == "watchlist":
            return {"_kind": k, **check_watchlist(trade_date, year_month)}
        if k == "cursor":
            if not stream_key:
                return {"_kind": k, "_error": "cursor 需要 --stream-key 参数"}
            return {"_kind": k, "stream_key": stream_key,
                    **check_cursor(stream_key, year_month)}
        if k == "cursors":
            cs = check_cursors(year_month)
            return {"_kind": k, "count": len(cs), "cursors": cs}
        if k == "db_list":
            return {"_kind": k, "db_files": check_db_list()}
        if k == "table_list":
            return {"_kind": k, "year_month": year_month,
                    "tables": check_table_list(year_month)}
        if k == "table_info":
            if not table_name:
                return {"_kind": k, "_error": "table_info 需要 --table-name 参数"}
            return {"_kind": k, **check_table_info(year_month, table_name)}
        if k == "overview":
            return {"_kind": k, **check_overview(trade_date, year_month)}
        return {"_kind": k, "_error": f"未知 kind: {kind}"}
    except Exception as e:
        return {"_kind": k, "_error": f"{type(e).__name__}: {e}"}


def _run_kinds(kinds: list[str], *, trade_date: str, year_month: str,
               stream_key: str, table_name: str) -> dict:
    """多 kind 检查 — 单 kind 异常不影响其他"""
    results = {}
    for k in kinds:
        results[k] = _run_kind(
            k,
            trade_date=trade_date,
            year_month=year_month,
            stream_key=stream_key,
            table_name=table_name,
        )
    return {
        "check_time": datetime.now().strftime("%Y-%m-%d %H:%M:%f"),
        "trade_date": trade_date,
        "year_month": year_month,
        "kinds": results,
    }


def _format_pretty(results: dict) -> str:
    """人读输出(简易表格)"""
    lines = []
    lines.append(
        f"=== SQLite 检查结果({results.get('check_time', '')}, "
        f"trade_date={results.get('trade_date', '')}, "
        f"ym={results.get('year_month', '')})==="
    )
    kinds = results.get("kinds", {})
    for kind, r in kinds.items():
        lines.append("")
        lines.append(f"--- {kind.upper()} ---")
        if "_error" in r and "_kind" in r:
            lines.append(f"  ERROR: {r['_error']}")
            continue
        for kk, vv in r.items():
            if kk == "_kind":
                continue
            if isinstance(vv, list) and len(str(vv)) > 80:
                lines.append(f"    {kk}: ({len(vv)} 项)")
                for item in vv[:10]:
                    lines.append(f"      - {item}")
                if len(vv) > 10:
                    lines.append(f"      ... (还有 {len(vv) - 10} 项)")
            else:
                lines.append(f"    {kk}: {vv}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="SQLite 落盘检查(2026-09-15 v6.7)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例:
  %(prog)s --trade-date 20260915                    # 默认 --kind all
  %(prog)s --kind zt --trade-date 20260915
  %(prog)s --kind snapshot,orderbook --trade-date 20260915
  %(prog)s --kind watchlist --trade-date 20260915 --format json
  %(prog)s --kind cursor --stream-key online:auction:stream --trade-date 20260915
  %(prog)s --kind cursors --trade-date 20260915
  %(prog)s --kind table_info --year-month 202609 --table-name watchlist_20260915
  %(prog)s --kind db_list""",
    )
    parser.add_argument(
        "--kind",
        type=str,
        default="all",
        help=(
            "要检查的 kind(逗号分隔多选,默认 all)。可选:"
            "snapshot, orderbook, minute, auction, zt, break, anomaly, hot, "
            "limitperf, watchlist, cursor, cursors, db_list, table_list, "
            "table_info, overview, all"
        ),
    )
    parser.add_argument(
        "--trade-date",
        type=str,
        default="",
        help="交易日(YYYYMMDD,默认今天)— 除 db_list 外都需要",
    )
    parser.add_argument(
        "--year-month",
        type=str,
        default="",
        help="年月(YYYYMM,默认从 trade-date 推)— db_list/table_info 必填",
    )
    parser.add_argument(
        "--stream-key",
        type=str,
        default="",
        help="cursor 专用: stream key,比如 online:auction:stream",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="",
        help="table_info 专用: 表名,比如 watchlist_20260915",
    )
    parser.add_argument(
        "--format",
        choices=["pretty", "json"],
        default="pretty",
        help="输出格式(默认 pretty 人读,json 机器读)",
    )
    args = parser.parse_args()

    # trade_date 默认今天(YYYYMMDD)
    trade_date = args.trade_date
    if not trade_date:
        trade_date = datetime.now().strftime("%Y%m%d")

    # year_month 推导
    year_month = args.year_month
    if not year_month:
        year_month = trade_date[:6] if len(trade_date) >= 6 else ""

    # 解析 kind 列表
    if args.kind.lower() == "all":
        kinds = _OVERVIEW_KINDS + ["watchlist", "cursors"]
    else:
        kinds = [k.strip() for k in args.kind.split(",") if k.strip()]
        kinds = ["limitperf" if k == "limitperformance" else k for k in kinds]

    # db_list 单独路径(无需 trade_date/year_month 推导)
    if kinds == ["db_list"]:
        results = _run_kinds(
            kinds,
            trade_date=trade_date,
            year_month=year_month,
            stream_key=args.stream_key,
            table_name=args.table_name,
        )
    else:
        results = _run_kinds(
            kinds,
            trade_date=trade_date,
            year_month=year_month,
            stream_key=args.stream_key,
            table_name=args.table_name,
        )

    # 输出
    if args.format == "json":
        print(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    else:
        print(_format_pretty(results))

    return 0


if __name__ == "__main__":
    sys.exit(main())

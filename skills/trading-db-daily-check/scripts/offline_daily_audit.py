"""offline_daily_audit.py — offline 数据库完整度日检(9:15 / 22:00 各跑一次)。

核心目的:
  把"offline 数据库每天更新到哪一天"这件事,跨 4 个 db + 多张表 + 4 个 ctrl 表
  一次性查清,发飞书。

检测内容(2026-10-17 实战确认 5 类):
  1. 每个 db 的 ctrl 表 → key + max_date + updated_at
  2. 每个 db 的数据表 → SELECT MAX(trade_date) FROM tbl_xxx
  3. ctrl vs 数据表实际 MAX 一致性检查(双向日期差绝对值不超过 1 天)
  4. 对照 A 股交易日历计算最新已完成交易日
  5. 对有明确规则的数据源按发布时间检查时效(如 pro.kpl_list 次日 09:05)

输出:飞书卡片,正常给简洁摘要,异常给每张表详情。

退出码:
  0 = 全部一致
  1 = 有不一致 / 数据落后
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, time, timedelta
from pathlib import Path

DB_DIR = Path.home() / "TradingDatabase" / "offline"

# 数据表名 → ctrl key 的映射(ctrl 表里的 key)
# 2026-10-17 实战:basic 库 ctrl key 大部分不带 cn_ 前缀,但表名带 cn_;kpl/news/index 各异
TABLE_CTRL_MAPPING = {
    # basic db — 22:00 跑完整更新
    "db_cn_basic.db": {
        "ctrl_table": "tbl_basic_ctrl",
        "tables": [
            ("tbl_cn_basic", "cn_basic"),
            ("tbl_cn_tradecal", "cn_tradecal_SSE"),  # ctrl 里有 SSE/SZSE 两个,取 SSE
            ("tbl_cn_day", "cn_daily"),
            ("tbl_cn_adj_factor", "cn_adj_factor"),
            ("tbl_cn_week", None),  # 派生自 daily,无独立 ctrl
            ("tbl_cn_month", None),
            ("tbl_cn_daily_basic", "cn_daily_basic"),
            ("tbl_cn_stk_limit", "cn_stk_limit"),
            ("tbl_cn_suspend", "cn_suspend"),
            ("tbl_cn_moneyflow", "cn_moneyflow"),
            ("tbl_cn_margin", "cn_margin"),
            ("tbl_cn_margin_detail", "cn_margin_detail"),
            ("tbl_cn_cyq_perf", "cn_cyq_perf"),
        ],
        "date_col": "trade_date",
    },
    # kpl db
    "db_cn_kpl.db": {
        "ctrl_table": "tbl_kpl_ctrl",
        "tables": [
            ("tbl_cn_kpl_list", "cn_kpl_list"),
            ("tbl_cn_kpl_concept_cons", "cn_kpl_concept_cons"),
            ("tbl_cn_kpl_limit_performance", "cn_kpl_limit_performance"),
            ("tbl_cn_top_list", "cn_top_list"),
            ("tbl_cn_top_inst", "cn_top_inst"),
            ("tbl_cn_block_trade", "cn_block_trade"),
            ("tbl_cn_ggt_daily", "cn_ggt_daily"),
            ("tbl_cn_hsgt_top10", "cn_hsgt_top10"),
            ("tbl_cn_limit_list", "cn_limit_list"),
        ],
        "date_col": "trade_date",
    },
    # news db — 用 src(每条新闻一个 ctrl 行)
    "db_cn_news.db": {
        "ctrl_table": "tbl_news_ctrl",
        "tables": [
            # news 表特殊:schema 用 datetime 列,按 src 分 ctrl;简化,直接看 ctrl 表本身
            (None, None),  # marker — news 用 src 而非 key
        ],
        "date_col": None,
    },
    # index db
    "db_cn_index.db": {
        "ctrl_table": "tbl_index_ctrl",
        "tables": [
            ("tbl_cn_index_basic", "cn_index_basic"),
            ("tbl_cn_index_daily", "cn_index_daily"),
        ],
        "date_col": "trade_date",
    },
}

# 静态表(无 trade_date / cal_date 之类的日期列,只有 list_date / 字符串标识)
# 这类表只关心"是否最近被 refresh",不参与"滞后天数"判定
STATIC_TABLES = {
    "tbl_cn_basic",       # 股票基本表,全量刷,看 ctrl_updated_at
    "tbl_cn_tradecal",    # 交易日历全量刷
    "tbl_cn_index_basic", # 指数基本表,全量刷
}

# 一个 A 股交易日历查找(从 basic db 读,tushare 已经拉过)
TRADECAL_QUERY = """
SELECT cal_date FROM tbl_cn_tradecal
WHERE is_open = 1
ORDER BY cal_date DESC
LIMIT 30
"""

# 数据源有明确延迟规则时在此登记，避免把“最新交易日”误当成所有表的
# 当日应到日期。Tushare pro.kpl_list 在次日早上更新，scheduler 09:05 运行。
KPL_LIST_PUBLISH_TIME = time(9, 5)
TABLE_FRESHNESS_POLICIES = {
    "tbl_cn_kpl_list": "kpl_list_t_plus_one_morning",
}


def _open(db_filename: str) -> sqlite3.Connection:
    path = DB_DIR / db_filename
    if not path.exists():
        raise FileNotFoundError(path)
    return sqlite3.connect(str(path))


def _max_date(conn: sqlite3.Connection, table: str, date_col: str) -> str | None:
    """读数据表的 MAX(date_col)。SQLite MAX 字符串排序对 YYYYMMDD / ISO 都对。

    若指定 date_col 不存在,自动探测 trade_date / cal_date / datetime / list_date。
    """
    candidates = [date_col, "trade_date", "cal_date", "datetime", "list_date"]
    # 去重保序
    seen = set()
    cols = [c for c in candidates if not (c in seen or seen.add(c))]

    for col in cols:
        try:
            row = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
            return row[0] if row else None
        except sqlite3.OperationalError:
            continue
    return None


def _table_max_summary(db_filename: str) -> list[dict]:
    """对每个 db 返回 [{table, ctrl_key, ctrl_max, ctrl_updated_at, table_max}]。"""
    cfg = TABLE_CTRL_MAPPING[db_filename]
    ctrl_table = cfg["ctrl_table"]
    date_col = cfg.get("date_col")

    out = []
    conn = _open(db_filename)
    try:
        # 读 ctrl 全表
        ctrl_rows = {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                f"SELECT key, max_date, updated_at FROM {ctrl_table}"
            ).fetchall()
        }

        # 派生表(week/month 等由 daily 派生而来):无 ctrl,不参与一致性判定
        for table, ctrl_key in cfg["tables"]:
                if table is None:
                    continue  # marker row for news
                if ctrl_key is None:
                    # 派生表 — 只看数据 MAX,不告警
                    table_max = _max_date(conn, table, date_col) if date_col else None
                    out.append({
                        "db": db_filename,
                        "table": table,
                        "ctrl_key": None,
                        "ctrl_max": None,
                        "ctrl_updated_at": None,
                        "table_max": table_max,
                        "is_static": False,
                        "is_derived": True,
                    })
                    continue
                ctrl_max, ctrl_updated = ctrl_rows.get(ctrl_key, (None, None))
                table_max = _max_date(conn, table, date_col) if date_col else None
                is_static = table in STATIC_TABLES
                out.append({
                    "db": db_filename,
                    "table": table,
                    "ctrl_key": ctrl_key,
                    "ctrl_max": ctrl_max,
                    "ctrl_updated_at": ctrl_updated,
                    "table_max": table_max,
                    "is_static": is_static,
                    "is_derived": False,
                })
    finally:
        conn.close()
    return out


def _news_summary() -> list[dict]:
    """news db 特殊:按 src 而非 key。直接看 ctrl 表。"""
    out = []
    conn = _open("db_cn_news.db")
    try:
        for src, max_date, updated_at in conn.execute(
            "SELECT src, max_date, updated_at FROM tbl_news_ctrl ORDER BY src"
        ).fetchall():
            out.append({
                "db": "db_cn_news.db",
                "src": src,
                "ctrl_max": max_date,
                "ctrl_updated_at": updated_at,
            })
    finally:
        conn.close()
    return out


def _recent_trade_dates(n: int = 10) -> list[str]:
    """从 basic db 读最近 n 个 A 股交易日。"""
    conn = _open("db_cn_basic.db")
    try:
        rows = conn.execute(TRADECAL_QUERY).fetchall()
        return [r[0] for r in rows[:n]]
    finally:
        conn.close()


def _latest_open_date_on_or_before(cutoff: datetime) -> str | None:
    """交易日历中不晚于 cutoff 日期的最近开市日。"""
    cutoff_date = cutoff.strftime("%Y%m%d")
    conn = _open("db_cn_basic.db")
    try:
        row = conn.execute(
            "SELECT cal_date FROM tbl_cn_tradecal "
            "WHERE is_open = 1 AND cal_date <= ? "
            "ORDER BY cal_date DESC LIMIT 1",
            (cutoff_date,),
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _expected_latest_trade_date(as_of: datetime | None = None) -> str | None:
    """截至检查时点最新已收盘的交易日，不把尚未收盘的当天算作完成。"""
    as_of = as_of or datetime.now()
    cutoff = as_of - timedelta(days=1) if as_of.time() < time(15, 0) else as_of
    return _latest_open_date_on_or_before(cutoff)


def _expected_kpl_list_date(as_of: datetime) -> str | None:
    """KPL list 是 T+1 数据:次日 09:05 更新后才要求前一自然日数据。

    09:05 前再向前退一天；交易日历负责跳过周末和节假日。
    """
    days_back = 1 if as_of.time() >= KPL_LIST_PUBLISH_TIME else 2
    return _latest_open_date_on_or_before(as_of - timedelta(days=days_back))


def _normalize_date(s: str | None) -> str | None:
    """归一化日期格式:YYYYMMDD / YYYY-MM-DD... → YYYYMMDD。"""
    if not s or not isinstance(s, str):
        return s
    s = s.replace("-", "").replace("T", " ")[:8]
    return s if len(s) == 8 else None


def audit() -> dict:
    """主入口,跑所有检查,返回报告 dict。"""
    rows = []
    for db in ["db_cn_basic.db", "db_cn_kpl.db", "db_cn_index.db"]:
        rows.extend(_table_max_summary(db))

    news_ctrl = _news_summary()

    # 计算一致性
    as_of = datetime.now()
    expected = _normalize_date(_expected_latest_trade_date(as_of))
    freshness_expected = {
        "kpl_list_t_plus_one_morning": _normalize_date(_expected_kpl_list_date(as_of)),
    }
    for r in rows:
        ctrl_max_n = _normalize_date(r["ctrl_max"])
        table_max_n = _normalize_date(r["table_max"])
        r["ctrl_max_n"] = ctrl_max_n
        r["table_max_n"] = table_max_n
        policy = TABLE_FRESHNESS_POLICIES.get(r["table"])
        r["freshness_expected_date"] = freshness_expected.get(policy)

        if r.get("is_derived"):
            r["status"] = "OK"
            r["lag_days"] = None
            continue

        if r["is_static"]:
            # 静态表不参与滞后判定,只看 ctrl 是否最近刷过(24h 内)
            if r["ctrl_updated_at"]:
                updated = datetime.fromisoformat(r["ctrl_updated_at"].replace("Z", ""))
                age_h = (datetime.now() - updated).total_seconds() / 3600
                r["status"] = "OK" if age_h < 24 * 7 else "STALE"  # 静态表一周刷一次算正常
                r["lag_days"] = None
                r["static_age_h"] = round(age_h, 1)
            else:
                r["status"] = "NO_CTRL"
                r["lag_days"] = None
            continue

        # 一致性:ctrl 与表内 MAX 的日期差绝对值不超过 1 天。
        if ctrl_max_n and table_max_n:
            diff = (
                datetime.strptime(ctrl_max_n, "%Y%m%d")
                - datetime.strptime(table_max_n, "%Y%m%d")
            ).days
            r["ctrl_table_diff_days"] = diff
            r["lag_days"] = abs(diff)
            r["status"] = "OK" if abs(diff) <= 1 else "LATE"
        elif not table_max_n:
            r["lag_days"] = None
            r["status"] = "EMPTY"
        else:
            r["lag_days"] = None
            r["status"] = "NO_CTRL"

        # ctrl 与物理表可能同时落后，所以对有明确发布时间的数据源单独
        # 检查时效；取两者较旧日期，避免仅更新一侧就被判为已到齐。
        expected_date = r.get("freshness_expected_date")
        if expected_date and ctrl_max_n and table_max_n:
            actual_date = min(ctrl_max_n, table_max_n)
            r["freshness_actual_date"] = actual_date
            r["freshness_late"] = actual_date < expected_date
            if r["freshness_late"]:
                r["status"] = "LATE"
        else:
            r["freshness_actual_date"] = None
            r["freshness_late"] = False

    # news ctrl 落后判定:每个 src 的 max_date 跟今天日期对比
    today = datetime.now().strftime("%Y%m%d")
    for n in news_ctrl:
        if n["ctrl_max"] and n["ctrl_max"] >= "2025-01-01":  # 是 ISO 时间戳
            ctrl_date = n["ctrl_max"][:10].replace("-", "")
            n["ctrl_date"] = ctrl_date
            # news 是分钟级,today 应该有;weekend 允许 1 天滞后
            try:
                lag = (datetime.strptime(today, "%Y%m%d") - datetime.strptime(ctrl_date, "%Y%m%d")).days
                n["lag_days"] = lag
                n["status"] = "OK" if lag <= 1 else "LATE"
            except ValueError:
                n["status"] = "?"
        else:
            n["status"] = "?"

    # news 一些 src 长期没更新(如 fenghuang/yuncaijing 卡在 09-10)是已知问题,标记但不告警
    KNOWN_STALE_NEWS_SRCS = {"fenghuang", "yuncaijing"}

    return {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "expected_latest_trade_date": expected,
        "rows": rows,
        "news": news_ctrl,
        "known_stale_news_srcs": KNOWN_STALE_NEWS_SRCS,
    }


def format_report(report: dict, *, verbose: bool = False) -> str:
    """把 audit 结果格式化成飞书 markdown。"""
    lines = []
    expected = report["expected_latest_trade_date"]
    rows = report["rows"]
    news = report["news"]
    known = report["known_stale_news_srcs"]

    lines.append(f"## 🗂 offline 数据库日检 @ {report['ts']}")
    lines.append("")
    lines.append(f"**期望最新交易日**: {expected or '未知'}")
    lines.append("")

    # 整体一致性
    issues = []
    ok_count = 0
    for r in rows:
        if r["status"] == "OK":
            ok_count += 1
        elif r["status"] == "LATE":
            if r.get("freshness_late"):
                issues.append(
                    f"⚠️ {r['db']}/{r['table']}: 尚未达到数据源发布时间要求 "
                    f"(应到={r['freshness_expected_date']}, ctrl={r['ctrl_max']}, 数据={r['table_max']})"
                )
            else:
                issues.append(
                    f"⚠️ {r['db']}/{r['table']}: ctrl={r['ctrl_max']} 数据={r['table_max']} "
                    f"日期差 {r.get('ctrl_table_diff_days', r['lag_days'])} 天"
                )
        elif r["status"] == "EMPTY":
            issues.append(f"⚠️ {r['db']}/{r['table']}: 表为空 / 无法读 MAX")
        elif r["status"] == "NO_CTRL":
            issues.append(f"⚠️ {r['db']}/{r['table']}: 数据有但 ctrl 无 key={r['ctrl_key']}")

    # news 异常
    news_issues = []
    news_ok = 0
    for n in news:
        if n["status"] == "LATE" and n["src"] not in known:
            news_issues.append(f"⚠️ news src={n['src']}: max_date={n['ctrl_max']} 滞后 {n.get('lag_days')} 天")
        elif n["status"] == "OK":
            news_ok += 1

    if not issues and not news_issues:
        lines.append(f"✅ **全部一致** — {ok_count}/{len(rows)} 表 ctrl 与数据一致,{news_ok}/{len(news)} news src 正常")
        lines.append("")

    if verbose or issues or news_issues:
        # 表细节
        lines.append("### 数据表 vs ctrl 一致性")
        lines.append("")
        lines.append("| db | 数据表 | ctrl key | ctrl_max | 数据 MAX | 应到日期 | 状态/日期差 |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in rows:
            if r.get("is_derived"):
                icon = "↪️"
                detail = "派生自 daily"
            elif r.get("is_static"):
                icon = "📦" if r["status"] == "OK" else "⚠️"
                detail = f"{r.get('static_age_h', '-')}h 前"
            else:
                icon = {"OK": "✅", "LATE": "⚠️", "EMPTY": "❌", "NO_CTRL": "❓"}.get(r["status"], "·")
                if r.get("freshness_late"):
                    detail = f"{icon} 未到齐({r.get('freshness_actual_date')})"
                else:
                    detail = f"{icon} 日期差 {r.get('ctrl_table_diff_days', r.get('lag_days', '-'))}"
            lines.append(
                f"| {r['db'][:10]} | {r['table']} | {r['ctrl_key'] or '-'} | "
                f"{r['ctrl_max'] or '-'} | {r['table_max'] or '-'} | "
                f"{r.get('freshness_expected_date') or '-'} | {detail} |"
            )
        lines.append("")

        # news 细节
        lines.append("### 新闻源 ctrl")
        lines.append("")
        lines.append("| src | max_date | 滞后 | 状态 |")
        lines.append("|---|---|---|---|")
        for n in news:
            status_icon = "✅" if n["status"] == "OK" else "⚠️"
            known_mark = " (已知停更)" if n["src"] in known else ""
            lines.append(
                f"| {n['src']} | {n['ctrl_max']} | {n.get('lag_days', '-')} | "
                f"{status_icon}{known_mark} |"
            )
        lines.append("")

    if issues:
        lines.append("### 异常汇总")
        for i in issues:
            lines.append(f"- {i}")
        lines.append("")
    if news_issues:
        lines.append("### 新闻源异常")
        for i in news_issues:
            lines.append(f"- {i}")
        lines.append("")

    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--verbose", "-v", action="store_true",
                   help="详细模式:即使无异常也列全部表")
    p.add_argument("--quiet", action="store_true",
                   help="静默模式:有异常才打印")
    args = p.parse_args()

    report = audit()
    text = format_report(report, verbose=args.verbose)

    if args.quiet and "异常" not in text:
        return 0
    print(text)

    issues = any(
        r["status"] in ("LATE", "EMPTY", "NO_CTRL")
        for r in report["rows"]
        if not r.get("is_derived")
    )
    news_issues = any(
        n["status"] == "LATE" and n["src"] not in report["known_stale_news_srcs"]
        for n in report["news"]
    )
    return 1 if (issues or news_issues) else 0


if __name__ == "__main__":
    sys.exit(main())

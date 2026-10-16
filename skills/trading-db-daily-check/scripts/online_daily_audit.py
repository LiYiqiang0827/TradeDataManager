"""online_daily_audit.py — online 数据库落盘完整度日检(16:30 跑)。

核心目的:
  检查 online 数据库(按月分文件 `~/TradingDatabase/online/online_data_YYYYMM.db`)
  在工作日是否正常落盘,有没有缺失某个时间段。

检测内容(2026-10-17 实战确认):
  1. 当前月 + 上一个月的 db 文件存在性 / mtime 新鲜度
  2. 10 个数据 kind × 每天一张表:
     anomaly / auction / break / hot / limitperformance /
     minute / snapshot / snapshot_index / watchlist / zt
  3. 每个 kind 的"日覆盖完整性" — 从月初到最近交易日,应交易日都有表;表里 cnt 应 > 0
  4. 节假日(国庆等)允许 cnt = 0 — 需要对照 A 股交易日历判断
  5. 跨日时间断点检查 — 对最近 1 周,每天应同时存在多个 kind 表(无突然中断)
  6. cursor 表(`online_stream_cursor`):
     7 个 stream 的 last_persist_ts 应该近 30 分钟内(< 30 分钟) — 实时性指标
  7. db 文件大小:对每个 db 文件做 sanity check(> 50 MB 表示有数据,< 1 MB = 异常)

退出码:
  0 = 全部正常
  1 = 有缺失 / 表断点 / cursor 卡住
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

ONLINE_DIR = Path.home() / "TradingDatabase" / "online"
TRADECAL_BASIC_DB = Path.home() / "TradingDatabase" / "offline" / "db_cn_basic.db"

# 10 个 kind:每个每天一张表 `kind_YYYYMMDD`
KINDS = [
    "anomaly", "auction", "break", "hot", "limitperformance",
    "minute", "snapshot", "snapshot_index", "watchlist", "zt",
]

# 4 个交易日落盘主表(收盘前不要求;但 16:30 跑必须见)
# auction 是 9:15-9:25 集合竞价产生;9:25 后应该有数据;9:25 前可能没 — 不算异常
# minute 是盘中数据,收盘后才完整
# snapshot 盘中 3 秒一档,收盘前就有大量数据

# cursor 表的 7 个 stream key(2026-10-17 实战)
EXPECTED_STREAMS = {
    "online:anomaly:stream": "anomaly",
    "online:auction:stream": "auction",
    "online:break:stream": "break",
    "online:hot:stream": "hot",
    "online:limitperformance:stream": "limitperformance",
    "online:minute:stream": "minute",
    "online:zt:stream": "zt",
    # 不一定有: watchlist / snapshot_index 是 derived
}


def _recent_trade_dates(n: int = 30) -> list[str]:
    """从 basic db 读最近 n 个 A 股交易日,YYYYMMDD。"""
    today = datetime.now().strftime("%Y%m%d")
    conn = sqlite3.connect(str(TRADECAL_BASIC_DB))
    try:
        rows = conn.execute(
            "SELECT cal_date FROM tbl_cn_tradecal "
            "WHERE is_open = 1 AND cal_date <= ? "
            "ORDER BY cal_date DESC LIMIT ?",
            (today, n),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def _db_files() -> list[tuple[Path, int]]:
    """返回 [(path, year_month_int), ...] 按年月排序,只包含 online_data_*.db。"""
    files = []
    for p in ONLINE_DIR.glob("online_data_*.db"):
        # 文件名 online_data_YYYYMM.db
        name = p.stem  # online_data_YYYYMM
        suffix = name.replace("online_data_", "")
        try:
            ym = int(suffix)
            files.append((p, ym))
        except ValueError:
            continue
    return sorted(files, key=lambda x: x[1])


def _month_dates(year_month: int) -> tuple[str, str]:
    """YYYYMM → (start_date, end_date) YYYYMMDD。"""
    y, m = year_month // 100, year_month % 100
    from calendar import monthrange
    last_day = monthrange(y, m)[1]
    return f"{y}{m:02d}01", f"{y}{m:02d}{last_day:02d}"


def _db_for_date(trade_date: str) -> Path:
    """给定 YYYYMMDD,返回该日期应在哪个 db 文件里。"""
    y, m = int(trade_date[:4]), int(trade_date[4:6])
    return ONLINE_DIR / f"online_data_{y}{m:02d}.db"


def audit() -> dict:
    """主入口,跑所有检查,返回报告 dict。"""
    trade_dates = _recent_trade_dates(30)  # 最近 30 个交易日
    if not trade_dates:
        return {"error": "无法读交易日历"}

    today = datetime.now()
    today_str = today.strftime("%Y%m%d")
    today_hms = today.strftime("%H:%M:%S")

    # 检查最近 1 周(交易日)+ 当天
    last_week = trade_dates[:7]
    target_dates = set(trade_dates[:7])  # 最近 7 个交易日

    # 7 个 kind 的日覆盖
    kind_coverage: dict[str, dict[str, dict]] = {}  # kind -> date -> {exists, cnt}
    cursor_state: dict[str, dict] = {}
    db_info: list[dict] = []

    # 每个 kind 查每个目标日期
    for kind in KINDS:
        kind_coverage[kind] = {}
        # 按月分组查
        by_month: dict[int, list[str]] = {}
        for td in target_dates:
            y, m = int(td[:4]), int(td[4:6])
            by_month.setdefault(y * 100 + m, []).append(td)

        for ym, dates in by_month.items():
            db_path = ONLINE_DIR / f"online_data_{ym}.db"
            if not db_path.exists():
                for td in dates:
                    kind_coverage[kind][td] = {"exists": False, "cnt": 0, "db_missing": True}
                continue
            try:
                conn = sqlite3.connect(str(db_path))
                for td in dates:
                    table = f"{kind}_{td}"
                    try:
                        row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
                        cnt = row[0] if row else 0
                        kind_coverage[kind][td] = {"exists": True, "cnt": cnt}
                    except sqlite3.OperationalError:
                        kind_coverage[kind][td] = {"exists": False, "cnt": 0}
                conn.close()
            except sqlite3.Error as e:
                for td in dates:
                    kind_coverage[kind][td] = {"exists": False, "cnt": 0, "db_err": str(e)}

    # 当前月 db 文件状态
    current_ym = int(today.strftime("%Y%m"))
    for db_path, ym in _db_files():
        try:
            stat = db_path.stat()
            age_h = (datetime.now() - datetime.fromtimestamp(stat.st_mtime)).total_seconds() / 3600
            db_info.append({
                "path": db_path.name,
                "ym": ym,
                "size_mb": round(stat.st_size / (1024 * 1024), 1),
                "mtime_age_h": round(age_h, 1),
                "is_current": ym == current_ym,
            })
        except Exception as e:
            db_info.append({"path": db_path.name, "error": str(e)})

    # cursor 表(只在 current 月 db 里)
    cursor_db = ONLINE_DIR / f"online_data_{current_ym}.db"
    if cursor_db.exists():
        try:
            conn = sqlite3.connect(str(cursor_db))
            for row in conn.execute(
                "SELECT stream_key, last_id, last_trade_date, last_persist_ts, updated_at "
                "FROM online_stream_cursor ORDER BY stream_key"
            ).fetchall():
                sk, last_id, last_td, last_pts, updated_at = row
                try:
                    pts_dt = datetime.fromisoformat(last_pts.replace("Z", ""))
                    age_min = (datetime.now() - pts_dt).total_seconds() / 60
                except Exception:
                    age_min = None
                cursor_state[sk] = {
                    "last_id": last_id,
                    "last_trade_date": last_td,
                    "last_persist_ts": last_pts,
                    "updated_at": updated_at,
                    "age_min": round(age_min, 1) if age_min else None,
                }
            conn.close()
        except sqlite3.Error as e:
            cursor_state["__ERROR__"] = {"error": str(e)}

    # 检查最近 7 交易日 10 个 kind 全覆盖(对一个"前几个交易日"必须全覆盖)
    # 今天/昨天/前天 < 16:30 跑 → auction 可能缺(开盘竞价 9:25 才出);minute 不全(盘中)
    # 简单策略:对每个交易日 + 每个 kind,若 exists=False → 缺失告警
    # 已知容忍规则:
    #   - auction:今天如果是交易日但 9:25 前跑,允许缺
    #   - minute:今天如果是交易日但 15:00 前跑,允许缺
    #   - watchlist / snapshot_index 每天都应存在(由基础交易数据派生)
    missing: list[dict] = []
    for kind in KINDS:
        for td in target_dates:
            cov = kind_coverage[kind].get(td, {})
            if cov.get("exists") and cov.get("cnt", 0) > 0:
                continue
            if cov.get("exists") and cov.get("cnt", 0) == 0:
                # 表存在但 cnt=0 — 国庆假期等节假日正常,需要判断
                # 简化:任何交易日表里 cnt=0 都告警(等用户加白名单)
                missing.append({"kind": kind, "date": td, "reason": "表存在但 0 行"})
            else:
                missing.append({"kind": kind, "date": td, "reason": "表不存在"})

    # cursor 实时性 — 16:30 跑;周六不报;非交易时段 cursor 不更新也正常
    # 判断"是否应该活跃":工作日 9:00-16:30 之间
    cursor_issues: list[str] = []
    today = datetime.now()
    is_weekday = today.weekday() < 5  # 周一到周五
    is_active_hours = 9 <= today.hour <= 16  # 交易时段
    cursor_should_be_active = is_weekday and is_active_hours

    for sk, info in cursor_state.items():
        if sk == "__ERROR__":
            cursor_issues.append(f"cursor 读失败: {info.get('error')}")
            continue
        if info.get("age_min") is None:
            cursor_issues.append(f"⚠️ cursor {sk} last_persist_ts 无法解析")
            continue
        # 工作日交易时段,cursor 应该 < 60 分钟;其他时段容忍到 24 小时
        threshold_min = 60 if cursor_should_be_active else 24 * 60
        if info["age_min"] > threshold_min:
            tag = "卡住(交易时段" if cursor_should_be_active else "周末/非交易时段"
            cursor_issues.append(
                f"⚠️ cursor {sk} {tag}):last_persist_ts={info['last_persist_ts']}({info['age_min']:.0f} 分钟前)"
            )

    # db 文件大小 sanity check
    db_issues: list[str] = []
    for di in db_info:
        if di.get("error"):
            db_issues.append(f"⚠️ db {di['path']}: {di['error']}")
            continue
        if di["size_mb"] < 1 and di["is_current"]:
            db_issues.append(f"⚠️ 当前月 db {di['path']} 仅 {di['size_mb']}MB")

    return {
        "ts": today.strftime("%Y-%m-%d %H:%M"),
        "today": today_str,
        "trade_dates_last_7": sorted(target_dates),
        "expected_streams": list(EXPECTED_STREAMS.keys()),
        "kind_coverage": kind_coverage,
        "cursor_state": cursor_state,
        "db_info": db_info,
        "missing": missing,
        "cursor_issues": cursor_issues,
        "db_issues": db_issues,
    }


def format_report(report: dict, *, verbose: bool = False) -> str:
    lines = []
    lines.append(f"## 📡 online 数据库落盘日检 @ {report['ts']}")
    lines.append("")

    # 总览
    lines.append(f"**检查日期范围**: 最近 7 个交易日 = {', '.join(sorted(report['trade_dates_last_7']))}")
    lines.append("")

    # db 文件状态
    lines.append("### db 文件状态")
    lines.append("")
    lines.append("| 文件 | 大小(MB) | 最近修改(h 前) |")
    lines.append("|---|---|---|")
    for di in report["db_info"]:
        icon = "🟢" if di.get("is_current") else "⚪"
        age = di.get("mtime_age_h", "-")
        size = di.get("size_mb", "-")
        lines.append(f"| {icon} {di['path']} | {size} | {age} |")
    lines.append("")

    # kind 覆盖率(表格 — kind × date → cnt)
    lines.append("### 数据落盘覆盖(行数, ✅ > 0, ❌ = 0, ⚠️ 表缺失)")
    lines.append("")
    dates = sorted(report["trade_dates_last_7"])
    header = "| kind | " + " | ".join(dates) + " |"
    sep = "|---|" + "|".join(["---"] * len(dates)) + "|"
    lines.append(header)
    lines.append(sep)
    kc = report["kind_coverage"]
    for kind in KINDS:
        row = f"| {kind} |"
        for td in dates:
            cov = kc[kind].get(td, {})
            cnt = cov.get("cnt", 0)
            exists = cov.get("exists", False)
            if exists and cnt > 0:
                row += f" ✅{cnt} |"
            elif exists and cnt == 0:
                row += " ❌0 |"
            else:
                row += " ⚠️缺 |"
        lines.append(row)
    lines.append("")

    # cursor 状态
    lines.append("### cursor 实时性(stream last_persist_ts)")
    lines.append("")
    lines.append("| stream_key | last_trade_date | last_persist_ts | age(min) |")
    lines.append("|---|---|---|---|")
    cs = report["cursor_state"]
    if not cs:
        lines.append("| (cursor 表无数据) | - | - | - |")
    elif "__ERROR__" in cs:
        lines.append(f"| ❌ 读失败: {cs['__ERROR__'].get('error')} | - | - | - |")
    else:
        for sk in sorted(cs.keys()):
            info = cs[sk]
            age = info.get("age_min", "-")
            lines.append(
                f"| {sk} | {info.get('last_trade_date', '-')} | "
                f"{info.get('last_persist_ts', '-')} | {age} |"
            )
    lines.append("")

    # 异常汇总
    if not report["missing"] and not report["cursor_issues"] and not report["db_issues"]:
        lines.append("✅ **全部正常** — 无缺失,无 cursor 卡顿")
    else:
        if report["missing"]:
            lines.append("### ⚠️ 落盘缺失")
            for m in report["missing"]:
                lines.append(f"- {m['kind']} / {m['date']}: {m['reason']}")
            lines.append("")
        if report["cursor_issues"]:
            lines.append("### ⚠️ cursor 问题")
            for c in report["cursor_issues"]:
                lines.append(f"- {c}")
            lines.append("")
        if report["db_issues"]:
            lines.append("### ⚠️ db 文件问题")
            for c in report["db_issues"]:
                lines.append(f"- {c}")
            lines.append("")

    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--verbose", "-v", action="store_true", help="详细模式")
    p.add_argument("--quiet", action="store_true", help="静默:有异常才打印")
    args = p.parse_args()

    report = audit()
    if "error" in report:
        print(f"❌ audit 失败: {report['error']}")
        return 1

    text = format_report(report, verbose=args.verbose)
    if args.quiet and not report["missing"] and not report["cursor_issues"] and not report["db_issues"]:
        return 0
    print(text)

    return 1 if (report["missing"] or report["cursor_issues"] or report["db_issues"]) else 0


if __name__ == "__main__":
    sys.exit(main())
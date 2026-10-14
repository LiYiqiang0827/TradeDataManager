"""
core/watchlist_fetch.py
=======================

从 offlineDataManager 数据库读取涨停/连板股票,生成实时监控 watchlist。

数据源:
    ~/TradingAgent/offlineDataManager/data/db_cn_kpl.db
    (取代旧 ~/database/cn_hotstock.db — MyATM cn_data 项目 2026-09-10 整体迁移
     到 offlineDataManager 后,watchlist 也跟着切到新位置)

调用方:
    service/service_writeredis_watchlist.py(盘前一次性写 Redis SET)

设计原则:
- core/ 只提供"读 DB"的接口,不写 Redis 不调 API
- "写 Redis" 在 service 层;core 纯函数,好测试
- 所有 SQL 查询带参数化,防止注入

表结构(offlineDataManager/data/db_cn_kpl.db.tbl_cn_kpl_list):
    PRIMARY KEY (ts_code, trade_date)
    关键字段:
        ts_code   : '002162.SZ' 这种带后缀的标准码
        trade_date: '20260909' YYYYMMDD
        tag       : '涨停' / '跌停' / '炸板' / 等
        status    : '首板' / '2连板' / '3连板' / '3天2板' / '4天2板' / '4天3板' / '5连板' / ...
        snap_ts   : ISO 格式 '2026-09-10T09:00:05'(离线数据入库时间)
"""

from __future__ import annotations

import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# ============================================================
# 优先级 / reason 常量(2026-09-11 用户最新指令)
# ============================================================
# priority: 数字越大级别越高,目前所有 source 统一写 0(占位,后续按 source 分级)
# reason : 来源说明,目前统一"盘前最近涨停股"
DEFAULT_PRIORITY = 0   # int 0-3,默认 0
DEFAULT_REASON = "盘前最近涨停股"

# ============================================================
# 数据库路径(2026-09-10 已迁移到 offlineDataManager)
# ============================================================
# offlineDataManager 目录是固定的;用户最新架构定义:
# ~/TradingAgent/offlineDataManager/data/db_cn_kpl.db
# 不依赖 common.py,也不依赖 cwd,纯路径推导
from coreClient._paths import manager_root, offline_data_dir

ROOT = manager_root(__file__, 3)
ONLINE_DATA_ROOT = ROOT / "onlineDataManager"
OFFLINE_DATA_ROOT = ROOT / "offlineDataManager"
OFFLINE_DB_DIR = offline_data_dir()

# 数据来源(offlineDataManager 的 kpl.db)
DB_KPL = OFFLINE_DB_DIR / "db_cn_kpl.db"
TABLE_KPL_LIST = "tbl_cn_kpl_list"


# ============================================================
# 板型识别
# ============================================================
# 涨停 status 字段里的"连板"/"板型"分两类:
# - "N连板":  2连板/3连板/4连板/... — 严格连板
# - "N天M板": 3天2板/4天2板/...        — 中间可能有断板
#
# 用户规则:"前几日涨停中 >=2 板" — 严格意义是板数 >= 2
# 所以这两种都算,但 "首板" 不算
#
# 正则:
#   N连板       → >=2连板
#   N天M板(M>=2) → M>=2
#
_RE_LIANBAN = re.compile(r"^(\d+)连板$")
_RE_TIANBAN = re.compile(r"^(\d+)天(\d+)板$")


def parse_lianban_level(status: str | None) -> int:
    """解析涨停 status,返回板数(>=2)。不是连板或解析失败返回 0。

    Examples:
        >>> parse_lianban_level("首板")
        0
        >>> parse_lianban_level("2连板")
        2
        >>> parse_lianban_level("3连板")
        3
        >>> parse_lianban_level("3天2板")
        2
        >>> parse_lianban_level("4天3板")
        3
        >>> parse_lianban_level(None)
        0
    """
    if not status:
        return 0
    s = status.strip()
    m = _RE_LIANBAN.match(s)
    if m:
        n = int(m.group(1))
        return n if n >= 2 else 0
    m = _RE_TIANBAN.match(s)
    if m:
        m_boards = int(m.group(2))
        return m_boards if m_boards >= 2 else 0
    return 0


# ============================================================
# 数据类
# ============================================================
@dataclass(frozen=True)
class WatchlistEntry:
    """watchlist 中的一条记录

    2026-09-12 v3 改造(用户最新指令:加 watchlist_timestamp):
        watchlist_timestamp : str,ISO 字符串,生成时打的 wall clock
                                (从 unix 秒秒转 ISO,本地时区,带 tz)
        priority            : int 0-3,数字越大级别越高,默认 0
        reason              : 来源说明,默认"盘前最近涨停股"

    2026-09-13 v4 改造(用户最新指令:双数据源 watchlist):
        priority 含义:
            3 = 最新日 + >=2 板(最新连板股,最重要)
            2 = 历史窗口(10天) + >=2 板(已连板股)
            1 = 最新日 + 1 板(最新首板)
            0 = 保留(未用,占位)

    v3 精简:
        - 删 data_timestamp(改名 watchlist_timestamp,落盘转 ISO 字符串 TEXT)
        - 删 save_timestamp(落盘时间戳改 created_at,SQLite DEFAULT 自动加)
        - 2026-09-16 v6.14:watchlist_timestamp 改 int 毫秒(对齐 auction/snapshot/orderbook);Redis 存原始 int,落盘转 ISO
    """
    ts_code: str
    source: str  # 'latest_limit_lb' / 'latest_limit' / 'prev_lianban' / 'latest_lb+prev' 等
    watchlist_timestamp: int = 0     # ★ v6.14:生成瞬间(int 毫秒,落盘转 ISO 字符串)
    priority: int = DEFAULT_PRIORITY  # 0-3
    reason: str = DEFAULT_REASON    # 来源说明


@dataclass
class Watchlist:
    """观察列表 — service 拿到这个对象写 Redis

    2026-09-13 v4(双数据源):新增字段追踪数据源决策
        latest_date       : 最新有数据的 trade_date
        latest_source     : 'limit_performance' 或 'list'(谁更新)
        prev_window_start : 历史窗口起点
        prev_window_end   : 历史窗口终点(latest_date 前一天)
        latest_lb_count   : 最新日连板股数
        latest_count      : 最新日全部涨停数(含 1 板)
        prev_lb_count     : 历史窗口连板股数
    """
    entries: list[WatchlistEntry] = field(default_factory=list)
    latest_date: str = ""
    prev_window: tuple[str, str] = ("", "")
    latest_source: str = ""  # 'limit_performance' / 'list'
    latest_lb_count: int = 0
    latest_count: int = 0
    prev_lb_count: int = 0

    @property
    def ts_codes(self) -> list[str]:
        return [e.ts_code for e in self.entries]

    @property
    def count(self) -> int:
        return len(self.entries)

    def sources_map(self) -> dict[str, str]:
        return {e.ts_code: e.source for e in self.entries}


# ============================================================
# DB 查询函数
# ============================================================
def _connect_kpl() -> sqlite3.Connection:
    """打开 kpl.db 连接,带 row_factory"""
    if not DB_KPL.exists():
        raise FileNotFoundError(
            f"kpl.db not found: {DB_KPL}\n"
            "确认 offlineDataManager/data/db_cn_kpl.db 是否存在"
        )
    conn = sqlite3.connect(str(DB_KPL), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_yesterday_limitup(trade_date: str) -> list[str]:
    """拉某一天的全部涨停 ts_code

    Args:
        trade_date: YYYYMMDD

    Returns:
        ts_code 列表(已 DISTINCT)
    """
    with _connect_kpl() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT ts_code FROM {TABLE_KPL_LIST}
            WHERE tag='涨停' AND trade_date=?
            """,
            (trade_date,),
        ).fetchall()
    return [r["ts_code"] for r in rows]


def fetch_prev_lianban(start_date: str, end_date: str, min_level: int = 2) -> list[tuple[str, int]]:
    """拉一段时间范围内 status 板数 >= min_level 的涨停股票

    Args:
        start_date: YYYYMMDD(包含)
        end_date  : YYYYMMDD(包含)
        min_level : 最少板数,默认 2(>=2 连板才算)

    Returns:
        [(ts_code, max_level), ...]  max_level 是这段时间内该股票的最高板数
    """
    with _connect_kpl() as conn:
        rows = conn.execute(
            f"""
            SELECT ts_code, status FROM {TABLE_KPL_LIST}
            WHERE tag='涨停'
              AND trade_date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        ).fetchall()

    # 在 Python 端解析 status,因为板型分两类(连板 + 天板)
    code_to_max_level: dict[str, int] = {}
    for r in rows:
        level = parse_lianban_level(r["status"])
        if level < min_level:
            continue
        code = r["ts_code"]
        if level > code_to_max_level.get(code, 0):
            code_to_max_level[code] = level

    # 板数从高到低排序
    return sorted(code_to_max_level.items(), key=lambda x: -x[1])


def fetch_prev_all(start_date: str, end_date: str) -> list[str]:
    """拉一段时间范围内的全部涨停 ts_code(不限连板)

    用于:数据库最新 trade_date 还停在昨日的情况(回退方案)
    """
    with _connect_kpl() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT ts_code FROM {TABLE_KPL_LIST}
            WHERE tag='涨停'
              AND trade_date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        ).fetchall()
    return [r["ts_code"] for r in rows]


def fetch_latest_trade_date() -> str | None:
    """从 kpl.db 里读最新一个有涨停数据的 trade_date"""
    with _connect_kpl() as conn:
        row = conn.execute(
            f"SELECT MAX(trade_date) AS d FROM {TABLE_KPL_LIST} WHERE tag='涨停'"
        ).fetchone()
    return row["d"] if row else None


# ============================================================
# 2026-09-13 新增:双数据源支持(kpl_list + kpl_limit_performance)
# ============================================================
# 背景:
#   - tbl_cn_kpl_list 来自 tushare,第二天早上才有(滞后 T+1)
#   - tbl_cn_kpl_limit_performance 来自 kpl API,16:30+ 实时
#   - 两表数据源互补,通过 tbl_basic_ctrl.max_date 比对"谁最新"
#
# 决策规则:
#   - cn_kpl_limit_performance.max_date >= cn_kpl_list.max_date
#       → 用 kpl_limit_performance(更及时,字段更精确)
#   - 反之 → 用 kpl_list(历史回退)
#
# 注意:
#   - 这里只读 kpl.db,basic.db 不依赖(架构简洁)
#   - 但如果想读 tbl_basic_ctrl 统一断点表,可以用 offline_db_client.get_ctrl_basic()
TABLE_KPL_LP = "tbl_cn_kpl_limit_performance"


def fetch_latest_trade_date_from_lp() -> str | None:
    """从 tbl_cn_kpl_limit_performance 读最新 trade_date"""
    with _connect_kpl() as conn:
        row = conn.execute(
            f"SELECT MAX(trade_date) AS d FROM {TABLE_KPL_LP}"
        ).fetchone()
    return row["d"] if row else None


def fetch_kpl_ctrl_max_dates() -> dict[str, str | None]:
    """从 db_cn_basic.db:tbl_basic_ctrl 读 kpl_list / kpl_limit_performance 的 max_date

    架构(2026-09-13):
      - 统一断点表: db_cn_basic.db:tbl_basic_ctrl(规范)
      - 旧表(兼容): db_cn_kpl.db:tbl_kpl_ctrl(2026-09-10 前用过)

    优先从 tbl_basic_ctrl 读(规范),如果读不到或表不存在,
    回退到 tbl_kpl_ctrl(兼容),如果都没有返回 None。

    返回:{'cn_kpl_list': '20260911', 'cn_kpl_limit_performance': '20260911', ...}
    """
    result = {"cn_kpl_list": None, "cn_kpl_limit_performance": None}

    # 1. 优先从 db_cn_basic.db:tbl_basic_ctrl 读(规范位置)
    db_basic = OFFLINE_DB_DIR / "db_cn_basic.db"
    if db_basic.exists():
        try:
            with sqlite3.connect(str(db_basic), timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT key, max_date FROM tbl_basic_ctrl WHERE key IN (?, ?)",
                    ("cn_kpl_list", "cn_kpl_limit_performance"),
                ).fetchall()
                for r in rows:
                    if r["key"] in result:
                        result[r["key"]] = r["max_date"]
        except Exception as e:
            pass  # 静默失败,继续走 fallback

    # 2. fallback 到 db_cn_kpl.db:tbl_kpl_ctrl(兼容)
    with _connect_kpl() as conn:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tbl_kpl_ctrl'"
        ).fetchone()
        if exists:
            for key in ("cn_kpl_list", "cn_kpl_limit_performance"):
                if result[key] is None:  # 只在 basic 没读到时才用旧表
                    row = conn.execute(
                        "SELECT max_date FROM tbl_kpl_ctrl WHERE key = ?", (key,)
                    ).fetchone()
                    if row:
                        result[key] = row["max_date"]

    return result


def fetch_latest_limitup(
    trade_date: str,
    prefer_table: str = "limit_performance",
) -> list[tuple[str, int]]:
    """拉最新一天的涨停股票 + 板数(去重)

    数据源(按 prefer_table):
      - 'limit_performance':  tbl_cn_kpl_limit_performance(board_count 字段精确)
      - 'list':               tbl_cn_kpl_list(需从 status 解析板数)

    Args:
        trade_date: YYYYMMDD
        prefer_table: 优先用哪张表

    Returns:
        [(ts_code, max_board_count), ...]  按 max_board_count DESC
    """
    if prefer_table == "limit_performance":
        with _connect_kpl() as conn:
            rows = conn.execute(
                f"""
                SELECT ts_code, MAX(board_count) AS bc
                FROM {TABLE_KPL_LP}
                WHERE trade_date = ?
                GROUP BY ts_code
                """,
                (trade_date,),
            ).fetchall()
        return [(r["ts_code"], r["bc"]) for r in rows]

    # 走 kpl_list 路径
    with _connect_kpl() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT ts_code, status FROM {TABLE_KPL_LIST}
            WHERE tag='涨停' AND trade_date=?
            """,
            (trade_date,),
        ).fetchall()

    code_to_max_level: dict[str, int] = {}
    for r in rows:
        level = parse_lianban_level(r["status"]) or 1  # 首板 = 1
        code = r["ts_code"]
        if level > code_to_max_level.get(code, 0):
            code_to_max_level[code] = level
    return sorted(code_to_max_level.items(), key=lambda x: (-x[1], x[0]))


def fetch_prev_lianban_from_lp(
    start_date: str,
    end_date: str,
    min_level: int = 2,
) -> list[tuple[str, int]]:
    """从 tbl_cn_kpl_limit_performance 拉一段时间内的连板股 + 最大板数

    Args:
        start_date/end_date: YYYYMMDD(包含)
        min_level: 最少板数(默认 2)

    Returns:
        [(ts_code, max_board_count), ...]  按 max_board_count DESC
    """
    with _connect_kpl() as conn:
        rows = conn.execute(
            f"""
            SELECT ts_code, MAX(board_count) AS bc
            FROM {TABLE_KPL_LP}
            WHERE trade_date BETWEEN ? AND ?
              AND board_count >= ?
            GROUP BY ts_code
            ORDER BY bc DESC
            """,
            (start_date, end_date, min_level),
        ).fetchall()
    return [(r["ts_code"], r["bc"]) for r in rows]


def fetch_prev_lianban_range(
    start_date: str,
    end_date: str,
    min_level: int = 2,
    exclude_date: str | None = None,
    prefer_table: str = "list",
) -> list[tuple[str, int]]:
    """拉一段时间内连板股 + 最大板数(去重)

    Args:
        start_date/end_date: YYYYMMDD(包含)
        min_level: 最少板数
        exclude_date: 排除某个日期(用于排除最新日,避免重复)
        prefer_table: 'list' / 'limit_performance'(默认 list,因为存得久)

    Returns:
        [(ts_code, max_level), ...]  按 max_level DESC
    """
    if prefer_table == "limit_performance":
        return fetch_prev_lianban_from_lp(start_date, end_date, min_level)

    with _connect_kpl() as conn:
        if exclude_date:
            sql = f"""
                SELECT DISTINCT ts_code, status FROM {TABLE_KPL_LIST}
                WHERE tag='涨停'
                  AND trade_date BETWEEN ? AND ?
                  AND trade_date != ?
            """
            params = (start_date, end_date, exclude_date)
        else:
            sql = f"""
                SELECT DISTINCT ts_code, status FROM {TABLE_KPL_LIST}
                WHERE tag='涨停'
                  AND trade_date BETWEEN ? AND ?
            """
            params = (start_date, end_date)
        rows = conn.execute(sql, params).fetchall()

    code_to_max_level: dict[str, int] = {}
    for r in rows:
        level = parse_lianban_level(r["status"])
        if level < min_level:
            continue
        code = r["ts_code"]
        if level > code_to_max_level.get(code, 0):
            code_to_max_level[code] = level
    return sorted(code_to_max_level.items(), key=lambda x: -x[1])


# ============================================================
# 主函数:组装 watchlist
# ============================================================
def generate_watchlist(
    prev_window_days: int = 10,
    min_level: int = 2,
    use_fallback: bool = True,
) -> Watchlist:
    """生成观察列表(2026-09-13 v4 双数据源版本)

    算法(用户最新指令):
      1. 从 tbl_kpl_ctrl 读 cn_kpl_list / cn_kpl_limit_performance 的 max_date
      2. 谁新用谁(limit_performance 优先,因为字段精确)
      3. 从最新表拉最新一天的涨停股(ts_code, board_count)
         - >=2 板 → priority=3
         - 1 板    → priority=1
      4. 历史窗口:从 cn_kpl_list 拉 prev_window_days 天的 >=2 板
         - 已存在于最新日的:不重复入,但 source 标记 'latest_lb+prev'
         - 仅历史出现的:priority=2
      5. 去重、按 priority DESC → lu_time ASC 排序

    Args:
        prev_window_days: 历史窗口天数(默认 10 天)
        min_level:        连板最小板数(默认 2)
        use_fallback:     最新表无数据时是否回退到 kpl_list(默认 True)

    Returns:
        Watchlist 对象(service 层用它写 Redis)

    Raises:
        RuntimeError: 两表都无数据
    """
    # 1. 决策数据源
    max_dates = fetch_kpl_ctrl_max_dates()
    md_lp = max_dates.get("cn_kpl_limit_performance")
    md_list = max_dates.get("cn_kpl_list")

    # 兼容回退:如果某张表无 ctrl 数据,从表里 MAX(trade_date) 取
    if not md_lp:
        md_lp = fetch_latest_trade_date_from_lp() or ""
    if not md_list:
        md_list = fetch_latest_trade_date() or ""

    # 谁新用谁(同日期优先 limit_performance,字段更精确)
    if md_lp and (not md_list or md_lp >= md_list):
        latest_date = md_lp
        latest_source = "limit_performance"
    elif md_list:
        latest_date = md_list
        latest_source = "list"
    else:
        raise RuntimeError("kpl.db 里没有任何涨停数据(cn_kpl_list 和 cn_kpl_limit_performance 都空)")

    # 2. 拉最新日涨停
    latest_pairs = fetch_latest_limitup(latest_date, prefer_table=latest_source)
    # fetch_latest_limitup 返回 [(ts_code, max_board_count)]
    # 按 board_count DESC 排序,优先展示高板数

    # 3. 拉历史窗口连板(exclude 最新日避免重复)
    prev_end_dt = _parse_yyyymmdd(latest_date) - _1_day
    prev_start_dt = prev_end_dt - timedelta(days=prev_window_days - 1)
    prev_window = (_fmt_yyyymmdd(prev_start_dt), _fmt_yyyymmdd(prev_end_dt))

    prev_pairs = fetch_prev_lianban_range(
        *prev_window,
        min_level=min_level,
        exclude_date=latest_date,  # 排除最新日,避免重复
        prefer_table="list",        # 历史用 kpl_list(数据全)
    )

    # 4. 组装 entries(去重 + 优先级)
    seen: dict[str, WatchlistEntry] = {}  # ts_code → entry,用于去重

    # 4a. 最新日涨停(priority 1 或 3)
    latest_lb_count = 0
    latest_total_count = 0
    for code, bc in latest_pairs:
        if bc >= 2:
            priority = 3
            latest_lb_count += 1
            source = "latest_limit_lb" if latest_source == "limit_performance" else "latest_list_lb"
        else:
            priority = 1
            source = "latest_limit" if latest_source == "limit_performance" else "latest_list"
        latest_total_count += 1
        seen[code] = WatchlistEntry(
            ts_code=code,
            source=source,
            priority=priority,
            reason=f"最新日({latest_date})涨停 {bc}板",
        )

    # 4b. 历史窗口连板(priority 2)
    # 如果已经在最新日见过,合并 source;否则独立 entry
    prev_lb_count = 0
    for code, level in prev_pairs:
        prev_lb_count += 1
        if code in seen:
            # 已存在最新日:合并 source(主来源仍是最新日)
            old = seen[code]
            seen[code] = WatchlistEntry(
                ts_code=code,
                source=old.source + "+prev",
                priority=old.priority,  # 沿用最新日的 priority
                reason=old.reason + f" + 历史连板{level}板",
            )
        else:
            seen[code] = WatchlistEntry(
                ts_code=code,
                source="prev_lianban",
                priority=2,
                reason=f"历史连板{level}板",
            )

    # 5. 打 watchlist_timestamp(int 毫秒,与 auction_timestamp 等对齐)
    ts_unix_ms = int(time.time() * 1000)  # ★ v6.14:存原始 int 毫秒(Redis);落盘转 ISO 字符串

    # 6. 组装最终 entries(按 priority DESC → 板块 DESC → ts_code 排序)
    def _entry_sort_key(e: WatchlistEntry) -> tuple:
        # priority 高的在前;同一 priority 内按 ts_code 字典序(稳定性)
        return (-e.priority, e.ts_code)

    entries = sorted(seen.values(), key=_entry_sort_key)
    # 批量加 watchlist_timestamp
    entries = [
        WatchlistEntry(
            ts_code=e.ts_code,
            source=e.source,
            watchlist_timestamp=ts_unix_ms,
            priority=e.priority,
            reason=e.reason,
        )
        for e in entries
    ]

    return Watchlist(
        entries=entries,
        latest_date=latest_date,
        prev_window=prev_window,
        latest_source=latest_source,
        latest_lb_count=latest_lb_count,
        latest_count=latest_total_count,
        prev_lb_count=prev_lb_count,
    )


# ============================================================
# 内部 helpers
# ============================================================
from datetime import datetime, timedelta

_7_days = timedelta(days=7)
_1_day = timedelta(days=1)


def _parse_yyyymmdd(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")


def _fmt_yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


# ============================================================
# CLI:手工测试
# ============================================================
if __name__ == "__main__":
    import json

    wl = generate_watchlist()
    print(json.dumps(
        {
            "latest_date": wl.latest_date,
            "latest_source": wl.latest_source,
            "prev_window": list(wl.prev_window),
            "latest_count": wl.latest_count,
            "latest_lb_count": wl.latest_lb_count,
            "prev_lb_count": wl.prev_lb_count,
            "total_count": wl.count,
            "ts_codes": wl.ts_codes[:20],  # 只显示前 20
            "first_10_entries": [
                {
                    "ts_code": e.ts_code,
                    "source": e.source,
                    "priority": e.priority,
                    "reason": e.reason,
                    "watchlist_timestamp": e.watchlist_timestamp,
                }
                for e in wl.entries[:10]
            ],
        },
        indent=2,
        ensure_ascii=False,
    ))

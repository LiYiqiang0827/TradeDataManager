"""
query_redis: Redis 数据拉取工具类(只读)

目的:
    给 AI Agent 提供"按需从 Redis 拉数据"的统一接口。
    只取数据,不返回状态汇总 —— 状态检查请用 check_redis。

API 列表(按数据条目,fetch_* 系列):
    WATCHLIST       fetch_watchlist() / fetch_watchlist_sources()  -> ts_code 列表 / source 映射
    SNAPSHOT        fetch_snapshot_window(ts_code) / fetch_snapshot_history(count)
                    fetch_snapshot_stream_since(min_id)
    ORDERBOOK       fetch_orderbook_window(ts_code) / fetch_orderbook_history(count)
    MINUTE          fetch_minute_bars(ts_code, bar_idx_min, bar_idx_max)
    AUCTION         fetch_auction_window(ts_code) / fetch_auction_history(count)
                    fetch_auction_timeline(count) / fetch_auction_archive_latest()
                    fetch_auction_snapshot_at(seconds_before)
                    (v6.2 2026-09-14:全市场快照索引,不滑窗 12h 兜底)
    ZT              fetch_zt_stream(count) / fetch_zt_timeline(count)
                    fetch_zt_archive_latest() / fetch_zt_snapshot_at(seconds_before)
                    / fetch_zt_history(count) / fetch_zt_stream_since(min_id)
    BREAK           fetch_break_stream(count) / fetch_break_timeline(count)
                    fetch_break_archive_latest() / fetch_break_snapshot_at(seconds_before)
    ANOMALY         fetch_anomaly_stream(count) / fetch_anomaly_timeline(count)
                    fetch_anomaly_archive_latest() / fetch_anomaly_snapshot_at(seconds_before)
    HOT             fetch_hot_stream(count) / fetch_hot_timeline(count)
                    fetch_hot_archive_latest() / fetch_hot_snapshot_at(seconds_before)
    LIMITPERFORMANCE fetch_limitperformance_stream(count) / fetch_limitperformance_timeline(count)
                    fetch_limitperformance_archive_latest() / fetch_limitperformance_snapshot_at(seconds_before)
    META            fetch_meta() / fetch_meta_key(key)

只读:无任何 SET/HSET/DEL/XADD/ZADD。所有方法都是 GET/READ 类操作。
"""


import sys
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _THIS_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import logging
from typing import Any

from core.redis_online import OnlineRedis, PREFIX

_log = logging.getLogger("query_redis")

# ============================================================
# WATCHLIST
# ============================================================


def fetch_watchlist() -> list[str]:
    """拉 watchlist 全部 ts_code"""
    return OnlineRedis().get_watchlist() or []


def fetch_watchlist_sources() -> dict[str, str]:
    """拉 watchlist sources 映射 {ts_code: 'yest' | 'prev_lianban' | 'yest+prev_lianban'}"""
    raw = OnlineRedis().get_watchlist_sources() or {}
    # bytes 转 str
    return {
        (k.decode() if isinstance(k, bytes) else k):
        (v.decode() if isinstance(v, bytes) else v)
        for k, v in raw.items()
    }


# ============================================================
# SNAPSHOT(连续竞价快照)
# ============================================================


def fetch_snapshot_window(ts_code: str, idx_min: float = 0, idx_max: float = 240) -> list[dict]:
    """拉某股 snapshot window 数据(score = idx,通常 0-239)"""
    return OnlineRedis().get_snapshot_window(ts_code, idx_min, idx_max) or []


def fetch_snapshot_history(count: int = 100) -> list[tuple[str, dict]]:
    """拉 snapshot STREAM 历史最新 N 条"""
    return OnlineRedis().get_snapshot_history(count) or []


def fetch_snapshot_stream_since(min_id: str = "-", count: int = 100) -> list[tuple[str, dict]]:
    """拉 snapshot STREAM 自某 ID 之后的所有"""
    r = OnlineRedis()
    try:
        raw = r.r.xrange(f"{PREFIX}:snapshot:stream", min=min_id, count=count)
        return [(sid, r._decode(fields["payload"])) for sid, fields in raw]
    except Exception as e:
        _log.warning(f"fetch_snapshot_stream_since failed: {e}")
        return []


# 2026-09-14 v6.3:snapshot 全市场快照索引接口
def fetch_snapshot_timeline(count: int = 50) -> list[dict]:
    """读 snapshot 全市场快照 timeline(时序索引)"""
    return OnlineRedis().get_snapshot_timeline(count)


def fetch_snapshot_archive_latest() -> dict[str, dict]:
    """读最新一份全市场 snapshot 快照(timeline 末位)"""
    return OnlineRedis().get_snapshot_archive_latest() or {}


def fetch_snapshot_snapshot_at(seconds_before: int) -> dict[str, dict]:
    """读 N 秒前最近的 snapshot 全市场快照"""
    return OnlineRedis().get_snapshot_snapshot_at(seconds_before) or {}


# ============================================================
# ORDERBOOK(5档盘口)
# ============================================================


def fetch_orderbook_window(ts_code: str, idx_min: float = 0, idx_max: float = 240) -> list[dict]:
    """拉某股 orderbook window"""
    return OnlineRedis().get_orderbook_window(ts_code, idx_min, idx_max) or []


def fetch_orderbook_history(count: int = 100) -> list[tuple[str, dict]]:
    """拉 orderbook STREAM 历史最新 N 条"""
    return OnlineRedis().get_orderbook_history(count) or []


# ============================================================
# MINUTE(分时 K 线)
# ============================================================


def fetch_minute_bars(ts_code: str, bar_idx_min: int = 0, bar_idx_max: int = 240) -> list[dict]:
    """拉某股的分钟 K 线(score = bar_idx,通常 0-239)"""
    return OnlineRedis().get_minute_bars(ts_code, bar_idx_min, bar_idx_max) or []


# ============================================================
# AUCTION(集合竞价)
# ============================================================


def fetch_auction_window(ts_code: str, idx_min: float = 0, idx_max: float = 240) -> list[dict]:
    """拉某股 auction window"""
    return OnlineRedis().get_auction_window(ts_code, idx_min, idx_max) or []


def fetch_auction_history(count: int = 100) -> list[tuple[str, dict]]:
    """拉 auction STREAM 历史最新 N 条"""
    return OnlineRedis().get_auction_history(count) or []


# 2026-09-14 v6.2:全市场快照索引接口
def fetch_auction_timeline(count: int = 50) -> list[dict]:
    """拉 auction 全市场快照 timeline 索引(时序)"""
    return OnlineRedis().get_auction_timeline(count) or []


def fetch_auction_archive_latest() -> dict[str, dict]:
    """拉最新一份全市场快照(替代 N 次 fetch_auction_window)"""
    return OnlineRedis().get_auction_archive_latest() or {}


def fetch_auction_snapshot_at(seconds_before: int) -> dict[str, dict]:
    """拉 N 秒前最近一份全市场快照"""
    return OnlineRedis().get_auction_snapshot_at(seconds_before) or {}


# ============================================================
# ZT(涨停池)— MyATM 风格(timeline + archive HSET,2026-09-14 v6)
# 接口列表:
#   1. fetch_zt_stream         落盘用 STREAM 拉取
#   2. fetch_zt_timeline       拉 timeline 索引
#   3. fetch_zt_archive_latest 拉最新全量快照
#   4. fetch_zt_snapshot_at    按 timeline 拉 N 秒前快照
# ============================================================


def fetch_zt_archive_latest() -> dict[str, dict]:
    """拉涨停池最新全量快照

    Returns:
        {ts_code: row_dict, ...}  最新一份 snapshot
    """
    return OnlineRedis().get_zt_archive_latest() or {}


def fetch_zt_snapshot_at(seconds_before: float) -> dict[str, dict]:
    """拉 N 秒前涨停快照全量

    Args:
        seconds_before: 多少秒前的 snapshot

    Returns:
        {ts_code: row_dict, ...}  N 秒前最近一份 snapshot
    """
    return OnlineRedis().get_zt_snapshot_at(seconds_before) or {}


def fetch_zt_timeline(count: int = 50) -> list[dict]:
    """拉涨停池 timeline 索引

    Returns:
        [{archive_key, ts}, ...]  从最新到最旧
    """
    return OnlineRedis().get_zt_timeline(count=count) or []


def fetch_zt_stream(count: int = 100) -> list[tuple[str, dict]]:
    """拉涨停池 STREAM 最新 N 条

    v3.1 重构后字段展开(不再有 payload 字段),用基类 _read_stream 兼容
    """
    r = OnlineRedis()
    try:
        return r.get_zt_stream(count=count)
    except Exception as e:
        _log.warning(f"fetch_zt_stream failed: {e}")
        return []


def fetch_zt_stream_since(min_id: str = "-", count: int = 100) -> list[tuple[str, dict]]:
    """拉涨停池 STREAM 自某 ID 之后的所有

    v3.1 重构后字段展开(不再有 payload 字段)
    """
    r = OnlineRedis()
    try:
        raw = r.r.xrange(f"{PREFIX}:zt:stream", min=min_id, count=count)
        # v3.1:字段展开,直接 _decode 整个 fields dict
        return [(sid, r._decode(dict(fields))) for sid, fields in raw]
    except Exception as e:
        _log.warning(f"fetch_zt_stream_since failed: {e}")
        return []


# ============================================================
# BREAK(炸板池)
# ============================================================


def fetch_break_stream(count: int = 100) -> list[tuple[str, dict]]:
    """拉炸板池 STREAM 最新 N 条(用基类 _read_stream 兼容 v3.1 字段展开)"""
    return OnlineRedis().get_break_stream(count=count)


def fetch_break_timeline(count: int = 50) -> list[dict]:
    """拉炸板 timeline ZSet 索引"""
    return OnlineRedis().get_break_timeline(count=count)


def fetch_break_archive_latest() -> dict[str, dict]:
    """拉炸板最新一份快照"""
    return OnlineRedis().get_break_archive_latest()


def fetch_break_snapshot_at(seconds_before: int) -> dict[str, dict]:
    """拉炸板 N 秒前最近一份快照"""
    return OnlineRedis().get_break_snapshot_at(seconds_before)


# ============================================================
# ANOMALY(异动清单)
# ============================================================


def fetch_anomaly_stream(count: int = 100) -> list[tuple[str, dict]]:
    """拉异动 STREAM 最新 N 条(用基类 _read_stream 兼容 v3.1 字段展开)"""
    return OnlineRedis().get_anomaly_stream(count=count)


def fetch_anomaly_timeline(count: int = 50) -> list[dict]:
    """拉异动 timeline ZSet 索引"""
    return OnlineRedis().get_anomaly_timeline(count=count)


def fetch_anomaly_archive_latest() -> dict[str, dict]:
    """拉异动最新一份快照"""
    return OnlineRedis().get_anomaly_archive_latest()


def fetch_anomaly_snapshot_at(seconds_before: int) -> dict[str, dict]:
    """拉异动 N 秒前最近一份快照"""
    return OnlineRedis().get_anomaly_snapshot_at(seconds_before)


# ============================================================
# HOT(热股榜)
# ============================================================


def fetch_hot_stream(count: int = 100) -> list[tuple[str, dict]]:
    """拉热股 STREAM 最新 N 条(用基类 _read_stream 兼容 v3.1 字段展开)"""
    return OnlineRedis().get_hot_stream(count=count)


def fetch_hot_timeline(count: int = 50) -> list[dict]:
    """拉热股 timeline ZSet 索引"""
    return OnlineRedis().get_hot_timeline(count=count)


def fetch_hot_archive_latest() -> dict[str, dict]:
    """拉热股最新一份快照"""
    return OnlineRedis().get_hot_archive_latest()


def fetch_hot_snapshot_at(seconds_before: int) -> dict[str, dict]:
    """拉热股 N 秒前最近一份快照"""
    return OnlineRedis().get_hot_snapshot_at(seconds_before)


# ============================================================
# LIMITPERFORMANCE(涨停表现详情)v6 MyATM 风格(2026-09-14 新增)
# ============================================================
def fetch_limitperformance_stream(count: int = 1000) -> list[tuple[str, dict]]:
    """拉涨停表现详情 STREAM 最新 N 条(用基类 _read_stream 兼容 v3.1 字段展开)"""
    return OnlineRedis().get_limitperformance_stream(count=count)


def fetch_limitperformance_timeline(count: int = 50) -> list[dict]:
    """拉涨停表现详情 timeline ZSet 索引"""
    return OnlineRedis().get_limitperformance_timeline(count=count)


def fetch_limitperformance_archive_latest() -> dict[str, dict]:
    """拉涨停表现详情最新一份快照"""
    return OnlineRedis().get_limitperformance_archive_latest()


def fetch_limitperformance_snapshot_at(seconds_before: int) -> dict[str, dict]:
    """拉涨停表现详情 N 秒前最近一份快照"""
    return OnlineRedis().get_limitperformance_snapshot_at(seconds_before)


# ============================================================
# META
# ============================================================


def fetch_meta() -> dict[str, str]:
    """拉 online:meta HASH 全部 field"""
    r = OnlineRedis()
    raw = r.r.hgetall(f"{PREFIX}:meta") or {}
    result = {}
    for k, v in raw.items():
        ks = k.decode() if isinstance(k, bytes) else k
        vs = v.decode() if isinstance(v, bytes) else v
        result[ks] = vs
    return result


def fetch_meta_key(key: str) -> str | None:
    """拉 meta 单个 field"""
    r = OnlineRedis()
    val = r.r.hget(f"{PREFIX}:meta", key)
    if val is None:
        return None
    return val.decode() if isinstance(val, bytes) else str(val)


# ============================================================
# CLI 自测
# ============================================================


if __name__ == "__main__":
    print("=== WATCHLIST ===")
    print(f"  ts_codes count: {len(fetch_watchlist())}")
    print(f"  sources count: {len(fetch_watchlist_sources())}")

    print("\n=== SNAPSHOT ===")
    print(f"  600519.SH window: {len(fetch_snapshot_window('600519.SH'))} bars")

    print("\n=== MINUTE ===")
    bars = fetch_minute_bars("600519.SH")
    print(f"  600519.SH minute: {len(bars)} bars")

    print("\n=== ZT ===")
    print(f"  timeline: {len(fetch_zt_timeline(count=5))} 个 archive")
    print(f"  archive latest: {len(fetch_zt_archive_latest())} 只")
    print(f"  stream latest 3: {len(fetch_zt_stream(3))} 条")

    print("\n=== META (前 5 个) ===")
    for i, (k, v) in enumerate(fetch_meta().items()):
        if i >= 5:
            break
        print(f"  {k} = {v}")
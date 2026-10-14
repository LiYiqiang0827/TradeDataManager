"""
check_redis: Redis 状态检查工具类(只读)

目的:
    给 AI Agent 提供"检查 Redis online: 数据条目状态"的统一接口。
    只检查,不返回具体数据 —— 拉数据请用 query_redis。

API 列表(按数据条目):
    WATCHLIST       check_watchlist()                    -> 总数 + yest/prev_lb 分布
    SNAPSHOT        check_snapshot() / check_snapshot_for_stock(ts_code)
    ORDERBOOK       check_orderbook() / check_orderbook_for_stock(ts_code)
    MINUTE          check_minute() / check_minute_for_stock(ts_code)
    AUCTION         check_auction() / check_auction_for_stock(ts_code)
    ZT(涨停池)      check_zt()
    BREAK(炸板池)   check_break()
    ANOMALY(异动)   check_anomaly()
    HOT(热股榜)     check_hot()
    META            check_meta()
    CURSOR          check_cursor(stream_key)
    ALL KEYS        check_all_keys()
    OVERVIEW        check_overview()                    -> 一键总览

只读:无任何 SET/HSET/DEL/XADD/ZADD。
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
from coreClient.redis_client import META_LAST_ID_PREFIX

_log = logging.getLogger("check_redis")


# ============================================================
# 工具函数
# ============================================================


def _decode_meta_value(v):
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode()
    return str(v)


def _decode_meta_dict(d):
    return {
        (k.decode() if isinstance(k, bytes) else k): _decode_meta_value(v)
        for k, v in d.items()
    }


def _type_str(r, k):
    t = r.r.type(k)
    return t.decode() if isinstance(t, bytes) else t


# ============================================================
# WATCHLIST
# ============================================================


def check_watchlist() -> dict[str, Any]:
    """watchlist 当日状态(2026-09-13 v4:适配双数据源)

    source 命名:
      - latest_limit_lb / latest_limit:最新日涨停(limit_performance 数据源)
      - latest_list_lb / latest_list:    最新日涨停(list 数据源)
      - prev_lianban:                    历史窗口连板
      - latest_*+prev:                   最新日+历史连板(合并)
    """
    try:
        r = OnlineRedis()
        codes = r.get_watchlist() or []
        sources = r.get_watchlist_sources() or {}

        # 按新 source 分类统计
        latest_count = sum(1 for v in sources.values() if v.startswith("latest_"))
        prev_count = sum(1 for v in sources.values()
                         if v == "prev_lianban" or v.endswith("+prev"))
        latest_lb_count = sum(1 for v in sources.values()
                              if "_lb" in v or v.startswith("latest_") and "_lb" in v)
        prev_lb_count = sum(1 for v in sources.values()
                            if v == "prev_lianban" or v.endswith("+prev"))

        # 兼容旧的 'yest' / 'prev_lianban' 命名(平滑迁移)
        old_yest_count = sum(1 for v in sources.values() if "yest" in v)
        if old_yest_count > 0 and latest_count == 0:
            latest_count = old_yest_count
            latest_lb_count = old_yest_count  # 旧版全当最新日

        return {
            "count": len(codes),
            "sources_count": len(sources),
            "latest_count": latest_count,
            "latest_lb_count": latest_lb_count,
            "prev_lb_count": prev_lb_count,
            "consistent": len(codes) == len(sources),
            "latest_date": r.get_meta("watchlist_latest_date"),
            "latest_source": r.get_meta("watchlist_latest_source"),
            "prev_window": r.get_meta("watchlist_prev_window"),
        }
    except Exception as e:
        _log.warning(f"check_watchlist failed: {e}")
        return {"_error": str(e), "count": 0}


def check_watchlist_three() -> dict[str, Any]:
    """watchlist 三件套细分检查(2026-09-15 v6.7 新增)

    对齐 check_zt() 风格:stream + timeline + archive:{ts} 全覆盖
    - stream:     online:watchlist:stream        XLEN + TTL
    - timeline:   online:watchlist:timeline      ZCARD + TTL + 最新 member
    - archive:    online:watchlist:archive:*     KEY 数 + 最新 HASH hsize + TTL
    """
    try:
        r = OnlineRedis()
        prefix = f"{PREFIX}:watchlist"
        stream_key = f"{prefix}:stream"
        timeline_key = f"{prefix}:timeline"

        # stream
        stream_len = int(r.r.xlen(stream_key) or 0)
        stream_ttl = int(r.r.ttl(stream_key))

        # timeline
        timeline_size = int(r.r.zcard(timeline_key) or 0)
        timeline_ttl = int(r.r.ttl(timeline_key))
        latest_member = ""
        latest_ts = ""
        if timeline_size > 0:
            top = r.r.zrevrange(timeline_key, 0, 0, withscores=True)
            if top:
                latest_member = top[0][0].decode() if isinstance(top[0][0], bytes) else top[0][0]
                latest_ts = top[0][1]

        # archive keys
        archive_keys = sorted(
            [k.decode() if isinstance(k, bytes) else k for k in (r.r.keys(f"{prefix}:archive:*") or [])]
        )
        archive_count = len(archive_keys)
        latest_archive_key = archive_keys[-1] if archive_keys else ""
        latest_archive_size = (
            int(r.r.hlen(latest_archive_key)) if latest_archive_key else 0
        )
        latest_archive_ttl = (
            int(r.r.ttl(latest_archive_key)) if latest_archive_key else 0
        )

        # 用 OnlineRedis 公开接口拿 codes + sources(避免猜测 HASH 字段结构)
        codes = r.get_watchlist() or []
        sources = r.get_watchlist_sources() or {}
        source_dist = {}
        for v in sources.values():
            source_dist[v] = source_dist.get(v, 0) + 1

        return {
            "stream_length": stream_len,
            "stream_ttl": stream_ttl,
            "timeline_size": timeline_size,
            "timeline_ttl": timeline_ttl,
            "timeline_latest_member": latest_member,
            "timeline_latest_ts": latest_ts,
            "archive_count": archive_count,
            "archive_keys": archive_keys,
            "latest_archive_key": latest_archive_key,
            "latest_archive_size": latest_archive_size,
            "latest_archive_ttl": latest_archive_ttl,
            "codes_count": len(codes),
            "sources_count": len(sources),
            "source_dist": source_dist,
            "consistent": stream_len >= latest_archive_size if latest_archive_size else True,
        }
    except Exception as e:
        _log.warning(f"check_watchlist_three failed: {e}")
        return {"_error": str(e)}


# ============================================================
# SNAPSHOT
# ============================================================


def check_snapshot() -> dict[str, Any]:
    """snapshot 全局状态(2026-09-14 v6.3:加 timeline + archive 字段)"""
    try:
        r = OnlineRedis()
        stream_len = r.r.xlen(f"{PREFIX}:snapshot:stream")
        try:
            last = r.r.xrevrange(f"{PREFIX}:snapshot:stream", count=1)
            last_id = last[0][0] if last else ""
        except Exception:
            last_id = ""
        try:
            first = r.r.xrange(f"{PREFIX}:snapshot:stream", min="-", max="+", count=1)
            first_id = first[0][0] if first else ""
        except Exception:
            first_id = ""

        all_keys = r.r.keys(f"{PREFIX}:snapshot:window:*") or []
        window_total = 0
        for k in all_keys:
            try:
                window_total += r.r.zcard(k)
            except Exception:
                pass

        # v6.3 全市场快照索引
        timeline_size = r.r.zcard(f"{PREFIX}:snapshot:timeline") or 0
        timeline_ttl = r.r.ttl(f"{PREFIX}:snapshot:timeline") or -2
        latest_archive_key = ""
        latest_archive_size = 0
        latest_archive_ttl = -2
        try:
            latest = r.r.zrevrange(f"{PREFIX}:snapshot:timeline", 0, 0)
            if latest:
                latest_archive_key = latest[0].decode() if isinstance(latest[0], bytes) else latest[0]
                latest_archive_size = r.r.hlen(latest_archive_key) or 0
                latest_archive_ttl = r.r.ttl(latest_archive_key) or -2
        except Exception:
            pass

        return {
            "stream_length": stream_len,
            "stream_first_id": first_id,
            "stream_last_id": last_id,
            "window_key_count": len(all_keys),
            "window_total_size": window_total,
            # v6.3 全市场快照索引
            "timeline_size": timeline_size,
            "timeline_ttl": timeline_ttl,
            "latest_archive_key": latest_archive_key,
            "latest_archive_size": latest_archive_size,
            "latest_archive_ttl": latest_archive_ttl,
        }
    except Exception as e:
        _log.warning(f"check_snapshot failed: {e}")
        return {"_error": str(e)}


def check_snapshot_for_stock(ts_code: str) -> dict[str, Any]:
    """snapshot 单股 window 状态"""
    try:
        r = OnlineRedis()
        key = f"{PREFIX}:snapshot:window:{ts_code}"
        exists = r.r.exists(key) > 0
        if not exists:
            return {"ts_code": ts_code, "key": key, "exists": False}
        return {
            "ts_code": ts_code,
            "key": key,
            "exists": True,
            "size": r.r.zcard(key),
            "ttl": r.r.ttl(key),
        }
    except Exception as e:
        _log.warning(f"check_snapshot_for_stock failed: {e}")
        return {"_error": str(e), "ts_code": ts_code}


# ============================================================
# ORDERBOOK
# ============================================================


def check_orderbook() -> dict[str, Any]:
    """orderbook 全局状态"""
    try:
        r = OnlineRedis()
        stream_len = r.r.xlen(f"{PREFIX}:orderbook:stream")
        all_keys = r.r.keys(f"{PREFIX}:orderbook:window:*") or []
        window_total = 0
        for k in all_keys:
            try:
                window_total += r.r.zcard(k)
            except Exception:
                pass
        return {
            "stream_length": stream_len,
            "window_key_count": len(all_keys),
            "window_total_size": window_total,
        }
    except Exception as e:
        _log.warning(f"check_orderbook failed: {e}")
        return {"_error": str(e)}


def check_orderbook_for_stock(ts_code: str) -> dict[str, Any]:
    """orderbook 单股 window 状态"""
    try:
        r = OnlineRedis()
        key = f"{PREFIX}:orderbook:window:{ts_code}"
        exists = r.r.exists(key) > 0
        if not exists:
            return {"ts_code": ts_code, "key": key, "exists": False}
        return {
            "ts_code": ts_code,
            "key": key,
            "exists": True,
            "size": r.r.zcard(key),
            "ttl": r.r.ttl(key),
        }
    except Exception as e:
        _log.warning(f"check_orderbook_for_stock failed: {e}")
        return {"_error": str(e), "ts_code": ts_code}


# ============================================================
# MINUTE
# ============================================================


def check_minute() -> dict[str, Any]:
    """minute 全局状态"""
    try:
        r = OnlineRedis()
        all_keys = r.r.keys(f"{PREFIX}:minute:*") or []
        total_bars = 0
        for k in all_keys:
            try:
                total_bars += r.r.zcard(k)
            except Exception:
                pass
        return {
            "key_count": len(all_keys),
            "bar_count_total": total_bars,
            "sample_keys": sorted(all_keys)[:5],
        }
    except Exception as e:
        _log.warning(f"check_minute failed: {e}")
        return {"_error": str(e)}


def check_minute_for_stock(ts_code: str) -> dict[str, Any]:
    """minute 单股状态"""
    try:
        r = OnlineRedis()
        key = f"{PREFIX}:minute:{ts_code}"
        exists = r.r.exists(key) > 0
        if not exists:
            return {"ts_code": ts_code, "key": key, "exists": False}
        size = r.r.zcard(key)
        all_raw = r.r.zrange(key, 0, -1, withscores=True)
        if all_raw:
            score_min = int(all_raw[0][1])
            score_max = int(all_raw[-1][1])
        else:
            score_min = score_max = 0
        return {
            "ts_code": ts_code,
            "key": key,
            "exists": True,
            "bar_count": size,
            "ttl": r.r.ttl(key),
            "score_min": score_min,
            "score_max": score_max,
        }
    except Exception as e:
        _log.warning(f"check_minute_for_stock failed: {e}")
        return {"_error": str(e), "ts_code": ts_code}


# ============================================================
# AUCTION
# ============================================================


def check_auction() -> dict[str, Any]:
    """auction 全局状态(2026-09-14 v6.2:加 timeline + archive 字段)"""
    try:
        r = OnlineRedis()
        all_keys = r.r.keys(f"{PREFIX}:auction:window:*") or []
        window_total = 0
        for k in all_keys:
            try:
                window_total += r.r.zcard(k)
            except Exception:
                pass

        # v6.2 timeline + archive
        timeline_key = f"{PREFIX}:auction:timeline"
        timeline_size = r.r.zcard(timeline_key)
        timeline_ttl = r.r.ttl(timeline_key)
        latest = r.r.zrevrange(timeline_key, 0, 0)
        latest_archive_key = latest[0].decode() if latest and isinstance(latest[0], bytes) else (latest[0] if latest else None)
        latest_archive_size = r.r.hlen(latest_archive_key) if latest_archive_key else 0
        latest_archive_ttl = r.r.ttl(latest_archive_key) if latest_archive_key else -2

        return {
            "window_key_count": len(all_keys),
            "window_total_size": window_total,
            # v6.2 新增
            "timeline_size": timeline_size,
            "timeline_ttl": timeline_ttl,
            "latest_archive_key": latest_archive_key,
            "latest_archive_size": latest_archive_size,
            "latest_archive_ttl": latest_archive_ttl,
            "stream_length": r.r.xlen(f"{PREFIX}:auction:stream"),
        }
    except Exception as e:
        _log.warning(f"check_auction failed: {e}")
        return {"_error": str(e)}


def check_auction_for_stock(ts_code: str) -> dict[str, Any]:
    """auction 单股 window 状态"""
    try:
        r = OnlineRedis()
        key = f"{PREFIX}:auction:window:{ts_code}"
        exists = r.r.exists(key) > 0
        if not exists:
            return {"ts_code": ts_code, "key": key, "exists": False}
        return {
            "ts_code": ts_code,
            "key": key,
            "exists": True,
            "size": r.r.zcard(key),
            "ttl": r.r.ttl(key),
        }
    except Exception as e:
        _log.warning(f"check_auction_for_stock failed: {e}")
        return {"_error": str(e), "ts_code": ts_code}


# ============================================================
# ZT
# ============================================================


def check_zt() -> dict[str, Any]:
    """涨停池全局状态(MyATM 风格:timeline + archive HSET,2026-09-14 v6)

    v6 改动:
        - 删 pool_size(SET 已废)
        - 加 timeline_size + latest_archive_key + latest_archive_size
    """
    try:
        r = OnlineRedis()
        timeline = r.get_zt_timeline(count=1)
        latest_archive_key = timeline[0]["archive_key"] if timeline else ""
        latest_archive_size = (
            len(r.r.hgetall(latest_archive_key)) if latest_archive_key else 0
        )
        return {
            "timeline_size": r.r.zcard(f"{PREFIX}:zt:timeline"),
            "latest_archive_key": latest_archive_key,
            "latest_archive_size": latest_archive_size,
            "timeline_ttl": r.r.ttl(f"{PREFIX}:zt:timeline"),
            "stream_length": r.r.xlen(f"{PREFIX}:zt:stream"),
        }
    except Exception as e:
        _log.warning(f"check_zt failed: {e}")
        return {"_error": str(e)}


# ============================================================
# LIMITPERFORMANCE(2026-09-14 v5 新增)
# ============================================================


def check_limitperformance() -> dict[str, Any]:
    """涨停表现详情实时状态(v6 MyATM 风格:timeline_size + latest_archive_size)

    数据源:kpl_client.fetch_realtime_limit_performance(apphwhq 实时 host,30s/轮)
    2026-09-14 v6 改 MyATM:HSET archive + ZSet timeline + STREAM
    预期:盘中实时 48 行/轮左右,9:14-9:25 多为 0,9:25 后逐渐累积
    """
    try:
        r = OnlineRedis()
        timeline = r.get_limitperformance_timeline(count=1)
        latest_key = timeline[0]["archive_key"] if timeline else None
        return {
            "timeline_size": len(r.get_limitperformance_timeline(count=500)),
            "latest_archive_key": latest_key,
            "latest_archive_size": (len(r.get_limitperformance_archive_latest()) if latest_key else 0),
            "latest_archive_ttl": (int(r.r.ttl(latest_key)) if latest_key else -2),
            "timeline_ttl": int(r.r.ttl(f"{PREFIX}:limitperformance:timeline")),
            "stream_length": r.get_limitperformance_xlen(),
            "trade_date_meta": r.get_meta("limitperformance_trade_date"),
        }
    except Exception as e:
        _log.warning(f"check_limitperformance failed: {e}")
        return {"_error": str(e)}


# ============================================================
# BREAK
# ============================================================


def check_break() -> dict[str, Any]:
    """炸板池全局状态(v6 MyATM 风格:timeline_size + latest_archive_size)"""
    try:
        r = OnlineRedis()
        timeline = r.get_break_timeline(count=1)
        latest_key = timeline[0]["archive_key"] if timeline else None
        latest_size = r.r.hlen(latest_key) if latest_key else 0
        return {
            "timeline_size": r.r.zcard(f"{PREFIX}:break:timeline"),
            "latest_archive_key": latest_key,
            "latest_archive_size": latest_size,
            "latest_archive_ttl": r.r.ttl(latest_key) if latest_key else -2,
            "timeline_ttl": r.r.ttl(f"{PREFIX}:break:timeline"),
            "stream_length": r.r.xlen(f"{PREFIX}:break:stream"),
        }
    except Exception as e:
        _log.warning(f"check_break failed: {e}")
        return {"_error": str(e)}


# ============================================================
# ANOMALY
# ============================================================


def check_anomaly() -> dict[str, Any]:
    """异动清单全局状态(v6 MyATM 风格:timeline_size + latest_archive_size)"""
    try:
        r = OnlineRedis()
        timeline = r.get_anomaly_timeline(count=1)
        latest_key = timeline[0]["archive_key"] if timeline else None
        latest_size = r.r.hlen(latest_key) if latest_key else 0
        return {
            "timeline_size": r.r.zcard(f"{PREFIX}:anomaly:timeline"),
            "latest_archive_key": latest_key,
            "latest_archive_size": latest_size,
            "latest_archive_ttl": r.r.ttl(latest_key) if latest_key else -2,
            "timeline_ttl": r.r.ttl(f"{PREFIX}:anomaly:timeline"),
            "stream_length": r.r.xlen(f"{PREFIX}:anomaly:stream"),
        }
    except Exception as e:
        _log.warning(f"check_anomaly failed: {e}")
        return {"_error": str(e)}


# ============================================================
# HOT
# ============================================================


def check_hot() -> dict[str, Any]:
    """热股榜全局状态(v6 MyATM 风格:timeline_size + latest_archive_size)"""
    try:
        r = OnlineRedis()
        timeline = r.get_hot_timeline(count=1)
        latest_key = timeline[0]["archive_key"] if timeline else None
        latest_size = r.r.hlen(latest_key) if latest_key else 0
        return {
            "timeline_size": r.r.zcard(f"{PREFIX}:hot:timeline"),
            "latest_archive_key": latest_key,
            "latest_archive_size": latest_size,
            "latest_archive_ttl": r.r.ttl(latest_key) if latest_key else -2,
            "timeline_ttl": r.r.ttl(f"{PREFIX}:hot:timeline"),
            "stream_length": r.r.xlen(f"{PREFIX}:hot:stream"),
        }
    except Exception as e:
        _log.warning(f"check_hot failed: {e}")
        return {"_error": str(e)}


# ============================================================
# META
# ============================================================


def check_meta() -> dict[str, str]:
    """online:meta HASH 全部 field"""
    try:
        r = OnlineRedis()
        raw = r.r.hgetall(f"{PREFIX}:meta") or {}
        return _decode_meta_dict(raw)
    except Exception as e:
        _log.warning(f"check_meta failed: {e}")
        return {"_error": str(e)}


# ============================================================
# CURSOR
# ============================================================


def check_cursor(stream_key: str) -> dict[str, Any]:
    """某个 STREAM 的 meta 游标 + 实际最新 ID"""
    try:
        r = OnlineRedis()
        short_name = stream_key.replace(PREFIX + ":", "")
        meta_id = r.get_meta(f"{META_LAST_ID_PREFIX}{short_name}")
        try:
            raw = r.r.xrevrange(stream_key, count=1)
            stream_id = raw[0][0] if raw else ""
        except Exception:
            stream_id = ""
        return {
            "stream_key": stream_key,
            "meta_last_id": meta_id,
            "stream_last_id": stream_id,
        }
    except Exception as e:
        _log.warning(f"check_cursor failed: {e}")
        return {"_error": str(e), "stream_key": stream_key}


# ============================================================
# ALL KEYS
# ============================================================


def check_all_keys(pattern: str = "online:*") -> list[dict]:
    """列出所有 online: 前缀 key 的元信息"""
    try:
        r = OnlineRedis()
        keys = sorted(r.r.keys(pattern) or [])
        results = []
        for k in keys:
            try:
                ktype = _type_str(r, k)
                ttl = r.r.ttl(k)
                entry = {"key": k, "type": ktype, "ttl": ttl}
                if ktype == "string":
                    entry["length"] = 0
                elif ktype == "list":
                    entry["length"] = r.r.llen(k)
                elif ktype == "set":
                    entry["cardinality"] = r.r.scard(k)
                elif ktype == "zset":
                    entry["cardinality"] = r.r.zcard(k)
                elif ktype == "hash":
                    entry["fields"] = len(r.r.hkeys(k))
                elif ktype == "stream":
                    entry["length"] = r.r.xlen(k)
                results.append(entry)
            except Exception as ex:
                results.append({"key": k, "_error": str(ex)})
        return results
    except Exception as e:
        _log.warning(f"check_all_keys failed: {e}")
        return [{"_error": str(e)}]


# ============================================================
# OVERVIEW
# ============================================================


def check_overview() -> dict[str, Any]:
    """一键总览"""
    return {
        "watchlist": check_watchlist(),
        "snapshot": check_snapshot(),
        "orderbook": check_orderbook(),
        "minute": check_minute(),
        "auction": check_auction(),
        "zt": check_zt(),
        "break": check_break(),
        "anomaly": check_anomaly(),
        "hot": check_hot(),
        "limitperformance": check_limitperformance(),    # 2026-09-14 v5 新增
        "meta": check_meta(),
    }


if __name__ == "__main__":
    import pprint
    pprint.pprint(check_overview(), width=120)
"""
core/redis_online.py
====================

onlineDataManager Redis 业务层:继承 coreClient.RedisBase,组合通用模板实现 8 类数据 put_*/get_*。

架构(2026-09-12 v3 + coreClient 架构调整):
    ~/TradingAgent/coreClient/redis_client.py   ← RedisBase(连接 + JSON + meta + 通用 STREAM/ZSET/SET/LIST/HASH 模板)— 跨业务共用
    ~/TradingAgent/coreClient/redis_config.py   ← 连接参数 + 通用配置常量(无业务前缀)
    ~/TradingAgent/onlineDataManager/scripts/core/redis_online.py  ← 本文件(onlineDataManager 业务层)

业务层职责:
    - 业务常量:PREFIX = "online" + 各 kind 的 TTL/MAXLEN(放业务侧,不放 redis_config)
    - 业务游标:get_last_stream_id(基于基类 set_meta)
    - 8 类业务方法(watchlist/auction/snapshot/orderbook/minute/zt/break/anomaly/hot)只调基类通用模板
    - 业务层不直接调 pipe.sadd / pipe.lpush / pipe.xadd / pipe.zadd / pipe.expire 等底层 redis 命令

时间戳约定(2026-09-12 v3):
    数据时间戳:每 kind 用独立字段名(<kind>_timestamp,如 auction_timestamp / zt_timestamp)
    落盘时间戳:由 SQLite DEFAULT (datetime('now', 'localtime')) 自动加,Redis 不存
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

# 让 from coreClient.redis_client import ... 工作
# onlineDataManager/scripts/core/redis_online.py → 上 3 层 = ~/TradingAgent/
_HERE = Path(__file__).resolve().parent
_PROJ = _HERE.parent.parent.parent
if str(_PROJ) not in sys.path:
    sys.path.insert(0, str(_PROJ))

from coreClient.redis_client import (
    DEFAULT_DROP_KEYS,
    DEFAULT_STREAM_TTL,
    META_LAST_ID_PREFIX,
    RedisBase,
)


# ============================================================
# onlineDataManager 业务常量(2026-09-12 v3)
# ============================================================
PREFIX = "online"   # 所有 key 前缀

# Window TTL(秒)
WINDOW_TTL_AUCTION = 86400          # 1 天(auction 是全天窗口)
# v6.2(2026-09-14):auction 全市场快照索引 — 不滑窗,整体 12h 兜底过期
AUCTION_TIMELINE_TTL = 12 * 3600   # 43200s,timeline ZSet 整体过期
AUCTION_ARCHIVE_TTL = 12 * 3600    # 43200s,archive HSET 各自过期(跟 timeline 一致)
WINDOW_TTL_SNAPSHOT = 12 * 3600      # v6.3(2026-09-14):单股 window 12h 兜底(滑动清30min前的)
# v6.3 snapshot 全市场快照索引(2026-09-14)— 不滑窗兜底,滑窗由 commit 主动裁剪
SNAPSHOT_TIMELINE_TTL = 12 * 3600   # 43200s,timeline ZSet 整体 12h 兜底
SNAPSHOT_ARCHIVE_TTL_BASE = 30 * 60 # 1800s,普通时段 30min 过期
SNAPSHOT_ARCHIVE_TTL_LUNCH = 7200   # 11:00-11:30 写入时用,跨午休 2h 兜底
SNAPSHOT_SLIDE_WINDOW_BASE = 30 * 60  # 1800s,timeline 滑窗裁剪窗口
SNAPSHOT_SLIDE_WINDOW_LUNCH = 7200    # 11:00-11:30 写入时,timeline 滑窗扩到 2h
WINDOW_TTL_ORDERBOOK = 3 * 60 + 30 # 3 分钟滑窗 + 30s 余量
WINDOW_TTL_MINUTE = 86400          # 1 天(分钟线整天保留)
WINDOW_TTL_WATCHLIST = 86400 * 7   # 7 天

# ZT MyATM 风格(timeline + archive)TTL(秒)
# 2026-09-14 v6:抄 MyATM 设计,SET 改 HSET+ZSet
ZT_ARCHIVE_TTL = 300          # archive HSET 各自 5 分钟过期
ZT_TIMELINE_TTL = 12 * 3600   # timeline ZSet 整体 12 小时兜底过期
ZT_TIMELINE_PRUNE_SCORE = 10 * 60  # 滑窗 10 分钟(每次写入 ZREMRANGEBYSCORE 主动删老 member)

# BREAK MyATM 风格 TTL(秒)(2026-09-14 v6 — 用户拍板 archive TTL = 300s)
BREAK_ARCHIVE_TTL = 300            # archive HSET 各自 5 分钟过期
BREAK_TIMELINE_TTL = 12 * 3600     # timeline ZSet 整体 12 小时兜底过期
BREAK_TIMELINE_PRUNE_SCORE = 10 * 60   # 滑窗 10 分钟(同 zt)

# HOT MyATM 风格 TTL(秒)(2026-09-14 v6 — 用户拍板 archive TTL = 1200s/20min)
HOT_ARCHIVE_TTL = 20 * 60          # archive HSET 各自 20 分钟过期
HOT_TIMELINE_TTL = 12 * 3600       # timeline ZSet 整体 12 小时兜底过期
HOT_TIMELINE_PRUNE_SCORE = 10 * 60 # 滑窗 10 分钟

# ANOMALY MyATM 风格 TTL(秒)(2026-09-14 v6 — 用户拍板 archive TTL = 1200s/20min)
ANOMALY_ARCHIVE_TTL = 20 * 60      # archive HSET 各自 20 分钟过期
ANOMALY_TIMELINE_TTL = 12 * 3600   # timeline ZSet 整体 12 小时兜底过期
ANOMALY_TIMELINE_PRUNE_SCORE = 10 * 60  # 滑窗 10 分钟

# LIMITPERFORMANCE MyATM 风格 TTL(秒)(2026-09-14 v6 — 同 zt:30s/轮)
LIMITPERFORMANCE_ARCHIVE_TTL = 300   # archive HSET 各自 5 分钟过期
LIMITPERFORMANCE_TIMELINE_TTL = 12 * 3600  # timeline ZSet 整体 12 小时兜底过期
LIMITPERFORMANCE_TIMELINE_PRUNE_SCORE = 10 * 60  # 滑窗 10 分钟

# STREAM MAXLEN(防爆)
STREAM_MAXLEN_AUCTION = 50000      # 集合竞价(全天写,限长)
STREAM_MAXLEN_SNAPSHOT = 200000    # 行情快照(高频)
STREAM_MAXLEN_ORDERBOOK = 100000   # 5档盘口(高频)
STREAM_MAXLEN_ZT = 5000            # 涨停/炸板/异动/热股(低频)
STREAM_MAXLEN_LIMITPERFORMANCE = 5000   # 涨停表现详情(盘中 30s/轮,累计 30s/次 × 240min ≈ 480 轮/天,× 48 行 ≈ 23k,
                                         # 但绝大多数是同一批股重复,5000 够保存当日;尾盘前会被落盘 service 消费)
STREAM_TTL = DEFAULT_STREAM_TTL    # 6 小时(21600s,真值见 coreClient/redis_config.py L52)

# v6.7(2026-09-15):watchlist 走 STREAM + timeline + archive 三件套(对齐 auction/snapshot)
WATCHLIST_STREAM_TTL = 12 * 3600   # 43200s,STREAM 整体 12h 兜底
WATCHLIST_TIMELINE_TTL = 12 * 3600 # 43200s,timeline ZSet 整体 12h 兜底
WATCHLIST_ARCHIVE_TTL = 12 * 3600  # 43200s,archive HSET 各自 12h 兜底
WATCHLIST_STREAM_MAXLEN = 5000     # watchlist 每天生成 4-5 次 × ~124 只 ≈ 620 条,5000 够

# v6.10 (2026-09-16):snapshot_index 指数行情快照(8 只指数,30s/轮)
# 用户原话:timeline+archive+stream 三件套,stream 12h / timeline 12h / archive 6h
# 数据源:ths_client.fetch_index_snapshot → hithink-finance index snapshot
STREAM_MAXLEN_SNAPSHOT_INDEX = 3000       # 8 只 × 30s 间隔 × 4.5h ≈ 4320,3000 够(走 STREAM_TTL 兜底)
WINDOW_TTL_SNAPSHOT_INDEX = 12 * 3600     # 单只指数 window 12h 兜底(滑动由 timeline 统一管,window 仅兜底)
SNAPSHOT_INDEX_TIMELINE_TTL = 12 * 3600  # timeline ZSet 整体 12h 兜底(用户原话)
SNAPSHOT_INDEX_ARCHIVE_TTL = 6 * 3600    # archive HSET 各自 6h 过期(用户原话)
STREAM_TTL_SNAPSHOT_INDEX = 12 * 3600     # STREAM 整体 12h 兜底(覆盖 DEFAULT_STREAM_TTL=6h)

# ZT/BREAK/ANOMALY/HOT stream maxlen(都用 5000)
POOL_STREAM_MAXLEN = STREAM_MAXLEN_ZT

# LIST 通用最大条数(anomaly / hot 业务读)
LIST_MAX_SIZE = 500


# ============================================================
# 业务 key 拼装快捷(基于 RedisBase.build_key)
# ============================================================
def _k(*parts: str) -> str:
    """业务 key 快捷拼装:build_key(PREFIX, *parts)"""
    return RedisBase.build_key(PREFIX, *parts)


# v6.3(2026-09-14):snapshot archive EXPIRE / timeline 滑窗 N 动态计算
# 11:00-11:30 跨午休补偿:30min 业务 + 90min 午休 = 120min(撑到 13:00 开盘还能读到)
def calc_snapshot_archive_ttl(now_dt) -> int:
    """根据写入时间动态算 snapshot archive EXPIRE(秒)"""
    if now_dt.weekday() >= 5:
        return SNAPSHOT_ARCHIVE_TTL_BASE
    hh, mm = now_dt.hour, now_dt.minute
    if (11, 0) <= (hh, mm) < (11, 30):
        return SNAPSHOT_ARCHIVE_TTL_LUNCH
    return SNAPSHOT_ARCHIVE_TTL_BASE


def calc_snapshot_slide_window(now_dt) -> int:
    """根据写入时间动态算 snapshot timeline 滑窗 N(秒)"""
    if now_dt.weekday() >= 5:
        return SNAPSHOT_SLIDE_WINDOW_BASE
    hh, mm = now_dt.hour, now_dt.minute
    if (11, 0) <= (hh, mm) < (11, 30):
        return SNAPSHOT_SLIDE_WINDOW_LUNCH
    return SNAPSHOT_SLIDE_WINDOW_BASE


# ============================================================
# OnlineRedis 业务层
# ============================================================
class OnlineRedis(RedisBase):
    """onlineDataManager Redis 业务层

    继承 RedisBase,组合通用模板实现 8 类数据 put_*/get_*/window/history。
    类名仍是 OnlineRedis(保持调用方 import 不变)。

    所有 put_xxx 调用方约定:
        ts_code: 股票代码(形如 "000001.SZ")
        data: dict — 含业务字段 + 数据时间戳字段(<kind>_timestamp)
    """

    PREFIX = PREFIX

    # ============================================================
    # 业务游标(各 kind STREAM 的最后消费 id — savedata 落盘用)
    # ============================================================
    def get_last_stream_id(self, kind: str) -> str:
        """获取某 kind STREAM 的最后消费 id(savedata 落盘用)

        默认 '$' = 从最新开始读,不消费历史
        """
        return self.get_meta(f"{META_LAST_ID_PREFIX}{kind}", "$") or "$"

    # ============================================================
    # watchlist(监控列表)— v6.7(2026-09-15)走 STREAM + timeline + archive 三件套
    # ============================================================
    def put_watchlist(self, ts_codes: list[str], sources_map: dict[str, str], watchlist_timestamp: int | float | str) -> str | None:
        """v6.7:写一次 watchlist 全量快照(对齐 auction/snapshot 模板)

        写入 3 个 key:
            online:watchlist:stream    STREAM — 每只 ts_code 一条(field=ts_code, value=encoded data)
                ttl = WATCHLIST_STREAM_TTL(12h)
                maxlen = WATCHLIST_STREAM_MAXLEN(5000)
            online:watchlist:timeline  ZSET  — score=watchlist_timestamp(unix), member=archive_key
                ttl = WATCHLIST_TIMELINE_TTL(12h)
                单次生成只 1 个 archive,12h 内所有历史都在
            online:watchlist:archive:{unix_ts}  HASH — 该次生成的所有 ts_code → encoded data
                ttl = WATCHLIST_ARCHIVE_TTL(12h)

        入参:
            ts_codes:        当次 watchlist 的 ts_code 列表
            sources_map:     {ts_code: source, ...} 每只股的来源(同旧 SET/HASH 语义)
            watchlist_timestamp: 数据时间戳(2026-09-16 v6.14 起:与其它 kind 对齐,推荐传 int 毫秒)
                - int 毫秒(如 1789527000000):直接用
                - float 秒(如 1789527000.123):转 int 毫秒
                - str ISO:反解析(兼容旧调用,新代码别用)

        返回:
            archive_key(供 commit 引用)
        """
        if not ts_codes:
            return None

        # watchlist_timestamp → int 毫秒(v6.14:统一格式;落盘端 sqlite_client._mill_unix_to_iso 转 ISO)
        if isinstance(watchlist_timestamp, (int, float)):
            ms_unix = int(watchlist_timestamp)
            # > 10^12 视为毫秒,否则视为秒
            if ms_unix < 1e12:
                ms_unix = int(ms_unix * 1000)
            ts_unix = ms_unix / 1000.0
        elif isinstance(watchlist_timestamp, str):
            # 兼容旧代码传 ISO 字符串
            try:
                from datetime import datetime as _dt
                ts_unix = _dt.fromisoformat(watchlist_timestamp).timestamp()
            except (ValueError, TypeError):
                ts_unix = time.time()
        else:
            ts_unix = time.time()

        ms = int((ts_unix - int(ts_unix)) * 1000)
        archive_key = _k("watchlist", "archive", f"{int(ts_unix)}.{ms:03d}")
        stream_key = _k("watchlist", "stream")
        timeline_key = _k("watchlist", "timeline")

        flat_records = []
        hset_mapping = {}
        for ts_code in ts_codes:
            src = sources_map.get(ts_code, "")
            item = {
                "ts_code": ts_code,
                "source": src,
                "watchlist_timestamp": watchlist_timestamp,
            }
            flat = self._flat_record(
                ts_code, item,
                drop_keys=DEFAULT_DROP_KEYS,
                ts_field="watchlist_timestamp",
                ts_value=watchlist_timestamp,
            )
            flat_records.append(flat)
            hset_mapping[ts_code] = self._encode(item)

        pipe = self.r.pipeline()
        for flat in flat_records:
            self._xadd_to_stream(
                pipe, stream_key, flat,
                maxlen=WATCHLIST_STREAM_MAXLEN, ttl=WATCHLIST_STREAM_TTL,
            )
        pipe.zadd(timeline_key, {archive_key: ts_unix})
        pipe.expire(timeline_key, WATCHLIST_TIMELINE_TTL)
        pipe.hset(archive_key, mapping=hset_mapping)
        pipe.expire(archive_key, WATCHLIST_ARCHIVE_TTL)
        pipe.execute()
        return archive_key

    def get_watchlist(self) -> list[str]:
        """读监控列表 ts_code 列表(v6.7:读最新 archive 的 field keys)

        兼容签名 — 老 caller(service_writeredis_auction/snapshot/orderbook/minute)不动
        实现:从 archive:<最新 score> 这个 HASH 里读所有 field key
        """
        archive_key = self._get_latest_watchlist_archive_key()
        if not archive_key:
            return []
        keys = self.r.hkeys(archive_key)
        return [k.decode() if isinstance(k, bytes) else k for k in keys]

    def get_watchlist_sources(self) -> dict[str, str]:
        """读监控列表 source 映射(v6.7:读最新 archive 的 HVALUES)

        兼容签名 — 老 caller(check_redis/query_redis)不动
        实现:从 archive:<最新 score> 这个 HASH 里读所有 value 解码
        """
        archive_key = self._get_latest_watchlist_archive_key()
        if not archive_key:
            return {}
        raw = self.r.hgetall(archive_key)
        out = {}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            try:
                decoded = self._decode(val)
            except Exception:
                decoded = {"source": val}
            src = decoded.get("source", "") if isinstance(decoded, dict) else ""
            out[key] = src
        return out

    def _get_latest_watchlist_archive_key(self) -> str | None:
        """内部 helper:timeline 找 score 最大的 archive key(= 最新一次 watchlist 生成)

        行为同 MyATM auction/snapshot:看最新一次 archive 即可拿到当日最新 watchlist
        """
        timeline_key = _k("watchlist", "timeline")
        result = self.r.zrevrange(timeline_key, 0, 0, withscores=True)
        if not result:
            return None
        key = result[0][0]
        return key.decode() if isinstance(key, bytes) else key

    def get_watchlist_stream_cursor(self) -> str:
        """v6.7:savedata 落盘用 — 读 online_stream_cursor 里 watchlist 的 last_id

        默认 '$' = 从最新开始读(首次跑)
        """
        return self.get_last_stream_id("watchlist")

    def read_watchlist_after(self, last_id: str, count: int = 100) -> list[tuple[str, dict]]:
        """v6.7:savedata 落盘用 — XREAD watchlist:stream > last_id

        Returns:
            [(stream_id, flat_dict), ...]
        """
        stream_key = _k("watchlist", "stream")
        try:
            res = self.r.xread({stream_key: last_id}, count=count, block=0)
        except Exception:
            return []
        if not res:
            return []
        # res = [(stream_key, [(id, fields), ...])]
        out = []
        for _stream, entries in res:
            for sid, fields in entries:
                # fields 是 dict[bytes|str, bytes|str],解码
                flat = {}
                for k, v in fields.items():
                    key = k.decode() if isinstance(k, bytes) else k
                    val = v.decode() if isinstance(v, bytes) else v
                    flat[key] = val
                out.append((sid.decode() if isinstance(sid, bytes) else sid, flat))
        return out

    # ============================================================
    # auction(集合竞价)
    # ============================================================
    def put_auction(self, ts_code: str, data: dict) -> None:
        """写 auction:STREAM(落盘)+ ZSET window(全量,1 天)

        2026-09-12 v3 精简 + 2026-09-14 v6.2 全市场快照索引 + 2026-09-16 v6.13 简化:
            - 数据时间戳字段 auction_timestamp:同花顺接口必返回,无需 snap_ts / snap_ts_unix 兜底
            - 落盘时间戳 created_at 由 SQLite DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')) 自动加
            - 全市场快照由 commit_auction_snapshot() 在 service 一轮末尾统一聚合写入
            - 不再记录抓取时刻(同花顺 auction_timestamp 已经够用)
        """
        # 数据时间戳:auction 同花顺必返回;缺失兜底 wall clock(防御性,实际不会触发)
        data_ts = data.get("auction_timestamp")
        if not data_ts:
            data_ts = time.time()
        data["auction_timestamp"] = data_ts
        score = float(data_ts)

        # 用基类 _flat_record 展开 + 精简字段
        flat = self._flat_record(
            ts_code, data,
            drop_keys=DEFAULT_DROP_KEYS | {"snapshot_timestamp", "save_timestamp_unix"},
            ts_field="auction_timestamp",
            ts_value=data_ts,
        )

        stream_key = _k("auction", "stream")
        window_key = _k("auction", "window", ts_code)
        window_member = self._encode({"ts_code": ts_code, **data})

        pipe = self.r.pipeline()
        self._zadd_to_window(pipe, window_key, window_member, score, ttl=WINDOW_TTL_AUCTION)
        self._xadd_to_stream(
            pipe, stream_key, flat,
            maxlen=STREAM_MAXLEN_AUCTION, ttl=STREAM_TTL,
        )
        pipe.execute()

    def commit_auction_snapshot(self, snapshot_data: dict[str, dict]) -> str | None:
        """v6.2 聚合写一个全市场快照(由 service_writeredis_auction 在一轮末尾调用)

        写入:
            online:auction:archive:{unix_ts}  HSET  —  全市场快照(各 ts_code 一条 field)
            online:auction:timeline          ZSet  —  score=unix_ts, member=archive_key

        设计:不滑窗,整体 12h 兜底过期(timeline 与 archive 一致)
            auction 是 09:15-09:25 短时竞价,12h 内全部保留(用户拍板)

        入参:
            snapshot_data: {ts_code: item_dict, ...}  — 该轮拉到的所有个股
                item_dict 必须含 ts_code(基类方法内部加,这里直接传也行)

        返回:
            archive_key 字符串(成功)/ None(数据为空)
        """
        if not snapshot_data:
            return None

        now_unix = time.time()
        archive_key = _k("auction", "archive", f"{now_unix:.6f}")
        timeline_key = _k("auction", "timeline")

        # 构造 HSET mapping(ts_code → JSON item)
        mapping = {ts_code: self._encode(item) for ts_code, item in snapshot_data.items()}

        pipe = self.r.pipeline()
        pipe.hset(archive_key, mapping=mapping)
        pipe.expire(archive_key, AUCTION_ARCHIVE_TTL)
        pipe.zadd(timeline_key, {archive_key: now_unix})
        pipe.expire(timeline_key, AUCTION_TIMELINE_TTL)
        pipe.execute()

        return archive_key

    def get_auction_timeline(self, count: int = 50) -> list[dict]:
        """读 auction 全市场快照 timeline(时序索引)"""
        try:
            timeline_key = _k("auction", "timeline")
            raw = self.r.zrevrange(timeline_key, 0, count - 1, withscores=True)
            result = []
            for member, score in raw:
                key_str = member.decode() if isinstance(member, bytes) else member
                result.append({
                    "archive_key": key_str,
                    "ts": float(score),
                })
            return result
        except Exception:
            return []

    def get_auction_archive_latest(self) -> dict[str, dict]:
        """读最新一份全市场快照(替代原来 N 次 fetch_auction_window)"""
        try:
            timeline_key = _k("auction", "timeline")
            latest = self.r.zrevrange(timeline_key, 0, 0)
            if not latest:
                return {}
            archive_key = latest[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_auction_snapshot_at(self, seconds_before: int) -> dict[str, dict]:
        """读 N 秒前最近一份全市场快照(score ≤ now - seconds_before)"""
        try:
            timeline_key = _k("auction", "timeline")
            target_ts = time.time() - seconds_before
            raw_keys = self.r.zrevrangebyscore(
                timeline_key, target_ts, "-inf", start=0, num=1
            )
            if not raw_keys:
                return {}
            archive_key = raw_keys[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_auction_window(self, ts_code: str, score_min: float = 0, score_max: float = "+inf") -> list[dict]:
        """读 auction 窗口(全量,score_min 默认为 0)"""
        return self._read_window(_k("auction", "window", ts_code), score_min, score_max)

    def get_auction_history(self, count: int = 1000) -> list[tuple[str, dict]]:
        """读 auction STREAM(v2 字段展开,兼容旧 payload 格式)"""
        return self._read_stream(_k("auction", "stream"), count=count)

    # ============================================================
    # snapshot(行情快照)
    # ============================================================
    def put_snapshot(self, ts_code: str, data: dict) -> None:
        """写 snapshot:STREAM(落盘)+ ZSET window(每只股 30 分钟滑动)

        2026-09-11 v2 重构 + 2026-09-12 v3 精简:
            - STREAM 字段直接展开(不再 payload JSON)
            - 数据时间戳字段名 snapshot_timestamp(int 毫秒)
            - 落盘时间戳 created_at 由 SQLite DEFAULT 自动加

        入参兼容:data['data_timestamp'] → snapshot_timestamp(旧版兜底)
        """
        # 数据时间戳:int 毫秒
        snap_ts_raw = data.get("snapshot_timestamp")
        if snap_ts_raw is None:
            legacy = data.get("data_timestamp")
            if legacy is not None:
                snap_ts_raw = int(legacy) if float(legacy) > 1e12 else int(legacy) * 1000
                data["snapshot_timestamp"] = snap_ts_raw

        if snap_ts_raw is not None:
            score = float(snap_ts_raw) / 1000.0
        else:
            score = time.time()
            data["snapshot_timestamp"] = int(score * 1000)
            snap_ts_raw = int(score * 1000)

        # 源头展开 + STREAM 字段序列化(完整保留 snapshot_timestamp + ts_code)
        flat = self._flat_record(
            ts_code, data,
            drop_keys=DEFAULT_DROP_KEYS - {"snapshot_timestamp"},  # snapshot_timestamp 要保留
            ts_field="snapshot_timestamp", ts_value=snap_ts_raw,
        )

        window_member = self._encode({"ts_code": ts_code, **data})
        stream_key = _k("snapshot", "stream")
        window_key = _k("snapshot", "window", ts_code)

        pipe = self.r.pipeline()
        self._xadd_to_stream(
            pipe, stream_key, flat,
            maxlen=STREAM_MAXLEN_SNAPSHOT, ttl=STREAM_TTL,
        )
        # 30 分钟滑动窗口(用基类通用模板)
        self._slide_zset_window(pipe, window_key, {window_member: score}, ttl=WINDOW_TTL_SNAPSHOT)
        pipe.execute()

    def put_snapshots_batch(self, items: list[tuple[str, dict]]) -> int:
        """批量写 snapshot(1 次 pipeline,1 个 RTT)

        Args:
            items: [(ts_code, data_dict), ...],data_dict 是同花顺原始 16 字段扁平 dict

        Returns:
            实际写入的股票数
        """
        if not items:
            return 0

        stream_key = _k("snapshot", "stream")
        pipe = self.r.pipeline()
        written = 0
        save_ts = time.time()
        fields_list = []  # 收集 STREAM fields,用基类 _xadd_many 批量写

        for ts_code, data in items:
            if not ts_code:
                continue

            # 数据时间戳处理(同 put_snapshot 逻辑)
            snap_ts_raw = data.get("snapshot_timestamp")
            if snap_ts_raw is None:
                legacy = data.get("data_timestamp")
                if legacy is not None:
                    snap_ts_raw = int(legacy) if float(legacy) > 1e12 else int(legacy) * 1000
                    data["snapshot_timestamp"] = snap_ts_raw
            if snap_ts_raw is None:
                snap_ts_raw = int(save_ts * 1000)
                data["snapshot_timestamp"] = snap_ts_raw
            score = float(snap_ts_raw) / 1000.0

            flat = self._flat_record(
                ts_code, data,
                drop_keys=DEFAULT_DROP_KEYS - {"snapshot_timestamp"},
                ts_field="snapshot_timestamp", ts_value=snap_ts_raw,
            )
            fields_list.append(flat)

            window_member = self._encode({"ts_code": ts_code, **data})
            window_key = _k("snapshot", "window", ts_code)
            # 每只股各自滑窗(用基类通用模板)
            self._slide_zset_window(pipe, window_key, {window_member: score}, ttl=WINDOW_TTL_SNAPSHOT)
            written += 1

        # 批量 STREAM 写入
        self._xadd_many(
            pipe, stream_key, fields_list,
            maxlen=STREAM_MAXLEN_SNAPSHOT, ttl=STREAM_TTL,
        )
        pipe.execute()
        return written

    # ============================================================
    # v6.3 (2026-09-14) snapshot 全市场快照索引
    # ============================================================
    def commit_snapshots_batch(self, items: list[tuple[str, dict]], now_dt=None) -> str | None:
        """v6.3 一轮末尾聚合写全市场 snapshot 快照索引

        行为:
            1) archive_key = online:snapshot:archive:{unix}.{ms}
            2) HSET archive_key ts_code_1 <encoded> ...(全市场一个 HSET)
            3) EXPIRE archive_key ttl(动态:11:00-11:30 用 7200s,其它 1800s)
            4) ZADD timeline + EXPIRE timeline 43200s(整体 12h 兜底)
            5) 滑窗裁剪:删 < (now_unix - slide_window) 的 timeline entry + 连带 archive_key
        """
        if not items:
            return None

        now_dt = now_dt or datetime.now()
        now_unix = now_dt.timestamp()
        ttl = calc_snapshot_archive_ttl(now_dt)
        slide_n = calc_snapshot_slide_window(now_dt)
        ms = int((now_unix - int(now_unix)) * 1000)
        archive_key = _k("snapshot", "archive", f"{int(now_unix)}.{ms:03d}")
        timeline_key = _k("snapshot", "timeline")

        mapping = {ts_code: self._encode(data) for ts_code, data in items if ts_code}
        if not mapping:
            return None

        pipe = self.r.pipeline()
        pipe.hset(archive_key, mapping=mapping)
        pipe.expire(archive_key, ttl)
        pipe.zadd(timeline_key, {archive_key: now_unix})
        pipe.expire(timeline_key, SNAPSHOT_TIMELINE_TTL)
        pipe.execute()
        # v6.3 滑窗裁剪(pipeline 里 read 命令阻塞,所以放外面)
        cutoff = now_unix - slide_n
        old_keys = self.r.zrangebyscore(timeline_key, "-inf", f"({cutoff}")
        if old_keys:
            self.r.zremrangebyscore(timeline_key, "-inf", f"({cutoff}")
            if old_keys:
                self.r.delete(*old_keys)
        return archive_key

    def get_snapshot_timeline(self, count: int = 50) -> list[dict]:
        """v6.3 读 snapshot 全市场快照 timeline(时序索引)"""
        try:
            timeline_key = _k("snapshot", "timeline")
            raw = self.r.zrevrange(timeline_key, 0, count - 1, withscores=True)
            if not raw:
                return []
            keys = [k.decode() if isinstance(k, bytes) else k for k, _ in raw]
            scores = [s for _, s in raw]
            pipe = self.r.pipeline()
            for k in keys:
                pipe.hlen(k)
                pipe.ttl(k)
            results = pipe.execute()
            out = []
            for i, k in enumerate(keys):
                size = results[i * 2] or 0
                t = results[i * 2 + 1] or 0
                out.append({
                    "archive_key": k,
                    "snap_ts": scores[i],
                    "snap_ts_iso": datetime.fromtimestamp(scores[i]).isoformat(timespec="milliseconds"),
                    "size": size,
                    "ttl": t,
                })
            return out
        except Exception as e:
            print(f"get_snapshot_timeline 异常: {e}")
            return []

    def get_snapshot_archive_latest(self) -> dict[str, dict]:
        """v6.3 读最新一份全市场 snapshot 快照(timeline 末位)"""
        try:
            timeline_key = _k("snapshot", "timeline")
            latest = self.r.zrevrange(timeline_key, 0, 0)
            if not latest:
                return {}
            archive_key = latest[0].decode() if isinstance(latest[0], bytes) else latest[0]
            raw = self.r.hgetall(archive_key)
            return {k.decode() if isinstance(k, bytes) else k: self._decode(v) for k, v in raw.items()}
        except Exception as e:
            print(f"get_snapshot_archive_latest 异常: {e}")
            return {}

    def get_snapshot_snapshot_at(self, seconds_before: int, now_dt=None) -> dict[str, dict]:
        """v6.3 读 N 秒(物理秒)前最近的 snapshot 全市场快照"""
        try:
            timeline_key = _k("snapshot", "timeline")
            now_dt = now_dt or datetime.now()
            target_ts = now_dt.timestamp() - seconds_before
            raw = self.r.zrevrangebyscore(
                timeline_key, target_ts, "-inf",
                start=0, num=1,
            )
            if not raw:
                return {}
            archive_key = raw[0].decode() if isinstance(raw[0], bytes) else raw[0]
            raw_data = self.r.hgetall(archive_key)
            return {k.decode() if isinstance(k, bytes) else k: self._decode(v) for k, v in raw_data.items()}
        except Exception as e:
            print(f"get_snapshot_snapshot_at 异常: {e}")
            return {}

    def get_snapshot_window(self, ts_code: str, score_min: float = 0, score_max: float = "+inf") -> list[dict]:
        """读 snapshot 窗口(ZSET,JSON pack — 兼容旧数据)"""
        return self._read_window(_k("snapshot", "window", ts_code), score_min, score_max)

    def get_snapshot_history(self, count: int = 1000) -> list[tuple[str, dict]]:
        """读 snapshot STREAM(字段展开格式,直接返 dict)

        2026-09-11 v2:不再 json.loads payload 字段,直接读展开的 field
        """
        return self._read_stream(_k("snapshot", "stream"), count=count)

    # ============================================================
    # v6.10 (2026-09-16) snapshot_index 指数行情快照(8 只指数)
    # ============================================================
    # 数据源:ths_client.fetch_index_snapshot → hithink-finance index snapshot
    # 架构:STREAM(落盘用)+ 每只 window(ZSet 滑动,12h 兜底)+ 一轮末尾 archive(HSET,6h)+ timeline(ZSet 索引,12h)
    # 设计:
    #   - 写入 30s/轮(用户原话)
    #   - stream 12h 兜底(timeline 12h 兜底,archive 6h,window 12h 兜底)
    #   - timeline 不做滑窗清理(用户原话"timeline 数据也不做滑窗清理")
    def put_snapshot_index(self, ts_code: str, data: dict) -> None:
        """写单只指数 snapshot_index:STREAM(落盘)+ ZSET window(12h 兜底)

        入参兼容:data['snapshot_timestamp'] 优先,旧版 data['data_timestamp'] 兜底
        """
        # 数据时间戳:int 毫秒
        snap_ts_raw = data.get("snapshot_timestamp")
        if snap_ts_raw is None:
            legacy = data.get("data_timestamp")
            if legacy is not None:
                snap_ts_raw = int(legacy) if float(legacy) > 1e12 else int(legacy) * 1000
                data["snapshot_timestamp"] = snap_ts_raw

        if snap_ts_raw is not None:
            score = float(snap_ts_raw) / 1000.0
        else:
            score = time.time()
            data["snapshot_timestamp"] = int(score * 1000)
            snap_ts_raw = int(score * 1000)

        # 字段展开 + STREAM 字段序列化
        flat = self._flat_record(
            ts_code, data,
            drop_keys=DEFAULT_DROP_KEYS - {"snapshot_timestamp"},
            ts_field="snapshot_timestamp", ts_value=snap_ts_raw,
        )

        window_member = self._encode({"ts_code": ts_code, **data})
        stream_key = _k("snapshot_index", "stream")
        window_key = _k("snapshot_index", "window", ts_code)

        pipe = self.r.pipeline()
        self._xadd_to_stream(
            pipe, stream_key, flat,
            maxlen=STREAM_MAXLEN_SNAPSHOT_INDEX, ttl=STREAM_TTL_SNAPSHOT_INDEX,
        )
        self._slide_zset_window(pipe, window_key, {window_member: score}, ttl=WINDOW_TTL_SNAPSHOT_INDEX)
        pipe.execute()

    def put_snapshots_index_batch(self, items: list[tuple[str, dict]]) -> int:
        """批量写 snapshot_index(1 次 pipeline,1 个 RTT,8 只指数一次写完)

        Args:
            items: [(ts_code, data_dict), ...],data_dict 是同花顺 11 字段扁平 dict

        Returns:
            实际写入的指数数
        """
        if not items:
            return 0

        stream_key = _k("snapshot_index", "stream")
        pipe = self.r.pipeline()
        written = 0
        save_ts = time.time()
        fields_list = []  # 收集 STREAM fields,用基类 _xadd_many 批量写

        for ts_code, data in items:
            if not ts_code:
                continue

            # 数据时间戳处理
            snap_ts_raw = data.get("snapshot_timestamp")
            if snap_ts_raw is None:
                legacy = data.get("data_timestamp")
                if legacy is not None:
                    snap_ts_raw = int(legacy) if float(legacy) > 1e12 else int(legacy) * 1000
                    data["snapshot_timestamp"] = snap_ts_raw
            if snap_ts_raw is None:
                snap_ts_raw = int(save_ts * 1000)
                data["snapshot_timestamp"] = snap_ts_raw
            score = float(snap_ts_raw) / 1000.0

            # snapshot_index 保留 thscode / ticker(指数对账需要)
            flat = self._flat_record(
                ts_code, data,
                drop_keys=DEFAULT_DROP_KEYS - {"thscode", "ticker", "snapshot_timestamp"},
                ts_field="snapshot_timestamp", ts_value=snap_ts_raw,
            )
            fields_list.append(flat)

            window_member = self._encode({"ts_code": ts_code, **data})
            window_key = _k("snapshot_index", "window", ts_code)
            self._slide_zset_window(pipe, window_key, {window_member: score}, ttl=WINDOW_TTL_SNAPSHOT_INDEX)
            written += 1

        # 批量 STREAM 写入(用基类 _xadd_many)
        self._xadd_many(
            pipe, stream_key, fields_list,
            maxlen=STREAM_MAXLEN_SNAPSHOT_INDEX, ttl=STREAM_TTL_SNAPSHOT_INDEX,
        )

        pipe.execute()
        return written

    def commit_snapshots_index_batch(self, items: list[tuple[str, dict]], now_dt=None) -> str | None:
        """一轮末尾聚合写 8 只指数 snapshot_index 快照索引

        行为(对照 snapshot 的 commit_snapshots_batch):
          1) archive_key = online:snapshot_index:archive:{unix}.{ms}
          2) HSET archive_key ts_code_1 <encoded> ...(8 只一个 HSET)
          3) EXPIRE archive_key SNAPSHOT_INDEX_ARCHIVE_TTL(6h,无 lunch 动态)
          4) ZADD timeline + EXPIRE timeline 43200s(整体 12h 兜底)
          5) 不做滑窗清理(用户原话"timeline 不做滑窗清理")

        Returns:
            archive_key(str)或 None(items 空时)
        """
        if not items:
            return None

        now_dt = now_dt or datetime.now()
        now_unix = now_dt.timestamp()
        ms = int((now_unix - int(now_unix)) * 1000)
        archive_key = _k("snapshot_index", "archive", f"{int(now_unix)}.{ms:03d}")
        timeline_key = _k("snapshot_index", "timeline")

        mapping = {ts_code: self._encode(data) for ts_code, data in items if ts_code}
        if not mapping:
            return None

        pipe = self.r.pipeline()
        pipe.hset(archive_key, mapping=mapping)
        pipe.expire(archive_key, SNAPSHOT_INDEX_ARCHIVE_TTL)
        pipe.zadd(timeline_key, {archive_key: now_unix})
        pipe.expire(timeline_key, SNAPSHOT_INDEX_TIMELINE_TTL)
        pipe.execute()
        # 用户原话"timeline 不做滑窗清理" → 这里不调用 ZREMRANGEBYSCORE
        return archive_key

    def get_snapshot_index_timeline(self, count: int = 50) -> list[dict]:
        """读 snapshot_index timeline(时序索引,8 只指数)"""
        try:
            timeline_key = _k("snapshot_index", "timeline")
            raw = self.r.zrevrange(timeline_key, 0, count - 1, withscores=True)
            if not raw:
                return []
            keys = [k.decode() if isinstance(k, bytes) else k for k, _ in raw]
            scores = [s for _, s in raw]
            pipe = self.r.pipeline()
            for k in keys:
                pipe.hlen(k)
                pipe.ttl(k)
            results = pipe.execute()
            out = []
            for i, k in enumerate(keys):
                size = results[i * 2] or 0
                t = results[i * 2 + 1] or 0
                out.append({
                    "archive_key": k,
                    "snap_ts": scores[i],
                    "snap_ts_iso": datetime.fromtimestamp(scores[i]).isoformat(timespec="milliseconds"),
                    "size": size,
                    "ttl": t,
                })
            return out
        except Exception as e:
            print(f"get_snapshot_index_timeline 异常: {e}")
            return []

    def get_snapshot_index_archive_latest(self) -> dict[str, dict]:
        """读最新一份 snapshot_index 指数快照(timeline 末位)"""
        try:
            timeline_key = _k("snapshot_index", "timeline")
            latest = self.r.zrevrange(timeline_key, 0, 0)
            if not latest:
                return {}
            archive_key = latest[0].decode() if isinstance(latest[0], bytes) else latest[0]
            raw = self.r.hgetall(archive_key)
            return {k.decode() if isinstance(k, bytes) else k: self._decode(v) for k, v in raw.items()}
        except Exception as e:
            print(f"get_snapshot_index_archive_latest 异常: {e}")
            return {}

    def get_snapshot_index_history(self, count: int = 1000) -> list[tuple[str, dict]]:
        """读 snapshot_index STREAM(字段展开格式,直接返 dict)"""
        return self._read_stream(_k("snapshot_index", "stream"), count=count)

    # ============================================================
    # orderbook(5档盘口)
    # ============================================================
    def put_orderbook(self, ts_code: str, data: dict) -> None:
        """写 orderbook:STREAM(落盘)+ ZSET window(每只股 3 分钟滑动)

        2026-09-12 v3 精简:
            数据时间戳字段 orderbook_timestamp(同花顺给或 ths_client 打)
            落盘时间戳 created_at 由 SQLite DEFAULT 自动加

        入参兼容:data['snap_ts_unix'] / data['data_timestamp'] → orderbook_timestamp
        """
        data_ts = (
            data.get("orderbook_timestamp")
            or data.get("data_timestamp")
            or data.get("snap_ts_unix")
        )
        if not data_ts:
            data_ts = time.time()
        data["orderbook_timestamp"] = data_ts
        score = float(data_ts)

        flat = self._flat_record(
            ts_code, data,
            drop_keys=DEFAULT_DROP_KEYS | {"save_timestamp_unix"},
            ts_field="orderbook_timestamp", ts_value=data_ts,
        )

        stream_key = _k("orderbook", "stream")
        window_key = _k("orderbook", "window", ts_code)
        window_member = self._encode({"ts_code": ts_code, **data})

        pipe = self.r.pipeline()
        self._xadd_to_stream(
            pipe, stream_key, flat,
            maxlen=STREAM_MAXLEN_ORDERBOOK, ttl=STREAM_TTL,
        )
        # 3 分钟滑窗(用基类通用模板)
        self._slide_zset_window(pipe, window_key, {window_member: score}, ttl=WINDOW_TTL_ORDERBOOK)
        pipe.execute()

    def get_orderbook_window(self, ts_code: str, score_min: float = 0, score_max: float = "+inf") -> list[dict]:
        return self._read_window(_k("orderbook", "window", ts_code), score_min, score_max)

    def get_orderbook_history(self, count: int = 1000) -> list[tuple[str, dict]]:
        """读 orderbook STREAM(v2 字段展开,兼容旧 payload)"""
        return self._read_stream(_k("orderbook", "stream"), count=count)

    # ============================================================
    # minute(分时 K 线)
    # ============================================================
    def put_minute(self, ts_code: str, data: dict) -> None:
        """写 minute:每只股 ZSET(1 天,score=time_idx 整数)

        2026-09-13 v3.1:对齐 coreClient/tdx_client.get_minute_kline 新字段
            - bar 字段:time_idx(原 bar_idx)+ datetime(原 bar_time)+ price + vol + data_timestamp
            - 顶层 minute_timestamp 由 persist_minute 从 bars[0] 推导
            - 落盘时间戳 created_at 由 SQLite DEFAULT 自动加

        每次写入前 DEL 全清(pytdx 每次返全日 0..N bars,会跟旧数据重叠)
        """
        minute_key = _k("minute", ts_code)
        bars = data.get("bars", [])

        members_scores = {}
        for i, bar in enumerate(bars):
            time_idx = int(bar.get("time_idx", bar.get("bar_idx", i)))
            clean_bar = {
                "time_idx": time_idx,
                "datetime": bar.get("datetime") or bar.get("bar_time") or bar.get("time") or "",
                "price": bar.get("price"),
                "vol": bar.get("vol"),
                "data_timestamp": bar.get("data_timestamp"),  # client 源头打的 wall-clock ISO 字符串
            }
            members_scores[self._encode(clean_bar)] = time_idx

        pipe = self.r.pipeline()
        self._delete_key(minute_key)  # 清空(用基类通用模板)
        self._zadd_bulk(pipe, minute_key, members_scores, expire=WINDOW_TTL_MINUTE)
        pipe.execute()

    def get_minute_bars(self, ts_code: str, bar_idx_min: int = 0, bar_idx_max: int = 999) -> list[dict]:
        """读 minute 全部分时 K 线(或按 bar_idx 范围切片)

        2026-09-11:参数名 score_min/max → bar_idx_min/max(score 改为 idx)
        """
        raw = self._read_zset_by_score(
            _k("minute", ts_code), bar_idx_min, bar_idx_max,
        )
        return [self._decode(r) for r in raw]

    # ============================================================
    # zt(涨停监控)— SET + STREAM 分离
    # ============================================================
    def put_zt_pool(self, zt_list: list[dict], data_timestamp: float | None = None) -> int:
        """写涨停池(MyATM 风格):archive HSET + timeline ZSet + 详情流(STREAM)

        2026-09-14 v6:抄 MyATM 设计,SET 改 HSET+ZSet
            数据时间戳字段 zt_timestamp(同花顺给或 ths_client 打)
            落盘时间戳 created_at 由 SQLite 自动加

        存储设计(替代原 SET):
            online:zt:archive:{unix_ts}   HSET(field=ts_code, value=JSON row 全字段含 lu_time 等)
                各自 EXPIRE 300s
            online:zt:timeline            ZSet(member=archive_key, score=unix_ts)
                整体 EXPIRE 12h 兜底 + 写入时 prune 10min 前的 member
                prune_min_score 触发时同时 DEL 对应 archive_key(prune_linked_keys=True)
            online:zt:stream              STREAM(全部涨停详情 + zt_timestamp,字段展开)— 不动
        """
        timeline_key = _k("zt", "timeline")
        stream_key = _k("zt", "stream")

        # 数据时间戳字段统一处理(逻辑同 v3.1 精简版)
        now_unix = float(data_timestamp or time.time())
        archive_key = f"online:zt:archive:{now_unix}"

        # 准备 archive HSET mapping + STREAM fields
        mapping: dict[str, str] = {}    # archive HSET 完整 JSON pack
        fields_list: list[dict] = []    # STREAM 字段展开

        for zt in zt_list:
            # 兼容旧字段名
            if "thscode" in zt and "ts_code" not in zt:
                zt["ts_code"] = zt.pop("thscode")
            zt.pop("ticker", None)
            ts_code = zt.get("ts_code", "")
            if not ts_code:
                continue

            # 数据时间戳 zt_timestamp(unix 秒 float,落盘时 persist 转 ISO)
            zt_ts_unix = float(zt.get("zt_timestamp") or data_timestamp or time.time())
            zt["zt_timestamp"] = zt_ts_unix

            # archive HSET:整行 JSON pack(field=ts_code, value=JSON)
            mapping[ts_code] = self._encode(zt)
            # STREAM:字段展开(同 v3.1 模式)
            fields_list.append({k: self._encode_value(v) for k, v in zt.items() if v is not None})

        # 读取需要 prune 的 archive_key(pipeline 外读,执行时延无害,30s/轮)
        prune_score = now_unix - ZT_TIMELINE_PRUNE_SCORE  # 10 分钟前
        old_archive_keys: list[str] = []
        try:
            old_archive_keys = self.r.zrangebyscore(timeline_key, "-inf", prune_score) or []
            if old_archive_keys:
                # 兼容 bytes 返回
                old_archive_keys = [
                    k.decode() if isinstance(k, bytes) else k
                    for k in old_archive_keys
                ]
        except Exception:
            old_archive_keys = []

        # Pipeline 写
        pipe = self.r.pipeline()
        if mapping:
            pipe.hset(archive_key, mapping=mapping)
        pipe.expire(archive_key, ZT_ARCHIVE_TTL)  # 各自 300s 过期

        pipe.zadd(timeline_key, {archive_key: now_unix})
        # 滑窗核心:主动删 10 分钟前的 member(prune_min_score)
        pipe.zremrangebyscore(timeline_key, "-inf", prune_score)
        # prune_linked_keys=True:同时 DEL 早于阈值的 archive_key
        if old_archive_keys:
            pipe.delete(*old_archive_keys)
        # 整体 EXPIRE 兜底(每次写入重设 TTL,滑动 12h 窗口)
        pipe.expire(timeline_key, ZT_TIMELINE_TTL)

        # STREAM 批量写入(同 v3.1,落盘端用)
        self._xadd_many(pipe, stream_key, fields_list, maxlen=POOL_STREAM_MAXLEN, ttl=STREAM_TTL)
        pipe.execute()
        return len(zt_list)

    def get_zt_archive_latest(self) -> dict[str, dict]:
        """读最新涨停快照全量(timeline → archive_key → HGETALL)

        Returns:
            {ts_code: row_dict, ...}  最新一份 snapshot 的全字段 dict
        """
        try:
            timeline_key = _k("zt", "timeline")
            latest = self.r.zrevrange(timeline_key, 0, 0)
            if not latest:
                return {}
            archive_key = latest[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_zt_snapshot_at(self, seconds_before: float) -> dict[str, dict]:
        """读 N 秒前涨停快照全量(timeline 反查 → archive_key → HGETALL)

        Args:
            seconds_before: 多少秒前的 snapshot(now_unix - seconds_before 之前的最近一份)

        Returns:
            {ts_code: row_dict, ...}  N 秒前最近一份 snapshot 的全字段 dict
        """
        try:
            timeline_key = _k("zt", "timeline")
            target_ts = time.time() - seconds_before
            # ZREVRANGEBYSCORE:找 target_ts 之前的最近一个 member
            raw_keys = self.r.zrevrangebyscore(
                timeline_key, target_ts, "-inf", start=0, num=1
            )
            if not raw_keys:
                return {}
            archive_key = raw_keys[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_zt_timeline(self, count: int = 50) -> list[dict]:
        """读 timeline 索引(列 archive_key + unix_ts)

        Returns:
            [{archive_key: "...", ts: 1757...}, ...]  从最新到最旧
        """
        try:
            timeline_key = _k("zt", "timeline")
            raw = self.r.zrevrange(timeline_key, 0, count - 1, withscores=True)
            result = []
            for member, score in raw:
                key_str = member.decode() if isinstance(member, bytes) else member
                result.append({
                    "archive_key": key_str,
                    "ts": float(score),
                })
            return result
        except Exception:
            return []

    def get_zt_stream(self, count: int = 100) -> list[tuple[str, dict]]:
        """获取涨停 stream 详情(用基类 _decode_stream 兼容旧 payload)"""
        return self._read_stream(_k("zt", "stream"), count=count)

    # ============================================================
    # break(炸板池)v6 2026-09-14 MyATM 风格(timeline ZSet + archive HSET)
    # ============================================================
    def put_break_pool(self, break_list: list[dict], data_timestamp: float | None = None) -> int:
        """写炸板快照 — MyATM 风格:HSET archive + ZSet timeline + STREAM

        archive_key = online:break:archive:{unix_ts}      HSET 300s 各
        timeline     = online:break:timeline               ZSet 12h 兜底 + 滑窗 10min
        stream_key   = online:break:stream                 STREAM 落盘用 1d

        数据时间戳字段 break_timestamp
        落盘时间戳 created_at 由 SQLite 自动加
        """
        archive_key = _k("break", f"archive:{data_timestamp or time.time()}")
        timeline_key = _k("break", "timeline")
        stream_key = _k("break", "stream")
        now_unix = data_timestamp or time.time()
        prune_score = now_unix - BREAK_TIMELINE_PRUNE_SCORE

        # 先取老 archive_key (滑窗期前的),pipeline 执行后 DEL
        old_archive_keys = self.r.zrangebyscore(timeline_key, "-inf", prune_score)

        mapping: dict[str, str] = {}
        fields_list: list[dict[str, str]] = []
        for item in break_list:
            if "thscode" in item and "ts_code" not in item:
                item["ts_code"] = item.pop("thscode")
            item.pop("ticker", None)
            ts_code = item.get("ts_code", "")
            if not ts_code:
                continue
            ts_val = float(
                item.get("break_timestamp")
                or data_timestamp
                or time.time()
            )
            item["break_timestamp"] = ts_val
            # v3 精简:STREAM 写前 pop 老字段
            for k in ("data_timestamp", "data_timestamp_iso", "snap_ts_unix",
                      "save_timestamp", "save_timestamp_iso"):
                item.pop(k, None)
            mapping[ts_code] = self._encode(item)
            fields_list.append({k: self._encode_value(v) for k, v in item.items() if v is not None})

        pipe = self.r.pipeline()
        if mapping:
            pipe.hset(archive_key, mapping=mapping)
            pipe.expire(archive_key, BREAK_ARCHIVE_TTL)
            pipe.zadd(timeline_key, {archive_key: now_unix})
        # 滑窗:主动删老 archive_key + ZSet member
        if old_archive_keys:
            pipe.zremrangebyscore(timeline_key, "-inf", prune_score)
            pipe.delete(*old_archive_keys)
        # timeline 整体兜底
        pipe.expire(timeline_key, BREAK_TIMELINE_TTL)
        # STREAM 落盘用
        if fields_list:
            self._xadd_many(pipe, stream_key, fields_list, maxlen=POOL_STREAM_MAXLEN, ttl=STREAM_TTL)
        pipe.execute()
        return len(break_list)

    def get_break_stream(self, count: int = 100) -> list[tuple[str, dict]]:
        """获取炸板 stream 详情(落盘用)"""
        return self._read_stream(_k("break", "stream"), count=count)

    def get_break_timeline(self, count: int = 50) -> list[dict]:
        """获取炸板 timeline ZSet 索引(member=archive_key, score=unix_ts)"""
        members = self.r.zrevrange(_k("break", "timeline"), 0, count - 1, withscores=True)
        return [{"archive_key": m, "ts": s} for m, s in members]

    def get_break_archive_latest(self) -> dict[str, dict]:
        """获取最新一份炸板快照(field=ts_code, value=row JSON)"""
        members = self.r.zrevrange(_k("break", "timeline"), 0, 0)
        if not members:
            return {}
        raw = self.r.hgetall(members[0])
        if not raw:
            return {}
        return {
            (k.decode() if isinstance(k, bytes) else k): self._decode(v)
            for k, v in raw.items()
        }

    def get_break_snapshot_at(self, seconds_before: int) -> dict[str, dict]:
        """获取 N 秒前最近一份炸板快照"""
        try:
            cutoff = time.time() - seconds_before
            members = self.r.zrevrangebyscore(
                _k("break", "timeline"), cutoff, "-inf", start=0, num=1
            )
            if not members:
                return {}
            archive_key = members[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}


    # ============================================================
    # anomaly(异动清单)v6 2026-09-14 MyATM 风格(timeline ZSet + archive HSET)
    # ============================================================
    def put_anomaly(self, anomaly_list: list[dict], data_timestamp: float | None = None) -> int:
        """写异动快照 — MyATM 风格:HSET archive + ZSet timeline + STREAM

        archive_key = online:anomaly:archive:{unix_ts}        HSET 1200s 各(20min 用户拍板)
        timeline     = online:anomaly:timeline                 ZSet 12h 兜底 + 滑窗 10min
        stream_key   = online:anomaly:stream                   STREAM 落盘用 1d

        数据时间戳字段 anomaly_timestamp
        落盘时间戳 created_at 由 SQLite DEFAULT 自动加
        STREAM 字段展开(替代 payload JSON 嵌套)
        """
        archive_key = _k("anomaly", f"archive:{data_timestamp or time.time()}")
        timeline_key = _k("anomaly", "timeline")
        stream_key = _k("anomaly", "stream")
        now_unix = data_timestamp or time.time()
        prune_score = now_unix - ANOMALY_TIMELINE_PRUNE_SCORE

        old_archive_keys = self.r.zrangebyscore(timeline_key, "-inf", prune_score)

        mapping: dict[str, str] = {}
        fields_list: list[dict[str, str]] = []
        for item in anomaly_list:
            item_ts = (
                item.get("anomaly_timestamp")
                or item.get("data_timestamp")
                or item.get("snap_ts_unix")
                or data_timestamp
                or time.time()
            )
            item["anomaly_timestamp"] = item_ts

            # 精简字段(删 thscode/ticker/老时间戳字段)
            flat = {
                "ts_code": item.get("ts_code") or item.get("thscode") or "",
                **{k: v for k, v in item.items() if k not in {
                    "thscode", "ticker", "data_timestamp", "data_timestamp_iso",
                    "snap_ts_unix", "save_timestamp", "save_timestamp_iso",
                }},
            }
            ts_code = flat.get("ts_code", "")
            if not ts_code:
                continue

            mapping[ts_code] = self._encode(flat)
            fields_list.append({k: self._encode_value(v) for k, v in flat.items() if v is not None})

        pipe = self.r.pipeline()
        if mapping:
            pipe.hset(archive_key, mapping=mapping)
            pipe.expire(archive_key, ANOMALY_ARCHIVE_TTL)
            pipe.zadd(timeline_key, {archive_key: now_unix})
        if old_archive_keys:
            pipe.zremrangebyscore(timeline_key, "-inf", prune_score)
            pipe.delete(*old_archive_keys)
        pipe.expire(timeline_key, ANOMALY_TIMELINE_TTL)
        if fields_list:
            self._xadd_many(pipe, stream_key, fields_list, maxlen=POOL_STREAM_MAXLEN, ttl=STREAM_TTL)
        pipe.execute()
        return len(anomaly_list)

    def get_anomaly_stream(self, count: int = 100) -> list[tuple[str, dict]]:
        """获取异动 stream 详情(落盘用)"""
        return self._read_stream(_k("anomaly", "stream"), count=count)

    def get_anomaly_timeline(self, count: int = 50) -> list[dict]:
        """获取异动 timeline ZSet 索引"""
        members = self.r.zrevrange(_k("anomaly", "timeline"), 0, count - 1, withscores=True)
        return [{"archive_key": m, "ts": s} for m, s in members]

    def get_anomaly_archive_latest(self) -> dict[str, dict]:
        """获取最新一份异动快照"""
        try:
            members = self.r.zrevrange(_k("anomaly", "timeline"), 0, 0)
            if not members:
                return {}
            archive_key = members[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_anomaly_snapshot_at(self, seconds_before: int) -> dict[str, dict]:
        """获取 N 秒前最近一份异动快照"""
        try:
            cutoff = time.time() - seconds_before
            members = self.r.zrevrangebyscore(
                _k("anomaly", "timeline"), cutoff, "-inf", start=0, num=1
            )
            if not members:
                return {}
            archive_key = members[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    # ============================================================
    # hot(热股榜)v6 2026-09-14 MyATM 风格(timeline ZSet + archive HSET)
    # ============================================================
    def put_hot_rank(self, hot_list: list[dict], data_timestamp: float | None = None) -> int:
        """写热股榜快照 — MyATM 风格:HSET archive + ZSet timeline + STREAM

        archive_key = online:hot:archive:{unix_ts}         HSET 1200s 各(20min 用户拍板)
        timeline     = online:hot:timeline                  ZSet 12h 兜底 + 滑窗 10min
        stream_key   = online:hot:stream                    STREAM 落盘用 1d

        数据时间戳字段 hot_timestamp
        落盘时间戳 created_at 由 SQLite DEFAULT 自动加
        """
        archive_key = _k("hot", f"archive:{data_timestamp or time.time()}")
        timeline_key = _k("hot", "timeline")
        stream_key = _k("hot", "stream")
        now_unix = data_timestamp or time.time()
        prune_score = now_unix - HOT_TIMELINE_PRUNE_SCORE

        old_archive_keys = self.r.zrangebyscore(timeline_key, "-inf", prune_score)

        mapping: dict[str, str] = {}
        fields_list: list[dict[str, str]] = []
        for item in hot_list:
            if "thscode" in item and "ts_code" not in item:
                item["ts_code"] = item.pop("thscode")
            item.pop("ticker", None)
            ts_code = item.get("ts_code", "")
            if not ts_code:
                continue

            ts_val = float(
                item.get("hot_timestamp")
                or item.get("data_timestamp")
                or item.get("snap_ts_unix")
                or data_timestamp
                or time.time()
            )
            item["hot_timestamp"] = ts_val
            item.pop("data_timestamp", None)
            item.pop("data_timestamp_iso", None)
            item.pop("snap_ts_unix", None)
            item.pop("save_timestamp", None)
            item.pop("save_timestamp_iso", None)

            mapping[ts_code] = self._encode(item)
            fields_list.append({k: self._encode_value(v) for k, v in item.items() if v is not None})

        pipe = self.r.pipeline()
        if mapping:
            pipe.hset(archive_key, mapping=mapping)
            pipe.expire(archive_key, HOT_ARCHIVE_TTL)
            pipe.zadd(timeline_key, {archive_key: now_unix})
        if old_archive_keys:
            pipe.zremrangebyscore(timeline_key, "-inf", prune_score)
            pipe.delete(*old_archive_keys)
        pipe.expire(timeline_key, HOT_TIMELINE_TTL)
        if fields_list:
            self._xadd_many(pipe, stream_key, fields_list, maxlen=POOL_STREAM_MAXLEN, ttl=STREAM_TTL)
        pipe.execute()
        return len(hot_list)

    def get_hot_stream(self, count: int = 100) -> list[tuple[str, dict]]:
        """获取热股 stream 详情(落盘用)"""
        return self._read_stream(_k("hot", "stream"), count=count)

    def get_hot_timeline(self, count: int = 50) -> list[dict]:
        """获取热股 timeline ZSet 索引"""
        members = self.r.zrevrange(_k("hot", "timeline"), 0, count - 1, withscores=True)
        return [{"archive_key": m, "ts": s} for m, s in members]

    def get_hot_archive_latest(self) -> dict[str, dict]:
        """获取最新一份热股快照"""
        try:
            members = self.r.zrevrange(_k("hot", "timeline"), 0, 0)
            if not members:
                return {}
            archive_key = members[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_hot_snapshot_at(self, seconds_before: int) -> dict[str, dict]:
        """获取 N 秒前最近一份热股快照"""
        try:
            cutoff = time.time() - seconds_before
            members = self.r.zrevrangebyscore(
                _k("hot", "timeline"), cutoff, "-inf", start=0, num=1
            )
            if not members:
                return {}
            archive_key = members[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_meta(self, field: str) -> Optional[str]:
        """读 `online:meta:<field>` HASH 字段(标准化 meta helper)"""
        try:
            return self.r.hget(_k("meta"), field)
        except Exception:
            return None

    # ============================================================
    # limitperformance(涨停表现详情)— STREAM 详细流(2026-09-14 v5 新增)
    # ============================================================
    def put_limitperformance(self, lp_list: list[dict], *, data_timestamp: float | None = None) -> int:
        """写涨停表现详情快照 — MyATM 风格:HSET archive + ZSet timeline + STREAM

        2026-09-14 v5 新增 / 2026-09-14 v6 改 MyATM 风格
          数据源:coreClient/kpl_client.KPLClient.fetch_realtime_limit_performance()
                  (apphwhq 实时盯盘 host,盘中可用)
          存储设计:
            online:limitperformance:archive:{unix_ts}  HSET(每只股每次拉的快照,field=ts_code, value=JSON)
            online:limitperformance:timeline           ZSet 12h 兜底 + 滑窗 10min
            online:limitperformance:stream             STREAM(每只股每次拉的快照 1 条 entry,
                                                      UNIQUE key = (ts_code, limitperformance_timestamp))
          每条 entry 字段(源头展开):
            ts_code / name / board_type / board_count / lu_time(int unix) /
            theme / limit_reason / is_break / amplitude / turnover_rate /
            limit_order / lu_limit_order / net_change / main_in / main_out /
            amount / free_float / close_price / pct_chg / board_period /
            theme_id / sector_id / trade_date /
            limitperformance_timestamp(float unix int)  ★ 数据时间戳,落盘时 persist 转 ISO

        Args:
            lp_list:        kpl_client 返回的 DataFrame 转 list[dict](25 列)
            data_timestamp: 拉取瞬间 wall clock(unix float),作为本批 limitperformance_timestamp 兜底

        Returns:
            XADD 写入的条目数

        幂等:STREAM entry id 自动单调,主键在落盘表上是 (ts_code, limitperformance_timestamp);
            同 ts_code 多次拉 → 多 entry(保留涨停列表演化轨迹)
        """
        if not lp_list:
            return 0

        archive_key = _k("limitperformance", f"archive:{data_timestamp or time.time()}")
        timeline_key = _k("limitperformance", "timeline")
        stream_key = _k("limitperformance", "stream")
        now_unix = data_timestamp or time.time()
        prune_score = now_unix - LIMITPERFORMANCE_TIMELINE_PRUNE_SCORE

        old_archive_keys = self.r.zrangebyscore(timeline_key, "-inf", prune_score)

        mapping: dict[str, str] = {}
        fields_list: list[dict] = []
        for lp in lp_list:
            if "thscode" in lp and "ts_code" not in lp:
                lp["ts_code"] = lp.pop("thscode")
            lp.pop("ticker", None)

            ts_code = lp.get("ts_code", "")
            if not ts_code:
                continue

            lp.setdefault("limitperformance_timestamp", data_timestamp or time.time())

            mapping[ts_code] = self._encode(lp)
            fields_list.append({k: self._encode_value(v) for k, v in lp.items() if v is not None})

        pipe = self.r.pipeline()
        if mapping:
            pipe.hset(archive_key, mapping=mapping)
            pipe.expire(archive_key, LIMITPERFORMANCE_ARCHIVE_TTL)
            pipe.zadd(timeline_key, {archive_key: now_unix})
        if old_archive_keys:
            pipe.zremrangebyscore(timeline_key, "-inf", prune_score)
            pipe.delete(*old_archive_keys)
        pipe.expire(timeline_key, LIMITPERFORMANCE_TIMELINE_TTL)
        if fields_list:
            self._xadd_many(
                pipe, stream_key, fields_list,
                maxlen=STREAM_MAXLEN_LIMITPERFORMANCE, ttl=STREAM_TTL,
            )
        pipe.execute()
        return len(fields_list)

    def get_limitperformance_stream(self, count: int = 5000) -> list[tuple[str, dict]]:
        """读涨停表现详情 stream(落盘用)"""
        return self._read_stream(_k("limitperformance", "stream"), count=count)

    def get_limitperformance_timeline(self, count: int = 50) -> list[dict]:
        """获取涨停表现详情 timeline ZSet 索引"""
        members = self.r.zrevrange(_k("limitperformance", "timeline"), 0, count - 1, withscores=True)
        return [{"archive_key": m, "ts": s} for m, s in members]

    def get_limitperformance_archive_latest(self) -> dict[str, dict]:
        """获取最新一份涨停表现详情快照"""
        try:
            members = self.r.zrevrange(_k("limitperformance", "timeline"), 0, 0)
            if not members:
                return {}
            archive_key = members[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_limitperformance_snapshot_at(self, seconds_before: int) -> dict[str, dict]:
        """获取 N 秒前最近一份涨停表现详情快照"""
        try:
            cutoff = time.time() - seconds_before
            members = self.r.zrevrangebyscore(
                _k("limitperformance", "timeline"), cutoff, "-inf", start=0, num=1
            )
            if not members:
                return {}
            archive_key = members[0]
            if isinstance(archive_key, bytes):
                archive_key = archive_key.decode()
            raw = self.r.hgetall(archive_key)
            if not raw:
                return {}
            return {
                (k.decode() if isinstance(k, bytes) else k): self._decode(v)
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def get_limitperformance_xlen(self) -> int:
        """STREAM 当前条数(诊断用)"""
        return self._xlen(_k("limitperformance", "stream"))

    # ============================================================
    # 业务统计(info 扩展)— 保留原 OnlineRedis.info() 行为
    # ============================================================
    def info(self) -> dict:
        """Redis online 业务统计(覆盖基类 info)"""
        out = super().info()
        out.update({
            "watchlist_count": self._set_count(_k("watchlist")),
            # 2026-09-14 v6:break_pool_count 删除(MyATM 风格不再用 SET pool,改 timeline_size)
            # 2026-09-14 v6:anomaly_list_count 删除(MyATM 风格不再用 LIST,改 timeline_size)
            # 2026-09-14 v6:hot_rank_count 删除(MyATM 风格不再用 LIST rank,改 timeline_size)
            "auction_window_count": self._scan_count(_k("auction", "window", "*")),
            # 2026-09-14 v6.2:auction 全市场快照索引(timeline + archive)
            "auction_timeline_size": self.r.zcard(_k("auction", "timeline")),

            "snapshot_window_count": self._scan_count(_k("snapshot", "window", "*")),
            # 2026-09-14 v6.3:snapshot 全市场快照索引(timeline + archive)
            "snapshot_timeline_size": self.r.zcard(_k("snapshot", "timeline")),
            "orderbook_window_count": self._scan_count(_k("orderbook", "window", "*")),
            "minute_count": self._scan_count(_k("minute", "*")),
        })
        # STREAM 长度(用基类通用模板)
        for kind in ("zt", "break", "anomaly", "hot", "auction", "snapshot", "orderbook"):
            stream_key = _k(kind, "stream")
            out[f"{kind}_stream_len"] = self._xlen(stream_key)
        return out


# ============================================================
# CLI 测试
# ============================================================
if __name__ == "__main__":
    r = OnlineRedis()
    print(f"Ping: {r.r.ping()}")
    print(f"Info: {r.info()}")

    # 测 watchlist(v6.7 — 走 STREAM + timeline + archive 三件套)
    import time as _t
    _ts_unix = _t.time()
    r.put_watchlist(["000001.SZ", "600519.SH"], {"000001.SZ": "test", "600519.SH": "test"}, _ts_unix)
    r.commit_watchlist_snapshot(_ts_unix)
    print(f"Watchlist: {r.get_watchlist()}")
    print(f"Watchlist sources: {r.get_watchlist_sources()}")

    # 测 auction
    r.put_auction("000001.SZ", {
        "price": 10.5, "vol": 1000, "snap_ts": "2026-09-12 09:24:55",
        "snap_ts_unix": time.time(),
    })
    print(f"Auction window: {len(r.get_auction_window('000001.SZ'))} items")
    print(f"Auction history: {len(r.get_auction_history(10))} items")

    # 测 snapshot
    r.put_snapshot("000001.SZ", {
        "price": 10.5, "vol": 1000,
        "snapshot_timestamp": int(time.time() * 1000),
    })
    print(f"Snapshot history: {len(r.get_snapshot_history(10))} items")

    # 测 minute
    r.put_minute("000001.SZ", {
        "bars": [
            {"time": "2026-09-12 09:30:00", "price": 10.5, "vol": 100},
            {"time": "2026-09-12 09:31:00", "price": 10.6, "vol": 200},
        ],
    })
    print(f"Minute bars: {len(r.get_minute_bars('000001.SZ'))} items")

    # 测 zt
    r.put_zt_pool([
        {"ts_code": "600519.SH", "name": "贵州茅台", "zt_timestamp": time.time()},
        {"ts_code": "000001.SZ", "name": "平安银行", "zt_timestamp": time.time()},
    ])
    # v6:SET 已删除,改用 get_zt_archive_latest 验证
    latest = r.get_zt_archive_latest()
    print(f"ZT archive latest size: {len(latest)}, timeline entries: {len(r.get_zt_timeline())}")

    # 测业务游标
    r.set_meta("last_stream_id:zt", "1234-0")
    print(f"Last stream id (zt): {r.get_last_stream_id('zt')}")

    # 清理
    r.cleanup()
    print(f"清理后: {r.info()}")
"""
~/TradingAgent/coreClient/redis_client.py
=========================================

Redis 基础层:连接 + JSON 序列化 + 通用 HASH meta + 通用 STREAM/ZSET/SET/LIST/HASH 模板。

**作为整个 TradingAgent 项目的基础底座**(2026-09-12 架构调整):
- ~/TradingAgent/coreClient/redis_client.py   ← 本文件(跨业务复用)
- ~/TradingAgent/coreClient/redis_config.py   ← 连接参数 + 通用配置
- ~/TradingAgent/onlineDataManager/scripts/core/redis_online.py  ← onlineDataManager 业务方法(继承 RedisBase)
- (未来) ~/TradingAgent/monitor/.../redis_monitor.py    ← 监控业务方法(继承 RedisBase)

**基础能力(本文件)**:
    RedisBase
        __init__                连接 + ping
        _encode/_decode         JSON 序列化(完整 JSON pack)
        _encode_value/_decode_value  STREAM field 序列化(单值)
        set_meta/get_meta/incr_meta  通用 HASH meta(键值对元信息)
        build_key               通用 key 构建器(拼接 prefix + parts)
        cleanup/info            按前缀清理 + 统计

**各类数据通用功能**(各种 Redis 容器操作模板):
    # 源头展开
    _flat_record              源头展开 + 字段精简 + STREAM field 序列化

    # STREAM 通用(落盘用)
    _xadd_to_stream           单条 STREAM 写入(带 MAXLEN + EXPIRE)
    _xadd_many                批量 STREAM 写入(zt/break/anomaly/hot 用)
    _decode_stream            通用 STREAM 反序列化(支持旧 payload 格式)
    _read_stream              通用 STREAM 读(XREVRANGE + _decode_stream)
    _xlen                     XLEN(异常返 0)
    _scan_count               SCAN_ITER + count

    # ZSET 通用(滑动窗口用)
    _zadd_to_window           通用 ZSET 窗口写入(带 TTL + 可选滑窗清理)
    _zadd_bulk                通用 ZSET 批量写入(无滑窗,minute 用)
    _slide_zset_window        滑窗 ZSET 写入(snapshot/orderbook 用)
    _read_window              通用 ZSET window 读(JSON 自动 decode)
    _read_zset_by_score       通用 ZSET 读(返原始字符串,minute 用)

    # SET 通用(去重 / 池子用)
    _set_replace              DEL + SADD 全量替换
    _set_read                 SMEMBERS 读
    _set_count                SCARD

    # LIST 通用(最近 N 条 / 业务读)
    _list_push_trim           LPUSH + LTRIM(最近 N 条窗口)
    _list_read                LRANGE 读全部
    _list_read_decoded        LRANGE + JSON decode
    _list_count               LLEN

    # HASH 单字段 + DEL(基础操作)
    _hset_field               HSET 单字段(自动 _encode_value)
    _hgetall                  HGETALL(返 dict)
    _delete_key               DEL

    # Key 构建
    build_key                 通用 key 构建器(prefix + ":" + parts)

**业务层只装业务**(例 OnlineRedis):只组合基类的通用模板,不直接调 pipe.sadd/pipe.lpush 等底层命令。
"""

from __future__ import annotations

import json
import logging
import sys
import time as _time
from pathlib import Path
from typing import Any, Iterable

import redis

# 让 from coreClient.redis_config import ... 工作
# (当 PYTHONPATH 包含 ~/TradingAgent/ 时,Python 会自动找到 coreClient 这个目录)
_HERE = Path(__file__).resolve().parent  # ~/TradingAgent/coreClient
_PARENT = _HERE.parent                    # ~/TradingAgent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from coreClient.redis_config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_DB,
    META_LAST_ID_PREFIX,
    DEFAULT_DROP_KEYS,
    DEFAULT_STREAM_TTL,
    DEFAULT_WINDOW_TTL,
    REDIS_RETRY_MAX,
    REDIS_RETRY_SLEEP,
    REDIS_PING_TIMEOUT,
)

logger = logging.getLogger("redis_client")
if not logger.handlers:
    # 简易 fallback logger,避免依赖外部 loguru 配置
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s | %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ============================================================
# 通用 drop_keys(各类数据源头展开后,统一要删的冗余字段)
# 2026-09-12 v3:thscode/ticker 已被源头 fetch_* 删,这里再兜底一次
# 业务可以追加自己的 drop_keys(传给 _flat_record)
# ============================================================
# DEFAULT_DROP_KEYS 已从 redis_config 导入


# ============================================================
# RedisBase
# ============================================================
class RedisBase:
    """Redis 基础能力 + STREAM/ZSET/SET/LIST/HASH 通用模板

    约定:
        set_meta(key, value)      通用 HASH 元信息(self.r.hset "<prefix>:meta" key value)
        get_meta(key, default)    通用 HASH 读
        _encode/_decode           完整 JSON pack(ZSET member 用)
        _encode_value/_decode_value STREAM field 单值序列化
        _flat_record              通用源头展开 + 字段精简(各类 put_xxx 复用)
        _xadd_to_stream           通用 STREAM 写入(各类 put_xxx 复用)
        _zadd_to_window           通用 ZSET 窗口写入(各类 put_xxx 复用)
        _decode_stream            通用 STREAM 反序列化
        _read_window              通用 ZSET window 读
        cleanup(prefix)           按前缀清理(默认 self.PREFIX)
        info(prefix)              keyspace 统计(默认 self.PREFIX)

    业务侧约定:
        - 子类覆盖 PREFIX(onlineDataManager 用 "online",monitor 用 "monitor",...)
        - 业务游标(last_id / last_round / last_count)用 set_meta/get_meta 实现
    """

    # 子类覆盖(onlineDataManager 业务设 "online")
    PREFIX: str = ""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        db: int = DEFAULT_DB,
        *,
        prefix: str | None = None,
    ):
        self.r = redis.Redis(
            host=host, port=port, db=db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=None,
            health_check_interval=30,
        )
        if prefix is not None:
            self.PREFIX = prefix
        try:
            self.r.ping()
        except redis.exceptions.ConnectionError as e:
            raise RuntimeError(f"Redis 不可用: {e}")

    # ============================================================
    # JSON 序列化
    # ============================================================
    @staticmethod
    def _encode(data: Any) -> str:
        """完整 dict/list → JSON 字符串(ZSET member 用)"""
        return json.dumps(data, ensure_ascii=False, default=str)

    @staticmethod
    def _decode(raw: str) -> Any:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    @staticmethod
    def _encode_value(v: Any) -> str:
        """STREAM field 序列化:数字/字符串/布尔都转 str,dict/list 序列化为 JSON 字符串

        STREAM 的 field 必须是字符串,所以全部 str()
        """
        if isinstance(v, str):
            return v
        if isinstance(v, (int, float, bool)):
            return str(v)
        # dict / list → JSON 字符串(比如 keyword_list)
        return json.dumps(v, ensure_ascii=False, default=str)

    @staticmethod
    def _decode_value(v: str) -> Any:
        """STREAM field 反序列化:尝试还原类型

        策略:
            - 看起来像数字 → 还原数字
            - 看起来像 JSON 对象/数组 → json.loads
            - 否则保持字符串
        """
        if not isinstance(v, str):
            return v
        # 数字字符串还原
        try:
            return int(v)
        except ValueError:
            pass
        try:
            return float(v)
        except ValueError:
            pass
        # JSON 还原
        if v.startswith(("{", "[", '"')):
            try:
                return json.loads(v)
            except (json.JSONDecodeError, TypeError):
                pass
        return v

    # ============================================================
    # 通用元信息(self.r.hset "<PREFIX>:meta" ...)
    # 业务层 last_id / last_round / last_count 等都放这里
    # ============================================================
    def _meta_key(self, key: str) -> str:
        """业务 HASH key 拼装 — 留 hook 给子类调整(目前固定 <PREFIX>:meta)"""
        return f"{self.PREFIX}:meta"

    def set_meta(self, key: str, value: Any) -> None:
        self.r.hset(self._meta_key(key), key, self._encode_value(value))

    def get_meta(self, key: str, default: Any = None) -> Any:
        v = self.r.hget(self._meta_key(key), key)
        if v is None:
            return default
        return self._decode_value(v)

    def incr_meta(self, key: str) -> int:
        return self.r.hincrby(self._meta_key(key), key, 1)

    # ============================================================
    # 通用 STREAM/ZSET 模板(各类数据共用)
    # ============================================================
    def _flat_record(
        self,
        ts_code: str,
        data: dict,
        *,
        drop_keys: Iterable[str] = DEFAULT_DROP_KEYS,
        ts_field: str | None = None,
        ts_value: Any = None,
    ) -> dict:
        """源头展开 + 字段精简 + STREAM field 序列化

        Args:
            ts_code: 股票代码(顶层 ts_code 字段)
            data: 同花顺原始 dict
            drop_keys: 要 pop 的冗余字段集合(默认 DEFAULT_DROP_KEYS)
            ts_field: 数据时间戳字段名(如 "auction_timestamp")— None 表示不动 data 里的时间戳
            ts_value: 数据时间戳值(可选,None 表示从 data 里提取)

        Returns:
            {k: 字符串值, ...}  — 已可直接 xadd
        """
        flat = {"ts_code": ts_code, **data}
        # 兼容:thscode → ts_code(老数据兜底)
        if "thscode" in flat and "ts_code" not in data:
            flat["ts_code"] = flat.pop("thscode")
        # 删冗余字段
        for k in drop_keys:
            flat.pop(k, None)
        # 注入数据时间戳
        if ts_field and ts_value is not None:
            flat[ts_field] = ts_value
        # STREAM field 序列化(字符串值)
        return {k: self._encode_value(v) for k, v in flat.items() if v is not None}

    def _xadd_to_stream(
        self,
        pipe,
        stream_key: str,
        fields: dict,
        *,
        maxlen: int | None = None,
        ttl: int = DEFAULT_STREAM_TTL,
    ) -> None:
        """通用 STREAM 写入(带 MAXLEN + EXPIRE)"""
        if maxlen is not None:
            pipe.xadd(stream_key, fields, maxlen=maxlen, approximate=True)
        else:
            pipe.xadd(stream_key, fields)
        pipe.expire(stream_key, ttl)

    def _zadd_to_window(
        self,
        pipe,
        window_key: str,
        member: str,
        score: float,
        *,
        ttl: int = DEFAULT_WINDOW_TTL,
        slide_window: bool = False,
    ) -> None:
        """通用 ZSET 窗口写入(带 TTL + 可选滑窗清理)

        Args:
            pipe: redis pipeline
            window_key: ZSET key
            member: ZSET member(JSON pack 字符串)
            score: 时间分数(unix 秒或 bar_idx)
            ttl: EXPIRE 秒数
            slide_window: True → zremrangebyscore 滑窗(score 之前 ttl 秒)
        """
        pipe.zadd(window_key, {member: score})
        if slide_window:
            pipe.zremrangebyscore(window_key, "-inf", score - ttl)
        pipe.expire(window_key, ttl)

    def _decode_stream(self, raw: list) -> list[tuple[str, dict]]:
        """通用 STREAM 反序列化(支持旧 payload 格式)

        xrevrange 返回 [(id, {field: value, ...}), ...]
        旧 payload 格式(2026-09-11 v2 之前):payload 是 JSON 字符串
        新字段展开格式(2026-09-11 v2 起):每个字段独立
        """
        result = []
        for sid, fields in raw:
            decoded = {}
            for k, v in fields.items():
                # 兼容旧 payload 格式
                if k == "payload" and isinstance(v, str):
                    try:
                        old = self._decode(v)
                        if isinstance(old, dict):
                            decoded.update(old)
                            continue
                    except Exception:
                        pass
                decoded[k] = self._decode_value(v)
            result.append((sid, decoded))
        return result

    def _read_window(
        self,
        window_key: str,
        score_min: float = 0,
        score_max: float = "+inf",
    ) -> list[dict]:
        """通用 ZSET window 读(JSON pack → dict)"""
        raw = self.r.zrangebyscore(window_key, min=score_min, max=score_max)
        return [self._decode(r) for r in raw]

    def _read_zset_by_score(
        self,
        zset_key: str,
        score_min: float = 0,
        score_max: float = "+inf",
    ) -> list:
        """通用 ZSET 读(ZRANGEBYSCORE 返原始字符串列表)— 给 minute 这种"原始 JSON 包"用

        与 _read_window 的区别:
            _read_window 返 list[dict](自动 json.loads)
            _read_zset_by_score 返 list(原始字符串,调用方自己处理)
        """
        return self.r.zrangebyscore(zset_key, min=score_min, max=score_max)

    def _zadd_bulk(
        self,
        pipe,
        zset_key: str,
        members_scores: dict[str, float],
        *,
        expire: int | None = None,
    ) -> None:
        """通用 ZSET 批量写入(ZADD + 可选 EXPIRE)— 不带滑窗清理

        业务模式:minute 整组写入(无滑窗)、初次冷启动补齐历史
        """
        if members_scores:
            pipe.zadd(zset_key, members_scores)
        if expire is not None:
            pipe.expire(zset_key, expire)

    def _slide_zset_window(
        self,
        pipe,
        window_key: str,
        members_scores: dict[str, float],
        *,
        ttl: int,
    ) -> None:
        """通用滑窗 ZSET 写入:ZADD + 每条按 score 滑窗清理 + EXPIRE

        业务模式:snapshot / orderbook 的 N 分钟滑窗

        注意:每条 member 用自己的 score 作为滑窗基准。
        """
        if not members_scores:
            pipe.expire(window_key, ttl)
            return
        for member, score in members_scores.items():
            pipe.zadd(window_key, {member: score})
            pipe.zremrangebyscore(window_key, "-inf", score - ttl)
        pipe.expire(window_key, ttl)

    def _xlen(self, stream_key: str) -> int:
        """通用 STREAM 长度读(XLEN)— 异常返 0"""
        try:
            return self.r.xlen(stream_key)
        except Exception:
            return 0

    def _scan_count(self, match_pattern: str) -> int:
        """通用 SCAN 计数(SCAN_ITER + count)"""
        return sum(1 for _ in self.r.scan_iter(match_pattern))

    def _xadd_many(
        self,
        pipe,
        stream_key: str,
        fields_list: list[dict],
        *,
        maxlen: int | None = None,
        ttl: int = DEFAULT_STREAM_TTL,
        trim_after: bool = True,
    ) -> int:
        """通用 STREAM 批量写入:XADD + (可选 XTRIM) + EXPIRE

        业务模式:zt / break / anomaly / hot 的 STREAM 落盘

        Returns:
            实际写入条数
        """
        written = 0
        for fields in fields_list:
            if maxlen is not None:
                pipe.xadd(stream_key, fields, maxlen=maxlen, approximate=True)
            else:
                pipe.xadd(stream_key, fields)
            written += 1
        if trim_after and maxlen is not None:
            pipe.xtrim(stream_key, maxlen=maxlen, approximate=True)
        pipe.expire(stream_key, ttl)
        return written

    def _read_stream(self, stream_key: str, count: int | None = None) -> list[tuple[str, dict]]:
        """通用 STREAM 读(XREVRANGE + _decode_stream)

        业务模式:各类 get_xxx_history / get_xxx_stream
        """
        if count is None:
            raw = self.r.xrevrange(stream_key)
        else:
            raw = self.r.xrevrange(stream_key, count=count)
        return self._decode_stream(raw)

    def _hset_field(self, hash_key: str, field: str, value: Any) -> None:
        """通用 HASH 单字段写(HSET)— 自动 _encode_value 序列化"""
        self.r.hset(hash_key, field, self._encode_value(value))

    def _hgetall(self, hash_key: str) -> dict:
        """通用 HASH 全字段读(HGETALL)"""
        return self.r.hgetall(hash_key) or {}

    def _delete_key(self, key: str) -> None:
        """通用 DEL(供业务 reset 用,例 put_minute 的清空重建)"""
        self.r.delete(key)

    # ============================================================
    # SET 通用(去重 / 池子)— zt/break/watchlist 都用 SET 全量替换
    # ============================================================
    def _set_replace(
        self,
        pipe,
        set_key: str,
        members: Iterable[str],
        *,
        expire: int | None = None,
    ) -> None:
        """通用 SET 全量替换:DEL + SADD + (可选 EXPIRE)

        业务模式:每轮"当前池"语义(zt pool / break pool / watchlist)
        """
        members = list(members)
        pipe.delete(set_key)
        if members:
            pipe.sadd(set_key, *members)
        if expire is not None:
            pipe.expire(set_key, expire)

    def _set_read(self, set_key: str) -> list[str]:
        """通用 SET 读(SMEMBERS)"""
        return list(self.r.smembers(set_key))

    def _set_count(self, set_key: str) -> int:
        """通用 SET count(SCARD)"""
        return self.r.scard(set_key)

    # ============================================================
    # LIST 通用(最近 N 条窗口)— anomaly/hot 都用 LPUSH+LTRIM 500
    # ============================================================
    def _list_push_trim(
        self,
        pipe,
        list_key: str,
        payload: str,
        *,
        max_size: int = 500,
        ttl: int = DEFAULT_STREAM_TTL,
    ) -> None:
        """通用 LIST 写入:LPUSH + LTRIM(最近 N 条)+ EXPIRE

        业务模式:anomaly / hot 的"最近 500 条"语义
        """
        pipe.lpush(list_key, payload)
        pipe.ltrim(list_key, 0, max_size - 1)
        pipe.expire(list_key, ttl)

    def _list_read(self, list_key: str) -> list:
        """通用 LIST 读(LRANGE 0..-1)— 原始字符串,调用方自己 decode"""
        return self.r.lrange(list_key, 0, -1)

    def _list_read_decoded(self, list_key: str) -> list[dict]:
        """通用 LIST 读 + JSON decode(便捷方法)"""
        return [self._decode(r) for r in self._list_read(list_key)]

    def _list_count(self, list_key: str) -> int:
        """通用 LIST count(LLEN)"""
        return self.r.llen(list_key)

    # ============================================================
    # 通用 key 构建器(纯字符串拼装,无业务语义)
    # ============================================================
    @staticmethod
    def build_key(prefix: str, *parts: str) -> str:
        """通用 key 构建器:prefix + ":" + parts 拼装

        例: build_key("online", "zt", "pool") → "online:zt:pool"
            build_key("online", "snapshot", "window", ts_code) → "online:snapshot:window:000001.SZ"
        """
        return ":".join((prefix, *parts))

    # ============================================================
    # 清理 & 诊断(按 PREFIX 过滤)
    # ============================================================
    def cleanup(self, prefix: str | None = None) -> int:
        """清理指定前缀的所有 key(谨慎,通常只在调试时用)

        Args:
            prefix: 要清理的前缀,None → 用 self.PREFIX
        """
        pfx = prefix if prefix is not None else self.PREFIX
        if not pfx:
            return 0
        keys = list(self.r.scan_iter(f"{pfx}:*"))
        if keys:
            return self.r.delete(*keys)
        return 0

    def info(self, prefix: str | None = None) -> dict:
        """按前缀返回 keyspace 概览

        子类覆盖 _info_extra() 增加业务特定的统计(如 auction_window_count)
        """
        pfx = prefix if prefix is not None else self.PREFIX
        out = {
            "prefix": pfx,
            "ping": self.r.ping(),
        }
        return out

    # 注意:last_stream_id 是 onlineDataManager 业务概念(各类 STREAM 的落盘游标)
    # 已在 OnlineRedis 实现,基类只提供通用的 set_meta/get_meta


# ============================================================
# CLI 测试
# ============================================================
if __name__ == "__main__":
    r = RedisBase()
    print(f"Ping: {r.r.ping()}")
    print(f"Info: {r.info()}")

    # 测 _flat_record
    data = {
        "volume": 1000, "last_price": 10.5,
        "thscode": "600519.SH", "ticker": "600519",
        "data_timestamp": 1234567890, "save_timestamp": 1234567891,
    }
    flat = r._flat_record("600519.SH", data, ts_field="snapshot_timestamp", ts_value=1234567890)
    print(f"Flat record: {flat}")
    # 期望:ts_code=600519.SH / volume=1000 / last_price=10.5 / snapshot_timestamp=1234567890
    # 没有 thscode/ticker/data_timestamp/save_timestamp

    # 测 _decode_stream(payload 兼容)
    raw = [
        ("1-0", {"ts_code": "600519.SH", "volume": "1000"}),
        ("2-0", {"payload": '{"ts_code": "000001.SZ", "volume": 500}'}),
    ]
    print(f"Decoded stream: {r._decode_stream(raw)}")

    r.cleanup()

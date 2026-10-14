"""
core/persist_client.py
=======================

落盘接口 — 从 Redis 取数据,写入 SQLite。

设计(2026-09-11 重构后):
- service_savedata 调用这里提供的接口
- 按 kind 路由到不同的 persist_xxx 方法
- 使用 STREAM XREAD + last_id 游标模式:
  - **2026-09-11 改造**:游标持久化到 SQLite `online_stream_cursor` 表
  - 进程重启后从 SQLite 读 last_id 继续读,不丢不重
- minute 从每只股 ZSET 读 ZRANGEBYSCORE(1 天范围内)
- zt / break / hot / anomaly / limitperformance 统一从 STREAM + timeline ZSet 索引 + archive HSET(MyATM 风格,v6)

为什么不直接由 service_savedata 操作 SQLite:
- 接口统一,以后落盘逻辑变(比如加压缩/校验)只改这里
- 测试容易(可以 mock redis/sqite 连接)
- 与"core 只提供接口"原则一致

2026-09-11 游标持久化方案:
- 旧:游标在 Redis `online:meta.last_stream_id:{kind}`,进程重启/Redis 重启可能丢
- 新:游标在 SQLite `online_stream_cursor` 表,持久化,重启后自动续上
- 兼容:启动时检查 SQLite cursor;Redis 里如有旧 cursor,迁移一次
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any

from core.redis_online import OnlineRedis, PREFIX
from core.sqlite_client import (
    connect,
    count_snapshots,
    insert_snapshots_batch,
    insert_minute_bars_batch,
    read_cursor,
    update_cursor,
)
from core.logger import setup_logger

logger = setup_logger("persist_client")


# 一次 XREAD 拉多少条
STREAM_BATCH = 1000
# XREAD block 时长(ms)
STREAM_BLOCK_MS = 2000


# ============================================================
# STREAM 类的 kind(auction / snapshot / orderbook)
# ============================================================
def _get_cursor(redis_client: OnlineRedis, conn, kind: str) -> str:
    """读游标优先级:SQLite > '0'(从最早开始)

    2026-09-12 v3 简化:删 Redis legacy meta fallback
      - 单一事实源:online_stream_cursor 表
      - 没有 → "0" (从头读,适合首次启动 / 测试场景)
      - 删了旧的 online:last_stream_id:<kind> meta key 兼容层
    """
    stream_key = f"{PREFIX}:{kind}:stream"
    last_id = read_cursor(conn, stream_key)
    if last_id:
        return last_id
    return "0"


def persist_stream_kind(
    redis_client: OnlineRedis,
    *,
    kind: str,
    trade_date: str,
    batch_size: int = STREAM_BATCH,
    drain: bool = False,
    max_batches: int = 100,
) -> int:
    """从 STREAM 读游标之后的新消息,落盘到 SQLite

    2026-09-11 v2 重构:源头展开
        - STREAM 字段已展开成 N 个独立 field(无 payload 嵌套)
        - 直接读 fields 作为 record dict,无需 json.loads
        - 兼容旧格式:有 payload 字段就 json.loads(读 redis_client 兼容层)

    2026-09-15 v6.8 加 drain 模式:
        - drain=True 时,落盘后若本轮 batch 满(batch_size 条)立即再读下一批
        - 直到 batch 不满 / STREAM 空 / 达到 max_batches 上限 / 出错
        - 治本:savedata 间隔长也不积压,有多少落多少

    Args:
        kind: 'auction' / 'snapshot' / 'orderbook'
        trade_date: YYYYMMDD
        batch_size: 单次 XREAD 的 count
        drain: 是否 drain(连续读完所有积压,默认 False 单 batch)
        max_batches: drain 模式安全上限(防止游标错乱时死循环,默认 100)

    Returns:
        写入条数(去重后,多 batch 累计)
    """
    if kind not in ("auction", "snapshot", "orderbook", "snapshot_index"):
        raise ValueError(f"kind 必须是 auction/snapshot/orderbook/snapshot_index,实际 {kind!r}")

    stream_key = f"{PREFIX}:{kind}:stream"

    ym = trade_date[:6]
    conn = connect(ym)
    save_ts_unix = time.time()  # ★ 本次落盘 wall clock(所有本次 INSERT 共用)
    save_ts_iso = datetime.fromtimestamp(save_ts_unix).isoformat(timespec="milliseconds")
    total_inserted = 0
    try:
        # ★ v6.8 drain 模式:while 循环连续读完所有积压
        #   退出条件:batch 不满 / STREAM 空 / 达到 max_batches / 落盘失败
        #   非 drain 模式:只读一批就 return
        batch_idx = 0
        while True:
            batch_idx += 1
            last_id = _get_cursor(redis_client, conn, kind)

            # 1. XREAD 读游标之后的新消息
            raw = redis_client.r.xread({stream_key: last_id}, count=batch_size, block=STREAM_BLOCK_MS)
            if not raw:
                logger.debug(f"[{kind}] STREAM 无新消息")
                break

            # raw 格式:[(stream_key, [(id, {field: value, ...}), ...]), ...]
            msgs = raw[0][1] if raw else []
            if not msgs:
                break

            # 2. 解析 STREAM fields → record dict(2026-09-11 v2:字段已展开,无需 json.loads)
            snapshots = []
            max_id = last_id
            for stream_id, fields in msgs:
                max_id = stream_id  # 记录最大 id,落盘成功后推进游标

                # v2:直接用 fields 作为 record dict(类型自动还原在 redis_client 那边完成)
                #    兼容旧格式:有 payload 字段就 json.loads
                if "payload" in fields and isinstance(fields["payload"], str):
                    # 旧格式
                    try:
                        record = json.loads(fields["payload"])
                    except json.JSONDecodeError:
                        continue
                else:
                    # v2 格式:字段已展开,直接读
                    record = dict(fields)

                ts_code = record.get("ts_code", "")
                if not ts_code:
                    continue

                # 2026-09-12 v3 精简(2026-09-12 升级:支持任意 <kind>_timestamp):
                #   - 只保留 <kind>_timestamp(数据时间戳,整数毫秒 → 转 ISO 字符串)
                #   - created_at 由 SQLite DEFAULT 自动加
                #   - 不再依赖 save_timestamp / data_timestamp / snap_ts / snap_ts_unix
                kind_ts_field = f"{kind}_timestamp"  # auction_timestamp / orderbook_timestamp / snapshot_timestamp
                snap_ts_raw = record.get(kind_ts_field)
                if snap_ts_raw is None:
                    # 兼容旧字段名(防御性)
                    snap_ts_raw = (
                        record.get("data_timestamp")
                        or record.get("snap_ts_unix")
                    )
                if snap_ts_raw is not None:
                    try:
                        # int 毫秒(或 float 秒) → datetime 本地时区 → ISO 字符串
                        try:
                            ms = int(snap_ts_raw)
                            # > 10^12 视为毫秒,否则视为秒
                            if ms > 1e12:
                                ts_dt = datetime.fromtimestamp(ms / 1000.0)
                            else:
                                ts_dt = datetime.fromtimestamp(ms)
                        except (ValueError, TypeError):
                            # 字符串 ISO → 直接用
                            ts_dt = datetime.fromisoformat(snap_ts_raw)
                        record[kind_ts_field] = ts_dt.isoformat(timespec="milliseconds")
                    except (ValueError, TypeError, OSError):
                        # 兜底:用落盘瞬间
                        record[kind_ts_field] = save_ts_iso
                else:
                    # <kind>_timestamp 缺失 → 用落盘瞬间兜底
                    record[kind_ts_field] = save_ts_iso

                # ★ v2 源头展开:record 顶层直接是字段,executor 从顶层取列
                #    不再嵌套 payload 子 dict(sqlite_client._field() 优先读顶层)
                snapshots.append({
                    "ts_code": ts_code,
                    **record,  # ★ 顶层展平 record 所有字段,executor _field() 优先读
                })

            if not snapshots:
                # 没有有效数据但有消息,还是要推进游标避免死循环
                update_cursor(conn, stream_key=stream_key, last_id=max_id, trade_date=trade_date)
                # 非 drain 模式:已无有效数据,不再继续
                if not drain:
                    return 0
                # drain 模式:这一批 0 条,直接退出循环(避免游标卡在同位置)
                break

            # 3. 落盘
            inserted = insert_snapshots_batch(
                conn,
                kind=kind,
                trade_date=trade_date,
                snapshots=snapshots,
            )
            logger.info(
                f"[{kind}] STREAM 落盘 {inserted}/{len(snapshots)} 条到 {ym}/{kind}_{trade_date},"
                f"last_id={max_id}, save_ts={save_ts_iso}"
                + (f" [drain batch {batch_idx}/{max_batches}]" if drain else "")
            )

            # 4. 推进游标到 SQLite(只有成功插入后才推进,失败重读不丢)
            # 2026-09-15 v6.7:inserted=-1 表示 SQLite 真失败(区别于 INSERT OR IGNORE 全去重返回 0),不推进游标
            if inserted < 0:
                logger.error(
                    f"[{kind}] 落盘失败,游标未推进(下次会重读 STREAM > last_id={last_id}),last_id={max_id}"
                )
                return inserted
            update_cursor(conn, stream_key=stream_key, last_id=max_id, trade_date=trade_date)
            total_inserted += inserted

            # 5. ★ v6.8 drain 模式判断:batch 满 + 未达 max_batches → 继续下一批
            if not drain:
                return total_inserted
            if len(msgs) < batch_size:
                # batch 不满说明 STREAM 已读完
                break
            if batch_idx >= max_batches:
                logger.warning(
                    f"[{kind}] drain 达到 max_batches={max_batches} 上限,停止"
                    f"(本轮已落 {total_inserted} 条,可能仍有积压,下次再追)"
                )
                break

        return total_inserted
    finally:
        conn.close()


# ============================================================
# watchlist(监控列表)— v6.7(2026-09-15)从 STREAM 增量落盘
# ============================================================
def persist_watchlist(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
    drain: bool = False,
    max_batches: int = 100,
) -> int:
    """v6.7:读 watchlist STREAM 增量落盘到 watchlist_<YYYYMMDD> 表

    与其他 8 kind 不同:
      - watchlist 每天允许多份(09:10 生成、11:30 重生成、14:30 再生成 等)
      - 表 schema 改为 UNIQUE(ts_code+watchlist_timestamp),增量 INSERT
      - 走 online_stream_cursor 统一游标表(kind='watchlist')

    流程:
      1. 读 online_stream_cursor 里 watchlist 的 last_id
      2. XREAD watchlist:stream > last_id 拿新消息
      3. 解析 STREAM fields → record dict(每条 = 1 只股票)
      4. insert_snapshots_batch(kind="watchlist", ...) 增量落盘
      5. update_cursor 推进到 SQLite online_stream_cursor(kind='watchlist')

    2026-09-15 v6.8 加 drain 模式(同 persist_stream_kind):
      - drain=True 时连续读完所有积压,batch 满就再读下一批
      - watchlist 一天最多 4-5 批,通常不需要 drain;但保留入口兜底

    Returns:
        写入条数(去重后,多 batch 累计)
    """
    stream_key = f"{PREFIX}:watchlist:stream"
    ym = trade_date[:6]
    conn = connect(ym)
    save_ts_iso = datetime.now().isoformat(timespec="milliseconds")
    total_inserted = 0

    try:
        # ★ v6.8 drain 模式:while 循环连续读完所有积压
        batch_idx = 0
        while True:
            batch_idx += 1
            # 1. 读游标(走 online_stream_cursor 统一表)
            last_id = _get_cursor(redis_client, conn, "watchlist")

            # 2. XREAD
            raw = redis_client.r.xread({stream_key: last_id}, count=STREAM_BATCH, block=STREAM_BLOCK_MS)
            if not raw:
                logger.debug("[watchlist] STREAM 无新消息")
                break

            msgs = raw[0][1] if raw else []
            if not msgs:
                break

            # 3. 解析 → snapshots
            snapshots = []
            max_id = last_id
            for stream_id, fields in msgs:
                max_id = stream_id

                # v6.7:watchlist 字段已源头展开
                # 必有:ts_code / source / watchlist_timestamp
                record = dict(fields)  # {field_name: string_value, ...}
                ts_code = record.get("ts_code", "")
                if not ts_code:
                    continue

                # watchlist_timestamp 兼容性:v6.14 起是 int 毫秒(对齐其它 kind);
                # 老数据可能是 ISO 字符串(早期 v6.7)、float 秒(更老);
                # 落盘统一转 ISO 字符串 TEXT
                ts_field = "watchlist_timestamp"
                ts_raw = record.get(ts_field)
                if ts_raw:
                    try:
                        # 试 ISO 解析(老 ISO 字符串路径)
                        datetime.fromisoformat(ts_raw)
                        # 已 ISO,保持
                    except (ValueError, TypeError):
                        # 转 unix → ISO;需要识别毫秒 vs 秒(> 1e12 视为毫秒)
                        try:
                            ts_num = float(ts_raw)
                            if ts_num > 1e12:
                                ts_num = ts_num / 1000.0  # 毫秒 → 秒
                            record[ts_field] = datetime.fromtimestamp(ts_num).isoformat(timespec="milliseconds")
                        except (ValueError, TypeError):
                            record[ts_field] = save_ts_iso

                # ★ watchlist 表的 source/priority/reason 字段处理:
                # priority/reason 不在 STREAM 里(没有这个业务字段),用 source 当 reason 兜底
                if "priority" not in record or not record["priority"]:
                    record["priority"] = 0
                if "reason" not in record or not record["reason"]:
                    record["reason"] = record.get("source", "")

                snapshots.append({
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    **record,
                })

            if not snapshots:
                update_cursor(conn, stream_key=stream_key, last_id=max_id, trade_date=trade_date)
                if not drain:
                    return 0
                break

            # 4. 落盘
            inserted = insert_snapshots_batch(
                conn,
                kind="watchlist",
                trade_date=trade_date,
                snapshots=snapshots,
            )
            logger.info(
                f"[watchlist] STREAM 落盘 {inserted}/{len(snapshots)} 条到 {ym}/watchlist_{trade_date},"
                f"last_id={max_id}, save_ts={save_ts_iso}"
                + (f" [drain batch {batch_idx}/{max_batches}]" if drain else "")
            )

            # 5. 推进游标(2026-09-15 v6.7:失败 -1 不推进,区别于全去重 0)
            if inserted < 0:
                logger.error(
                    f"[watchlist] 落盘失败,游标未推进(下次会重读 STREAM > last_id={last_id}),last_id={max_id}"
                )
                return inserted
            update_cursor(conn, stream_key=stream_key, last_id=max_id, trade_date=trade_date)
            total_inserted += inserted

            # 6. ★ v6.8 drain 模式判断
            if not drain:
                return total_inserted
            if len(msgs) < STREAM_BATCH:
                break
            if batch_idx >= max_batches:
                logger.warning(
                    f"[watchlist] drain 达到 max_batches={max_batches} 上限,停止"
                    f"(本轮已落 {total_inserted} 条,可能仍有积压,下次再追)"
                )
                break

        return total_inserted
    finally:
        conn.close()


# ============================================================
# minute:每只股 ZSET
# ============================================================
def persist_minute(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
) -> int:
    """从每只股 ZSET 读今日分时 K 线,展开落盘到 SQLite

    2026-09-13 v4 重写:
      - 彻底废弃顶层 minute 表 + minute_bars 长表双表结构
      - 统一用 1 张 minute_<date> 表,每根 K 线 1 行
      - 字段对齐 tdx_client.get_minute_kline(ts_code, date) 输出:
        ts_code / trade_date / time_idx / datetime / price / vol / data_timestamp
      - + 我们自己加的 created_at(落盘时间,SQLite DEFAULT 自动加)
      - PK = (ts_code, trade_date, time_idx) 天然去重,重复 INSERT 自动跳过

    注意:minute 的数据是"每只股 1 天 1 个 ZSET",不是 STREAM。
    这里直接 ZRANGE 全部 → 展开 → 落盘 → 保留(不清空,因为 1 天 EXPIRE 自动清)。
    """
    # 找所有 minute:* ZSET(从 watchlist 拿 ts_code 列表,避免 SCAN 全 keyspace)
    watchlist = redis_client.get_watchlist()
    if not watchlist:
        # 没 watchlist 时 SCAN 兜底
        watchlist = [
            k.replace(f"{PREFIX}:minute:", "")
            for k in redis_client.r.scan_iter(f"{PREFIX}:minute:*")
        ]

    if not watchlist:
        logger.debug("[minute] 没有 minute:* ZSET,跳过")
        return 0

    # 把所有 ZSET 里的 bars 展开成 snapshots(每根 K 线 1 个 snapshot)
    snapshots = []
    for ts_code in watchlist:
        bars = redis_client.get_minute_bars(ts_code)
        if not bars:
            continue
        for i, b in enumerate(bars):
            snapshots.append({
                "ts_code": ts_code,
                "trade_date": trade_date,
                "time_idx": int(b.get("time_idx", b.get("bar_idx", i))),
                "datetime": b.get("datetime") or b.get("bar_time") or b.get("time") or "",
                "price": b.get("price"),
                "vol": b.get("vol"),
                "data_timestamp": b.get("data_timestamp"),  # client 源头 wall-clock ISO
            })

    if not snapshots:
        return 0

    ym = trade_date[:6]
    conn = connect(ym)
    try:
        inserted = insert_snapshots_batch(
            conn,
            kind="minute",
            trade_date=trade_date,
            snapshots=snapshots,
        )
        logger.info(
            f"[minute] 落盘 {inserted}/{len(snapshots)} 行到 {ym}/minute_{trade_date},"
            f"覆盖 {len(set((s['ts_code'],) for s in snapshots))} 只股票"
        )
        return inserted
    finally:
        conn.close()


# ============================================================
# zt(涨停/炸板/异动/热股) — 2026-09-11 改造:从 STREAM 落盘
# ============================================================
def _persist_stream_to_sqlite(
    redis_client: OnlineRedis,
    conn,
    *,
    kind: str,
    stream_key: str,
    trade_date: str,
    batch_size: int = STREAM_BATCH,
) -> int:
    """通用:从 STREAM 读游标之后新消息落盘(zt/break/anomaly/hot 共用)

    2026-09-11:每个落盘条打双时间戳
        - data_timestamp   数据时间(数据源/客户端拿到数据时打,来自 payload)
        - save_timestamp   落盘时间(写入 SQLite 的瞬间,本批 INSERT 共用)

    Args:
        kind: 落盘表名('zt' / 'break' / 'anomaly' / 'hot')
        stream_key: 完整 Redis STREAM key(如 'online:zt:stream')

    Returns:
        写入条数(去重后)
    """
    # 1. 读游标
    last_id = read_cursor(conn, stream_key)
    if last_id is None:
        # fallback:Redis meta(兼容旧版)
        # 注意:这里是 stream_key 而非 kind,没有现成 fallback,直接从 '0' 开始
        last_id = "0"

    save_ts_unix = time.time()  # ★ 本次落盘 wall clock
    save_ts_iso = datetime.fromtimestamp(save_ts_unix).isoformat(timespec="milliseconds")

    # 2. XREAD
    raw = redis_client.r.xread({stream_key: last_id}, count=batch_size, block=STREAM_BLOCK_MS)
    if not raw:
        logger.debug(f"[{kind}] STREAM 无新消息")
        return 0

    msgs = raw[0][1] if raw else []
    if not msgs:
        return 0

    # 3. 解析
    snapshots = []
    max_id = last_id
    for stream_id, fields in msgs:
        max_id = stream_id

        # ★ v3 适配(2026-09-12):STREAM 字段已展开,优先直接用顶层字段,fallback 到 payload 嵌套(兼容旧版)
        # 旧版:STREAM 只有 payload(JSON),所有数据要从 payload 拿
        # 新版:STREAM 字段全展开,直接在顶层

        # ★ 数据时间戳(<kind>_timestamp):优先顶层(新),fallback payload(旧)
        kind_ts_field = f"{kind}_timestamp"
        kind_ts_unix = (
            fields.get(kind_ts_field)  # ★ v3 新增:顶层 zt_timestamp 等
            or fields.get("data_timestamp")  # 旧字段
        )
        if kind_ts_unix is None:
            # 旧版 payload 嵌套
            payload_raw = fields.get("payload", "{}")
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                payload = {}
            kind_ts_unix = (
                payload.get(kind_ts_field)
                or payload.get("data_timestamp")
                or payload.get("snap_ts_unix")
            )
        else:
            # 字段展开版可能没有 payload,但仍要兼容
            payload_raw = fields.get("payload")
            if payload_raw:
                try:
                    payload = json.loads(payload_raw)
                except json.JSONDecodeError:
                    payload = {}
            else:
                payload = {}  # 顶层直接是数据字段,无 payload

        # 转 ISO 字符串(SQLite TEXT 列存 ISO,本地时区)
        if kind_ts_unix is not None:
            try:
                kind_ts_unix = float(kind_ts_unix)
                kind_ts_iso = datetime.fromtimestamp(kind_ts_unix).isoformat(timespec="milliseconds")
            except (ValueError, TypeError):
                kind_ts_unix = save_ts_unix
                kind_ts_iso = save_ts_iso
        else:
            kind_ts_unix = save_ts_unix
            kind_ts_iso = save_ts_iso

        # ts_code:优先顶层(新),fallback payload(旧)
        ts_code = (
            fields.get("ts_code")
            or payload.get("ts_code")
            or payload.get("thscode")
            or ""
        )
        if not ts_code and payload.get("ticker"):
            ts_code = payload["ticker"]
        if not ts_code:
            # 没 ts_code 用 kind 名当虚拟键(异动/热股列表可能没有特定 ts_code)
            ts_code = f"_{kind}_{kind_ts_unix}"

        # ★ v3 精简:只设置 zt_timestamp(替代 data_timestamp / save_timestamp)
        # payload 整 dict 仍传给 extractor(_field() 会优先用顶层,fallback 到 payload)
        # 顶层字段也加进去(extractor _field() 优先查顶层)
        record = {
            "ts_code": ts_code,
            kind_ts_field: kind_ts_iso,  # ★ 数据时间戳 ISO 字符串
            "payload": payload,
        }
        # 把所有顶层 STREAM 字段也展开进 record(extractor _field() 直接命中)
        for k, v in fields.items():
            if k == "payload":
                continue  # 已在 record["payload"]
            if k not in record:
                record[k] = v

        snapshots.append(record)

    if not snapshots:
        update_cursor(conn, stream_key=stream_key, last_id=max_id, trade_date=trade_date)
        return 0

    # 4. 落盘
    inserted = insert_snapshots_batch(
        conn,
        kind=kind,
        trade_date=trade_date,
        snapshots=snapshots,
    )
    logger.info(
        f"[{kind}] STREAM 落盘 {inserted}/{len(snapshots)} 条到 {kind}_{trade_date},"
        f"last_id={max_id}, save_ts={save_ts_iso}"
    )
    # 2026-09-15 v6.7:失败 -1 不推进游标,区别于 INSERT OR IGNORE 全去重返回 0
    if inserted < 0:
        logger.error(
            f"[{kind}] 落盘失败,游标未推进(下次会重读 STREAM > last_id={last_id}),last_id={max_id}"
        )
        return inserted
    update_cursor(conn, stream_key=stream_key, last_id=max_id, trade_date=trade_date)
    return inserted


def persist_zt(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
) -> int:
    """从 online:zt:stream 读,落盘到 SQLite(2026-09-11 改造:从 STREAM 而非 ZSET)"""
    ym = trade_date[:6]
    conn = connect(ym)
    try:
        return _persist_stream_to_sqlite(
            redis_client, conn,
            kind="zt",
            stream_key=f"{PREFIX}:zt:stream",
            trade_date=trade_date,
        )
    finally:
        conn.close()


def persist_break(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
) -> int:
    """炸板池落盘(从 STREAM 读)"""
    ym = trade_date[:6]
    conn = connect(ym)
    try:
        return _persist_stream_to_sqlite(
            redis_client, conn,
            kind="break",
            stream_key=f"{PREFIX}:break:stream",
            trade_date=trade_date,
        )
    finally:
        conn.close()


def persist_anomaly(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
    batch_size: int = STREAM_BATCH,
) -> int:
    """异动清单落盘(从 STREAM 读,2026-09-12 v3 改造,2026-09-16 v6.12 精简):
      - 顶层字段 → anomaly_<date> 表
      - keyword_list 不再拆 anomaly_keywords_<date> 长表(用户原话"没必要存")
      - 时间戳 anomaly_timestamp(顶层已存 ISO 字符串)
      """
    from core.sqlite_client import (
        _KIND_EXTRACTORS,
        _KIND_COLUMNS,
        ensure_table,
    )

    ym = trade_date[:6]
    conn = connect(ym)
    try:
        ensure_table(conn, kind="anomaly", trade_date=trade_date)

        stream_key = f"{PREFIX}:anomaly:stream"

        # 1. 读 STREAM(从游标位置开始,简单 xread,不分组)
        last_id = _get_cursor(redis_client, conn, "anomaly")
        raw = redis_client.r.xread({stream_key: last_id}, count=batch_size)
        if not raw:
            logger.info(f"[anomaly] STREAM {stream_key} 无新记录")
            return 0

        msgs = raw[0][1] if raw else []
        if not msgs:
            return 0

        # 2. 提取顶层字段
        # msgs 是 [(stream_id, {field: value}), ...],fields 已是 dict(str→str)
        extractor = _KIND_EXTRACTORS["anomaly"]
        top_rows = []
        max_id = last_id
        save_ts_iso = datetime.now().isoformat(timespec="milliseconds")
        for stream_id, fields in msgs:
            # ★ v3:把 ms/unix 时间戳转 ISO 字符串(统一存盘格式)
            anomaly_ts_raw = fields.get("anomaly_timestamp") or fields.get("data_timestamp")
            if anomaly_ts_raw:
                try:
                    ts_float = float(anomaly_ts_raw)
                    if ts_float > 1e12:  # ms
                        ts_float /= 1000
                    anomaly_ts_iso = datetime.fromtimestamp(ts_float).isoformat(timespec="milliseconds")
                except (ValueError, TypeError):
                    anomaly_ts_iso = save_ts_iso
            else:
                anomaly_ts_iso = save_ts_iso
            fields_with_iso = {**fields, "anomaly_timestamp": anomaly_ts_iso}
            rec = fields_with_iso
            top_rows.append(extractor(rec, trade_date=trade_date))

            # 2026-09-16 v6.12:keyword_list 不再拆 anomaly_keywords 长表(用户要求"没必要存")

            if stream_id > max_id:
                max_id = stream_id

        # 3. 写顶层
        cols = _KIND_COLUMNS["anomaly"]
        placeholders = ",".join("?" for _ in cols)
        col_names = ",".join(cols)
        table_name = f"anomaly_{trade_date}"
        cur = conn.executemany(
            f"INSERT OR IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})",
            top_rows,
        )
        top_inserted = cur.rowcount

        conn.commit()

        logger.info(
            f"[anomaly] STREAM 落盘 {top_inserted}/{len(top_rows)} 条到 {table_name},"
            f"last_id={max_id}"
        )
        update_cursor(conn, stream_key=stream_key, last_id=max_id, trade_date=trade_date)
        return top_inserted
    finally:
        conn.close()


def persist_hot(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
) -> int:
    """热股榜落盘(从 STREAM 读)"""
    ym = trade_date[:6]
    conn = connect(ym)
    try:
        return _persist_stream_to_sqlite(
            redis_client, conn,
            kind="hot",
            stream_key=f"{PREFIX}:hot:stream",
            trade_date=trade_date,
        )
    finally:
        conn.close()


def persist_limitperformance(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
) -> int:
    """涨停表现详情落盘(2026-09-14 v5 新增)

    数据源:online:limitperformance:stream
          (service_writeredis_limitperformance.py 30s/轮写入)
    表:limitperformance_<YYYYMMDD>
    通用模板 _persist_stream_to_sqlite 已经支持任意 kind:
      - 自动按 kind 拼接时间戳字段(本例 = limitperformance_timestamp)
      - 自动转 ISO 字符串
      - 自动 INSERT OR IGNORE(主键冲突跳过)
    """
    ym = trade_date[:6]
    conn = connect(ym)
    try:
        return _persist_stream_to_sqlite(
            redis_client, conn,
            kind="limitperformance",
            stream_key=f"{PREFIX}:limitperformance:stream",
            trade_date=trade_date,
        )
    finally:
        conn.close()


# ============================================================
# 路由入口
# ============================================================
def persist_kind(
    redis_client: OnlineRedis,
    *,
    kind: str,
    trade_date: str,
    drain: bool = False,
) -> int:
    """落盘一种 kind(自动路由到 STREAM / ZSET / 等)

    2026-09-15 v6.8:加 drain 参数,只对 STREAM kind(auction/snapshot/orderbook/watchlist)有效。
    drain=True 时连续读完所有积压,治本(savedata 间隔长也不积压)。
    ZSET 类 kind(minute/zt/break/anomaly/hot/limitperformance)无需 drain,参数被忽略。
    """
    if kind in ("auction", "snapshot", "orderbook", "snapshot_index"):  # 2026-09-16 v6.10 新增 snapshot_index
        return persist_stream_kind(redis_client, kind=kind, trade_date=trade_date, drain=drain)
    elif kind == "watchlist":                                       # 2026-09-15 v6.7 新增
        return persist_watchlist(redis_client, trade_date=trade_date, drain=drain)
    elif kind == "minute":
        return persist_minute(redis_client, trade_date=trade_date)
    elif kind == "zt":
        return persist_zt(redis_client, trade_date=trade_date)
    elif kind == "break":
        return persist_break(redis_client, trade_date=trade_date)
    elif kind == "anomaly":
        return persist_anomaly(redis_client, trade_date=trade_date)
    elif kind == "hot":
        return persist_hot(redis_client, trade_date=trade_date)
    elif kind == "limitperformance":                              # 2026-09-14 v5 新增
        return persist_limitperformance(redis_client, trade_date=trade_date)
    else:
        raise ValueError(
            f"kind 必须是 auction/snapshot/orderbook/snapshot_index/watchlist/minute/zt/break/anomaly/hot/limitperformance,实际 {kind!r}"
        )


def persist_all(
    redis_client: OnlineRedis,
    *,
    trade_date: str,
    kinds: list[str] | None = None,
    drain: bool = False,
) -> dict[str, int]:
    """落盘所有 kind

    Args:
        trade_date: YYYYMMDD
        kinds: 要落盘的 kind 列表,None = 全部 9 种(含 limitperformance)
        drain: 是否 drain(同 persist_kind),默认 False 单 batch

    Returns:
        {kind: inserted_count, ...}
    """
    if kinds is None:
        kinds = ["auction", "snapshot", "orderbook", "watchlist", "minute", "zt", "break", "anomaly", "hot", "limitperformance"]

    results = {}
    for kind in kinds:
        try:
            results[kind] = persist_kind(redis_client, kind=kind, trade_date=trade_date, drain=drain)
        except Exception as e:
            logger.error(f"[{kind}] 落盘失败: {e}", exc_info=True)
            results[kind] = -1

    return results


# ============================================================
# 统计接口
# ============================================================
def daily_summary(trade_date: str) -> dict[str, Any]:
    """返回某日的 SQLite 落盘统计"""
    ym = trade_date[:6]
    conn = connect(ym)
    try:
        return {
            "auction": count_snapshots(conn, kind="auction", trade_date=trade_date),
            "snapshot": count_snapshots(conn, kind="snapshot", trade_date=trade_date),
            "orderbook": count_snapshots(conn, kind="orderbook", trade_date=trade_date),
            "minute": count_snapshots(conn, kind="minute", trade_date=trade_date),
            "zt": count_snapshots(conn, kind="zt", trade_date=trade_date),
        }
    finally:
        conn.close()


# ============================================================
# CLI 测试
# ============================================================
if __name__ == "__main__":
    r = OnlineRedis()
    td = datetime.now().strftime("%Y%m%d")

    # 准备测试数据:写几条 snapshot 到 STREAM
    r.put_snapshot("000001.SZ", {"price": 10.5, "vol": 1000, "snap_ts": f"{datetime.now().date().isoformat()}T09:30:00", "snap_ts_unix": time.time()})
    r.put_snapshot("600519.SH", {"price": 1800.0, "vol": 500, "snap_ts": f"{datetime.now().date().isoformat()}T09:30:01", "snap_ts_unix": time.time()})
    print(f"Redis info: {r.info()}")

    # 落盘
    inserted = persist_kind(r, kind="snapshot", trade_date=td)
    print(f"落盘条数: {inserted}")

    # 看 SQLite 统计
    summary = daily_summary(td)
    print(f"今日落盘统计: {summary}")

    # 清理
    r.cleanup()
    print(f"✅ persist_client.py 测试通过")

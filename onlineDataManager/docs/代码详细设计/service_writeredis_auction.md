# service/service_writeredis_auction.py 详细设计

> **集合竞价快照 service**(09:15-09:25)
> **数据源**:同花顺 `fetch_auction_snapshots(stage='live')`
| **版本**:v6.2 MyATM 改造(2026-09-14)
| **v6.13 更新(2026-09-16)**:`snap_ts` / `snap_ts_unix` 死字段清理 — `service_writeredis_auction.py` daemon + --once 两段 L100/101 删除 `q.setdefault("snap_ts"/"snap_ts_unix", ...)` 注入,record 数据模型收敛到 `auction_timestamp` 单时间戳(同花顺 envelope.data.timestamp 必给,永远到不了 setdefault 兜底)。`redis_online.put_auction` / `core_persist_client.persist_auction` 兜底链同步简化(`snap_ts` / `snap_ts_unix` 兜底分支删除)。

---

## §1 职责

- 拉集合竞价快照
- watchlist = 在线 `online:watchlist` SET
- 周期 = 6 秒/轮(竞价阶段短,频率高;2026-09-11 v6 从 30s 调到 6s)
- 写 Redis `online:auction:*` 一整套(v6.2:STREAM + ZSET timeline + HSET archive)

## §2 v6.2 重构

- 保持 `put_auction` 单股粒度(STREAM 落盘 + window ZSET 1d)
- **一轮末尾**调 `commit_auction_snapshot` 聚合写全市场快照:
  - `online:auction:archive:{unix_ts}` HSET(各 ts_code 1 field)
  - `online:auction:timeline` ZSet(score=unix_ts, member=archive_key)
- timeline 与 archive 都 EXPIRE 43200s(12h),**不滑窗**(用户拍板)
  - 区别于 zt/break/hot 的 10min 滑窗 — auction 是 09:15-09:25 短时竞价
  - 12h 内所有快照都保留,12h 后整体 GC

## §3 调用链

```
service_writeredis_auction
  └─ OnlineRedis()
  └─ from coreClient.ths_client import fetch_auction_snapshots  # 同花顺(不是 TdxClient)
  └─ r.get_watchlist()                                          # 读 online:watchlist
  └─ fetch_auction_snapshots(watchlist, stage=stage)            # watchlist 是位置参数,stage 是关键字(2026-09-14 校正)
  └─ for ts_code, data: r.put_auction(ts_code, data)
  └─ r.commit_auction_snapshot(snapshot_data)                  # 一轮末尾聚合
  └─ sleep <interval> 秒(默认 6s)

> ⚠️ doc 早期版本写 "sleep 30 秒" 已废弃,实际 **interval 默认 6s**(v6.2 改)。

## §4 Redis 数据结构(2026-09-14 校正,auction 特殊性)

| key | 类型 | EXPIRE | 写入 |
|---|---|---:|---|
| `online:auction:stream` | STREAM | **21600s(6h)** = `STREAM_TTL` = `DEFAULT_STREAM_TTL`(**不是 12h**,与其它 kind 一致)| 每次 `put_auction` |
| `online:auction:window:{ts_code}` | ZSET | 86400s(1 天)= `WINDOW_TTL_AUCTION` | 每次 `put_auction` |
| `online:auction:timeline` | ZSET | **43200s(12h)** = `AUCTION_TIMELINE_TTL` | `commit_auction_snapshot` 时 |
| `online:auction:archive:{unix_ts}` | HASH | **43200s(12h)** = `AUCTION_ARCHIVE_TTL` | `commit_auction_snapshot` 时 |

> ⚠️ **auction 特殊分层**:
> - **STREAM** 跟其它 kind 一致 21600s(6h,标准 `STREAM_TTL`)
> - **timeline / archive** 才特殊 43200s(12h,v6.2 改),因为 commit 时需要全市场聚合快照供 savedata 兜底查全市场 12h 内任意时刻
> - ⚠️ doc 早期版本(包括本轮体检前)写 "STREAM 43200s(12h)" 是错的,真值 21600s(`redis_online.py:224 ttl=STREAM_TTL`)
> 理由:auction 是 09:15-09:25 短时竞价,数据有限,12h 完整保留供 savedata 兜底。

## §5 启动方式

```bash
python3 -m service.service_writeredis_auction --interval 6
python3 -m service.service_writeredis_auction --once
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_auction** | 读 STREAM `online:auction:stream` 落盘 |

## §7 历史变更

- **2026-09-11 v6.2 节奏调整**:周期 30s → 6s
- **2026-09-14 v6.2**:加 timeline / archive(参考 MyATM)
- **2026-09-16 v6.13**:`snap_ts` / `snap_ts_unix` 死字段清理(写入端删除 `q.setdefault(...)` 注入,record 数据模型收敛到 `auction_timestamp` 单时间戳)

# service/service_writeredis_snapshot.py 详细设计

> **连续竞价快照 service**(09:30-11:30 + 13:00-15:00)
> **数据源**:同花顺
| **版本**:v6.3 MyATM 改造(2026-09-14,原 realtime.py 改名)
| **v6.14 更新(2026-09-16)**:`snap_ts` / `snap_ts_unix` 死字段清理 — `service_writeredis_snapshot.py` daemon + --once 两段 L100/101 + L153/154 删除 `q.setdefault("snap_ts"/"snap_ts_unix", ...)` 注入 + 死代码 `now_iso=now_dt.isoformat(...)` / `now_unix=now_dt.timestamp()`(保留 `now_dt` 给 L105 `commit_snapshots_batch` 用)。record 数据模型收敛到 `snapshot_timestamp` 单时间戳(同花顺 envelope.data.timestamp 必给,永远到不了 setdefault 兜底)。

---

## §1 职责

- 拉连续竞价快照(每 6s 拉一次)
- watchlist = 在线 `online:watchlist` SET
- 写 Redis `online:snapshot:*` 一整套(v6.3:STREAM + ZSET timeline + HSET archive + ZSET window)

## §2 v6.3 重构(2026-09-14)

**从 v6.2 的 window 简单滑窗**升级到 v6.3 MyATM 风格:

- `put_snapshots_batch`(批量 pipeline)+ `commit_snapshots_batch`(一轮末尾聚合 commit)
- `commit_snapshots_batch` 内部:
  - ZADD `online:snapshot:timeline` (score=now, member=archive_key)
  - HSET `online:snapshot:archive:{now}` 各 ts_code 1 field
  - 滑窗裁剪 timeline(保留最近 N 秒)
  - EXPIRE = `calc_snapshot_archive_ttl(now_dt)` 动态算

## §3 动态 TTL / 滑窗(2026-09-14 用户拍板)

**两个独立的动态计算函数**(`redis_online.py:117-136`):

```python
calc_snapshot_archive_ttl(now_dt) -> int   # 11:00-11:30 → 7200s,其余 → 1800s(archive EXPIRE 各自过期)
calc_snapshot_slide_window(now_dt) -> int  # 11:00-11:30 → 7200s,其余 → 1800s(timeline ZSET 滑窗裁剪宽度)
```

| 时间段 | archive EXPIRE | timeline 滑窗宽度 | 共同理由 |
|---|---:|---:|---|
| 09:30-11:00 / 13:00-15:00 | 1800s | 1800s | 普通 30min |
| **11:00-11:30** | **7200s** | **7200s** | **跨午休补偿**:11:30-13:00 午休 90min,7200s = 30min 业务 + 90min 午休,撑到 13:00 开盘仍可查到 11:30 之前的数据 |
| 周末 | 1800s | 1800s | 无交易,降级到 BASE |

调用 `calc_snapshot_archive_ttl(now_dt)` / `calc_snapshot_slide_window(now_dt)` 动态算。

## §4 v6.3 修复的 bug

- **`pipe.zrangebyscore` 是 read 命令,pipeline 不会自动发** → 拆两次 execute(先写 pipeline,再单独 read + delete)
- **`get_snapshot_timeline` 缺 `from datetime import datetime` import** → 加 import

## §5 调用链

```
service_writeredis_snapshot
  └─ OnlineRedis()
  └─ from coreClient.ths_client import fetch_snapshots, HithinkCLIError  ← 同花顺 CLI(非 Tdx)
  └─ r.get_watchlist()
  └─ fetch_snapshots(watchlist)                          # ⚠️ 早期文档误写为 TdxClient.fetch_snapshot_quotes(codes=...);实际是同花顺 fetch_snapshots(watchlist),无 codes= 关键字
  └─ r.put_snapshots_batch([(ts_code, data), ...])    # pipeline 写 STREAM + HSET + window
  └─ r.commit_snapshots_batch(items, *, now_dt=None)             # 一轮末尾 commit(timeline + archive),**now_dt 关键字参数**(不是 data_timestamp)
  └─ sleep 6 秒
```

## §6 Redis 数据结构

| key | 类型 | EXPIRE | 写入 |
|---|---|---:|---|
| `online:snapshot:stream` | STREAM | **21600s(6 小时)**(= `STREAM_TTL`= `DEFAULT_STREAM_TTL`= `3600*6`)| MAXLEN 200000 |
| `online:snapshot:window:{ts_code}` | ZSET | 43200s(12h) = `WINDOW_TTL_SNAPSHOT` | put 时(滑动清 30min 前) |
| `online:snapshot:timeline` | ZSET | 动态 | commit 时 |
| `online:snapshot:archive:{unix_ts}` | HASH | 动态 | commit 时 |

> ⚠️ **没有 `online:snapshot:hash:{ts_code}` 这个 key** — v6.3 删了,所有数据走 STREAM + window + timeline/archive。
> ⚠️ doc 早期写"hash EXPIRE 动态"是残留,实际 v6.3 没这个 key。

## §7 启动方式

```bash
python3 -m service.service_writeredis_snapshot --interval 6
python3 -m service.service_writeredis_snapshot --once
```

## §8 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_snapshot** | 读 STREAM 落盘 |

## §9 历史变更

- **2026-09-12 v3**:从 snapshot.py 改名 realtime.py + 精简字段
- **2026-09-14 v6.3**:从 realtime.py 改名 snapshot.py + MyATM 改造 + commit pipeline bug 修复 + 动态 TTL
- **2026-09-16 v6.14**:`snap_ts` / `snap_ts_unix` 死字段清理(写入端删除 `q.setdefault(...)` + 死代码 `now_iso` / `now_unix`,保留 `now_dt` 给 L105 `commit_snapshots_batch` 用);record 数据模型收敛到 `snapshot_timestamp` 单时间戳

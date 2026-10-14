# service_writeredis_watchlist

## 0. 元信息

| 项 | 值 |
|---|---|
| 文件 | `service/service_writeredis_watchlist.py` |
| 阶段 | `watchlist`(09:10-09:14,1 次,v6.15 真值)|
| 数据源 | `core/watchlist_fetch.py` `generate_watchlist(prev_window_days, min_level, use_fallback)` |
| 数据目标 | **Redis 三件套**:`online:watchlist:stream` + `online:watchlist:timeline` + `online:watchlist:archive:{unix_ts}` |
| 落盘 | **不落盘**(由 `service_savedata_watchlist` 读 STREAM 增量落) |
| 子进程入口 | `service_writeredis_watchlist.main()` → `write_watchlist_to_redis()` |
| 触发方式 | `launchd StartInterval=...` 或 `--once` 一次性 |
| 父调度 | `scheduler/scheduler_onlineData.py` v6.15 阶段 `watchlist`(09:10-09:14,WRITER + SAVEDATA 共用)|
| 是否预生成 | 否(每次都重新调 `generate_watchlist`)|

**v6.7 改造(2026-09-15)**:从"只写 SET + sources HASH"改为"STREAM + timeline + archive 三件套",对齐 zt/break/anomaly 等其他 kind。**TTL 全 12h(43200s)**。
**v6.14 更新(2026-09-16)**:`watchlist_timestamp` 数据格式从 ISO 字符串 → int 毫秒(`int(time.time() * 1000)`)。对齐其它 kind 时间戳统一规则。`redis_online.put_watchlist` 类型签名同步扩展(`int | float | str` 兼容历史老数据 + 新 int 毫秒)。

## 1. 业务流程

```
[watchlist 09:10]
    ↓
service_writeredis_watchlist.main()
    ↓
write_watchlist_to_redis(OnlineRedis, log)
    ├─ 1. 调 generate_watchlist(prev_window_days=3, min_level=1, use_fallback=True)
    │     拉昨日涨停 + 前几日连板 + 跌停异动 → 返回 (ts_codes, sources)
    │     ts_codes: ['600519.SH', '000001.SZ', ...]  # 沪深 A 股代码列表
    │     sources: {'600519.SH': 'latest_limit', '000001.SZ': 'limit_follow_2', ...}
    │     watchlist_timestamp: int 毫秒(v6.14)/ str ISO(老数据)/ float 秒(老数据)
    ├─ 2. 调 redis_client.put_watchlist(ts_codes, sources, ts_unix)
    │     ├─ XADD online:watchlist:stream * ts_code=... source=... watchlist_timestamp=...
    │     │     EXPIRE 43200s
    │     ├─ ZADD online:watchlist:timeline score=ts_unix member=load_ts_iso
    │     │     EXPIRE 43200s
    │     └─ HSET online:watchlist:archive:{ts_unix} ts_code1=source1 ts_code2=source2 ...
    │           EXPIRE 43200s
    ├─ 3. 调 redis_client.commit_watchlist_snapshot(ts_unix)  # 写 archive:{ts} 聚合 + meta
    │     HSET online:watchlist:archive:{ts_unix} meta_count=N meta_source_csv=... meta_watchlist_timestamp=...
    └─ 4. 返回写入 STREAM 的 ts_code 数量
```

## 2. Redis 三件套

### 2.1 STREAM `online:watchlist:stream`

- **类型**:STREAM
- **写入**:XADD 每条 = 1 只股票(2026-09-15 v6.7,**一条股票一条记录**,不是聚合)
- **字段**:ts_code, source, watchlist_timestamp(ISO), load_ts_unix, meta_kv_json
- **TTL**:43200s(12h)
- **MAXLEN**:10000(防爆)
- **消费者**:`service_savedata_watchlist` 走 `XREAD > last_id`

### 2.2 TIMELINE ZSET `online:watchlist:timeline`

- **类型**:ZSET
- **写入**:ZADD score=unix_ts member=load_ts_iso
- **TTL**:43200s(12h)
- **清理策略**:**不做**时间窗口移动清理(用户拍板)
- **用途**:看历史 watchlist 加载时间点

### 2.3 ARCHIVE HASH `online:watchlist:archive:{unix_ts}`

- **类型**:HASH(每加载时间戳独立 key)
- **写入**:HSET 一次性把所有 ts_code/source 写入一个 HASH + meta 字段
- **TTL**:43200s(12h)
- **用途**:**最新 watchlist 快速读**(其他 service 调 `get_watchlist_sources()` 从 score最大的 archive 取)
- **同 MyATM 风格**:每 ~一次性加载 1 个 archive key,带 EXPIRE,不再使用即过期

## 3. TTL 设计(用户拍板)

| Key | TTL | 来源 |
|---|---|---|
| `online:watchlist:stream` | **43200s(12h)** | 用户拍板,三件套统一 |
| `online:watchlist:timeline` | **43200s(12h)** | 用户拍板,三件套统一 |
| `online:watchlist:archive:{ts}` | **43200s(12h)** | 用户拍板,三件套统一 |

**设计意图**:watchlist 现在只加载一次(09:10),但用户原话"虽然目前只加载一次,但是也要为未来做好准备"——三件套提供完整的可观测性。

## 4. 与其他 kind 的差异

| 维度 | watchlist | auction / snapshot / snapshot_index / minute / limitperformance / zt / break / anomaly / hot |
|---|---|---|
| 写频率 | **极低**(1 次/天)| 高(每 6s-120s/股,看 kind)|
| 数据量 | ~124 条/天(全市场) | ~30k 条/天 |
| STREAM 一条 = ? | **1 只股票** | 1 个时间点全市场聚合 |
| archive 用途 | "最新 watchlist 快速读" | "某时间点全市场快照" |

## 5. 设计意图

- **三件套模式与其他 9 kind 完全对齐**(v6.15:删 orderbook,加 snapshot_index):stream/timeline/archive 三个 key 各有清晰职责
- **archive 的核心价值**:其他 9 个 service(snapshot / snapshot_index / minute / zt / break / anomaly / hot / limitperformance / auction)调 `get_watchlist()` 时,**不需要扫 STREAM 全表**,直接从 score 最大的 archive 拿最新一份,O(1) 复杂度
- **timeline 仅记录"什么时候加载过 watchlist"**,不携带股票数据 — 用户拍板"timeline也没有按照时间窗口移动清理的需求"
- **前瞻性**:虽然目前 watchlist 只加载一次,完整三件套为未来"多次加载 watchlist"准备好(09:10 一次 / 11:30 一次 / 14:30 一次 等)

## 6. 历史变更

- **v3(2026-09-11)**:初版,只写 SET + sources HASH,2 个 key
- **v4(2026-09-13)**:generate_watchlist 函数化,支持 prev_window_days / min_level 参数
- **v6.7(2026-09-15)**:走 STREAM + timeline + archive 三件套,TTL 全 12h,落盘改读 STREAM
- **v6.14(2026-09-16)**:`watchlist_timestamp` 数据格式 ISO 字符串 → int 毫秒(`int(time.time() * 1000)`);`redis_online.put_watchlist` 类型签名扩展(`int | float | str` 兼容 3 种格式)
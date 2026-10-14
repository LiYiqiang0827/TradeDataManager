# service_savedata_watchlist

## 0. 元信息

| 项 | 值 |
|---|---|
| 文件 | `service/service_savedata_watchlist.py` |
| 周期 | **15 分钟一次**(v6.15 用户规范)|
| 数据源 | **Redis STREAM** `online:watchlist:stream`(v6.7 改造后) |
| 数据目标 | SQLite `watchlist_<YYYYMMDD>` 表(v6.7:增量 INSERT) |
| 子进程入口 | `service_savedata_watchlist.main(SavedataWatchlist)` |
| 触发方式 | 基类 `SavedataWatchlist.run_loop` 周期执行 |
| 父调度 | `scheduler/scheduler_onlineData.py` v6.15 **2 阶段**:`morning_savedata`(09:30-11:46)+ `afternoon_savedata`(13:00-15:16) |
| 是否游标 | **是**(`online_stream_cursor`(统一表)kind="watchlist" 行)|

**v6.7 改造(2026-09-15)**:**从"覆盖式落盘(自己调 generate_watchlist + replace_watchlist)"改为"读 STREAM 增量落盘(走 persist_kind(kind='watchlist') 统一模板)"**,与其他 kind 完全对齐(v6.15 共 9 kind:watchlist / auction / snapshot / snapshot_index / minute / zt / break / anomaly / hot / limitperformance,共 10 kind)。

## 1. 业务流程(v6.7 改造后)

```
[15 分钟周期(v6.15)]
    ↓
SavedataWatchlist._do_persist()
    ↓
调 persist_kind(redis_client, kind="watchlist", trade_date="20260915")
    ↓
调 persist_watchlist(redis_client, trade_date="20260915")  ← core/persist_client.py 新增
    ├─ 1. 读 online_stream_cursor(SELECT last_id WHERE stream_key LIKE '%watchlist%')
    │     → 返回 last_id(首次 = "0-0")
    ├─ 2. XREAD online:watchlist:stream > last_id COUNT 500
    │     → 拿到所有未落盘的新消息(每个 = 1 只股票)
    ├─ 3. 解析 fields → record dict:
    │     {
    │       ts_code: '603938.SH',
    │       source: 'latest_limit',
    │       watchlist_timestamp: '2026-09-15T09:10:30',
    │       ...
    │     }
    ├─ 4. insert_snapshots_batch(kind='watchlist', snapshots=[...])
    │     ├─ ensure_table('watchlist', '20260915') → watchlist_20260915
    │     ├─ _KIND_EXTRACTORS['watchlist'] = _extract_watchlist_row
    │     │     → (trade_date, ts_code, source, priority, reason, watchlist_timestamp)
    │     └─ INSERT OR IGNORE (UNIQUE(ts_code, watchlist_timestamp))
    │     → 落盘 124 条
    └─ 5. update_cursor(conn, stream_key, last_id, trade_date)
          → 推进游标到 SQLite online_stream_cursor 表
```

## 2. SQLite 表 schema(2026-09-15 v6.7)

```sql
CREATE TABLE IF NOT EXISTS watchlist_<YYYYMMDD> (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    source TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    watchlist_timestamp TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ts_code, watchlist_timestamp)   -- v6.7:增量 INSERT 去重
);
CREATE INDEX IF NOT EXISTS idx_<table>_priority ON <table>(priority DESC);
CREATE INDEX IF NOT EXISTS idx_<table>_trade_date ON <table>(trade_date);
CREATE INDEX IF NOT EXISTS idx_<table>_watchlist_ts ON <table>(watchlist_timestamp);
```

**v6.7 关键变化**:
- `ts_code UNIQUE` → `UNIQUE(ts_code, watchlist_timestamp)` 允许多份并存
- `replace_watchlist` 函数保留但 savedata 不再用(只在 test/兜底场景用)

## 3. 三层架构

```
writeredis(写 Redis)
    ↓
service_savedata_watchlist(读 STREAM 增量落盘)
    ↓
core/persist_client.persist_watchlist
    ↓
core/sqlite_client.insert_snapshots_batch(kind="watchlist")
    ↓
watchlist_<YYYYMMDD> 表(增量 INSERT)
```

## 4. 设计意图

- **走统一模板**:与其他 8 kind 完全一样,都是 XREAD STREAM > 游标 > 批量 INSERT,降低了心智负担
- **增量语义**:一天允许多份 watchlist(09:10 一次 / 11:30 一次 / 14:30 一次等),表里能看到完整历史,UNIQUE(ts_code, watchlist_timestamp) 去重
- **游标统一表**:`online_stream_cursor`(用户拍板),不走独立表
- **`replace_watchlist` 函数保留但不调用**:作为 legacy API,可能被外部 test 调用

## 5. 历史变更

- **v1(2026-09-11)**:初版,子类 override _do_persist,调 generate_watchlist 重写 watchlist_YYYYMMDD(覆盖)
- **v3(2026-09-12)**:generate_watchlist 支持 prev_window_days / min_level
- **v6.7(2026-09-15)**:**走 STREAM 增量落盘**(模板同其他 8 kind),表 schema 改为 UNIQUE(ts_code+watchlist_timestamp)
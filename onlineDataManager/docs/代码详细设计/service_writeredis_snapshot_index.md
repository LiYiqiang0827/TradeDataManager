# service/service_writeredis_snapshot_index.py 详细设计

> **指数行情快照 service**(8 只指数,**v6.15:10s/轮**,09:29-11:31 + 12:59-15:01)

> **v6.15 更新(2026-09-16)**:频率 30s → **10s**(用户最新统一规定,普通 kind 频率表)
> **数据源**:同花顺(`hithink-finance index snapshot`)
> **版本**:v6.10(2026-09-16,新增)
| **v6.14 更新(2026-09-16)**:`snapshot_unix_ms` 死字段清理 — `service_writeredis_snapshot_index.py` daemon + --once 两段删除 `q.setdefault("snapshot_unix_ms", now_unix)` 注入 + 死代码 `now_unix = now_dt.timestamp()`(保留 `now_dt` / `now_iso` 给 L127 commit 用)。`redis_online.put_snapshots_index_batch` / `commit_snapshots_index_batch` 完全无引用 `snapshot_unix_ms`,无需清理。

---

## §1 职责

- 每 30 秒拉一次 8 只固定指数的快照
- 写 Redis `online:snapshot_index:*` 一整套(timeline + archive + stream,标准三件套)
- 时间窗由 service 内部判断(`is_trading_window()`):
  - **写入**:09:29-11:31 + 12:59-15:01(其余时段 sleep)
  - **跨午休 11:31-12:59** 不写(自然落盘路径)

## §2 8 只固定指数(INDEX_LIST)

```python
INDEX_LIST = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创 50
    "000016.SH",  # 上证 50
    "000300.SH",  # 沪深 300
    "000905.SH",  # 中证 500
    "000852.SH",  # 中证 1000
]
```

**用户原话**:「指数就固定监测 9 个...」+ 「a 只用8个吧」(ths CLI 找不到北证 50,BJ 后缀全部失败)

## §3 时间窗(用户原话)

- **写入时间窗**:上午 **09:29-11:31**,下午 **12:59-15:01**
- **落盘时间窗**:上午 **09:29-11:45**,下午 **12:45-15:01**(11:45-12:45 落盘休息)
- **落盘频率**:每 **15 分钟一次**(基类 `DEFAULT_INTERVAL=900s`,落盘阶段启 daemon)
- **写入频率**:每 **30 秒一次**(INTERVAL_SEC=30)

## §4 v6.10 设计决策

| 决策 | 选择 | 原因 |
|---|---|---|
| 数据源 | `fetch_index_snapshot`(`hithink-finance index snapshot`) | 用户拍板"不动了,就叫 fetch_index_snapshot" |
| Phase 划分 | **共用 morning_writer / afternoon_writer**(不复用 snapshot phase) | 用户拍板"单独 phase" → service 内部 `is_trading_window()` 严格判断时间窗 |
| `ts_code` 字段 | 直接用 `_flat_record` 重命名后的 ts_code | 同花顺 thscode = 指数代码本身 |
| `ticker` 保留 | 是 | 8 只指数 ticker(1A0001/1B0016 等)对账需要 |
| `snapshot_unix` 列 | 必有(替代 lu_time) | 用户最高优先级:任何表都存 unix int64;指数无涨停时间,改名更合适 |

## §5 v6.10 修复的 bug(实施中遇到的)

- **`ths_client.fetch_index_snapshot` 给的 `snapshot_timestamp` 是 int unix ms**,不是 ISO 字符串
  → extractor `_extract_snapshot_index_row` 内部 `datetime.fromtimestamp(ms/1000)` 转换
- **`DEFAULT_DROP_KEYS` 默认 drop 掉 thscode / ticker**
  → `put_snapshots_index_batch` 用 `drop_keys=DEFAULT_DROP_KEYS - {"thscode", "ticker", "snapshot_timestamp"}` 保留

## §6 调用链

```
service_writeredis_snapshot_index
  └─ OnlineRedis()
  └─ from coreClient.ths_client import fetch_index_snapshot    # 指数专用接口
  └─ r.put_snapshots_index_batch([(ts_code, data), ...])    # pipeline 写 STREAM + HSET + window
  └─ r.commit_snapshots_index_batch(items)                    # 一轮末尾 commit(timeline + archive)
  └─ sleep 30 秒
```

## §7 redis_online 新增方法

```python
# 常量
STREAM_MAXLEN_SNAPSHOT_INDEX        = 3000     # 8 只 × 30s × 4h ≈ 3840,留 3000
WINDOW_TTL_SNAPSHOT_INDEX           = 12 * 3600
SNAPSHOT_INDEX_TIMELINE_TTL         = 12 * 3600
SNAPSHOT_INDEX_ARCHIVE_TTL          = 6 * 3600
STREAM_TTL_SNAPSHOT_INDEX           = 12 * 3600

# 写方法
put_snapshot_index(ts_code, data)
put_snapshots_index_batch(items)                  # pipeline STREAM + HSET window
commit_snapshots_index_batch(items)              # ZADD timeline + HSET archive + EXPIRE

# 读方法
get_snapshot_index_timeline(count=50)             # ZRANGE 读 timeline
```

## §8 v6.10 与 snapshot 差异

| 维度 | snapshot(个股) | snapshot_index(指数) |
|---|---|---|
| watchlist 源 | `online:watchlist` SET(动态) | **8 只固定** INDEX_LIST |
| 拉取频率 | 6 秒 | **30 秒** |
| 写入时间窗 | 09:30-11:30 / 13:00-15:00 | **09:29-11:31 / 12:59-15:01** |
| archive TTL 动态 | 是(lunch-aware) | **否**(固定 6h,无 lunch 动态) |
| timeline 滑窗 | **是**(`calc_snapshot_slide_window` 动态) | **否**(`snapshot_index` timeline **不做滑窗清理**,用户原话) |
| 落盘频率 | 15 分钟(基类默认) | **15 分钟**(基类默认) |
| 死字段(已清理) | `snap_ts` / `snap_ts_unix`(v6.14) | `snapshot_unix_ms`(v6.14)— 死字段清理 |

## §9 历史变更

- **2026-09-16 v6.10**:初版,新增 8 只固定指数(000300.SH / 000688.SH / 000905.SH / 399001.SZ / 399006.SZ / 399005.SZ / 000852.SH / 000016.SH)三件套
- **2026-09-16 v6.14**:`snapshot_unix_ms` 死字段清理(写入端 `q.setdefault(...)` 删除,保留 `now_dt` / `now_iso` 给 L127 commit 用)
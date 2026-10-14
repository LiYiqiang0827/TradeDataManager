# service/service_savedata_minute.py 详细设计

> **minute 落盘 daemon**(1 分钟 K 线)
> **特殊**:不走 STREAM,走 **ZSET**(`online:minute:{ts_code}`,**无 `:bars:` 中缀**)

---

## §1 职责

- 读 Redis ZSET **`online:minute:{ts_code}`**(score = time_idx 整数 0-239,value = data JSON)
- **不读 HSET**(`online:minute:hash:{ts_code}` **不存在**)
- **DEFAULT_INTERVAL** = `900s`(代码真值,2026-09-14 体检校正)
- 落盘 SQLite 表 `minute_YYYYMMDD`

**不走 STREAM**:minute 数据量大(124 只 × 240 根/天),STREAM MAXLEN 会丢数据。ZSET 用 `ZRANGE`(按 score 整数排序)取全量今天数据,`EXPIRE = WINDOW_TTL_MINUTE = 86400s(1d)` 滑窗只保留今天。

## §2 生命周期

scheduler morning_savedata / afternoon_savedata 阶段 spawn,15:35 **closed** 阶段 SIGTERM(`SAVEDATA_PHASES` 最后阶段是 `closed`,无 `post` 阶段)。

## §3 调用链

```
service_savedata_minute
  └─ OnlineRedis()                                                      # coreClient.redis_client
  └─ persist_kind(r, *, kind="minute", trade_date=trade_date)           # core.persist_client
     └─ persist_minute(redis_client, *, trade_date=trade_date)          # line 209 ZSET helper
        └─ r.get_watchlist()                                            # 读 online:watchlist
        └─ for ts_code in watchlist:
            └─ get_minute_bars(ts_code)                                 # 内部 ZRANGE 全 score 范围(0-239)
            └─ for bar: _extract_minute_row(bar, *, trade_date, now_save=...)
            └─ INSERT INTO minute_YYYYMMDD ...
        └─ conn.commit()
        └─ return inserted_count
```

**没 HGETALL**:不读 hash key。每根 K 线的 `data_timestamp` 从 ZSET member 解码(client 源头 wall-clock ISO)。

## §4 表结构(`sqlite_client.py:174-190` MINUTE_TABLE_SCHEMA 真值)

```sql
CREATE TABLE IF NOT EXISTS minute_YYYYMMDD (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date      TEXT    NOT NULL,
    ts_code         TEXT    NOT NULL,
    time_idx        INTEGER NOT NULL,    -- 该股第几根 K 线(0-239)
    datetime        TEXT    NOT NULL,    -- 'YYYY-MM-DD HH:MM:SS'
    price           REAL,
    vol             INTEGER,
    data_timestamp  TEXT,                -- ISO 字符串(client 源头 wall-clock)
    created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ts_code, trade_date, time_idx)         -- 唯一约束(不是复合 PK,id 是 AUTOINCREMENT PK)
);
CREATE INDEX IF NOT EXISTS idx_minute_YYYYMMDD_ts_code   ON minute_YYYYMMDD(ts_code);
CREATE INDEX IF NOT EXISTS idx_minute_YYYYMMDD_trade_date ON minute_YYYYMMDD(trade_date);
CREATE INDEX IF NOT EXISTS idx_minute_YYYYMMDD_datetime  ON minute_YYYYMMDD(datetime);
```

**字段来源说明**:

| 字段 | 来源 | 备注 |
|---|---|---|
| `id` | SQLite AUTOINCREMENT | PK |
| `trade_date` | 调用方传入 | `YYYYMMDD` |
| `ts_code` | client 拉数据时带的标的代码 | `SH`/`SZ` 前缀 |
| `time_idx` | client 实时算 | 0-239,09:31=0 |
| `datetime` | client 拼 | `YYYY-MM-DD HH:MM:SS` |
| `price` / `vol` | client 拉 | REAL / INTEGER |
| `data_timestamp` | client 源头 wall-clock ISO | 关键溯源字段 |
| `created_at` | SQLite DEFAULT | 落盘时间 |

**注意**:schema 是 **8 列**(无 open/high/low/close/volume/amount),minute 没有完整的 OHLCV 字段 — 1 分钟 K 线只存 `price` 当根收盘价和 `vol` 当成交量(2026-09-13 v4 精简,合并了原 `minute_bars_YYYYMMDD` 长表)。

## §5 启动方式

```bash
python3 -m service.service_savedata_minute             # daemon
python3 -m service.service_savedata_minute --once      # 单次
```

## §6 备注

- `persist_minute` 是独立函数(`core/persist_client.py:209-275`),不走 STREAM 通用模板
- `SavedataMinute` **不**override `_do_persist`,走基类默认实现 → `persist_kind(r, *, kind="minute", trade_date=trade_date)` → 内部路由(line 632-633)到独立的 `persist_minute` helper
- 因此 minute 的"特殊"(ZSET 来源)在 `persist_kind` 路由层处理,不在子类层
- **设计意图**:ZSET 而非 STREAM 是因为 minute 数据量 124×240=~3 万条/天,STREAM MAXLEN 会丢数据;ZSET 用 `time_idx` 作 score 可做高效范围查询和按时间分片

## §7 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_writeredis_minute** | 写 Redis ZSET `online:minute:{ts_code}`(v4 不传 trade_date 改 ZADD) |
| **core/persist_client.py:209** | `persist_minute(redis_client, *, trade_date)` 函数(走 ZSET 来源) |
| **core/persist_client.py:632** | `persist_kind` 路由表把 `kind="minute"` 指向 `persist_minute` |

## §8 历史变更

- **2026-09-11**:独立 SavedataMinute 类
- **2026-09-13 v4**:**schema 精简** — 12 列含 OHLCV 改 8 列(无 OHLCV,只 price+vol),长表 `minute_bars_YYYYMMDD` 合并到主表;writeredis 改 tdx_client 实时接口
- **2026-09-14 17:25**:writeredis --once bug 修复
- **2026-09-15 v1.x**:doc 校正 5 处(§1 key 名 + HSET 引用 + ZRANGEBYSCORE 术语 + §4 整段 schema + §3 HGETALL 引用)

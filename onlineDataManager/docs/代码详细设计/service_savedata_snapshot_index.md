# service/service_savedata_snapshot_index.py 详细设计

> **指数行情快照落盘 service**(15 分钟/轮,09:29-11:45 + 12:45-15:01)
> **数据源**:Redis `online:snapshot_index:stream`
> **版本**:v6.10(2026-09-16,新增)

---

## §1 职责

- 每 15 分钟读 Redis `online:snapshot_index:stream` → 写 SQLite `snapshot_index_{YYYYMMDD}`
- 落盘走 `persist_stream_kind(kind="snapshot_index")` 路径
- 时间窗由基类 `ServiceBase.is_trading_window()` 判断:
  - **上午 09:29-11:45 跑**
  - **中午 11:45-12:45 落盘休息**(用户原话)
  - **下午 12:45-15:01 跑**

## §2 v6.10 设计决策

| 决策 | 选择 | 原因 |
|---|---|---|
| 实现方式 | **纯基类复用**,只设 `KIND = "snapshot_index"` | 同 snapshot 的 savedata 模板,只改 KIND 常量 |
| 落盘频率 | 15 分钟(基类 `DEFAULT_INTERVAL=900s`) | 用户原话"落盘为每15分钟一次" |
| 落盘表名 | `snapshot_index_{YYYYMMDD}`(`table_name_for("snapshot_index", trade_date)`) | 与其它 kind 一致 |
| 落盘 schema | `SNAPSHOT_INDEX_TABLE_SCHEMA`(sqlite_client.py:L97-141) | 11 行情字段 + snapshot_timestamp + snapshot_unix + created_at |

## §3 调用链

```
service_savedata_snapshot_index
  └─ SavedataDaemon()  基类
  └─ r = OnlineRedis()  读 STREAM
  └─ persist_stream_kind(r, kind="snapshot_index", trade_date=today)
       └─ drain=True
       └─ _persist_stream_to_sqlite(...)
            └─ for entry in XREAD stream:
                 ├─ _decode(v)                          # 基类方法(用户约束)
                 ├─ _extract_snapshot_index_row(s, trade_date=today)   # 14 字段 extractor
                 └─ INSERT INTO snapshot_index_YYYYMMDD ...
  └─ sleep 15 分钟
```

## §4 与 savedata_snapshot 差异

| 维度 | snapshot | snapshot_index |
|---|---|---|
| KIND 常量 | `"snapshot"` | **`"snapshot_index"`** |
| 其它一切 | 相同(纯基类复用) | 相同 |

> 本文件唯一非空内容:**`KIND = "snapshot_index"`** 字符串。其余全部走基类。

## §5 SQLite 落盘表(snapshot_index_{YYYYMMDD})

```sql
CREATE TABLE IF NOT EXISTS snapshot_index_YYYYMMDD (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 000001.SH 等
    ticker                      TEXT,                  -- 1A0001 等
    volume                      INTEGER,
    turnover                    INTEGER,
    last_price                  REAL,
    price_change                REAL,
    price_change_ratio_pct      REAL,
    open_price                  REAL,
    high_price                  REAL,
    low_price                   REAL,
    prev_price                  REAL,
    snapshot_timestamp          TEXT NOT NULL,         -- ISO 字符串
    snapshot_unix               INTEGER NOT NULL,      -- int unix ms ★
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, snapshot_timestamp)
);
```
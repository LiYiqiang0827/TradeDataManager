# core/sqlite_client.py 详细设计

> **SQLite 数据库操作层**:db 连接 + 表 schema + INSERT + QUERY + 游标持久化
> **行数**:1252 行(`wc -l`,2026-09-15 校正)
> **受众**:AI 改 schema 或查询时
| **v1.x 更新(2026-09-15)**:**所有 `created_at` / `updated_at` schema DEFAULT 全部 `strftime('%Y-%m-%d %H:%M:%f','now','localtime')` 毫秒版**(原 `datetime('now','localtime')` 秒级)。12 处 schema + 5 处 Python `timespec="milliseconds"` isoformat 同步。
| **v6.10 更新(2026-09-16)**:`snapshot_index` 新增 `SNAPSHOT_INDEX_TABLE_SCHEMA`(15 字段,含 `snapshot_unix` 替代 lu_time)+ `_extract_snapshot_index_row` 14 字段 extractor + `_KIND_EXTRACTORS["snapshot_index"]` + `_KIND_COLUMNS["snapshot_index"]` + `KIND_SCHEMAS` 注册 + `table_name_for` 白名单。
| **v6.11 更新(2026-09-16)**:**`lu_time` 列类型 INTEGER → TEXT(`YYYY-MM-DD HH:MM:SS` 秒级字符串)**。extractor 把源头 unix int 转秒级字符串;**Redis STREAM 里仍存原始 int**(便于二次处理)。语义修正:lu_time = "涨停时间",**与** `limitperformance_timestamp`(数据刷新时刻)、`created_at`(落盘时刻)**完全独立**。迁移脚本 `scripts/_migrate_lu_time_to_text.py`(幂等,可重复运行;备份表 `*_bak_20260916_lu_time` 保留 7 天)。
| **v6.12 更新(2026-09-16)**:**`anomaly_keywords` 长表整套废弃**(用户原话"anomaly 落盘没必要存 anomaly_keywords,keyword_list 信息已在 analysis_content 里包含")。`ANOMALY_KEYWORDS_TABLE_SCHEMA` + `insert_anomaly_keywords_batch` 函数 + `LONG_TABLE_SCHEMAS["anomaly_keywords"]` 三处全删;`persist_anomaly` 不再处理 `keyword_list` 拆表逻辑;`LONG_TABLE_SCHEMAS` 仅剩 `minute_bars`。**历史 anomaly_keywords_<date> 表全部 DROP**(共 5 张,60132 行)。
| **v6.13 更新(2026-09-16)**:**auction / snapshot 个股路径**彻底删除 `snap_ts` / `snap_ts_unix` 死字段(用户原话"写入redis前都不需要再去生成snap_ts / snap_ts_unix 也不需要写入到redis里面")。`service_writeredis_auction.py` L100-101 / L162-163 删 4 行 `q.setdefault(...)` + 删死代码 `now_dt/now_iso/now_unix`;`service/service_writeredis_snapshot.py` 同款清理;`redis_online.py` `put_auction` / `put_snapshot` data_ts fallback 链简化;`persist_client.py` 通用 fallback 链不动(auction 走不到)。落盘端 `_extract_auction_row` v6.13 标注:auction_timestamp 同花顺必给,无需 snapshot_timestamp 兜底。**auction_20260916 落盘 5170 行,0 空 timestamp** ✅。
| **v6.14 更新(2026-09-16)**:三处时间戳统一收尾。
  - **`watchlist_timestamp` 改 int 毫秒**(对齐其它 7 kind):`WatchlistEntry.watchlist_timestamp` 类型 `str=""` → `int=0`;`watchlist_fetch.py` L589-603 `ts_iso = datetime.fromtimestamp(...).isoformat(...)` → `ts_unix_ms = int(time.time() * 1000)` 直接打 int 毫秒;`service/service_writeredis_watchlist.py` L74 fallback 改 int 毫秒;`redis_online.py` `put_watchlist` 类型签名 `str` → `int|float|str` + 兼容毫秒 vs 秒(>1e12 视为毫秒);`persist_client.py` `persist_watchlist` L311-328 加 3 种格式兼容(ISO / 毫秒 / 秒 → ISO);`_extract_watchlist_row` 第 779 行取值仍可处理两种。Redis STREAM 字段值统一为 int 毫秒字符串,SQLite 落盘仍转 ISO 字符串。**watchlist_20260916 1020 行,新数据毫秒 ISO** ✅。
  - **`snapshot_index.snapshot_unix_ms` 死字段清理**(对齐 v6.13 auction):`service/service_writeredis_snapshot_index.py` daemon + --once 删 `q.setdefault("snapshot_unix_ms", now_unix)` + 删死代码 `now_unix = now_dt.timestamp()`(变量名错,实际是秒 float);落盘端 `_extract_snapshot_index_row` L584 只读 `snapshot_timestamp`,`snapshot_unix_ms` 完全不读,自然无需清理。**snapshot_index_20260916 1568 行**,`snapshot_timestamp="2026-09-16T11:13:29.000"`(毫秒 000 是同花顺指数 API 秒级精度,**设计预期**,非代码问题)✅。
  - **`snapshot.snap_ts / snap_ts_unix` 死字段清理**(同款 v6.13 auction):`service/service_writeredis_snapshot.py` daemon + --once 删 2 行 setdefault + 死代码 `now_iso/now_unix`;落盘端 `_extract_snapshot_row` 只读 `snapshot_timestamp`(`snap_ts` / `snap_ts_unix` 完全不读),自然无需改 extractor。**snapshot_20260916 72170 行**,新落盘 170 行无 `snap_ts` / `snap_ts_unix` ✅。

---

## §1 设计约定

### 1.1 数据库命名

```
~/TradingAgent/onlineDataManager/data/online_data_YYYYMM.db
```

**一个月一个 db 文件**(2026-09-10 重构后),WAL 模式允许多进程并发。

### 1.2 表命名

```
<kind>_<YYYYMMDD>
```

| kind | 示例 |
|---|---|
| auction | `auction_20260910` |
| snapshot | `snapshot_20260910` |
| orderbook | `orderbook_20260910` |
| minute | `minute_20260910` |
| snapshot_index | `snapshot_index_20260910`(8 只指数,2026-09-16 v6.10 新增)|
| watchlist | `watchlist_20260910`(全表替换,不走 STREAM)|
| zt / break / anomaly / hot / limitperformance | `<kind>_20260910` |

**长表**(数组字段,2026-09-13 v4 全部废弃):
- `minute.bars` → ~~`minute_bars_<YYYYMMDD>`~~ 已并入 `minute_<YYYYMMDD>`
- `anomaly.keyword_list` → ~~`anomaly_keywords_<YYYYMMDD>`~~ 已并入 `anomaly_<YYYYMMDD>` 顶层 `analysis_content` 字段

### 1.3 通用列(2026-09-12 v3 精简后)

每个业务表都有:
- `id` (INTEGER PK AUTOINCREMENT) — 2026-09-12 v3 新增,所有表统一
- `trade_date` (TEXT, YYYY-MM-DD)
- `ts_code` (TEXT, NOT NULL)
- `<kind>_timestamp` (TEXT, ISO 字符串) — **数据时间戳**,各 kind 不同名:`snapshot_timestamp` / `auction_timestamp` / `orderbook_timestamp` / `minute_timestamp` / `snapshot_index_timestamp`(v6.10 指数专用)/ `watchlist_timestamp`(v6.14 已统一为 int 毫秒存 STREAM、ISO 字符串存 SQLite)/ `zt_timestamp` / `break_timestamp` / `anomaly_timestamp` / `hot_timestamp` / `limitperformance_timestamp`
- `created_at` (TEXT) — **落盘时间**,SQLite DEFAULT `(datetime('now', 'localtime'))` 自动
- **没有** `save_timestamp` / `data_timestamp` 顶层通用列(已在 v3 删除)
- v3 改造:全 ISO 字符串(2026-09-11 之前混存 unix float 已修)

UNIQUE 约束都是 `(ts_code, <kind>_timestamp)`,**不是** 复合主键。

## §2 函数清单

### 2.1 路径与连接

| 函数 | 签名 | 用途 |
|---|---|---|
| `db_path_for_month` | `(year_month: str) -> Path` | 计算某月 db 路径 |
| `db_path_for_date` | `(trade_date: str) -> Path` | 计算某日 db 路径(同年同月) |
| `current_year_month` | `() -> str` | 当前 YYYYMM |
| `connect` | `(year_month: str, *, readonly: bool = False) -> sqlite3.Connection` | 打开 db,自动 WAL + journal_mode |

### 2.2 游标持久化(2026-09-11)

| 函数 | 签名 | 用途 |
|---|---|---|
| `read_cursor` | `(conn: sqlite3.Connection, stream_key: str) -> str | None` | 读 `online_stream_cursor` 表里某 stream 的 last_id |
| `update_cursor` | `(conn: sqlite3.Connection, *, stream_key: str, last_id: str, trade_date: str) -> None` | **更新 last_id + trade_date**(upsert + commit) |
| `list_cursors` | `(conn: sqlite3.Connection) -> list[dict]` | 列出所有游标(测试用) |

> ⚠️ `update_cursor` **内部自动 `conn.commit()`**,调用方无需再 commit。

### 2.3 Schema 维护

| 函数 | 签名 | 用途 |
|---|---|---|
| `table_name_for` | `(kind: str, trade_date: str) -> str` | 算表名 |
| `ensure_table` | `(conn: sqlite3.Connection, kind: str, trade_date: str) -> str` | 确保表存在(从 KIND_SCHEMAS 拿 schema),返回表名 |
| `ensure_long_table` | `(conn: sqlite3.Connection, long_kind: str, trade_date: str) -> str` | 确保长表存在(long_kind = `'minute_bars'` / `'anomaly_keywords'`),**v4 后基本不用了** |
| `ensure_watchlist_table` | `(conn: sqlite3.Connection, trade_date: str) -> str` | 确保 watchlist 表存在 |

### 2.4 字段提取(从 dict → tuple)

| 函数 | 签名 | 用途 |
|---|---|---|
| `_field` | `(s: dict, key: str, default=None)` | 安全取值 |
| `_extract_snapshot_row` | `(s, *, trade_date, now_save="") -> tuple` | snapshot dict → **12 列** tuple(trade_date + ts_code + 9 业务字段 + snapshot_timestamp);v6.14 后彻底只读 `snapshot_timestamp`,`snap_ts` / `snap_ts_unix` 死字段在写入端已删,extractor 无需兼容 |
| `_extract_auction_row` | `(s, *, trade_date, now_save="") -> tuple` | auction dict → 14 列 tuple;v6.13 后彻底只读 `auction_timestamp`,无需 snapshot_timestamp 兜底 |
| `_extract_orderbook_row` | `(s, *, trade_date, now_save="") -> tuple` | orderbook dict → 10 列 tuple |
| `_extract_minute_row` | `(s, *, trade_date, now_save="") -> tuple` | minute dict → **7 列** tuple(trade_date + ts_code + time_idx + datetime + price + vol + data_timestamp) |
| `_extract_zt_row` | `(s, *, trade_date, now_save="") -> tuple` | zt dict → **15 列** tuple(trade_date + ts_code + 12 业务字段 + zt_timestamp) |
| `_extract_break_row` | `(s, *, trade_date, now_save="") -> tuple` | break dict → 9 列 tuple |
| `_extract_anomaly_row` | `(s, *, trade_date, now_save="") -> tuple` | anomaly dict → 6 列 tuple(v6.12 后 keyword_list 不再拆长表,统一存 `analysis_content`)|
| `_extract_hot_row` | `(s, *, trade_date, now_save="") -> tuple` | hot dict → 8 列 tuple |
| `_extract_limitperformance_row` | `(s, *, trade_date, now_save="") -> tuple` | limitperformance dict → **24 列** tuple(trade_date + ts_code + 21 业务字段 + limitperformance_timestamp);v6.11 后 `lu_time` 落盘为秒级字符串(`YYYY-MM-DD HH:MM:SS`),Redis STREAM 仍存原始 int |
| `_extract_snapshot_index_row` | `(s, *, trade_date, now_save="") -> tuple` | snapshot_index dict → 14 列 tuple(trade_date + ts_code + ticker + 9 业务字段 + snapshot_timestamp ISO + snapshot_unix int ms);v6.10 新增,v6.14 落盘端无需改(snapshot_unix_ms 死字段在写入端已删,这里本来就不读)|
| `_extract_watchlist_row` | `(s, *, trade_date, now_save="") -> tuple` | watchlist dict → 6 列 tuple(trade_date + ts_code + source + priority + reason + watchlist_timestamp);v6.14 后字段值为 ISO 字符串(由 `persist_watchlist` 从 int 毫秒 / 老秒 float / 老 ISO 三种格式兼容转换) |

> ⚠️ 字段提取函数**不返回 `id`** 和 `created_at`(`id` 自增,`created_at` SQLite DEFAULT)。所以实际 INSERT 列数 = tuple 元素数 + 1(id) + 1(created_at) - 1(没有这 2 个)= 不变。

### 2.5 INSERT

| 函数 | 签名 | 用途 |
|---|---|---|
| `insert_snapshot` | `(conn, *, kind: str, trade_date: str, ts_code: str, data_timestamp: str, save_timestamp: str | None = None, payload: dict) -> bool` | 单条快照插入,**主键冲突 IGNORE 跳过**,返回 bool |
| `_columns_for_kind` | `(kind: str) -> list[str]` | 返回某 kind 的列名(供 INSERT 用) |
| `insert_snapshots_batch` | `(conn, *, kind: str, trade_date: str, snapshots: list[dict]) -> int` | 批量 INSERT(用 executemany),**`snapshots` 是 list[dict]**(每条含 `ts_code` / `data_timestamp` / `save_timestamp` / `payload` 四键),返回成功条数 |
| `insert_minute_bars_batch` | **(已 DEPRECATED)** | ⚠️ **2026-09-13 v4 已废弃** — 直接 `raise NotImplementedError`,改用 `insert_snapshots_batch(conn, kind="minute", ...)` |
| `insert_anomaly_keywords_batch` | `(conn, *, trade_date: str, keywords: list[dict]) -> int` | anomaly keywords 长表批量(2026-09-13 v4 后基本不用) |
| `replace_watchlist` | `(conn, trade_date: str, rows: list[dict]) -> int` | watchlist 整体替换(DELETE + INSERT) |

### 2.6 QUERY

| 函数 | 签名 | 用途 |
|---|---|---|
| `query_snapshots` | `(conn, *, kind: str, trade_date: str, ts_code: str | None = None, start_ts: str | None = None, end_ts: str | None = None, start_data_ts: str | None = None, end_data_ts: str | None = None, start_save_ts: str | None = None, end_save_ts: str | None = None, limit: int | None = None) -> list[dict]` | 通用查询,按 ts_code / 数据时间 / 落盘时间 / limit 过滤 |
| `count_snapshots` | `(conn, kind: str, trade_date: str) -> int` | 行数统计 |
| `query_watchlist` | `(conn, *, trade_date: str) -> list[dict]` | 读当日 watchlist(2026-09-14 改 kwarg-only) |
| `list_tables` | `(conn: sqlite3.Connection) -> list[str]` | 列出所有表(测试用) |

> 💡 `query_snapshots` 的 `start_ts` / `end_ts` 是旧 API,内部等价于 `start_data_ts` / `end_data_ts`。新代码**只用 `start_data_ts` / `end_data_ts`**。

## §3 KIND_SCHEMAS(核心 schema 表)

每个 kind 一份 schema,`ensure_table` 时按需取。**不在本文档展开** — 看源码 `L97-L361`。

注册 **11 种 kind**(2026-09-16 v6.10/v6.14 后):

```python
KIND_SCHEMAS = {
    "watchlist": WATCHLIST_TABLE_SCHEMA,         # 全表替换,不走 STREAM
    "snapshot": SNAPSHOT_TABLE_SCHEMA,
    "snapshot_index": SNAPSHOT_INDEX_TABLE_SCHEMA,   # 2026-09-16 v6.10 新增(8 只指数)
    "auction": AUCTION_TABLE_SCHEMA,
    "orderbook": ORDERBOOK_TABLE_SCHEMA,
    "minute": MINUTE_TABLE_SCHEMA,        # 2026-09-13 v4:删 MINUTE_BARS_TABLE_SCHEMA
    "zt": ZT_TABLE_SCHEMA,
    "break": BREAK_TABLE_SCHEMA,
    "anomaly": ANOMALY_TABLE_SCHEMA,      # 2026-09-16 v6.12 后不含 keywords 拆表
    "hot": HOT_TABLE_SCHEMA,
    "limitperformance": LIMITPERFORMANCE_TABLE_SCHEMA,  # 2026-09-14 v5 新增,2026-09-16 v6.11 lu_time 转 TEXT
}
```

## §4 调用示例

```python
from core.sqlite_client import connect, ensure_table, insert_snapshots_batch, update_cursor

ym = "202609"
conn = connect(ym)

# 1. 确保表存在
table_name = ensure_table(conn, "snapshot", "20260914")  # → "snapshot_20260914"

# 2. 批量插入(snapshots 是 list[dict],不是 list[tuple]!)
snapshots = [
    {
        "ts_code": "000001.SZ",
        "data_timestamp": "2026-09-14T13:30:00",
        "save_timestamp": "2026-09-14T13:30:01",  # 可选,默认 wall clock
        "payload": {
            "volume": 114296522,
            "last_price": 12.96,
            "price_change": 0.05,
            "price_change_ratio_pct": 0.39,
            "open_price": 12.91,
            "high_price": 12.98,
            "low_price": 12.90,
            "prev_price": 12.91,
            "turnover": 1482345678.0,
            "snapshot_timestamp": "2026-09-14T13:30:00",
        },
    },
]
inserted = insert_snapshots_batch(conn, kind="snapshot", trade_date="20260914", snapshots=snapshots)

# 3. 更新游标(update_cursor 内部自动 commit)
update_cursor(
    conn,
    stream_key="online:snapshot:stream",
    last_id="1234-0",
    trade_date="20260914",  # ⚠️ 必须传 trade_date
)

conn.close()
```

## §5 与其他模块的关系

| 谁会用 | 用哪些函数 |
|---|---|
| **core/persist_client.py** | `connect` / `read_cursor` / `update_cursor` / `ensure_table` / `ensure_long_table` / `_KIND_EXTRACTORS` |
| **service/savedata_loop.py** | 不直接调 sqlite_client,走 persist_client.persist_kind 包装 |
| **core/check_db.py** | `connect(readonly=True)` / `count_snapshots` / `list_tables` |
| **core/query_db.py** | `connect(readonly=True)` / `query_snapshots` / `query_watchlist` |

## §6 历史变更

- **2026-09-10**:db 按月分文件(原单文件 → YYYYMM)
- **2026-09-11**:字段全拆列(payload_json → 实际字段)+ 游标持久化表
- **2026-09-12 v3**:每表加 `id PK AUTOINCREMENT`,字段精简(删 `data_timestamp`/`save_timestamp`/`thscode`/`ticker`)
- **2026-09-13 v4**:删 `minute_bars` 长表,合并到 `minute_<date>`;删 `MINUTE_BARS_TABLE_SCHEMA`;`insert_minute_bars_batch` 标 DEPRECATED
- **2026-09-14 v5**:新增 `limitperformance` kind(26 字段)
- **2026-09-16 v6.10**:`snapshot_index` kind 上线,KIND_SCHEMAS 增至 10 种
- **2026-09-16 v6.11**:`lu_time` 列 INTEGER → TEXT(`YYYY-MM-DD HH:MM:SS` 秒级字符串);`limitperformance` schema 跟随迁移脚本 `_migrate_lu_time_to_text.py`
- **2026-09-16 v6.12**:`anomaly_keywords` 长表整套废弃;5 张历史 anomaly_keywords_<date> 表全部 DROP
- **2026-09-16 v6.13**:`_extract_auction_row` 加 v6.13 标注;`_extract_snapshot_row` 加 v6.14 标注(写入端 snap_ts / snap_ts_unix 已删,extractor 自然无需改)
- **2026-09-16 v6.14**:`_extract_watchlist_row` 字段值来源标注(int 毫秒 → ISO 字符串在 persist_watchlist 转换);`_extract_snapshot_index_row` 不动

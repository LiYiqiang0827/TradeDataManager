# core/check_redis.py + core/query_redis.py + core/check_db.py + core/query_db.py 详细设计

> **4 个 AI 工具类合一份文档**:Redis/SQLite 的状态汇总 + 数据拉取。所有函数**只读**,AI 不会误改数据。
> **行数**:check_redis.py 675 / query_redis.py 387 / check_db.py 389 / query_db.py 438;合计 1889 行(`wc -l`,2026-09-15 校正)
> **受众**:AI Agent 调试 / 检查 / 拉数据时。

---

## §1 设计原则

**check_*** 和 **query_*** 完全分开:
- `check_*` → 返回 `dict`(状态汇总),用于"现在什么状态"
- `fetch_*` (query_redis/db) → 返回 `list`/`dict`(原始数据),用于"拉数据"

**只读保证**:
- Redis 端:check / query 都**不**调 SET / XADD / ZADD / DEL / EXPIRE
- SQLite 端:全部 `connect(readonly=True)`,即使误写也会失败

## §2 check_redis.py(603 行,~20 个 check_*)

### 2.1 工具函数

| 函数 | 用途 |
|---|---|
| `_decode_meta_value(v)` | meta 值反序列化(JSON)|
| `_decode_meta_dict(d)` | 整个 meta dict 反序列化 |
| `_type_str(r, k)` | key 的 Redis 类型("stream"/"zset"/"hash"/"set"/"none")|

### 2.2 各 kind check(返回 dict)

| 函数 | 返回 dict 关键字段 |
|---|---|
| `check_watchlist` | count / sources_map / ttl |
| `check_snapshot` | timeline_size / timeline_ttl / latest_archive_size / latest_archive_ttl / window_count / window_ttl / stream_xlen |
| `check_snapshot_for_stock(ts_code)` | 单股:window_size / latest_snapshot / ttl |
| `check_orderbook` / `check_orderbook_for_stock` | 类似 snapshot,无 timeline/archive |
| `check_minute` / `check_minute_for_stock` | 单股:bar_count / latest_minute / ttl |
| `check_auction` / `check_auction_for_stock` | timeline / archive / window |
| `check_zt` | timeline_size / latest_archive_size / stream_xlen |
| `check_break` / `check_anomaly` / `check_hot` / `check_limitperformance` | 同 zt |
| `check_meta` | 整个 online:meta HSET dict |
| `check_cursor(stream_key)` | last_id / table 存在 / SQLite last_id 同步状态 |
| `check_all_keys(pattern)` | 列出所有 online:* key 信息(类型 / size / ttl)|
| `check_overview` | 全模块状态汇总 |

## §3 query_redis.py(386 行,~30 个 fetch_*)

**每个 fetch 函数都**对应一个 `check_*`,拉的是**原始数据**:

| 函数 | 签名 | 用途 |
|---|---|---|
| `fetch_watchlist` | `() -> list[str]` | 监控列表 |
| `fetch_watchlist_sources` | `() -> dict[str, str]` | 来源映射 |
| `fetch_snapshot_window` | `(ts_code, idx_min=0, idx_max=240) -> list[dict]` | 单股 window |
| `fetch_snapshot_history` | `(count=100) -> list[tuple[str, dict]]` | STREAM 历史 |
| `fetch_snapshot_stream_since` | `(min_id="-", count=100) -> list` | 指定 id 之后 |
| `fetch_snapshot_timeline` | `(count=50) -> list[dict]` | timeline |
| `fetch_snapshot_archive_latest` | `() -> dict[str, dict]` | 最新 archive |
| `fetch_snapshot_snapshot_at` | `(seconds_before: int) -> dict[str, dict]` | N 秒前 |
| (auction / orderbook / minute / zt / break / anomaly / hot / limitperformance 同模式)| | |

## §4 check_db.py(388 行,~15 个 check_*)

### 4.1 工具

| 函数 | 用途 |
|---|---|
| `_ym_from_date(trade_date)` | YYYYMMDD → YYYYMM |
| `_rows_to_count_ts_range` | 行数 + ts_code 范围 + 时间范围 |
| `_check_kind(kind, trade_date, year_month=None)` | **通用**:检查某 kind 当日表 |

### 4.2 各 kind check(返回 dict)

| 函数 | 返回 dict 关键字段 |
|---|---|
| `check_snapshot` / `check_orderbook` / `check_minute` / `check_auction` | row_count / ts_count / earliest_data_ts / latest_data_ts |
| `check_zt` / `check_break` / `check_anomaly` / `check_hot` / `check_limitperformance` | 同上 |
| `check_watchlist` | row_count / ts_codes / sources |
| `check_cursor(stream_key)` | last_id / 写入时间 / 与 Redis 同步状态 |
| `check_cursors` | 所有游标列表 |
| `check_db_list` | 所有 db 文件名 |
| `check_table_list(year_month)` | 某月所有表 |
| `check_table_info(year_month, table_name)` | 单表 schema + 行数 + 索引 |

## §5 query_db.py(437 行,~10 个 fetch_*)

### 5.1 各 kind fetch

| 函数 | 签名 | 用途 |
|---|---|---|
| `fetch_snapshot` | `(kind, trade_date, **filters) -> list[dict]` | **通用查询**:按 ts_code / 时间范围 / 价格范围过滤 |
| `fetch_orderbook` / `fetch_minute` / `fetch_auction` / `fetch_zt` / `fetch_break` / `fetch_anomaly` / `fetch_hot` | 同上 | |
| `fetch_watchlist` | `(trade_date, year_month=None) -> list[dict]` | 读当日 watchlist |
| `fetch_cursor(stream_key)` | 单游标 | |
| `fetch_all_cursors` | `(year_month=None) -> list[dict]` | 所有游标 |

### 5.2 DataFrame 转换

| 函数 | 签名 | 用途 |
|---|---|---|
| `to_dataframe` | `(rows: list[dict], columns=None) -> pd.DataFrame` | dict list → pandas DataFrame(AI 分析用)|

## §6 调用示例

```python
# AI 调试 snapshot 状态
from core.check_redis import check_snapshot
from core.check_db import check_snapshot as check_db_snapshot

print(check_snapshot())
# {
#   "timeline_size": 1,
#   "timeline_ttl": 43194,
#   "latest_archive_size": 124,
#   "latest_archive_ttl": 1794,
#   "window_count": 124,
#   "stream_xlen": 200019,
#   "types": {"snapshot:timeline": "zset", ...}
# }

print(check_db_snapshot("20260914"))
# {
#   "table": "snapshot_20260914",
#   "row_count": 150000,
#   "ts_count": 124,
#   "earliest_data_ts": "2026-09-14 09:30:01",
#   "latest_data_ts": "2026-09-14 15:00:00"
# }

# AI 拉数据
from core.query_redis import fetch_snapshot_archive_latest
from core.query_db import fetch_snapshot, to_dataframe

archive = fetch_snapshot_archive_latest()
df = to_dataframe([{"ts_code": k, **v} for k, v in archive.items()])
print(df.head())

db_rows = fetch_snapshot("snapshot", "20260914", ts_code="000001.SZ")
print(db_rows[:5])
```

## §7 与其他模块的关系

| 谁会用 | 用哪些函数 |
|---|---|
| **test_checkredis_*** | `check_*` + `fetch_*` |
| **test_checkdb_*** | `check_*` + `fetch_*` |
| **AI Agent 调试** | 直接 `from core.check_redis import check_snapshot` |
| **scheduler_once.py** | 不调 check / query |

## §8 历史变更

- **2026-09-12 v3**:check_* 加 v6 MyATM 各 kind 检查
- **2026-09-14 v6.1**:加 break / anomaly / hot / limitperformance check + fetch
- **2026-09-14 v6.2**:auction 加 timeline / archive_latest / snapshot_at
- **2026-09-14 v6.3**:snapshot 加 timeline / archive_latest / snapshot_at

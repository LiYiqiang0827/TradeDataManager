# core/persist_client.py 详细设计

> **Redis → SQLite 落盘接口**:service_savedata_* 调用这里,按 kind 路由到不同的 persist_xxx 方法。
> **行数**:849 行(`wc -l`,2026-09-15 校正)
> **受众**:AI 修落盘逻辑时
| **v1.x 更新(2026-09-15)**:**所有 `<kind>_timestamp` / `data_timestamp` / `created_at` 全部精确到毫秒(3 位小数)**。代码侧:`datetime.fromtimestamp(...).isoformat(timespec="milliseconds")`;`_stamp_stream_field()` 智能识别 ms/s float 上游精度。
| **v6.10 更新(2026-09-16)**:`snapshot_index` 加入 STREAM 落盘白名单(L105) + persist_stream_kind 路由(L825)。`_persist_stream_to_sqlite` 走通用路径,无需修改。
| **v6.12 更新(2026-09-16)**:`persist_anomaly` 删除 `insert_anomaly_keywords_batch` 调用 + SQL(`ANOMALY_KEYWORDS_TABLE_SCHEMA` 长表整套废弃)。整体 insert 块从"主表 + keywords 拆表 2 事务"→ 单事务 insert 主表。
| **v6.13 更新(2026-09-16)**:`persist_auction` data_ts fallback 链简化 — 删 `snap_ts` / `snap_ts_unix` 兜底分支(`service_writeredis_auction.py` 写入端已删,record 里这两个字段永远不会再出现)。
| **v6.14 更新(2026-09-16)**:1) `persist_watchlist` 兼容 `watchlist_timestamp` 三种格式(int 毫秒 / float 秒 / str ISO,详见 §3.7);2) `persist_snapshot_index` / `persist_snapshot` 不动(写入端死字段已删,persist 层只读业务时间戳,无需清理);3) `persist_anomaly` 删 `anomaly_keywords` 拆表逻辑后无变化。

---

## §1 设计要点

- **STREAM + 游标模式**:`XREAD > last_id` 拉新数据 → 批量 INSERT → `update_cursor` 推进 last_id → 失败返回 -1 **不**推进游标(2026-09-15 v6.7 修复:见 §5)
- **游标持久化**(2026-09-11):在 SQLite `online_stream_cursor` 表,**不**在 Redis(Redis 重启会丢)
- **按 kind 路由**:见 `persist_kind()` 函数
- **minute 特殊**:走 `persist_minute()`(line 209,从 ZSET 读 ZRANGE 整数 score 全范围,**不**走 ZRANGEBYSCORE),不走 STREAM

## §2 常量

```python
STREAM_BATCH = 1000        # 一次 XREAD 拉多少条
STREAM_BLOCK_MS = 2000     # XREAD block 时长(ms)
```

## §3 函数清单

### 3.1 辅助函数

| 函数 | 签名 | 用途 |
|---|---|---|
| `_get_cursor` | `(redis_client, conn, kind: str) -> str` | 读游标(SQLite `online_stream_cursor` 表优先,无 → `"0"`) |

### 3.2 STREAM 类落盘(3+ 个 kind,v6.10 加 snapshot_index)

| 函数 | 签名 | 用途 |
|---|---|---|
| `persist_stream_kind` | `(redis_client, *, kind: str, trade_date: str, batch_size=1000) -> int` | **通用 STREAM 落盘**(auction / snapshot / orderbook / **snapshot_index** / zt / break / hot / limitperformance),内部循环 XREAD + batch INSERT。**v6.10**:`snapshot_index` 加入白名单;**v6.11**:`limitperformance` `lu_time` 落盘转 TEXT |
| `persist_minute` | `(redis_client, *, trade_date: str) -> int` | **minute 特殊落盘**:每只股从 ZSET 读 ZRANGEBYSCORE(1 天范围)|
| `_persist_stream_to_sqlite` | `(redis_client, conn, *, kind: str, stream_key: str, trade_date: str) -> int` | persist_stream_kind 的内部实现,实际跑 XREAD + INSERT |

### 3.3 MyATM 类落盘(5 个 kind,v6 / v6.1,v6.12 anomaly 简化)

| 函数 | 签名 | 用途 |
|---|---|---|
| `persist_zt` | `(redis_client, *, trade_date: str) -> int` | 涨停池落盘(从 STREAM `online:zt:stream` 读)|
| `persist_break` | `(redis_client, *, trade_date: str) -> int` | 炸板池落盘 |
| `persist_anomaly` | `(redis_client, *, trade_date: str, batch_size=1000) -> int` | **异动落盘**:**v6.12** 顶层字段 → `anomaly_<date>` 单表(已无 keywords 长表),`keyword_list` 信息合并存 `analysis_content` 字段 |
| `persist_hot` | `(redis_client, *, trade_date: str) -> int` | 热股榜落盘 |
| `persist_limitperformance` | `(redis_client, *, trade_date: str) -> int` | 涨停表现落盘(v5 新增,**v6.11** `lu_time` 落盘转 TEXT 秒级字符串)|

### 3.6 watchlist(全表替换,1 个 kind)

| 函数 | 签名 | 用途 |
|---|---|---|
| `persist_watchlist` | `(redis_client, *, trade_date: str) -> int` | **全表替换**:不增删行,**REPLACE INTO** `watchlist_<date>` 整表重建。**v6.14**:`watchlist_timestamp` 三种格式兼容 — `int`(毫秒,>1e12)/ `float`(秒,<=1e12)/ `str`(老 ISO 字符串),落盘统一转 `datetime.isoformat(timespec="milliseconds")` 存 SQLite TEXT |

### 3.7 其它说明

- **anomaly_keywords 长表整套废弃**(v6.12) — 用户原话"anomaly 落盘没必要存 anomaly_keywords,keyword_list 信息已在 analysis_content 里包含"。`ANOMALY_KEYWORDS_TABLE_SCHEMA` + `insert_anomaly_keywords_batch` + `persist_anomaly` 内的 keywords 拆表逻辑全部清理
- **`snapshot_unix_ms` 死字段清理**(v6.14)— `service_writeredis_snapshot_index.py` 写入端删,`redis_online.py` + `persist_client.py` + `sqlite_client.py` 完全无引用
- **`snap_ts` / `snap_ts_unix` 死字段清理**(v6.13 / v6.14)— `service_writeredis_auction.py` + `service_writeredis_snapshot.py` 写入端删,`redis_online.py` + `persist_client.py` + `sqlite_client.py` 完全无引用

### 3.4 路由入口

| 函数 | 签名 | 用途 |
|---|---|---|
| `persist_kind` | `(redis_client, *, kind: str, trade_date: str) -> int` | **总入口**:按 kind 路由到对应 persist_xxx,支持 **10 种 kind**(auction/snapshot/snapshot_index/orderbook/minute/zt/break/anomaly/hot/limitperformance)。**注意**:`watchlist` **不在此路由表** — 因为 watchlist 走全表替换 `replace_watchlist`,由 `service_savedata_watchlist` 子类 override `_do_persist` 直接调,不走基类 `persist_kind`。所以 11 个 kind = 10 个走路由 + watchlist 1 个走子类 override。 |
| `persist_all` | `(redis_client, *, trade_date: str, kinds=None) -> dict[str, int]` | 落盘所有 kind,返回 `{kind: inserted_count}` |

### 3.5 统计

| 函数 | 签名 | 用途 |
|---|---|---|
| `daily_summary` | `(trade_date: str) -> dict[str, Any]` | 某日 SQLite 落盘统计(auction / snapshot / orderbook / minute / zt 五个 kind)|

## §4 调用示例

```python
from core.redis_online import OnlineRedis
from core.persist_client import persist_kind, persist_all

r = OnlineRedis()

# 落盘一个 kind
inserted = persist_kind(r, kind="snapshot", trade_date="20260914")
print(f"snapshot 落盘 {inserted} 条")

# 落盘所有
results = persist_all(r, trade_date="20260914")
for kind, count in results.items():
    print(f"{kind}: {count}")
```

## §5 关键设计:游标持久化

**读流程**(`persist_stream_kind` line 74-204 真值):
```
1. _get_cursor(redis_client, conn, kind) → SQLite 表 online_stream_cursor[stream_key]
2. XREAD STREAMS {stream_key} > {last_id} COUNT 1000
3. 拉到一批 records → 解析 → list[dict] 批行
4. insert_xxx_batch(conn, kind=..., trade_date=..., rows=...)   # 批量 INSERT(失败返回 0)
5. update_cursor(conn, stream_key=..., last_id=max_id, trade_date=...)  # 推进游标
6. conn.close()                                                  # 在 finally 块
```

**失败行为(真值,2026-09-15 v6.7 修复)**:

| 步骤 | 行为 |
|---|---|
| 步骤 3 `insert_snapshots_batch` 失败(IntegrityError / SQLite Error)| `insert_snapshots_batch` 返回 **-1**(2026-09-15 v6.7 新增,区别于"全去重返回 0")|
| 步骤 4 `persist_stream_kind / persist_watchlist` 检测 `inserted < 0` | **不**执行 `update_cursor`,日志打印 ERROR,函数返回 -1 |
| 步骤 5 下次调用 | XREAD 重读 STREAM > 旧 last_id → 读到同样消息 → 不会丢 |

**修复前(bug)**:`insert_snapshots_batch` 返回 0 与"全去重"无法区分,**所有路径都执行 update_cursor**,理论上 STREAM 里有 N 条,落盘时 1 条 UNIQUE 冲突 → 全部 0 inserted,游标仍推进 → 丢 1 条数据。

**修复后(v6.7)**:用 **-1** 标识真失败,游标不推进,下次重读不丢数据。

**实现位置**:
- `core/sqlite_client.py` `insert_snapshots_batch` 在 catch `IntegrityError` / `sqlite3.Error` 后 **返回 -1**
- `core/persist_client.py` `persist_stream_kind` / `persist_watchlist` / `_persist_stream_to_sqlite` 检查 `inserted < 0` 不推进游标

## §6 与其他模块的关系

| 谁会用 | 用哪些函数 |
|---|---|
| **service_savedata_***(10 个)| `persist_kind` 或 `persist_minute`(minute 特殊)|
| **service_savedata_loop.py:67-70**(基类)| `_do_persist` → `persist_kind(r, *, kind=KIND, trade_date=td)` |
| **scheduler_once.py** | 跑 `--once` 时 subprocess.run(savedata_*) |
| **test_checkdb_*** | 不直接调,改用 check_db 看落盘结果 |

## §7 历史变更

- **2026-09-11**:游标从 Redis meta 迁到 SQLite `online_stream_cursor` 表
- **2026-09-12 v3**:精简代码 + 删 legacy Redis cursor 兼容层
- **2026-09-14 v5**:新增 `persist_limitperformance`
- **2026-09-15 v6.7**:修真值 — 落盘失败(返回 -1)不推进游标,数据不丢
- **2026-09-16 v6.10**:`snapshot_index` 加入 STREAM 落盘白名单(L105) + `persist_stream_kind` 路由(L825)
- **2026-09-16 v6.11**:`lu_time` 落盘转 TEXT(`YYYY-MM-DD HH:MM:SS` 秒级字符串);`persist_limitperformance` 列类型跟随迁移脚本 `_migrate_lu_time_to_text.py`(原始 int 数据仍在 Redis STREAM 保留)
- **2026-09-16 v6.12**:`persist_anomaly` 删除 `insert_anomaly_keywords_batch` + `ANOMALY_KEYWORDS_TABLE_SCHEMA`;长表整套废弃
- **2026-09-16 v6.13**:`persist_auction` data_ts fallback 链简化(删 `snap_ts` / `snap_ts_unix` 兜底)
- **2026-09-16 v6.14**:`persist_watchlist` 三种 timestamp 格式兼容;`persist_snapshot` / `persist_snapshot_index` 无变化

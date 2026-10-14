# core/redis_online.py 详细设计

> **Redis 业务层**:继承 `~/TradingAgent/coreClient/redis_client.py` 的 `RedisBase`,组合通用模板实现 8+ 类数据 put_*/get_*。
| **行数**:1495 行(`wc -l`,2026-09-15 校正)
| **受众**:AI Agent 改 Redis 操作时
| **v1.x 更新(2026-09-15)**:`watchlist_timestamp` 写 + `timeline_iso` 读 共 3 处 `timespec="milliseconds"` isoformat。
| **v6.10 更新(2026-09-16)**:`snapshot_index` kind 新增 4 方法 + 5 常量(STREAM_MAXLEN_SNAPSHOT_INDEX=3000 / WINDOW_TTL=12h / TIMELINE_TTL=12h / ARCHIVE_TTL=6h / STREAM_TTL=12h)。`put_snapshots_index_batch` 内 `drop_keys` 移除 `thscode` `ticker` `snapshot_timestamp`(指数对账需要)。
| **v6.13 更新(2026-09-16)**:`put_auction` / `put_snapshot` data_ts fallback 链简化 — 删 `snap_ts` / `snap_ts_unix` 兜底分支(`service_writeredis_auction.py` / `service_writeredis_snapshot.py` 写入端已删 `q.setdefault(...)`,record 里这两个字段永远不会再出现,落盘端也只读业务时间戳)。
| **v6.14 更新(2026-09-16)**:`put_watchlist` 类型签名 `(ts_codes, sources_map, watchlist_timestamp: str)` → `(ts_codes, sources_map, watchlist_timestamp: int | float | str)`,兼容历史数据(老 float 秒 / 老 ISO 字符串)+ 新 int 毫秒(>1e12 视为毫秒,<=1e12 视为秒)。`put_snapshots_index_batch` / `commit_snapshots_index_batch` 不动(`snapshot_unix_ms` 死字段已在 service_writeredis_snapshot_index.py 写入端删,redis 层完全没引用过,无需清理)。

---

## §1 模块定位

```
~/TradingAgent/coreClient/redis_client.py   ← RedisBase(连接 + JSON + 通用 STREAM/ZSET/SET/LIST/HASH 模板)
~/TradingAgent/coreClient/redis_config.py   ← 连接参数 + 通用配置常量
~/TradingAgent/onlineDataManager/scripts/core/redis_online.py   ← 本文件(业务层)
```

**业务层职责**:
- 业务常量(`PREFIX = "online"` + 各 kind 的 TTL/MAXLEN)
- 业务游标(`get_last_stream_id`,基于基类 set_meta)
- 8+ 类业务方法(watchlist / auction / snapshot / orderbook / minute / zt / break / anomaly / hot / limitperformance)→ 只调基类通用模板
- 业务层**不直接调** `pipe.sadd` / `pipe.lpush` / `pipe.xadd` / `pipe.zadd` / `pipe.expire` 等底层命令

## §2 业务常量

### 2.1 前缀

```python
PREFIX = "online"   # 所有 key 前缀,如 online:snapshot:stream
```

### 2.2 Window TTL(秒)

| 常量 | 值 | 用途 |
|---|---:|---|
| `WINDOW_TTL_AUCTION` | 86400 | 集合竞价(全天窗口)|
| `WINDOW_TTL_SNAPSHOT` | 43200(v6.3) | 行情快照(12h 兜底 + 30min 滑窗)|
| `WINDOW_TTL_ORDERBOOK` | 210 | 5 档盘口(3min 滑窗 + 30s 余量)|
| `WINDOW_TTL_MINUTE` | 86400 | 1 分钟分时(整天保留)|
| `WINDOW_TTL_WATCHLIST` | 604800 | 自选股(7 天)|

### 2.3 Snapshot 动态 TTL(2026-09-14 v6.3)

| 常量 | 值 | 用途 |
|---|---:|---|
| `SNAPSHOT_ARCHIVE_TTL_BASE` | 1800 | 普通时段 archive EXPIRE |
| `SNAPSHOT_ARCHIVE_TTL_LUNCH` | 7200 | 11:00-11:30 写入的 archive(跨午休补偿)|
| `SNAPSHOT_SLIDE_WINDOW_BASE` | 1800 | 普通时段 timeline 滑窗 |
| `SNAPSHOT_SLIDE_WINDOW_LUNCH` | 7200 | 11:00-11:30 timeline 滑窗 |

### 2.4 STREAM MAXLEN(防爆)

> ⚠️ **有损语义**:`XADD MAXLEN ~ N` 是**近似裁剪**(Redis 用 radix tree 节点批量裁剪,实际保留条数可能在 N 的 ~10% 误差内),极端情况下可能比 N 略多。savedata 落后 writer 超过 MAXLEN 时,**早期条目会被静默丢弃,savedata 无法补救**。所以 savedata 间隔(`DEFAULT_INTERVAL`)必须 < `MAXLEN / 写入速率`,留足缓冲。

| 常量 | 值 | 用途 |
|---|---:|---|
| `STREAM_MAXLEN_AUCTION` | 50000 | 集合竞价全天写 |
| `STREAM_MAXLEN_SNAPSHOT` | 200000 | 行情快照(高频 6s/次)|
| `STREAM_MAXLEN_ORDERBOOK` | 100000 | 5 档盘口 |
| `STREAM_MAXLEN_ZT` | 5000 | 涨停/炸板/异动/热股(低频)|
| `STREAM_MAXLEN_LIMITPERFORMANCE` | 5000 | 涨停表现详情 |
| `POOL_STREAM_MAXLEN` | 5000 | ZT/BREAK/ANOMALY/HOT 共用 |
| `STREAM_TTL` | **21600** | **通用 6 小时**(`DEFAULT_STREAM_TTL = 3600*6`,2026-09-12 v3 改) |
| `LIST_MAX_SIZE` | 500 | LIST 通用最大条数 |

### 2.5 辅助函数

```python
def _k(*parts: str) -> str:
    """业务 key 拼装:build_key(PREFIX, *parts)"""

def calc_snapshot_archive_ttl(now_dt) -> int:
    """根据写入时间动态算 snapshot archive EXPIRE
    11:00-11:30 → 7200s(跨午休)
    其他 → 1800s
    周末 → 1800s"""

def calc_snapshot_slide_window(now_dt) -> int:
    """根据写入时间动态算 snapshot timeline 滑窗 N(秒)
    逻辑同上"""
```

## §3 OnlineRedis 类(53 方法)

> **2026-09-15 校正**:实际 `OnlineRedis` + module-level 函数共 53 个方法(原 doc 写"80+"系估算错误,需以代码真值 `grep -n '^def \|^    def ' core/redis_online.py | wc -l` 为准)。

### 3.1 业务游标(2 方法)

| 方法 | 签名 | 用途 |
|---|---|---|
| `get_last_stream_id` | `(kind: str) -> str` | 获取某 kind STREAM 最后消费 id(默认 `$` = 从最新开始)|

**底层**:基于基类 `set_meta` / `get_meta`,key 形如 `online:meta:cursor:<kind>`。写侧如需持久化 last_id,直接调 `set_meta("cursor:<kind>", last_id)` 即可(无独立 method)。

### 3.2 watchlist(5 方法,v6.7 MyATM 改造,v6.14 类型扩展)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_watchlist` | `(ts_codes: list[str], sources_map: dict[str, str], watchlist_timestamp: int \| float \| str) -> str \| None` | 全量设置监控列表(SET + STREAM + timeline + archive)。**v6.14**:`watchlist_timestamp` 兼容 `int`(毫秒)/ `float`(秒,兼容老数据)/ `str`(老 ISO);内部 `>1e12` 视为毫秒直接用,`<=1e12` 视为秒转毫秒,`str` 直接落 STREAM |
| `get_watchlist` | `() -> list[str]` | 读监控列表 ts_code |
| `get_watchlist_sources` | `() -> dict[str, str]` | 读全部来源映射 |
| (watchlist 写入端用 `put_watchlist`,**不是** `set_watchlist`)| | |

### 3.3 auction(7 方法,v6.2 MyATM 改造,v6.13 简化)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_auction` | `(ts_code: str, data: dict) -> None` | 单股写:HSET(最新)+ ZADD window + XADD stream。**v6.13**:data_ts fallback 链简化 — record 里无 `snap_ts` / `snap_ts_unix`(service_writeredis_auction.py 写入端已删 `q.setdefault(...)`),data_ts 直接用 `auction_timestamp` |
| `commit_auction_snapshot` | `(snapshot_data: dict[str, dict]) -> str \| None` | 一轮末尾聚合 commit:ZADD timeline + HSET archive + 裁剪 timeline |
| `get_auction_timeline` | `(count: int = 50) -> list[dict]` | 读 timeline(时间序列索引)|
| `get_auction_archive_latest` | `() -> dict[str, dict]` | 读最新 archive(全部股)|
| `get_auction_snapshot_at` | `(seconds_before: int) -> dict[str, dict]` | 读 N 秒前的快照 |
| `get_auction_window` | `(ts_code, score_min=0, score_max="+inf") -> list[dict]` | 读单股 window |
| `get_auction_history` | `(count: int = 1000) -> list[tuple[str, dict]]` | 读 STREAM 历史 |

### 3.4 snapshot(8 方法,v6.3 MyATM 改造,v6.14 简化)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_snapshot` | `(ts_code: str, data: dict) -> None` | 单股写。**v6.14**:data_ts fallback 链简化 — record 里无 `snap_ts` / `snap_ts_unix`(service_writeredis_snapshot.py 写入端已删 `q.setdefault(...)`),data_ts 直接用 `snapshot_timestamp` |
| `put_snapshots_batch` | `(items: list[tuple[str, dict]]) -> int` | 批量写(pipeline 优化) |
| `commit_snapshots_batch` | `(items, now_dt=None) -> str \| None` | 一轮 commit(v6.3 新增)|
| `get_snapshot_timeline` | `(count: int = 50) -> list[dict]` | 时间序列索引 |
| `get_snapshot_archive_latest` | `() -> dict[str, dict]` | 最新 archive 全部股 |
| `get_snapshot_snapshot_at` | `(seconds_before: int, now_dt=None) -> dict[str, dict]` | N 秒前快照 |
| `get_snapshot_window` | `(ts_code, score_min=0, score_max="+inf") -> list[dict]` | 单股 window |
| `get_snapshot_history` | `(count: int = 1000) -> list[tuple[str, dict]]` | STREAM 历史 |

**v6.3 commit pipeline bug 修复**(2026-09-14):
- `pipe.zrangebyscore` 是 read 命令,pipeline 不会自动发 → 阻塞
- 修复:拆两次 execute(先写 pipeline,再单独 read + delete)

### 3.5 orderbook(4 方法)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_orderbook` | `(ts_code: str, data: dict) -> None` | 写 HSET(最新)+ ZADD window(3min) |
| `get_orderbook_window` | `(ts_code, score_min, score_max) -> list[dict]` | 单股 window |
| `get_orderbook_history` | `(count: int = 1000) -> list[tuple[str, dict]]` | STREAM 历史 |
| (无 timeline / archive — 简化设计)| | |

### 3.6 minute(2 方法)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_minute` | `(ts_code: str, data: dict) -> None` | 写分时(整天保留)|
| `get_minute_bars` | `(ts_code, bar_idx_min, bar_idx_max) -> list[dict]` | 按 bar 索引读 |

### 3.7 zt(4 方法,v6 MyATM)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_zt_pool` | `(zt_list, data_timestamp=None) -> int` | 批量写涨停池(全量覆盖 ZSET)|
| `get_zt_archive_latest` | `() -> dict[str, dict]` | 最新 archive |
| `get_zt_snapshot_at` | `(seconds_before: float) -> dict[str, dict]` | N 秒前快照 |
| `get_zt_timeline` | `(count: int = 50) -> list[dict]` | timeline |
| `get_zt_stream` | `(count: int = 100) -> list[tuple[str, dict]]` | STREAM 历史 |

### 3.8 break(4 方法,v6.1 MyATM)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_break_pool` | `(break_list, data_timestamp=None) -> int` | 批量写 |
| `get_break_stream` | `(count: int = 100) -> list[tuple[str, dict]]` | STREAM |
| `get_break_timeline` | `(count: int = 50) -> list[dict]` | timeline |
| `get_break_archive_latest` | `() -> dict[str, dict]` | 最新 archive |
| `get_break_snapshot_at` | `(seconds_before: int) -> dict[str, dict]` | N 秒前快照 |

### 3.9 anomaly(4 方法,v6.1 MyATM)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_anomaly` | `(anomaly_list, data_timestamp=None) -> int` | 批量写 |
| `get_anomaly_stream` | `(count: int = 100) -> list[tuple[str, dict]]` | STREAM |
| `get_anomaly_timeline` | `(count: int = 50) -> list[dict]` | timeline |
| `get_anomaly_archive_latest` | `() -> dict[str, dict]` | 最新 archive |
| `get_anomaly_snapshot_at` | `(seconds_before: int) -> dict[str, dict]` | N 秒前快照 |

### 3.10 hot(4 方法,v6.1 MyATM)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_hot_rank` | `(hot_list, data_timestamp=None) -> int` | 批量写 |
| `get_hot_stream` | `(count: int = 100) -> list[tuple[str, dict]]` | STREAM |
| `get_hot_timeline` | `(count: int = 50) -> list[dict]` | timeline |
| `get_hot_archive_latest` | `() -> dict[str, dict]` | 最新 archive |
| `get_hot_snapshot_at` | `(seconds_before: int) -> dict[str, dict]` | N 秒前快照 |

### 3.11 limitperformance(6 方法,v6.1 MyATM)

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_limitperformance` | `(lp_list, *, data_timestamp=None) -> int` | 批量写 |
| `get_limitperformance_xlen` | `() -> int` | STREAM 长度 |
| `get_limitperformance_stream` | `(count: int = 100) -> list[tuple[str, dict]]` | STREAM 历史 |
| `get_limitperformance_timeline` | `(count: int = 50) -> list[dict]` | timeline |
| `get_limitperformance_archive_latest` | `() -> dict[str, dict]` | 最新 archive |
| `get_limitperformance_snapshot_at` | `(seconds_before: int) -> dict[str, dict]` | N 秒前快照 |

### 3.12 meta + info(3 方法)

| 方法 | 签名 | 用途 |
|---|---|---|
| `get_meta` | `(field: str) -> Optional[str]` | 读 meta HSET 字段 |
| `info` | `() -> dict` | Redis 整体状态 + 各 keyspace 大小 + v6.3 加 `snapshot_timeline_size` |
| (set_meta 来自基类)| | |

### 3.13 snapshot_index(4 方法,v6.10 新增,v6.14 死字段清理)

**8 只固定指数**(000300.SH / 000688.SH / 000905.SH / 399001.SZ / 399006.SZ / 399005.SZ / 000852.SH / 000016.SH),每 30 秒 1 次写入,落盘每 15 分钟 1 次(11:45-12:45 休息)。

| 方法 | 签名 | 用途 |
|---|---|---|
| `put_snapshots_index_batch` | `(items: list[tuple[str, dict]]) -> int` | 批量写 8 只指数(1 个 pipeline,1 个 RTT)。**v6.14**:`snapshot_unix_ms` 死字段完全不引用(写入端 service 已删,redis 层从未引入) |
| `commit_snapshots_index_batch` | `(items: list[tuple[str, dict]], now_dt=None) -> str \| None` | 一轮末尾聚合 commit(ZADD timeline + HSET archive,**不做滑窗清理**)。**v6.14**:同款,无死字段 |
| `get_snapshot_index_timeline` | `(count: int = 50) -> list[dict]` | 读时间序列索引 |
| `get_snapshot_index_archive_latest` | `() -> dict[str, dict]` | 读最新 archive(8 只指数) |

## §4 调用示例

```python
from core.redis_online import OnlineRedis, calc_snapshot_archive_ttl
from datetime import datetime

r = OnlineRedis()

# 写 watchlist
r.put_watchlist(["000001.SZ", "600519.SH"])

# 写 snapshot
r.put_snapshots_batch([
    ("000001.SZ", {"last_price": 12.96, "volume": 114296522, "snapshot_timestamp": 1789374812}),
    ("600519.SH", {"last_price": 1680.50, "volume": 12345, "snapshot_timestamp": 1789374812}),
])

# 写完 commit
now = datetime.now()
archive_key = r.commit_snapshots_batch([...], now_dt=now)

# 读 archive
latest = r.get_snapshot_archive_latest()
for ts_code, data in latest.items():
    print(ts_code, data["last_price"])
```

## §5 与其他模块的关系

| 谁会用 | 用哪些方法 |
|---|---|
| **service_writeredis_*** | `put_*` / `commit_*` |
| **service_savedata_*** | `get_*_stream` / `get_last_stream_id` |
| **check_redis.py** | `info` / `get_*_timeline` / `get_*_archive_latest` |
| **query_redis.py** | `fetch_*_window` / `fetch_*_archive_latest` / `fetch_*_timeline` |
| **test_checkredis_*** | 同 check_redis |

## §6 历史变更

- **2026-09-12 v3**:watchlist 全表替换 + timeline 改为混合(`online:watchlist:timeline` 整体 + `online:watchlist:ts_codes` SET),timeline 索引用 string 拼接
- **2026-09-13 v4**:watchlist `put_watchlist` 加 `sources_map` 参数;`watchlist_timestamp` 来源改为 service 端 wall clock
- **2026-09-14 v5**:新增 `limitperformance` 6 方法 + 7 个常量
- **2026-09-15 v6**:auction / snapshot / orderbook 兜底链加 `snap_ts` / `snap_ts_unix` 兼容字段(后由 v6.13/v6.14 删除)
- **2026-09-16 v6.10**:`snapshot_index` 4 方法 + 5 常量新增;`put_snapshots_index_batch` 移出 3 drop keys
- **2026-09-16 v6.13**:`put_auction` / `put_snapshot` data_ts fallback 链简化(删 `snap_ts` / `snap_ts_unix` 兜底,写入端已删字段);record 数据模型收敛
- **2026-09-16 v6.14**:`put_watchlist` 类型签名扩展(`str` → `int | float | str`,兼容 3 种 timestamp 格式);`put_snapshot` / `put_auction` / `put_snapshots_index_batch` / `commit_snapshots_index_batch` **不动**(写入端 `snapshot_unix_ms` / `snap_ts` / `snap_ts_unix` 死字段已删,redis 层完全无引用,无需清理)

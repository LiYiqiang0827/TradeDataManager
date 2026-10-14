# ths_client.md

> **版本**:v1.0(2026-09-15 新增)
> **文件路径**:`~/TradingAgent/coreClient/ths_client.py`
> **配置**:`~/TradingAgent/coreClient/ths_config.py`
> **目的**:同花顺(THS / hithink-finance CLI)API 客户端封装,把 CLI 调用封装成 Python 函数
| **v6.13 / v6.14 更新(2026-09-16)**:**同花顺 envelope.data.timestamp 100% 必给**,service 写入端已删除 `snap_ts` / `snap_ts_unix` / `snapshot_unix_ms` 三个 wall clock 兜底字段,record 数据模型收敛到 `<kind>_timestamp` 单业务时间戳(auction / snapshot / snapshot_index)。本客户端无变更,需在文档标注确认 9 个 fetch 函数中:auction / snapshots / snapshots_index(指数)/ watchlist(如有)/ limitperformance / anomaly 的返回数据,envelope.data.timestamp 必返,service 端不再做兜底。

---

## 1. 功能

**为什么需要这个**:
- 把 `hithink-finance` CLI 调用封装成 Python 函数,业务不用关心 subprocess / 重试 / 退避
- 单次调用 + 错误处理 + 重试 + 退避
- 批量调用(单次最多 100 thscode token,自动分批)— 见 `ths_config.THS_CHUNK_SIZE_DEFAULT`
- 所有函数返 `list[dict]`,空表示当前不可用

**提供 9 个 fetch 函数**:
| 函数 | 用途 | service 使用方 |
|---|---|---|
| `fetch_auction_snapshots()` | 集合竞价快照 | `service_writeredis_auction` |
| `fetch_snapshots()` | 连续竞价快照 | `service_writeredis_realtime` |
| `fetch_anomaly_list()` | 当日异动清单 | `service_writeredis_anomaly` |
| `fetch_limitup_pool()` | 当日涨停池 | `service_writeredis_zt`(也是 watchlist 补充来源) |
| `fetch_limitdown_pool()` | 当日跌停池 | 监控/预警(可选) |
| `fetch_limitbreak_pool()` | 当日炸板池 | `service_writeredis_break` |
| `fetch_index_snapshot()` | 指数实时快照 | `watchlist_fetch` |
| `fetch_skyrocket()` | 飙升榜 | `service_writeredis_hot`(可选) |
| `fetch_hot_stock()` | 热股榜 | `service_writeredis_hot` |

**异常**:`HithinkCLIError`(CLI 失败重试后仍失败时抛)。

---

## 2. v3 源头精简规则(2026-09-12)

所有 fetch 函数都自动做源头精简:
- `thscode → ts_code` 重命名(同值,只是统一名字)
- `ticker` 字段删除(可从 ts_code 推导)
- 数据时间戳字段命名:`<kind>_timestamp`(同花顺 envelope 给,毫秒;fallback ths_client 兜底打)

```python
# 源头 dict
{"thscode": "000426.SZ", "ticker": "000426", "last_price": 5.5, ...}

# v3 精简后
{"ts_code": "000426.SZ", "last_price": 5.5, ..., "snapshot_timestamp": 1757933234567}
```

---

## 3. 接口

### 3.1 内部辅助函数

#### `_stamp_snapshot_timestamp(items: list[dict], *, field="snapshot_timestamp") -> list[dict]`

**2026-09-12 精简**:只补缺失的 snapshot_timestamp(数据时间戳)。

**新规则**(用户 2026-09-12 决定):
- 数据时间戳命名:以数据名_timestamp 命名(snapshot_timestamp / zt_timestamp / ...)
- 同花顺 API 本身有时间戳 → 保留,不打
- 没有 → ths_client 兜底补 `<field>`(unix 秒 float)
- 存盘时由 persist_client 把 unix 秒 → ISO 字符串(SQLite TEXT 列)

**旧名兼容**:`_stamp_data_timestamp = _stamp_snapshot_timestamp`(部分代码可能引用旧名)。

---

#### `_run_cli(args: list, *, retries=THS_CLI_RETRY_MAX, timeout=THS_CLI_DEFAULT_TIMEOUT) -> dict`

执行 hithink-finance CLI,返回 parsed JSON envelope。

**退避**:`THS_CLI_RETRY_BASE_SLEEP * (2 ** attempt)`(指数退避)。

---

#### `_chunked(items: list, size: int) -> list[list]`

切批工具:`[items[i:i+size] for i in range(0, len(items), size)]`。

---

### 3.2 fetch 函数

#### `fetch_auction_snapshots(ts_codes, *, stage="live", chunk_size=THS_CHUNK_SIZE_DEFAULT) -> list[dict]`

**拉集合竞价快照**。自动分批(每批最多 100 thscode,默认 80 留 buffer)。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `ts_codes` | list[str] | 必 | A 股 thscode 列表,如 `['000426.SZ', '600519.SH']` |
| `stage` | str | `"live"` | `'live'` 或 `'final'` |
| `chunk_size` | int | `80` | 每批 token 数 |

**返回**:list of dict(源头已精简:thscode→ts_code + 删 ticker,已打 `auction_timestamp`)。

**注意**:同花顺 API 返回的外层字段 `auction_phase / data_status / timestamp` 是这一批的共同状态,自动注入到每个 item 里。

---

#### `fetch_snapshots(ts_codes, *, chunk_size=THS_CHUNK_SIZE_DEFAULT) -> list[dict]`

**拉普通行情快照**。自动分批。

源头已精简(`ts_code` 标识,无 `thscode / ticker`)。

---

#### `fetch_anomaly_list() -> list[dict]`

**全天异动清单**(带 AI 解读)。

返回字段:`ts_code / stock_name / analysis_content / keyword_list / tag_name`(已打 `anomaly_timestamp`)。

---

#### `fetch_limitup_pool(*, size=THS_POOL_SIZE_DEFAULT, sort_field="last_price", sort_dir="desc") -> list[dict]`

**当日涨停池**。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `size` | int | `200` | 每页 size(1-THS_POOL_SIZE_DEFAULT) |
| `sort_field` | str | `"last_price"` | 排序字段(`last_price / continue_day_cnt / seal_money / limit_up_time`) |
| `sort_dir` | str | `"desc"` | `asc` / `desc` |

(已打 `zt_timestamp`)

---

#### `fetch_limitdown_pool(*, size=THS_POOL_SIZE_DEFAULT) -> list[dict]`

**当日跌停池**。

字段:`thscode / last_price / price_change_ratio_pct / first_limit_time / last_limit_time / turnover_ratio_pct`。

---

#### `fetch_limitbreak_pool(*, size=THS_POOL_SIZE_DEFAULT) -> list[dict]`

**当日炸板池**(涨停后开板没回封)。

字段:`thscode / last_price / price_change_ratio_pct / first_limit_time / last_limit_time / open_times(开板次数) / turnover_ratio_pct / turnover`(已打 `break_timestamp`)。

---

#### `fetch_index_snapshot(ths_codes: list[str]) -> list[dict]`

**指数实时快照**。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `ths_codes` | list[str] | 必 | 最多 100 个指数代码,如 `['000001.SH', '399001.SZ', '399006.SZ', '000688.SH']` |

---

#### `fetch_skyrocket(*, period="hour") -> list[dict]`

**飙升榜**(按 heat 排序)。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `period` | str | `"hour"` | `'day'` 或 `'hour'` |

---

#### `fetch_hot_stock(*, period="hour") -> list[dict]`

**热股榜**(按 heat 排序)。

(已打 `hot_timestamp`)

---

### 3.3 异常类

#### `HithinkCLIError`

CLI 失败重试 `THS_CLI_RETRY_MAX=3` 次后仍失败。

调用方 try/except 处理:

```python
from coreClient.ths_client import fetch_limitup_pool, HithinkCLIError
try:
    items = fetch_limitup_pool()
except HithinkCLIError as e:
    logger.warning(f"涨停池拉取失败: {e}")
    items = []
```

---

## 4. 用法示例

### 4.1 拉集合竞价快照(早盘 9:25-9:30)

```python
from coreClient.ths_client import fetch_auction_snapshots

watchlist = ["000426.SZ", "600519.SH", "002594.SZ"]
items = fetch_auction_snapshots(watchlist, stage="live")
for item in items:
    print(f"{item['ts_code']}: phase={item['auction_phase']} price={item['auction_price']}")
```

### 4.2 拉涨停池(sort by 封单金额)

```python
from coreClient.ths_client import fetch_limitup_pool

items = fetch_limitup_pool(size=200, sort_field="seal_money", sort_dir="desc")
print(f"今日涨停 {len(items)} 只,封单最大: {items[0]['ts_code']} {items[0]['seal_money']}")
```

### 4.3 拉炸板池

```python
from coreClient.ths_client import fetch_limitbreak_pool

items = fetch_limitbreak_pool(size=200)
print(f"今日炸板 {len(items)} 只")
for item in items[:5]:
    print(f"  {item['ts_code']} 开板次数={item['open_times']} last={item['last_price']}")
```

### 4.4 拉指数快照

```python
from coreClient.ths_client import fetch_index_snapshot

items = fetch_index_snapshot(["000001.SH", "399001.SZ", "399006.SZ", "000688.SH"])
for item in items:
    print(f"{item['ts_code']}: {item['last_price']} pct={item['price_change_ratio_pct']}%")
```

### 4.5 拉热股榜(全天累加)

```python
from coreClient.ths_client import fetch_hot_stock

items = fetch_hot_stock(period="day")[:20]
print(f"今日热股 TOP 20:")
for item in items:
    print(f"  {item['ts_code']} heat={item['heat']} pct={item['price_change_ratio_pct']}%")
```

---

## 5. config 文件说明:ths_config.py

**位置**:`~/TradingAgent/coreClient/ths_config.py`

| 配置项 | 值 | 说明 |
|---|---|---|
| `THS_CLI_CMD` | `"hithink-finance"` | CLI 命令名(PATH 必须能解析到) |
| `THS_CLI_DEFAULT_TIMEOUT` | `30` | CLI 单次调用超时(秒) |
| `THS_CLI_RETRY_MAX` | `3` | CLI 失败重试次数 |
| `THS_CLI_RETRY_BASE_SLEEP` | `1` | 退避基础(秒,实际 `sleep = base * 2^attempt`) |
| `THS_CHUNK_SIZE_DEFAULT` | `80` | 批量调用每批 thscode token 数(API 硬限 100/批,默认 80 留 buffer) |
| `THS_POOL_SIZE_DEFAULT` | `200` | 涨停/跌停/炸板池默认页大小(1-200) |
| `THS_DEFAULT_TS_SUFFIXES` | `(".SH", ".SZ")` | thscode 后缀约定 |

**thscode 格式**:`"000001.SZ"` 形式,前 6 位是股票代码,后 3 位是市场标识。
同花顺 CLI 接受 `--thscodes "000001.SZ,600519.SH"` 形式批量传入。

---

## 6. 使用方

| 业务 | 文件 | 用途 |
|---|---|---|
| **onlineDataManager** | `scripts/service/service_writeredis_auction.py` | 拉集合竞价快照 → 写 Redis(`auction_stream`) |
| **onlineDataManager** | `scripts/service/service_writeredis_realtime.py` | 拉连续竞价快照 → 写 Redis(`snapshot_window`) |
| **onlineDataManager** | `scripts/service/service_writeredis_anomaly.py` | 拉异动清单 → 写 Redis(`anomaly_list`) |
| **onlineDataManager** | `scripts/service/service_writeredis_zt.py` | 拉涨停池 → 写 Redis(`zt_pool`)+ watchlist 补充 |
| **onlineDataManager** | `scripts/service/service_writeredis_break.py` | 拉炸板池 → 写 Redis(`break_pool`) |
| **onlineDataManager** | `scripts/service/service_writeredis_hot.py` | 拉热股榜 → 写 Redis(`hot_list`) |
| **onlineDataManager** | `scripts/core/watchlist_fetch.py` | 拉指数快照 + 涨停池 → 写 watchlist |
| **monitor**(未来) | 跌停/涨停监控 | 拉跌停/涨停池 → 预警 |

---

## 7. 关键设计原则

1. **源头精简自动**:所有 fetch 函数返回时已 `thscode → ts_code` + 删 ticker + 打数据时间戳,业务不用关心
2. **批量自动分页**:每次最多 80 个 thscode(API 硬限 100),自动按 80 切批
3. **退避策略**:指数退避 `THS_CLI_RETRY_BASE_SLEEP * 2^attempt`,第 1 次 1s,第 2 次 2s,第 3 次 4s
4. **小间隔 0.2s**:批量间 `time.sleep(0.2)` 避免被限流
5. **空结果合法**:CLI 失败重试后仍失败 → 抛 `HithinkCLIError`(不返 None,调用方 try/except)
6. **数据时间戳字段**:`<kind>_timestamp`(snapshot_timestamp / auction_timestamp / zt_timestamp / ...)

---

## 8. 验证清单

- [x] `ths_client.py` 现有代码 + 文档完整对齐(2026-09-15)
- [x] 9 个 fetch 函数全部文档化(auction / snapshot / anomaly / limitup / limitdown / limitbreak / index / skyrocket / hot)
- [x] v3 源头精简规则(thscode → ts_code + 删 ticker + 打时间戳)有说明
- [x] 异常类 `HithinkCLIError` 处理模式有示例
- [x] 7 项 config 配置全部说明
- [x] onlineDataManager 使用方文档化(7 个 service + watchlist_fetch)
- [x] 批量 + 退避 + 小间隔设计原则有说明
- [x] **2026-09-16 v6.13/v6.14**:envelope.data.timestamp 100% 必给,无 wall clock 兜底字段,记录到文档头部

## 9. 历史变更

- **2026-09-15 v1.0**:初版,9 个 fetch 函数文档化
- **2026-09-16 v6.13/v6.14**:**同花顺 envelope.data.timestamp 100% 必给** — service 写入端删除 `snap_ts` / `snap_ts_unix`(auction / snapshot)/ `snapshot_unix_ms`(snapshot_index)三个 wall clock 兜底字段;`ths_client.py` 无变更(仍 100% 透传 `envelope.data.timestamp`),仅在 §0 元信息顶部追加 v6.13/v6.14 标注
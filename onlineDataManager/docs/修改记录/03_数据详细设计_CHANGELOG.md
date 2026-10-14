# 03_数据详细设计 CHANGELOG

> **本体只保留最新内容**,变更记录走 CHANGELOG。

---

## v1.0 — 2026-09-14(初版)

- **新增**:03_数据详细设计.md(10.6KB)
- **内容**:
  - §1 SQLite 数据库位置(11 个当日表 + 2 个全局表)
  - §2 各 kind 表 schema(watchlist / auction / snapshot / orderbook / minute / zt / break / anomaly / hot / limit_performance)
  - §3 Redis 数据结构汇总
  - §4 SQL ↔ Redis 关系
  - §5 常用查询示例
  - §6 历史变更

## 后续变更

## v1.1 — 2026-09-14 18:55(体检)

- **修改**:§2.5 minute 周期"5 分钟一次" → "15 分钟一次"(按代码 DEFAULT_INTERVAL=900s)
- **修改**:§2.6 zt 周期"1 分钟一次" → "30 分钟一次"(按代码 DEFAULT_INTERVAL=1800s)
- **修改**:§2.4 / §2.7 / §2.8 / §2.9 / §2.10 补"写入脚本"行 5 处(原版只 2.5/2.6 有)
- **新增**:§2.7 break schema(原版空)— 按 sqlite_client.py:BREAK_TABLE_SCHEMA
- **新增**:§2.8 anomaly schema + 备注 anomaly_keywords 副表(原版空)— 按 sqlite_client.py:ANOMALY_TABLE_SCHEMA
- **新增**:§2.9 hot schema(原版空)— 按 sqlite_client.py:HOT_TABLE_SCHEMA
- **修改**:§2.10 limit_performance schema 从简化 16 字段 → 完整 23 字段(LIMITPERFORMANCE_TABLE_SCHEMA 全量)
- **修改**:§6 历史变更追加 v1.1 体检记录
- **原因**:体检发现 §2.7/§2.8/§2.9 schema 文档完全空,§2.10 是简版;§2.4-§2.10 缺"写入脚本"行
- **影响**:文档与代码 100% 对应;生产 0 影响

---

## 后续变更(模板)

```
## vX.Y — YYYY-MM-DD(变更简述)

- **新增/修改/删除**:...
- **原因**:...
- **影响**:...
```

---

## v1.2 — 2026-09-15(顶部加 05 引用 + 05 指南创建)

- **新增/修改**:
  - `03_数据详细设计.md` 顶部"受众"后加一行:"加新实时监控落盘数据?→ 直接看 05_新增实时监控落盘数据指南.md(v1.0,2026-09-15 新增)的 §6 (SQLite schema 新增)+ §4 (Redis 三件套设计)+ §5 (redis_online 接口规范)"
- **原因**:用户第 21 轮指令。03 是字段字典(schema + key),无法承接"加新实时监控管道"任务,新顶级文档 05 覆盖 schema 章节,需在 03 顶部建立跳转。
- **影响**:AI 接到"加新实时监控数据"任务时,从 03 顶部直接跳 05 §4/§5/§6,不再回头读字段字典
- **联动**:新建 `05_新增实时监控落盘数据指南.md` + `05_新增实时监控落盘数据指南_CHANGELOG.md`

---

## v1.3 — 2026-09-15(第 22 轮:doc ↔ code 全量一致性校正)

> **触发**:用户第 22 轮指令"再完整扫描一遍代码和文档,看看文档是否与代码一致,尤其是各个详细设计文档"。AI 全量核对 31 份详细设计 + 4 份主文档 + 1 份指南,发现 3 处表字段数错 + 1 处 auction window TTL 错。

### 修正清单

**表字段数真值点**(以 `sqlite_client.py KIND_SCHEMAS` + `WATCHLIST_TABLE_SCHEMA` 真值,2026-09-15 14:15+ 校正):
- `snapshot_YYYYMMDD`:**12 → 14**(+2,加 `data_status` + `created_at`)
- `anomaly_YYYYMMDD`:**9 → 8**(-1,删重复)
- `hot_YYYYMMDD`:**10 → 14**(+4)

**Redis key TTL 错**:
- §3 auction window TTL:"5min(300s)=DEFAULT_WINDOW_TTL" ❌ → **1 天(86400s)=WINDOW_TTL_AUCTION**(全天窗口,非滑窗)。代码真值 `redis_online.py:put_auction()` 用 `WINDOW_TTL_AUCTION = 86400`。

### 原因

用户第 22 轮明确指令"完整扫描一遍代码和文档,看看文档是否与代码一致,尤其是各个详细设计文档"。03 §2 表字段数和 §3 key TTL 是最容易被新 AI 抄错的真值点,本次全量核对清理 4 处错。

### 影响

- **生产 0 影响**(plist / 调度阶段 / SQLite 路径未变,纯文档校正)
- **AI 抄表 / 抄 TTL 时不再错**(auction window 是常用 key,TTL 错会导致 key 早过期)

## v1.4 — 2026-09-15(第 23 轮:**时间戳全部精确到毫秒**)

### 背景

用户原话(2026-09-15 ~12:40+):"下一步是非常关键的一步,我们在落盘的时候,kind timestamp 和 created at 这两个时间戳目前不是只精确到秒吗,请全部改为精确到毫秒(3 位)"

### 代码层变更(一次到位,13 处 isoformat + 12 处 schema DEFAULT)

- **`scripts/core/sqlite_client.py`** 12 处 schema DEFAULT:
  - `datetime('now','localtime')` → `strftime('%Y-%m-%d %H:%M:%f','now','localtime')`(SQLite ≥3.20,本机 3.45.3 ✅)
  - 影响 SNAPSHOT / AUCTION / ORDERBOOK / MINUTE / ZT / BREAK / ANOMALY / HOT / LIMITPERFORMANCE / WATCHLIST / CURSOR 共 11 张表(ANOMALY_KEYWORDS 无 created_at,跳过)
- **`scripts/core/sqlite_client.py`** 5 处 Python 端 `timespec="seconds"` → `timespec="milliseconds"`:L445 last_persist_ts / L780 save_timestamp / L898 now_save / L1097 created_at / L1240 data_timestamp
- **`scripts/core/persist_client.py`** 8 处 isoformat `timespec="seconds"` → `"milliseconds"`(覆盖 _persist_stream_to_sqlite + persist_stream_kind + persist_watchlist + persist_anomaly 等)
- **`scripts/core/redis_online.py`** L202 + L640 + L331 isoformat 改 milliseconds
- **`scripts/core/watchlist_fetch.py`** L590 isoformat
- **`scripts/core/check_db.py`** L122/127/132 isoformat
- **`scripts/service/service_check_db.py`** L158/254 strftime → milliseconds
- **`scripts/service/service_check_redis.py`** L165 isoformat
- **8 个 `service_writeredis_*.py`** 12 处 isoformat(anomaly / zt / auction / orderbook / watchlist / limitperformance / break / hot)
- **`scripts/scheduler/scheduler_onlineData.py`** L555 + L587 scheduler_started_at / scheduler_last_ts
- **`scripts/service/service_savedata_watchlist.py`** L79 watchlist_last_persist_ts
- **`coreClient/tdx_client.py`** L289 + L482 ts_iso
- **`scripts/service/service_savedata_snapshot.py`** schema 跟着 `sqlite_client` 自动获益(`strftime` 含 `%f`)
- **总残留**:`grep -rnE 'timespec="seconds"'` 全代码库 0 处 ✅

### 文档层

- **本文头部 §0** 新增"全局时间戳精度"声明(2 段,5 行):数据时间戳格式 / 落盘时间戳格式 / 保留例外 / 向后兼容
- **§6 历史变更** 追加 v1.4 一行
- **`代码详细设计/`** 6 处详细 md 与代码同步:core_persist_client / core_sqlite_client / core_redis_online / core_watchlist_fetch / check_db / service_savedata_snapshot / service_check_db / service_check_redis / 8 个 writeredis

### 验证

- **SQLite 兼容性**:`datetime.fromisoformat("2026-09-15T12:18:12")` ✅ 秒;`datetime.fromisoformat("2026-09-15T12:18:12.847")` ✅ 毫秒;`datetime.fromisoformat("2026-09-15 12:18:25.847")` ✅ SQLite strftime 格式
- **落盘真值**:drain 测试 2 条 `TEST_MS_001/002.SZ` 落盘后 `snapshot_timestamp` = `2026-09-15T12:47:11.334` / `2026-09-15T12:47:16.429`(含 `.NNN` 毫秒)✅
- **`scheduler_once`**:20/20 OK(16.5s,0 fail)✅
- **老数据兼容**:71882 条旧数据 `created_at` 无 `.`(秒),新数据含 `.`(毫秒),`fromisoformat` 兼容 ✅

### 保留例外(用户最高优先级约束)

- **`watchlist.lu_time`** / **`limitperformance.lu_time`** 仍是 int64 unix — 涨停时间不能丢精度,用户原话"千万不要删,涨停时间非常重要的数据"

### 不影响

- `data_timestamp == limitperformance_timestamp` 双时间戳统一原则不变

---

## v1.5 — 2026-09-16(snapshot_index kind 上线)

### 新增

- **`snapshot_index_YYYYMMDD`** 表(15 字段):trade_date / ts_code / ticker / volume / turnover / last_price / price_change / price_change_ratio_pct / open_price / high_price / low_price / prev_price / snapshot_timestamp(ISO 毫秒)/ snapshot_unix(int 毫秒,替代 lu_time)/ created_at
- 8 只固定指数:000001.SH / 399001.SZ / 399006.SZ / 000688.SH / 000016.SH / 000300.SH / 000905.SH / 000852.SH
- 数据源:ths_client.fetch_index_snapshot → hithink-finance index snapshot

### 设计决策

- **snapshot_unix 列替代 lu_time**(指数无涨停时间,改用 snapshot_unix 表达"数据时间戳的 unix 形式",用户最高优先级"任何表都存 unix int64")
- **drop_keys 去除 thscode / ticker**(redis_config.py 默认 drop,但指数对账需要保留)
- **archive TTL 不做 lunch 动态**(固定 6h,窗口比 snapshot 更窄)
- **timeline 不滑窗**(用户原话"timeline数据也不做滑窗清理")

### 文档更新

- **本文头部** 加"v6.10 新增"声明
- **`代码详细设计/`** 新建 `service_writeredis_snapshot_index.md` + `service_savedata_snapshot_index.md`
- **`05_新增实时监控落盘数据指南.md`** §12 完整实例(决策表 + 实施清单 + 验证结果 + 5 个踩坑)

---

## v1.6 — 2026-09-16(lu_time 列 INTEGER → TEXT,秒级字符串)

### 改动

- **`limitperformance_YYYYMMDD.lu_time` 列类型**:INTEGER → TEXT
- **存储格式**:`YYYY-MM-DD HH:MM:SS`(秒级字符串)
- **源头语义**:Redis STREAM 里仍存原始 unix int(便于二次处理)
- **落盘转换**:extractor `_extract_limitperformance_row` 把 int unix → 字符串

### 语义

- **lu_time** = "涨停时间"(这只股封板的那一秒)
- **limitperformance_timestamp** = "数据刷新时刻"(同花顺给的数据时间戳,毫秒 ISO)
- **created_at** = "落盘时刻"(SQLite 写入时间,毫秒)

三列**完全独立**,用户原话:"lu_time 是数据本来的一个重要数据是表示涨停时间的,和数据的记录时间等没有关系,不要混淆"。

### 迁移

- 一次性 12 步法迁移(2026-09-16 10:06 跑完,32003 行)
- 历史表:`limitperformance_20260914`(13550 行)/ `limitperformance_20260915`(16854 行)/ `limitperformance_20260916`(1599 行)
- 备份表:`*_bak_20260916_lu_time`(保留 7 天可回滚)
- 脚本:`scripts/_migrate_lu_time_to_text.py`(幂等,可重复运行)

### 文档更新

- **本文头部** 加"v6.11 更新"
- **`代码详细设计/core_sqlite_client.md`** 加 v6.11 标记

---

## v1.7 — 2026-09-16(anomaly_keywords 长表整套废弃)

### 改动

- **`ANOMALY_KEYWORDS_TABLE_SCHEMA`**:彻底删除(原本 5 字段 + 3 索引 + UNIQUE 约束)
- **`LONG_TABLE_SCHEMAS["anomaly_keywords"]`**:移除;`LONG_TABLE_SCHEMAS` 仅剩 `{"minute_bars": ...}`
- **`insert_anomaly_keywords_batch`**:函数删除
- **`persist_client.persist_anomaly`**:不再处理 `keyword_list` 字段,不再调 `insert_anomaly_keywords_batch`;日志不再输出"关键词 X/Y 条到 anomaly_keywords_<date>"
- **历史 anomaly_keywords_<date> 表**:5 张全 DROP(共 60132 行,20260911/12/14/15/16)

### 用户原话

> "下一个问题,为什么 anomally 数据在落盘时为什么还有个 anomally_keywords?这个没必要存啊"

### 关键判断

- `keyword_list` 短词(`['重整澄清', '整车低开', '资金流出']`)信息**已被 `analysis_content` 长文包含**,冗余
- anomaly 顶层表**不动**:7 字段保留(id/trade_date/ts_code/stock_name/analysis_content/tag_name/anomaly_timestamp/created_at)

### 验证

- 手工触发 `persist_anomaly` 落盘 135 条,**日志只剩** `[anomaly] STREAM 落盘 135/258 条到 anomaly_20260916,last_id=...`,无 anomaly_keywords 字样
- `SELECT name FROM sqlite_master WHERE name LIKE 'anomaly_keywords%'` 返回空

### 文档更新

- **`代码详细设计/core_sqlite_client.md`** 加 v6.12 标记
## v1.8 — 2026-09-16(auction.snap_ts / snap_ts_unix 死字段清理,v6.13)

### 触发

- 用户原话:"我的意思是对于 auction 来说,写入 redis 的前都不需要再去生成 snap_ts / snap_ts_unix 也不需要写入到 redis 里面"
- v6.13 写端删除 → 数据模型收敛 → 落盘端自然无需改

### Redis STREAM `auction` 字段变化

| 字段 | v6.13 前 | v6.13 后 |
|---|---|---|
| `auction_timestamp` | int 毫秒字符串 | int 毫秒字符串 |
| `snap_ts` | ISO 字符串(冗余) | **删除**(死字段) |
| `snap_ts_unix` | float 秒(冗余) | **删除**(死字段) |

### SQLite 落盘表 `auction_<YYYYMMDD>`

- 14 列不变
- 字段值来源只有 `auction_timestamp`(同花顺 `envelope.data.timestamp`,秒级精度末 3 位 000)
- 落盘后:`auction_timestamp="2026-09-16T11:00:13.000"`

### 不影响

- 同款 v6.14 snapshot(个股)/ snapshot_index(指数)死字段清理(详见 v1.9)
- 其它 kind 时间戳字段不变
- 任何 STREAM ID / TTL / SQLite schema 未动

## v1.9 — 2026-09-16(watchlist_timestamp int 毫秒 + snapshot_index.snapshot_unix_ms + snapshot.snap_ts/snap_ts_unix 死字段清理,v6.14 累计三项)

### 触发

- 用户原话:"为什么 watchlist 在 redis 里面写入的时候数据是字符串而不是原始的时间戳整型?" → 第一项
- 用户原话:"我看到 snap_index 这个数据在 redis 里面有 snapshot_timestamp 和 snapshot_unix_ms" → 第二项
- 用户原话:"最后一个要去人的是 snapshot 这个不是 snap_index 这个有三个时间戳" → 第三项

### Redis STREAM 字段变化

| kind | 字段 | v6.14 前 | v6.14 后 |
|---|---|---|---|
| **watchlist** | `watchlist_timestamp` | `"2026-09-16T11:04:29.768"`(ISO 字符串)| `"1789527869768"`(int 毫秒字符串) |
| **snapshot_index** | `snapshot_unix_ms` | float 秒(冗余) | **删除**(死字段)|
| **snapshot(个股)** | `snap_ts` | ISO 字符串(冗余) | **删除**(死字段)|
| **snapshot(个股)** | `snap_ts_unix` | float 秒(冗余) | **删除**(死字段)|

### SQLite 落盘表变化

| 表 | v6.14 前 | v6.14 后 |
|---|---|---|
| `watchlist_<YYYYMMDD>` | `watchlist_timestamp` ISO 字符串(wall clock) | `watchlist_timestamp` ISO 字符串(毫秒位非 000,真实 wall clock 毫秒精度)|
| `snapshot_index_<YYYYMMDD>` | `snapshot_timestamp` + `snapshot_unix` 14 列(后者秒级字符串冗余)| 14 列不变(写入端已删 `snapshot_unix_ms`,schema 不变)|
| `snapshot_<YYYYMMDD>` | `snapshot_timestamp` 12 列 | 12 列不变(写入端已删 `snap_ts` / `snap_ts_unix`,schema 不变)|

### 关键判断

- 全 kind 时间戳最终统一规则:**`_timestamp` 字段在 Redis STREAM 都是 int 毫秒字符串;落盘全部转 ISO 字符串;`snap_ts` / `snap_ts_unix` / `snapshot_unix_ms` 三个 wall clock 兜底字段全部清理**(同花顺 100% 给业务时间戳,wall clock 兜底冗余)
- 同花顺 API 业务时间戳毫秒位必为 000(秒级精度),设计意图是秒级毫秒表示
- watchlist 是唯一 wall clock 来源(snapshot / auction / snapshot_index 都是同花顺业务时间戳)

### 不影响

- 其它 kind:`auction_timestamp`(v1.8 已清) / `orderbook_timestamp` / `limitperformance_timestamp` / `anomaly_timestamp` / `bar_ts` 不动
- watchlist 落盘兼容老数据(ISO 字符串 / float 秒)
- 任何 STREAM ID / TTL / schema 未动
- 同花顺 client(`coreClient/ths_client.py`)无需改

## v1.10 — 2026-09-16(scheduler 重构,统一窗口,v6.15)

### 触发

- 同 02 v1.33(用户最新统一窗口指令)

### 改动

- 数据本身无任何字段 / schema / 类型改动(本次纯调度器重构)
- 所有 10 种 kind 的 `_timestamp` 字段语义不变(仍是同花顺 envelope.data.timestamp 毫秒)
- 唯一变化:各 kind 在 Redis STREAM 的写入 / 落盘频率(详见 service_writeredis_*.py 改动)

### 验证

- `scheduler_once` 22/22 OK
- 4 层验证:launchd PID 8959 / ps children 19 个 daemon(无 orderbook)/ Redis snapshot/snapshot_index 持续写入 / SQLite 10 表全有今日数据

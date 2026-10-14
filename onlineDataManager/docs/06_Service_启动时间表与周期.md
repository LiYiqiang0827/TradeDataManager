# 06 onlineDataManager Service 启动时间表与周期

> **本文是 scheduler_onlineData 的"操作手册 + 时间表 + 周期清单"**。
> **目的**:让 AI 在不读代码的前提下,精确知道:
> 1. 每个 service 在哪个时间窗被 scheduler 拉起 / 杀掉
> 2. 每个 writer 的拉数据周期
> 3. 每个 savedata 的落盘周期
> 4. 一次性 trigger(clean redis / savedata auction)
> 5. lunch / 周末 / 收盘后的边界
>
> **受众**:AI Agent(需要排查"为什么现在在跑 / 为什么没在跑"时第一份文档)。
> **最新状态**:v1.2(2026-09-16 v6.15 重构,阶段数 8→10,统一窗口,删 orderbook)。变更历史 → `06_Service_启动时间表与周期_CHANGELOG.md`。
> **代码权威源**:`scripts/scheduler/scheduler_onlineData.py`(L 60-260 阶段定义 + PHASE_SERVICES + ONCE_TRIGGERS)
> **加新实时监控落盘数据**?→ 仍然先看 [05_新增实时监控落盘数据指南.md](05_新增实时监控落盘数据指南.md),本文只覆盖"什么时候跑 / 多久跑一次"。

---

## §0 一图流 — 完整时间表(v6.15)

```text
时间  ┌──writer 阶段───────┬──savedata 阶段────┬──service 列表──────────────────┬──特别事件─────────────┐
09:00 │ pre_open           │ pre_open          │ (无)                            │ cleanredis --once ←   │
09:10 │ watchlist          │ watchlist         │ writeredis_watchlist(1次写)    │                       │
      │                    │                   │ savedata_watchlist(daemon)      │                       │
09:14 │ auction_writer     │ auction_savedata  │ writeredis_auction(daemon)      │                       │
      │                    │                   │                                 │                       │
09:26 │ idle_pre_morning   │ idle_pre_morning  │ (空窗,等普通 kind 启动)        │                       │
      │                    │                   │ (空窗,等落盘启动)              │                       │
09:29 │ morning_writer     │ morning_savedata  │ 8 个普通写入 daemon             │ savedata_auction ←   │
      │                    │                   │ 8 个普通落盘 + watchlist        │ (auction 落盘)        │
09:30 │ (同上 morning_…)   │ (同上 morning_…)  │ (同上)                          │                       │
11:31 │ lunch              │ morning_savedata  │ (无写入)                       │                       │
      │                    │                   │ 8 个落盘继续跑                  │                       │
11:46 │ lunch              │ savedata_paused   │ (全停)                         │                       │
12:59 │ idle_pre_afternoon │ idle_pre_afternoon│ (空窗)                         │                       │
13:00 │ afternoon_writer   │ afternoon_savedata│ 8 个写入 daemon 重启            │                       │
      │                    │                   │ 8 个落盘 daemon 重启            │                       │
15:01 │ writer_stopped     │ afternoon_savedata│ (无写入)                       │                       │
      │                    │                   │ 8 个落盘继续跑                  │                       │
15:15 │ writer_stopped     │ afternoon_savedata│ (无写入)                       │ savedata_auction ←   │
      │                    │                   │ 8 个落盘继续跑                  │ (auction 兜底补落)    │
15:16 │ writer_stopped     │ post_savedata     │ (全停)                         │                       │
      │                    │                   │                                │ scheduler 切 closed   │
16:00 │ closed             │ closed            │ (无)                            │                       │
03:00 │                    │                   │                                │ cleanredis --once ←   │
      │                    │                   │                                │ (调试产物,见 §4)      │
周末 │ closed             │ closed            │ (无,周一重置)                  │                       │
```

> **关键观察**(v6.15 用户最新统一规定):
> - **写入时间窗(普通 8 kind)**:**09:29-11:31 + 13:00-15:01**
> - **落盘时间窗(普通 8 kind)**:**09:30-11:46 + 13:00-15:16**(比写入晚 15 分钟,留出落完尾盘)
> - **auction 写**:09:14-09:26 / 落:09:29 + 15:15 双落
> - **watchlist 写**:09:10 一次性 / 落:09:30-11:46 + 13:00-15:16
> - **orderbook**:🔴 **完全停止抓取**(service 文件保留,scheduler 不再 spawn)
> - **10 阶段 = 8 旧阶段 + 2 新 idle 阶段**(`idle_pre_morning` 09:26-09:29/09:30 + `idle_pre_afternoon` 12:59-13:00)

---

## §1 阶段定义(权威源:`_get_writer_phase` / `_get_savedata_phase`)

### 1.1 写入阶段(**10 个**,v6.15 加 idle_pre_*)

| 阶段 | 时间窗 | 含义 | 持续 |
|---|---|---|---|
| `pre_open` | 09:00-09:10 | 盘前 | 10 min |
| `watchlist` | 09:10-09:14 | 盘前监控列表写入 | 4 min |
| `auction_writer` | 09:14-09:26 | 集合竞价写入(auction 唯一) | 12 min |
| `idle_pre_morning` 🆕 | 09:26-09:29 | **空窗**,等普通 kind 启动 | 3 min |
| `morning_writer` | **09:29-11:31**(v6.15 新窗口) | 早盘连续交易期(8 个普通 kind)| 2 h 2 min |
| `lunch` | **11:31-12:59**(v6.15 新窗口) | 午休(写入全停) | 1 h 28 min |
| `idle_pre_afternoon` 🆕 | 12:59-13:00 | **空窗**,等下午启动 | 1 min |
| `afternoon_writer` | **13:00-15:01**(v6.15 新窗口) | 午盘连续交易期(8 个普通 kind)| 2 h 1 min |
| `writer_stopped` | 15:01-16:00 | 收盘后(写入全停)| 59 min |
| `closed` | 16:00-09:00 | 全停 | 17 h |

### 1.2 落盘阶段(**10 个**,v6.15 加 idle_pre_*)

| 阶段 | 时间窗 | 含义 | 持续 |
|---|---|---|---|
| `pre_open` | 09:00-09:10 | 盘前 | 10 min |
| `watchlist` | 09:10-09:14 | 跟 writer 同步 | 4 min |
| `auction_savedata` | 09:14-09:26 | 等 09:29 触发 savedata_auction --once | 12 min |
| `idle_pre_morning` 🆕 | 09:26-09:30 | **空窗**,等 09:30 落盘启动 | 4 min |
| `morning_savedata` | **09:30-11:46**(v6.15 新窗口) | 早盘落盘(8 个普通 kind + watchlist) | 2 h 16 min |
| `savedata_paused` | **11:46-12:59**(v6.15 新窗口) | 午休落盘暂停 | 1 h 13 min |
| `idle_pre_afternoon` 🆕 | 12:59-13:00 | **空窗**,等 13:00 下午落盘 | 1 min |
| `afternoon_savedata` | **13:00-15:16**(v6.15 新窗口) | 午盘落盘(8 个普通 kind + watchlist) | 2 h 16 min |
| `post_savedata` 🆕 | 15:16-16:00 | 收盘后补落(落盘也停) | 44 min |
| `closed` | 16:00-09:00 | 全停 | 17 h |

### 1.3 写入/落盘时间窗分离(v6.15 关键)

```
写:    09:29 ────────────────── 11:31       lunch  13:00 ─────────────── 15:01   writer_stopped
落:    09:30 ───────────────────────── 11:46       13:00 ──────────────────── 15:16   post_savedata
            ─────── 15min ───────                        ─────── 15min ───────
            ^                                            ^
            普通 kind 落盘比写盘晚 15 分钟                普通 kind 落盘比写盘晚 15 分钟
```

**目的**:落盘要"等到上游没新数据再落",所以两边时间窗错开 15 分钟(从早上 30 min 缩到 15 min,落盘更紧凑)。

---

## §2 Writer Service 全清单(**10 个**,v6.15 含 1 个一次性 + 删 orderbook)

> 周期 = `service_writeredis_<kind>.py` 里 `interval_sec` 默认值。
> **不在 scheduler 阶段里**的 service(auction_writeredis_*)用 `scheduler_onlineData.check_once_triggers` 触发。

### 2.1 writeredis_watchlist(一次性)

| 项 | 值 |
|---|---|
| 启动阶段 | `watchlist`(09:10-09:14) |
| 关闭阶段 | `closed`(16:00+) |
| spawn 模式 | `--once`(daemon 模式 while True sleep,等 SIGTERM) |
| 周期 | **1 次生成**(09:10-09:14 之内调一次 `generate_watchlist()`,写 Redis STREAM + timeline + archive 三件套) |
| 数据源 | `~/TradingAgent/offlineDataManager/data/db_cn_kpl.db`(T+0) |
| 写 Redis key | `online:watchlist:stream`(STREAM,12h TTL)+ `online:watchlist:timeline`(ZSET)+ `online:watchlist:archive:{ts}`(HASH) |
| 备注 | v6.7(2026-09-15)改走 STREAM;read 端(get_watchlist / get_watchlist_sources)签名不变 |

### 2.2 writeredis_auction

| 项 | 值 |
|---|---|
| 启动阶段 | `auction_writer`(09:14-09:26) |
| 关闭阶段 | **`idle_pre_morning`**(09:26)— scheduler 阶段切换时 kill |
| spawn 模式 | daemon(while True + sleep)|
| **周期** | **6 秒/轮** |
| **🔒 内部时间窗** | `is_auction_window()` = **09:15-09:25** 工作日(10 分钟)— 窗口外只 sleep 不拉数据 |
| 数据源 | `fetch_auction_snapshots(stage='live')`(同花顺) |
| 写 Redis key | `online:auction:stream`(STREAM)+ timeline + archive |
| 备注 | **auction writer 在 09:26 就停止拉数据并写 Redis**(`morning_writer` 阶段不挂 auction,v6.15 9:26 切到 `idle_pre_morning` 时 scheduler kill)— savedata 兜底在 09:29 + 15:15 各 `--once` 落盘 |

### 2.3 writeredis_limitperformance

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01)|
| spawn 模式 | daemon |
| **周期** | **30 秒/轮**(v6.15 不变) |
| 数据源 | `KPLClient.fetch_realtime_limit_performance()`(apphwhq 实时盯盘 host,盘中可用) |
| 写 Redis key | `online:limitperformance:stream`(STREAM)+ timeline + archive |
| 备注 | 2026-09-14 v5 新增,**走 MyATM 风格**;v6.15 起不再在 `auction_writer` 阶段启动(只在 morning/afternoon_writer) |

### 2.4 writeredis_snapshot(**数据量最大**)

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01) |
| spawn 模式 | daemon |
| **周期** | **6 秒/轮**(v6.15 不变) |
| **🔒 内部时间窗** | `is_trading_window()` = **09:30-15:00** 工作日 — 窗口外只 sleep 不拉数据 |
| 数据源 | 同花顺 `fetch_snapshots(watchlist)`(85 只 watchlist) |
| 写 Redis key | `online:snapshot:stream`(STREAM,MAXLEN=200000)+ timeline + archive + window HASH |
| 备注 | **数据量最大**(2.5h 累计 ~6.5 万条),savedata 必须用 drain 模式(v6.8)。**双重保险**:scheduler 在 11:31 切 `lunch` 阶段时 kill,daemon 内部 `is_trading_window` 在 11:31-12:59 区间返回 False — 实际有效拉数据 = 09:30-11:31 + 13:00-15:01 |

### 2.5 writeredis_snapshot_index(v6.10 新增,v6.15 调频)

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01) |
| spawn 模式 | daemon |
| **周期** | **10 秒/轮**(v6.15 原 30s) |
| **🔒 内部时间窗** | `is_trading_window()` = **09:30-15:00** 工作日 — 窗口外只 sleep 不拉数据 |
| 数据源 | 同花顺 `fetch_index_snapshot(INDEX_LIST)`(8 只固定指数) |
| 写 Redis key | `online:snapshot_index:stream`(STREAM,MAXLEN=3000)+ timeline + archive + window HASH |
| 备注 | 8 只固定指数(000001.SH/399001.SZ/399006.SZ/000688.SH/000016.SH/000300.SH/000905.SH/000852.SH);v6.10 接入,v6.15 频率 30s→10s |

### 2.6 writeredis_minute

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01) |
| spawn 模式 | daemon |
| **周期** | **30 秒/轮**(v6.15 原 60s) |
| 数据源 | pytdx 分钟 K 线(`TdxClient().get_minute_kline(ts_code)`) |
| 写 Redis key | `online:minute:<ts_code>:bars`(ZSET,每只股 1 个) |
| 备注 | **唯一走 ZSET 不走 STREAM 的 kind**(每只股独立 ZSET,2026-09-13 v4 改) |

### 2.7 writeredis_zt

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01) |
| spawn 模式 | daemon |
| **周期** | **60 秒/轮**(v6.15 原 30s) |
| 数据源 | `fetch_limitup_pool()`(同花顺) |
| 写 Redis key | `online:zt:stream`(STREAM)+ timeline + archive |

### 2.8 writeredis_break

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01) |
| spawn 模式 | daemon |
| **周期** | **60 秒/轮**(v6.15 不变) |
| 数据源 | `fetch_limitbreak_pool()`(同花顺) |
| 写 Redis key | `online:break:stream`(STREAM)+ timeline + archive |

### 2.9 writeredis_anomaly

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01) |
| spawn 模式 | daemon |
| **周期** | **120 秒/轮**(v6.15 不变) |
| 数据源 | `fetch_anomaly_list()`(同花顺) |
| 写 Redis key | `online:anomaly:stream`(STREAM)+ timeline + archive |
| 备注 | 每个 archive HASH 1200s 各自过期(用户拍板 20 分钟) |

### 2.10 writeredis_hot

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_writer`(09:29)+ `afternoon_writer`(13:00) |
| 关闭阶段 | `lunch`(11:31)+ `writer_stopped`(15:01) |
| spawn 模式 | daemon |
| **周期** | **120 秒/轮**(v6.15 原 300s) |
| 数据源 | `fetch_hot_stock(period='hour')`(同花顺) |
| 写 Redis key | `online:hot:stream`(STREAM)+ timeline + archive |

### 🔴 2.11 writeredis_orderbook(**v6.15 停用,service 文件保留**)

| 项 | 值 |
|---|---|
| scheduler spawn | **🔴 不再 spawn**(v6.15 起)|
| 启动阶段 | — |
| 关闭阶段 | — |
| 数据源 | pytdx 5 档盘口(2026-09-16 测试 pytdx 1.72 整体失效,0 数据返回) |
| 备注 | **service 文件保留作备份**;scheduler `WRITER_DAEMONS_MORNING_AFTERNOON` 已删除此 entry;未来若换 tdx 客户端可恢复 |

---

## §3 Savedata Service 全清单(**9 个**,v6.15 删 orderbook + 加 snapshot_index)

> 周期 = `service_savedata_<kind>.py` 里 `DEFAULT_INTERVAL`。
> 阶段挂载:看 `SAVEDATA_PHASE_SERVICES`(`auction_savedata` / `morning_savedata` / `afternoon_savedata` / `post_savedata`)
> **2026-09-15 v6.8 新增 `DEFAULT_DRAIN`**:`snapshot` 子类默认 `True`(治本模式),其余子类默认 `False`。

### 3.1 savedata_watchlist

| 项 | 值 |
|---|---|
| 启动阶段 | `watchlist`(09:10)+ `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| **DEFAULT_INTERVAL** | **1800 秒 = 30 分钟**(实际跑 15 min 一次跟随 morning/afternoon_savedata 阶段切换) |
| DEFAULT_DRAIN | False |
| 落盘表 | `watchlist_<YYYYMMDD>`(PK UNIQUE(ts_code, watchlist_timestamp)) |
| 备注 | v6.7 新增;read 端 `get_watchlist` 兼容老接口 |

### 3.2 savedata_auction(**唯一 --once 触发**)

| 项 | 值 |
|---|---|
| 启动方式 | **不是 daemon**,scheduler 触发 `--once` |
| **trigger 时间点** | **09:29 + 15:15**(ONCE_TRIGGERS,v6.15 原 15:10) |
| DEFAULT_INTERVAL | 99999(几乎用不到) |
| 落盘表 | `auction_<YYYYMMDD>` |
| 备注 | 集合竞价只在 09:14-09:26 有数据;09:29 立刻落一次 + 15:15 兜底补落;两次 `--once` 比 daemon 更准 |

### 3.3 savedata_snapshot(**数据量最大**)

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| **DEFAULT_INTERVAL** | **900 秒 = 15 分钟** |
| **DEFAULT_DRAIN** | **True**(2026-09-15 v6.8 新增,治本) |
| 落盘表 | `snapshot_<YYYYMMDD>` |
| 数据速率 | ~378 条/分(开盘 2.5h 累计 ~65k 条) |
| 备注 | **15 min 1 轮不够快**,所以加 drain 模式:`while True` 连续读完所有积压直到 batch 不满 |

### 3.4 savedata_snapshot_index(v6.10 新增,v6.15 上线)

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| **DEFAULT_INTERVAL** | **900 秒 = 15 分钟** |
| DEFAULT_DRAIN | False |
| 落盘表 | `snapshot_index_<YYYYMMDD>` |
| 备注 | 8 只固定指数;v6.10 接入代码,v6.15 正式纳入 scheduler 落盘阶段 |

### 3.5 savedata_minute

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| DEFAULT_INTERVAL | **900 秒 = 15 分钟** |
| DEFAULT_DRAIN | False(ZSET 类不需要 drain) |
| 落盘表 | `minute_<YYYYMMDD>` |

### 3.6 savedata_zt

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| DEFAULT_INTERVAL | **1800 秒 = 30 分钟**(实际跑 15 min 一次跟随阶段) |
| DEFAULT_DRAIN | False |
| 落盘表 | `zt_<YYYYMMDD>` |

### 3.7 savedata_break

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| DEFAULT_INTERVAL | **1800 秒 = 30 分钟** |
| DEFAULT_DRAIN | False |
| 落盘表 | `break_<YYYYMMDD>` |

### 3.8 savedata_anomaly

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| DEFAULT_INTERVAL | **1800 秒 = 30 分钟** |
| DEFAULT_DRAIN | False |
| 落盘表 | `anomaly_<YYYYMMDD>` |

### 3.9 savedata_hot

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| DEFAULT_INTERVAL | **1800 秒 = 30 分钟** |
| DEFAULT_DRAIN | False |
| 落盘表 | `hot_<YYYYMMDD>` |

### 3.10 savedata_limitperformance

| 项 | 值 |
|---|---|
| 启动阶段 | `morning_savedata`(09:30)+ `afternoon_savedata`(13:00) |
| 关闭阶段 | `savedata_paused`(11:46)+ `post_savedata`(15:16) |
| DEFAULT_INTERVAL | **900 秒 = 15 分钟** |
| DEFAULT_DRAIN | False |
| 落盘表 | `limit_performance_<YYYYMMDD>`(PK UNIQUE(ts_code, limitperformance_timestamp)) |
| 备注 | v6.15 起不再在 `auction_savedata` 阶段挂(v6.14 还挂着,v6.15 下放回 morning/afternoon_savedata)|

### 🔴 3.11 savedata_orderbook(**v6.15 停用,service 文件保留**)

| 项 | 值 |
|---|---|
| scheduler spawn | **🔴 不再 spawn**(v6.15 起)|
| 启动阶段 | — |
| 关闭阶段 | — |
| 落盘表 | `orderbook_<YYYYMMDD>`(已停用) |
| 备注 | **service 文件保留作备份**;`SAVEDATA_DAEMONS_MORNING_AFTERNOON` 已删除此 entry |

---

## §4 一次性 Trigger(`ONCE_TRIGGERS`)

> 格式:`(hour, minute, service_name, args)`
> 触发方式:scheduler 主循环每分钟扫一次,命中 → spawn `--once`,今天不再触发(去重 set)

| 时间 | service | args | 用途 |
|---|---|---|---|
| **09:00** | `service_cleanredis_online` | `[]` | 盘前清空 Redis `online:*` 前缀的所有 key + 清 `online_stream_cursor` 表(STREAM ID 失效后从 0 重读) |
| **09:29** | `service_savedata_auction` | `[]` | 集合竞价落盘(09:14-09:26 数据在 09:29 立刻稳定) |
| **15:15** | `service_savedata_auction` | `[]` | **v6.15 兜底补落**(原 15:10;用户原话"auction 落盘在 15 点 15 分补落一次") |
| **03:00** | `service_cleanredis_online` | `[]` | ✅ **生产真值兜底清残留**(2026-09-16 用户拍板:默认 3 点,不再回 16:00)|

**清 Redis 不清 SQLite**:`clean_redis_online` 只删 `online:*` 键,**不删 9 个 kind 的历史表**(数据要保留)。

---

## §5 汇总速查表 — 一页纸

### 5.1 Writer 拉取周期(**10 个**,v6.15 删 orderbook + 加 snapshot_index 10s)

| service | 周期 | 启动阶段 | 内部时间窗(`is_*_window`) | 数据源 |
|---|---|---|---|---|
| `writeredis_watchlist` | 一次性 | `watchlist` | — | db_cn_kpl.db |
| `writeredis_auction` | **6 秒** | `auction_writer` | **09:15-09:25** | fetch_auction_snapshots |
| `writeredis_snapshot` | **6 秒** | morning + afternoon | **09:30-15:00** | fetch_snapshots(同花顺) |
| `writeredis_snapshot_index` 🆕 | **10 秒**(v6.15 原 30s) | morning + afternoon | **09:30-15:00** | fetch_index_snapshot(8 只指数) |
| `writeredis_minute` | **30 秒**(v6.15 原 60s) | morning + afternoon | (无内部窗口) | pytdx 分钟 K |
| `writeredis_zt` | **60 秒**(v6.15 原 30s) | morning + afternoon | (无内部窗口) | fetch_limitup_pool |
| `writeredis_break` | **60 秒** | morning + afternoon | (无内部窗口) | fetch_limitbreak_pool |
| `writeredis_anomaly` | **120 秒** | morning + afternoon | (无内部窗口) | fetch_anomaly_list |
| `writeredis_hot` | **120 秒**(v6.15 原 300s) | morning + afternoon | (无内部窗口) | fetch_hot_stock |
| `writeredis_limitperformance` | **30 秒** | morning + afternoon | (无内部窗口) | KPLClient.fetch_realtime_limit_performance |
| ~~`writeredis_orderbook`~~ | 🔴 **停用** | — | — | — |

### 5.2 Savedata 落盘周期(**9 个**,v6.15 删 orderbook + 加 snapshot_index)

| service | DEFAULT_INTERVAL | DEFAULT_DRAIN | 触发方式 |
|---|---|---|---|
| `savedata_watchlist` | 1800 秒 (30 min) | False | daemon(跟随 morning/afternoon_savedata) |
| `savedata_auction` | 99999(用不到) | False | **ONCE_TRIGGERS 09:29 + 15:15** |
| `savedata_snapshot` | **900 秒 (15 min)** | **True**(v6.8 治本) | daemon |
| `savedata_snapshot_index` 🆕 | 900 秒 (15 min) | False | daemon(跟随 morning/afternoon_savedata) |
| `savedata_minute` | 900 秒 (15 min) | False | daemon |
| `savedata_zt` | 1800 秒 (30 min) | False | daemon |
| `savedata_break` | 1800 秒 (30 min) | False | daemon |
| `savedata_anomaly` | 1800 秒 (30 min) | False | daemon |
| `savedata_hot` | 1800 秒 (30 min) | False | daemon |
| `savedata_limitperformance` | 900 秒 (15 min) | False | daemon |
| ~~`savedata_orderbook`~~ | 🔴 **停用** | — | — |

### 5.3 一次性 Trigger(2 个 service × 多个时间点)

| service | 触发点 | 用途 |
|---|---|---|
| `service_cleanredis_online` | 09:00, 03:00(生产真值,2026-09-16 用户拍板默认 3 点) | 清 Redis `online:*` |
| `service_savedata_auction` | 09:29, 15:15 | 集合竞价落盘 |

### 5.4 Check Service(诊断用,2 个)

> check service 不在阶段挂载里,**平时不跑**,只在手动 `--once` 时用。

| service | 周期 | 启动方式 |
|---|---|---|
| `service_check_redis` | 手动 | `--once`(默认 kind=all,看 10 个 STREAM + window + timeline + archive) |
| `service_check_db` | 手动 | `--once`(默认 kind=all,看 SQLite 9 个 kind 表的 max_timestamp + count) |

---

## §6 关键不变量(踩过的坑,v6.15 校准)

| 不变量 | 原因 |
|---|---|
| **写入停 → 落盘可以继续**(15 min 错开) | v6.15 落盘比写盘晚 15 分钟(原 25-30 min 缩到 15 min);落盘要"等到上游没新数据再落" |
| **🔒 双重保险(关键)** | writer 内部 `is_*_window()`(auction 09:15-09:25 / snapshot 09:30-15:00)即使 scheduler 阶段切换卡得不及时,daemon 也兜底不会在午休/收盘后拉数据;**savedata 无内部窗口**,只看游标 |
| **savedata_auction 走 --once 不走 daemon** | 集合竞价只有 09:14-09:26 数据,daemon 大部分时间空转,两次 `--once` 更准 |
| **auction writer 9:26 就停** | `idle_pre_morning` 阶段 services 不含 auction;9:26 scheduler 切阶段时 kill;daemon 内部 9:25 也停 — 实际停机时间 = **min(scheduler kill, is_auction_window 退出)** = 9:26 |
| **snapshot writer 11:31-12:59 停** | scheduler `lunch` 阶段 11:31 kill;daemon 内部 `is_trading_window` 11:31 起返回 False — 实际停机时间 = **min(scheduler kill, is_trading_window 退出)** = 11:31 |
| **snapshot 必须开 drain** | 15min/轮 × 1000 条/batch 跟不上 ~378 条/分的速率(v6.8 治本) |
| **watchlist writer 一次写 3 件套** | STREAM(落盘用)+ timeline(ZSET 查询)+ archive(HASH 历史)三件套对齐 auction/snapshot |
| **clean redis 不清 sqlite** | SQLite 是历史数据,Redis 是临时缓存,职责分离 |
| **15:01 后写入停,落盘继续到 15:16** | 给 savedata 15 min 落完尾盘;15:16→post_savedata→closed |
| **orderbook 完全停止** | v6.15 起 service 文件保留但 scheduler 不 spawn;`WRITER_DAEMONS_MORNING_AFTERNOON` 和 `SAVEDATA_DAEMONS_MORNING_AFTERNOON` 都已删除此 entry |
| **8 普通 kind 共用 morning/afternoon 阶段** | 8 个普通 kind(snapshot / snapshot_index / minute / limitperformance / zt / break / hot / anomaly)走同一套 morning_writer/afternoon_writer 阶段,**snapshot_index 内部 `is_trading_window()` 精准控制** |

---

## §7 排障指引

### 7.1 "现在 X 在跑吗?"

```bash
# 1. 看 scheduler 阶段
tail -20 ~/TradingAgent/onlineDataManager/logs/scheduler.launchd.log | grep "阶段变化"

# 2. 看实际进程
ps aux | grep -E "service_writeredis|service_savedata|service_cleanredis" | grep -v grep

# 3. 看 service 自己的 log
ls logs/service_*.log | tail -5
tail -30 logs/service_<kind>_$(date +%Y%m%d).log
```

### 7.2 "为什么 X 没在跑?"

| 现象 | 原因 | 解决 |
|---|---|---|
| morning_writer 阶段却没 writer 进程 | scheduler 没启动 / 已崩 | `launchctl list \| grep tradingagent`,看 PID |
| lunch 阶段有 writer 进程残留 | scheduler 没 kill 干净 | v6.15 dual-phase 自动 kill,旧版有残留 |
| afternoon_writer 阶段 savedata 没起 | `savedata_paused` 没切换到 `afternoon_savedata` | 看 scheduler 主循环的阶段变化日志 |
| `auction_savedata` 阶段没 daemon | 这是设计:等 09:29 ONCE_TRIGGER | 正常 |
| `idle_pre_*` 阶段没 daemon | 这是设计:空窗过渡,什么都不跑 | 正常 |
| `clean_redis` 跑了但 STREAM 还有数据 | 清完 STREAM 又有上游推 | 正常(清完到重启之间有空窗)|

### 7.3 "为什么 X 在跑(不应该)?"

| 现象 | 原因 |
|---|---|
| 11:31-12:59 还在写 STREAM | **writer 停了,但上游同花顺 API 还在推**(午休 88 min STREAM 累计 ~30k 条,drain 模式 1 次追平) |
| savedata 在 lunch 阶段还跑 | 11:31-11:46 是 `morning_savedata` 还在跑(设计:晚 15 min 停),11:46 才停 |
| savedata 在 15:01-15:16 还跑 | `afternoon_savedata` 写到 15:16(晚 15 min 停) |
| cleanredis 在 09:00 跑后又跑了一次 | trigger 去重 set 跨天 reset 没生效?看 scheduler 主循环日志 |

### 7.4 "auction 9:26 真的停了吗?snapshot 11:31-12:59 真的停了吗?"

**验证方法**(2026-09-16 v6.15):

```bash
# 1. 验证 auction — 9:26 后是否还有 writer 进程
ps aux | grep service_writeredis_auction | grep -v grep
# 期望:9:26 后无输出(scheduler 已 kill)

# 2. 验证 snapshot — 11:31-12:59 期间 daemon 内部不拉数据
#    看 daemon 自己的日志(每 6s 一行)
tail -100 logs/service_snapshot_$(date +%Y%m%d).log | grep "不在连续竞价窗口"
# 期望:11:31-12:59 期间持续打印"不在连续竞价窗口"

# 3. 看 STREAM 最新 snapshot_timestamp 字段
redis-cli XREVRANGE online:snapshot:stream + - COUNT 5 | grep snapshot_timestamp
# 期望:午餐时间(< 11:31 或 11:31-12:59)的 id 里 snapshot_timestamp 应该是 11:31 前的
```

**结论**:**双重保险**—
- auction:scheduler 9:26 kill + daemon 内部 9:25 is_auction_window 退出 = **9:26 停**
- snapshot:scheduler 11:31 kill + daemon 内部 11:31 is_trading_window 退出 = **11:31 停**

**两者实际停机时间**都等于 scheduler kill 时间(因为 scheduler kill 与 daemon 内部窗口基本同步)。

---

## §8 引用

- **架构**:`01_架构文档.md` §2.3 / §2.4(阶段表) / §4.1(周期表) / §4.3(cleanredis 时点)
- **代码细节**:`代码详细设计/scheduler_onlineData.md`(L 60-150 阶段定义 + L 180-230 PHASE_SERVICES + L 296-301 ONCE_TRIGGERS)
- **代码细节**:`代码详细设计/service_writeredis_*.md`(各 kind 拉取周期)
- **代码细节**:`代码详细设计/service_savedata_*.md`(各 kind 落盘周期)
- **数据**:`03_数据详细设计.md` §3(STREAM TTL / MAXLEN 表)
- **新实时监控**:`05_新增实时监控落盘数据指南.md`(v6.7 watchlist 三件套 + v6.8 drain 模式 + v6.10 snapshot_index 实例 + v6.15 阶段对齐)

---

> **写文档时间**:2026-09-16 14:45+
> **写文档人**:AI 助手(基于 `scheduler_onlineData.py` 全部 643 行真值 + 9 个 `service_writeredis_*.py` head interval_sec + 8 个 `service_savedata_*.py` DEFAULT_INTERVAL 真值 + 4 个 `ONCE_TRIGGERS` 真值 + 10 个 `WRITER_DAEMONS_MORNING_AFTERNOON` / 9 个 `SAVEDATA_DAEMONS_MORNING_AFTERNOON` 列表真值)
> **校准对象**:v6.15 重构 — 阶段数 8→10 / 时间窗统一 09:29-11:31+13:00-15:01 写 / 09:30-11:46+13:00-15:16 落 / 删 orderbook / 加 snapshot_index / 6 个 service 频率调整

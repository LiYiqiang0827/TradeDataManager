# coreClient 架构总览

> **版本**:v1.0(2026-09-15 新建)
> **目的**:coreClient/ 目录下的文档索引 + 各 client 作用一览 + 跨业务复用说明

---

## 1. 什么是 coreClient?

`~/TradingAgent/coreClient/` 是整个 TradingAgent 项目的**客户端基础层目录**,2026-09-12 架构调整后,所有跨业务复用的客户端都集中在这里:

- **统一配置管理**:每个 client 一个 `*_config.py`,token / 限流 / 重试 / hosts 都集中管理
- **统一 import 路径**:`from coreClient.xxx_client import ...`(当 `PYTHONPATH=~/TradingAgent/` 时)
- **跨业务共享**:onlineDataManager / offlineDataManager / monitor / quant-strategy 都可以用

---

## 2. coreClient/ 目录结构

```
~/TradingAgent/coreClient/
├── redis_client.py      # Redis 基础层 + STREAM/ZSET/SET/LIST/HASH 通用模板
├── redis_config.py      # Redis 连接参数 + 通用配置
│
├── tdx_client.py        # 通达信 pytdx 客户端(5档盘口 + 1分钟K + 历史ticks + 指数)
├── tdx_config.py        # pytdx IP 池 + 批量限制 + 重试
│
├── ths_client.py        # 同花顺 hithink-finance CLI 封装(9 个 fetch 函数)
├── ths_config.py        # CLI 命令名 + 批量大小 + 重试
│
├── kpl_client.py        # 开盘啦 HTTP API 客户端(单例 + 限流 + IP fallback)
├── kpl_config.py        # auth_token + hosts + 限流 + 重试
│
├── tushare_client.py    # Tushare pro API 客户端(单例 + 21 个便捷方法)
├── tushare_config.py    # TUSHARE_TOKEN + 限流 + 重试
│
├── data_provider.py     # 统一数据访问层:20 个 get_xxx,自动 database/online 路由(2026-09-17 新增)
│
├── test/                # 测试代码(目前只有 kpl_live_demo.py)
│   └── kpl_live_demo.py
│
└── docs/                # ← 软链接到 ~/LLM Wiki/TradingAgent/coreClient/
```

---

## 3. 文档索引

| 文档 | 路径 | 作用 | 阅读优先级 |
|---|---|---|---|
| **redis_client.md** | `~/LLM Wiki/TradingAgent/coreClient/redis_client.md` | Redis 基础层 + 通用 STREAM/ZSET/SET/LIST/HASH 模板 | 🔴 高(几乎所有业务都用) |
| **tdx_client.md** | `~/LLM Wiki/TradingAgent/coreClient/tdx_client.md` | 通达信 pytdx 客户端(5档盘口 + 1分钟 K + 历史 ticks + 涨停判断) | 🔴 高(onlineDataManager 核心数据源) |
| **ths_client.md** | `~/LLM Wiki/TradingAgent/coreClient/ths_client.md` | 同花顺 hithink-finance CLI 封装(9 个 fetch 函数) | 🔴 高(onlineDataManager 核心数据源) |
| **kpl_client.md** | `~/LLM Wiki/TradingAgent/coreClient/kpl_client.md` | 开盘啦 HTTP API 客户端(单例 + 限流 + IP fallback + 23 元素数组映射) | 🟡 中(onlineDataManager + offlineDataManager 题材轮动) |
| **tushare_client.md** | `~/LLM Wiki/TradingAgent/coreClient/tushare_client.md` | Tushare pro API 客户端(单例 + 21 个便捷方法) | 🟡 中(offlineDataManager 主数据源 + onlineDataManager watchlist) |
| **data_provider.md** | `~/LLM Wiki/TradingAgent/coreClient/data_provider.md` | **统一数据访问层:30 个 get_xxx(database / online 双模式,policyStudy 的唯一数据入口)** | 🔴 高(所有上层研究 Agent 都必须看) |

---

## 4. 各 client 速查表

### 4.1 核心特性对比

| Client | 类/模块 | 实例模式 | 主要用途 | 限流 | 重试 |
|---|---|---|---|---|---|
| **redis** | `RedisBase` | 普通类(继承 PREFIX) | Redis 基础 + STREAM/ZSET/SET/LIST/HASH 模板 | N/A | 应用层自管 |
| **tdx** | `TdxClient` | 普通类(单连接) | 5档盘口 + 1分钟K + 历史ticks + 涨停判断 | 无(连接级) | ✅ IP failover |
| **ths** | 顶层函数(无类) | N/A(函数调用) | 9 个 fetch 函数(快照/集合竞价/涨停池/异动等) | batch=80 | ✅ CLI 退避 |
| **kpl** | `KPLClient` | **单例** | 涨跌统计 + 涨停表现 + 龙虎榜 + 涨停基因 | 30/min | ✅ 指数退避 + IP fallback |
| **tushare** | `TushareClient` | **单例** | 21 个便捷方法(日K/复权/龙虎榜/融资融券/筹码等) | 200/min | ✅ 指数退避 |

### 4.2 数据源覆盖矩阵

| 数据 | Client | 端点 / 方法 | 时间戳字段 |
|---|---|---|---|
| **股票基础信息** | tushare | `stock_basic` | — |
| **交易日历** | tushare | `trade_cal` | — |
| **涨跌停价** | tushare | `stk_limit` | — |
| **日 K(不复权)** | tushare | `daily` | — |
| **日 K(前复权)** | tushare | `daily_qfq_range(qfq=True)` | — |
| **复权因子** | tushare | `adj_factor` | — |
| **龙虎榜每日** | tushare | `top_list` | — |
| **龙虎榜机构** | tushare | `top_inst` | — |
| **大宗交易** | tushare | `block_trade` | — |
| **港股通** | tushare | `ggt_daily` | — |
| **沪深股通 TOP10** | tushare | `hsgt_top10` | — |
| **资金流向** | tushare | `moneyflow` | — |
| **融资融券汇总** | tushare | `margin` | — |
| **融资融券明细** | tushare | `margin_detail` | — |
| **每日指标** | tushare | `daily_basic` | — |
| **筹码胜率** | tushare | `cyq_perf` | — |
| **涨跌停列表** | tushare | `limit_list_d` | — |
| **题材成分** | tushare | `kpl_concept_cons` | — |
| **题材排行** | tushare | `kpl_concept` | — |
| **集合竞价快照** | ths | `fetch_auction_snapshots` | `auction_timestamp` |
| **连续竞价快照** | ths | `fetch_snapshots` | `snapshot_timestamp` |
| **异动清单** | ths | `fetch_anomaly_list` | `anomaly_timestamp` |
| **涨停池** | ths | `fetch_limitup_pool` | `zt_timestamp` |
| **跌停池** | ths | `fetch_limitdown_pool` | `snapshot_timestamp` |
| **炸板池** | ths | `fetch_limitbreak_pool` | `break_timestamp` |
| **指数快照** | ths | `fetch_index_snapshot` | `snapshot_timestamp` |
| **飙升榜** | ths | `fetch_skyrocket` | `snapshot_timestamp` |
| **热股榜** | ths | `fetch_hot_stock` | `hot_timestamp` |
| **5档盘口** | tdx | `get_orderbook` / `get_orderbook_batched` | `orderbook_timestamp` |
| **指数 5档** | tdx | `get_index_orderbook` | `orderbook_timestamp` |
| **实时 1分钟 K** | tdx | `get_minute_kline` | `data_timestamp` |
| **历史 1分钟 K** | tdx | `get_history_minute` | — |
| **历史 ticks** | tdx | `get_history_ticks` | — |
| **涨跌统计** | kpl | `market_sentiment` | — |
| **涨停天梯** | kpl | `limit_ladder` | — |
| **涨停板数** | kpl | `daily_limit_index` | — |
| **涨停表现详情** | kpl | `limit_up_performance(date, board_type)` | — |
| **龙虎榜股票** | kpl | `lhb_stock_list(date)` | — |
| **个股分时** | kpl | `stock_trend(stock_id, day)` | — |
| **涨停基因** | kpl | `zt_gene(stock_id)` | — |
| **股票代码后缀补全** | kpl | `add_exchange_suffix` | — |

---

## 5. 跨业务复用关系

```
                                ┌──────────────────┐
                                │   onlineDataMgr  │
                                │  (实时 5 kind +  │
                                │   10 kind 落盘)  │
                                └─────────┬────────┘
                                          │
                       ┌──────────────────┼──────────────────┐
                       │                  │                  │
                       ▼                  ▼                  ▼
              ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
              │ redis_client │   │  ths_client  │   │  tdx_client  │
              │ (RedisBase)  │   │ (9 fetch)    │   │ (5档/1分K)   │
              └──────────────┘   └──────────────┘   └──────────────┘
                                          │
                                          ▼
                                  ┌──────────────┐
                                  │  kpl_client  │
                                  │ (单例/IP fb) │
                                  └──────────────┘

                                ┌──────────────────┐
                                │   offlineData   │
                                │  (离线 db 初始化  │
                                │   + 每日增量)    │
                                └─────────┬────────┘
                                          │
                       ┌──────────────────┼──────────────────┐
                       │                  │                  │
                       ▼                  ▼                  ▼
              ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
              │ tushare_     │   │  kpl_client  │   │  ths_client  │
              │ client       │   │ (历史涨停)    │   │ (历史快照)   │
              │ (21 接口)    │   └──────────────┘   └──────────────┘
              └──────────────┘

                                ┌──────────────────┐
                                │     monitor     │
                                │  (实时监控 +     │
                                │   预警)         │
                                └─────────┬────────┘
                                          │
                                          ▼
                                  ┌──────────────┐
                                  │ redis_client │
                                  │ (PREFIX=     │
                                  │  "monitor")  │
                                  └──────────────┘
```

---

## 6. 命名约定

### 6.1 字段命名

| 字段 | 类型 | 说明 |
|---|---|---|
| `ts_code` | str | 股票代码(`"000001.SZ"` 形式),**统一用 ts_code**(不用 thscode / code) |
| `trade_date` | str | `YYYY-MM-DD` |
| `data_timestamp` | str | 数据时间戳 ISO 格式(毫秒精度,2026-09-15 起) |
| `snapshot_timestamp` / `auction_timestamp` / `orderbook_timestamp` / `zt_timestamp` 等 | float \| str | 各 kind 自己的数据时间戳(秒或毫秒) |
| `created_at` | str | 落盘时间戳(毫秒精度,SQLite `strftime('%f',...)` 默认) |

### 6.2 配置命名

- 连接参数:`DEFAULT_HOST` / `DEFAULT_PORT` / `DEFAULT_DB`
- 限流:`*_RATE_LIMIT_PER_MIN`(每分钟最大调用次数)
- 重试:`*_RETRY_MAX` / `*_RETRY_SLEEP`(退避基础)
- hosts:`*_HOST_*`(多 host 时用 host_short 命名)
- 批量:`*_BATCH_MAX` / `*_CHUNK_SIZE_DEFAULT` / `*_PAGE_SIZE`

### 6.3 类命名

- **基础类**:`RedisBase`(业务继承,设 PREFIX)
- **客户端类**:`TdxClient` / `KPLClient` / `TushareClient`
- **辅助类**:`RateLimiter`(滑动窗口限流)
- **辅助函数(模块级)**:`market_of` / `code_of` / `_date_int_to_str` / `_minute_index_to_datetime` / `add_exchange_suffix`

---

## 7. AI 上手指南

### 7.1 5 分钟快速上手

**Step 1:看 redis_client.md**(10 分钟)
理解 `RedisBase` 提供的通用模板(STREAM / ZSET / SET / LIST / HASH),以及业务如何继承。

**Step 2:看 ths_client.md + tdx_client.md**(20 分钟)
理解两个核心数据源的接口形态(ths 是顶层函数,tdx 是 `TdxClient` 类)。

**Step 3:看 kpl_client.md**(15 分钟)
理解单例模式 + IP fallback + 限流,以及 23 元素数组的特殊解析。

**Step 4:看 tushare_client.md**(15 分钟)
理解 Tushare 单例 + 21 个便捷方法。

**Step 5:看实际 service**(30 分钟)
读 `~/TradingAgent/onlineDataManager/scripts/service/service_writeredis_*.py`,看业务如何组合 client。

### 7.2 常见任务速查

| 任务 | 用哪个 client | 哪个方法 |
|---|---|---|
| 拉某只股票的 5档盘口 | tdx | `get_orderbook([(market_of(code), code_of(code))])` |
| 拉全 watchlist 的连续竞价快照 | ths | `fetch_snapshots(watchlist)` |
| 拉今日涨停池 | ths | `fetch_limitup_pool()` |
| 拉某日某板的涨停表现详情 | kpl | `limit_up_performance(date, board_type=1)` |
| 拉某股的涨停基因 | kpl | `zt_gene(stock_id)` |
| 补全股票代码后缀 | kpl | `add_exchange_suffix("000978")` → `"000978.SZ"` |
| 拉某日龙虎榜 | tushare | `top_list(trade_date='...')` |
| 拉全历史日 K(前复权) | tushare | `daily_qfq_range(start_date, end_date, qfq=True)` |
| 写 Redis STREAM | redis | `_xadd_to_stream(pipe, stream_key, fields, maxlen=...)` |
| 读 Redis ZSET 窗口 | redis | `_read_window(window_key, score_min=...)` |
| **统一数据访问(db / online 自动路由)** | **data_provider** | **`from coreClient.data_provider import get_day, get_basic, ...`** |
| **某只股票某天分钟 K**(db 或 online) | **data_provider** | **`get_minute("000006.SZ", "2026-09-10")`** |
| **某天涨停榜**(db 或 online) | **data_provider** | **`get_kpl_list(trade_date="2026-09-17", tags="涨停")`** |
| **今日涨停详情盘中快照**(online 自动走实时接口) | **data_provider** | **`get_kpl_limit_performance(source="online")`** |

---

## 8. 版本历史

| 版本 | 日期 | 变更 |
|---|---|---|
| v1.0 | 2026-09-15 | 新建 coreClient/ 详细文档体系(redis_client + tdx_client + ths_client + kpl_client + tushare_client + doc_index) |
| v1.1 | 2026-09-18 | 新增 `data_provider.py` + `data_provider.md`(统一数据访问层,policyStudy 上层研究 Agent 入口) |
| v1.2 | 2026-09-18 | `data_provider.md` 更新到 v1.1:18 → 30 个接口(新增 12 个扩展接口 + 章节 3.1/3.2/4.18~4.29) |

---

## 9. 相关文档

| 文档 | 路径 | 关系 |
|---|---|---|
| onlineDataManager 架构 | `~/LLM Wiki/TradingAgent/onlineDataManager/01_架构文档.md` | coreClient 的主要消费者 |
| onlineDataManager 数据设计 | `~/LLM Wiki/TradingAgent/onlineDataManager/03_数据详细设计.md` | coreClient 数据的落盘 schema |
| offlineDataManager 数据设计 | `~/LLM Wiki/TradingAgent/offlineDataManager/...` | coreClient 的离线数据消费者 |
| trading:kpl-api skill | (Hermes skill) | kpl_client API 反向工程参考 |

---

## 10. 验证清单

- [x] 5 个 client + 5 份详细设计 md 创建完成(2026-09-15)
- [x] 1 份架构总览 doc_index.md 创建完成
- [x] 软链 `~/TradingAgent/coreClient/docs/` → `~/LLM Wiki/TradingAgent/coreClient/` 创建成功
- [x] 文档索引 + 跨业务复用关系图 + 命名约定 + AI 上手指南完整
- [x] 所有 client 的 method / config / 使用方文档化
- [x] 1 份 data_provider.md 详细文档创建完成(2026-09-18,30 个 get_xxx 接口 + db/online 模式对比 + 返回值字段表)
- [x] doc_index.md 同步更新(v1.2)
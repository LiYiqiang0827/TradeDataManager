# onlineDataManager 文档 AI Agent 上手评估 — 第 3 轮

> **评估人**:全新 AI agent(零代码上下文,仅读 docs)
> **目标**:验证 02 v1.3 修复后文档对 AI 上手的友好度
> **上轮**:8/10(参见 `AI_Agent_评估报告.md` 第 2 轮部分)
> **本轮**:见末尾「总分」

---

## 阅读路径(我实际走的顺序)

按 README §"阅读顺序" 走:04 → 01 → 02 → `代码详细设计/`(按需)→ 03。
具体读了的文档:`README.md`、`04_QuickStart.md`、`01_架构文档.md`、`02_代码详细设计.md`、`03_数据详细设计.md`、`02_代码详细设计_CHANGELOG.md`、`04_QuickStart_CHANGELOG.md`、`01_架构文档_CHANGELOG.md`、`03_数据详细设计_CHANGELOG.md`,以及 `代码详细设计/` 下的:`core_redis_online.md`、`core_persist_client.md`、`core_sqlite_client.md`、`core_watchlist_fetch.md`、`core_check_query_tools.md`、`service_savedata_loop.md`、`service_savedata_snapshot.md`、`service_savedata_zt.md`、`service_savedata_minute.md`、`service_savedata_watchlist.md`、`service_writeredis_snapshot.md`、`service_writeredis_zt.md`、`service_writeredis_auction.md`、`scheduler_onlineData.md`、`scheduler_once.md`。

---

## Q1. 模块定位

**回答**:onlineDataManager 是 A 股**实时行情采集与落盘**模块,负责在交易时段把同花顺 / pytdx 的实时数据(快照 / 盘口 / 分时 / 涨停池 / 炸板池 / 异动 / 热股榜 / 涨停表现 / 自选股)拉到 Redis 缓冲区,再批量落到月度 SQLite 库,供下游 `policyStudy` 做策略回测。

**引用**:
- `01_架构文档.md §4.4` 与下游模块关系:`offline + online → policyStudy`,实时跑
- `01_架构文档.md §3.1` SQLite 月度库 + `§3.2` Redis 在线缓存
- `04_QuickStart.md §1.1` AI 怎么读这套文档

---

## Q2. 数据完整链路(snapshot kind,7 步)

**回答**:

1. **scheduler 启动 writer**:`scheduler_onlineData.py` 在 `continuous_open`(09:30-11:30)/ `continuous_resume`(13:00-15:00)阶段 `subprocess.Popen(["python3", "-m", "service.service_writeredis_snapshot"])`。`代码详细设计/scheduler_onlineData.md §2` + `02_代码详细设计.md §6.1`
2. **拉数据源**:service 进程 `OnlineRedis()` + `TdxClient()`(同花顺),调 `client.fetch_snapshot_quotes(codes=...)`。watchlist 来自 `r.get_watchlist()`(在线 SET)。`service_writeredis_snapshot.md §5` 调用链
3. **写 Redis(三件套)**:
   - `r.put_snapshots_batch(items)` → pipeline 写 STREAM `online:snapshot:stream` + HASH `online:snapshot:hash:{ts}` + ZSET `online:snapshot:window:{ts}`(`§6` 表)
   - `r.commit_snapshots_batch(items, now_dt)` → 聚合写 ZSET `online:snapshot:timeline` + HASH `online:snapshot:archive:{unix_ts}`(`§2` v6.3)
4. **scheduler 启动 savedata**:`continuous_open / continuous_resume / lunch / post_market` 阶段 spawn `service_savedata_snapshot`,DEFAULT_INTERVAL=900s(15 分钟)。`scheduler_onlineData.md §2 + §4`、`service_savedata_snapshot.md §1`、`service_savedata_loop.md §6`
5. **落盘逻辑(daemon 子类)**:`SavedataSnapshot(SavedataDaemon)` 基类 `_do_persist` → `core.persist_client.persist_kind(r, conn, "snapshot", trade_date)`,走 `persist_stream_kind` 路由(`core_persist_client.md §3.2`)
6. **STREAM 读 + 批量 INSERT**:
   - `_get_cursor(conn, "snapshot")` 从 SQLite `online_stream_cursor` 表读 `last_stream_id`
   - `XREAD > last_id COUNT 1000` 拉一批
   - `_extract_snapshot_row(data, trade_date=...)`(`core_sqlite_client.md §2.4`)→ tuple
   - `insert_snapshots_batch(conn, "snapshot_YYYYMMDD", rows)` → `executemany`(`core_sqlite_client.md §2.5`)
   - `conn.commit()` → **然后**才 `update_cursor(conn, last_id=...)` → 失败回滚 → 重试(`core_persist_client.md §5` 关键设计)
7. **AI 调试**:通过 `from core.check_redis import check_snapshot` + `from core.query_db import fetch_snapshot("snapshot", "20260914")` 验证(`04_QuickStart.md §3.1/§3.3` + `core_check_query_tools.md §6`)

> **注意 1 处不一致**:Q2 是 snapshot,`core_persist_client.md §3.4` 写 `persist_kind` 支持 "9 种 kind(auction/snapshot/orderbook/minute/zt/break/anomaly/hot/limitperformance)",而 `service_savedata_watchlist.md §3` 明确说 watchlist **不在**路由表(因为走 `replace_watchlist`)。这个其实是合理的(9+1=10 kind,watchlist 不在通用路由),但文档没在主表里明说,容易踩坑。

---

## Q3. 加新 kind `sector_quote` 的 21 处模板

**回答(严格按 `04_QuickStart.md §4.3` A-I 9 大类)**:

- **A 数据获取**(2):① 新建 `core/sector_quote_fetch.py`(板块行情没有现成 fetch 函数)→ 写 `fetch_sector_quote(codes, ...)`;② 选数据源 → 同花顺(sector 数据一般同花顺有,选 `TdxClient` / `ths_client`)
- **B Redis 业务层**(1):`core/redis_online.py` 加 `put_sector_quote` / `commit_sector_quote` / 若干 `get_sector_quote_*`(参考 §3.4 snapshot 模式 8 方法 + §3.7 zt 模式 5 方法,自选 STREAM+ZSET+HSET 还是 timeline/archive 风格)
- **C SQLite 落盘**(3):③ `core/sqlite_client.py` 加 `insert_sector_quote_batch` + 在 `_SCHEMA` 注册;④ `core/persist_client.py` 加 `persist_sector_quote` + 在 `persist_kind()` 路由表注册;⑤(若全表替换)加 `replace_sector_quote`
- **D AI 工具**(4):⑥ `core/check_redis.py` 加 `check_sector_quote()`;⑦ `core/query_redis.py` 加 `fetch_sector_quote_*`;⑧ `core/check_db.py` 加 `check_sector_quote_db()`;⑨ `core/query_db.py` 加 `fetch_sector_quote_db()`
- **E Service 层**(2):⑩ 新建 `service/service_writeredis_sector_quote.py`(抄 `service_writeredis_auction.py` 模板);⑪ 新建 `service/service_savedata_sector_quote.py`(继承 `SavedataDaemon`,`KIND = "sector_quote"`,`DEFAULT_INTERVAL = 900.0`,参考 `service_savedata_loop.md §2` 子类示例)
- **F Scheduler 注册**(1):⑫ `scheduler/scheduler_onlineData.py` 的 `WRITER_SERVICES` / `SAVEDATA_SERVICES` 字典 + `PHASES` 列表加新服务,挂 `continuous_open` + `continuous_resume` 阶段
- **G 部署**(1):⑬ `launchctl unload + load ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist`
- **H 测试**(2):⑭ `test/test_checkredis_sector_quote.py` + `test/test_checkdb_sector_quote.py`
- **I 文档**(6):⑮ 新建 `代码详细设计/service_writeredis_sector_quote.md`;⑯ 新建 `代码详细设计/service_savedata_sector_quote.md`;⑰ `02_代码详细设计.md §3.2/§3.3/§3.5` 加行;⑱ `01_架构文档.md §2.3 + §4.1` 加表行;⑲ `03_数据详细设计.md §2` 加 schema(参考 `§2.6 zt` 18 字段模板);⑳ `04_QuickStart.md §4.3` 末尾加 1 行例举;最后 ㉑ 在 3 个 CHANGELOG 各追加 v1.X 节

**引用**:全程 `04_QuickStart.md §4.3` A-I,加 `service_savedata_loop.md §2` 子类模板,加 `core_persist_client.md §3.4` 路由表结构。

---

## Q4. scheduler 阶段

**回答**:

**一共有 9 个阶段**(`scheduler_onlineData.md §2` 表,与 `01_架构文档.md §2.2` + `02_代码详细设计.md §4.2` 同步):

1. `closed`(15:35-次日 09:00)— 全停
2. `pre_market`(09:00-09:15)— writer: watchlist;savedata: —
3. `auction_open`(09:15-09:25)— writer: watchlist + auction
4. `auction_collect`(09:25-09:30)— 同上
5. `auction_pause`(09:30-09:30,过渡)— writer 暂停
6. `continuous_open`(09:30-11:30)— writer: **8 个 continuous**(snapshot/orderbook/minute/zt/break/anomaly/hot/limitperformance);savedata: 全部
7. `lunch`(11:30-13:00)— writer: **暂停**;savedata: 继续(后台落盘消化)
8. `continuous_resume`(13:00-15:00)— 同 continuous_open
9. `post_market`(15:00-次日 03:00)— writer: 暂停 + 03:00 cleanredis;savedata: 收尾

**切到 lunch**:11:30 整(`continuous_open` 结束的瞬间),由 scheduler 主循环每分钟检查触发(`§5` 调用链)。

**为什么 lunch 完全停 writer**:
- `scheduler_onlineData.md §1 + §2` 明确"11:30 切到 lunch 时主动 kill continuous_open 阶段的 **8 个 writeredis daemon**"
- 原因在 `§2` "重要" 三连击:**(a)** 午休时段交易所没数据,writer 继续跑只会空转 + 浪费 API 配额 + 写垃圾数据;**(b)** `continuous_resume` 时重新 spawn(daemon 内部 `while True`,不感知 lunch),所以 kill 是安全的;**(c)** `savedata_*` **不**停,因为上午 + 午休期间 STREAM 里的数据要继续消化落盘,否则 13:00 开盘时积压会暴涨
- 也对应 `02_代码详细设计.md §4.2` 阶段表"writer 暂停;savedata 继续"
- 也对应 `01_架构文档.md §5 #5` "阶段式调度"原则

> **关键不变量**:`continuous_resume` 重新 spawn 时 daemon 内部 `while True` 不感知 lunch,所以 8 个 writeredis 各自从干净状态启动(`§2 重要 ②`)。

---

## Q5. service_savedata_zt 四大属性

**回答**:

| 属性 | 值 | 引用 |
|---|---|---|
| **DEFAULT_INTERVAL** | `1800s`(30 分钟) | `service_savedata_zt.md §1` 顶部粗体 + `service_savedata_loop.md §6` 速查表 |
| **数据源** | **同花顺** `fetch_limitup_pool()`(由 `service_writeredis_zt` 拉,savedata 自己不调) | `service_savedata_zt.md §1` "读 Redis `online:zt:stream (STREAM)`" + `service_writeredis_zt.md §1 + §5` "数据源:`同花顺 fetch_limitup_pool()`" |
| **父类** | `SavedataDaemon`(基类,定义在 `service/savedata_loop.py`) | `service_savedata_zt.md §3` 调用链 `SavedataDaemon._do_persist()` + `service_savedata_loop.md §1` 设计要点 |
| **落盘函数** | `core.persist_client.persist_zt(r, trade_date)`(`persist_kind` 路由到 `persist_zt`,`core_persist_client.md §3.3`)| `service_savedata_zt.md §3` "persist_zt(r, trade_date)" + `core_persist_client.md §3.3` |

> **额外属性(题目没问但值得记)**:`KIND = "zt"`(由子类必须声明,见 `service_savedata_loop.md §3.1`)、表名 `zt_YYYYMMDD`(`03_数据详细设计.md §2.6`)、18 字段含 `lu_time`(`03_数据详细设计.md §2.6` ⭐ 永不丢)。

---

## 跨文档矛盾 / 不一致清单

### 🔴 矛盾 1(本轮新发现):`scheduler_once.md` 自身内部数字打架

- `代码详细设计/scheduler_once.md §2 §20` 写:**"默认全部 20 个 service(writer 11 + savedata 8)"**
  - 问题 1:`writer 11 + savedata 8 = 19`,**不等于 20**(算错了)
  - 问题 2:`writer 11` 与 02 §3.2 修后的"10 个 writeredis_*"不符
  - 问题 3:`savedata 8` 与 02 §3.3 修后的"10 个 savedata_*"不符
- 同文件 `§4` 示例表也写"合计 10/10 OK / 10.1s"(完整版 19 个,可能需要 60s+)→ 19 又是另一数
- 同文件 `§6 验证`:"`--skip auction,break,anomaly,hot,limitperformance` 跑 5 kind → 10/10 全 OK"(5 writer + 5 savedata = 10,**这个算式对**)

**期望修正**:`§2 §20` 改为"默认全部 20 个 service(10 writer + 10 savedata)",`§4` 注释改"完整版 20 个"。**优先级:中**(数字层面已多轮修过但这个文件漏了)

### 🟡 矛盾 2(次要,延续上轮):`persist_kind` 支持 kind 数描述模糊

- `core_persist_client.md §3.4`:`persist_kind` "支持 9 种 kind(auction/snapshot/orderbook/minute/zt/break/anomaly/hot/limitperformance)"
- `service_savedata_watchlist.md §3` + `service_savedata_minute.md §6`:明确 minute **走 persist_kind** 但**内部路由到 persist_minute**(不是独立),watchlist **不走 persist_kind**(走 `replace_watchlist`)
- 也就是说:**9 种走 persist_kind 路由**(含 minute 但内部跳),1 种(watchlist)**完全不走**
- 读者第一次看到 `persist_kind` 9 种列表,会困惑"那 watchlist 怎么落盘?",必须跳到 `service_savedata_watchlist.md §3` 才明白

**期望**:在 `core_persist_client.md §3.4` 加一句:"watchlist 走 `replace_watchlist`,不走 persist_kind(详见 `service_savedata_watchlist.md`)"。

### 🟡 矛盾 3(小):README 与 04 §1.2 文件数描述微妙差异

- `README.md` §"4 大类文档":**29 个详细 md**
- `04_QuickStart.md §1.2` 目录图:"29 个详细 md(10 writeredis + 10 savedata + 1 savedata_loop + 1 scheduler + 5 core + 2 工具)"
  - 算式:10+10+1+1+5+2 = **29** ✅(对得上)
  - 但代码详细设计/ 子目录我 `ls` 看到的是:**5 core + 10 writeredis + 10 savedata + 1 savedata_loop + 1 cleanredis + 2 scheduler = 29** ✅
  - 04 把它写成"10 writeredis + 10 savedata + 1 savedata_loop + 1 scheduler + 5 core + 2 工具" = 29 ✓
  - 但分类里少了 `cleanredis`(实际有 `service_cleanredis_online.md`),"2 工具"指 cleanredis + ??? 模糊
- `02_代码详细设计.md §3.5 + §8` 也写"30 个详细 md"(CHANGELOG v1.2 第 2 轮修正为 29)

**期望**:统一表述为"5 core + 10 writeredis + 10 savedata + 2 其他 service(基类 + cleanredis) + 2 scheduler = 29",或直接给总数"29"。**优先级:低**(不影响上手,只是数数)

### 🟢 矛盾 4(无可查证,标出来但不算错):lunch 期间 savedata 频率

- `scheduler_onlineData.md §2 + §4` 矩阵:lunch 阶段 savedata 跑,但没说跑得多频繁
- 实际(savedata 各自 daemon DEFAULT_INTERVAL 不变):300s / 900s / 1800s 各跑各的
- 这不是矛盾,是文档省略 — OK,标出来让作者知道如果用户问"lunch 时 15min 落一次还是 30min 落一次"该怎么答

---

## 总分

# **9 / 10**

### 评分理由

**加分项(从 8 升到 9)**:
- ✅ Q1-Q5 全部能在 5 分钟内查清,Q3 加新 kind 模板真"21 处"全列出没漏
- ✅ 跨文档交叉验证 Q4 阶段名 / Q5 DEFAULT_INTERVAL / Q2 数据流,**三处核心数据已完全一致**(上轮 5 处数字错全部修干净)
- ✅ `04_QuickStart.md §4.3` 模板是真"无脑照抄",A-I 9 大类 + 数据源选择表 + plist 部署都全
- ✅ CHANGELOG 体系极清晰,看到 v1.2 就知道上轮 AI 帮修了什么、有据可查
- ✅ `代码详细设计/scheduler_onlineData.md §2` 阶段表 + `§1 + §2 重要` 解释清楚 lunch 为什么 kill,savedata 为什么不 kill — 关键设计原则文档化了
- ✅ "重要约定"(§5.1-§5.7)7 条铁律全在 04 里,新人第一份就看到

**扣分项(为什么不是 10)**:
- 🔴 `scheduler_once.md §2 §20` 算式错(`writer 11 + savedata 8 = 19` ≠ 20)— 第 2 轮大改数字时漏了这个文件,值得 1 扣
- 🟡 `persist_kind` 路由范围(9 种 vs 实际 10 种)描述容易让 AI 误以为 watchlist 也在路由里
- 🟡 README / 04 / 02 三处说"29/30 个详细 md"分类口径略不齐

### 改进建议(若想冲 10/10)

1. **必改**:修 `scheduler_once.md §2 §20` → `writer 10 + savedata 10 = 20`,§4 注释"完整版 20 个"(15 分钟事)
2. **建议改**:`core_persist_client.md §3.4` 加 1 句"watchlist 走 replace_watchlist,不在 persist_kind 路由表"
3. **锦上添花**:`README.md / 04 §1.2 / 02 §3.5` 三处"29 个详细 md"统一为一致分类口径(如"5 core + 10 writeredis + 10 savedata + 1 基类 + 1 cleanredis + 2 scheduler")
4. **未来需求**(超出本轮):lunch 阶段 savedata 实际跑频可写一行注解;`persist_kind` 路由表内部可贴 1 个伪代码 dict 示例
# onlineDataManager 文档评估报告(第 4 轮)

> **评估者**:全新 AI agent,零代码上下文,只看 docs。
> **任务**:5 个测试题 + 总分 + 矛盾清单 + 改进建议。
> **评估日期**:2026-09-14
> **前几轮**:7 → 8 → 9 → ?(本轮)

---

## 阅读顺序

按 `README.md` §阅读顺序 走:**04_QuickStart → 01_架构文档 → 02_代码详细设计 → 代码详细设计/ 子目录 → 03_数据详细设计**。
本评估额外查了:
- `代码详细设计/service_writeredis_snapshot.md`(Q2 拉-写-落 链)
- `代码详细设计/service_savedata_snapshot.md`(Q2 落盘链)
- `代码详细设计/service_savedata_zt.md` + `service_savedata_loop.md`(Q5 基类属性)
- `代码详细设计/scheduler_onlineData.md`(Q4 阶段细节)
- `代码详细设计/core_persist_client.md`(Q2 落盘函数真值)
- `03_数据详细设计.md` §2.3 snapshot 表结构

---

## Q1:模块定位(一句话)

**回答**:`onlineDataManager` 是 A 股**实时行情数据采集与持久化模块**——交易时段从同花顺/pytdx/KPL 多源拉数据 → 写 Redis(STREAM + ZSET + HSET) → 通过 STREAM 游标机制批量落 SQLite 月度库,供下游 `policyStudy` 回测消费。

**依据**:
- `01_架构文档.md` §2.1 分层图 + §3.1 SQLite 月度库 + §3.3 Redis↔SQLite 关系
- `04_QuickStart.md` §1.1 推荐阅读顺序 / §5.1 写入/落盘分离原则
- `01_架构文档.md` §4.4 数据流:`offline + online → policyStudy`
- `README.md` §关键约定 5 条:`写入/落盘分离` / `原子化` / `STREAM + 游标` / `v3 双时间戳` / `lu_time 永不丢`

**评判**:定位完全清晰,一句话能讲清楚。✅

---

## Q2:snapshot 数据完整链路(从数据源到 SQLite,7 步)

**回答**(以 `snapshot` kind 为例,从拉到存):

1. **数据源拉取**:`service_writeredis_snapshot` 调用 `TdxClient().fetch_snapshot_quotes(codes=...)`(同花顺客户端),watchlist 从 `r.get_watchlist()` 拿在线股票列表。(*`代码详细设计/service_writeredis_snapshot.md` §5*)
2. **pipeline 写 STREAM + HASH + Window**:`r.put_snapshots_batch(items)` 批量写入 `online:snapshot:stream`(STREAM, MAXLEN 200000)+ `online:snapshot:hash:{ts_code}`(HASH)+ `online:snapshot:window:{ts_code}`(ZSET, 滑动 30min)。(*§5 调用链 + §6 Redis 数据结构*)
3. **一轮末尾 commit**:`r.commit_snapshots_batch(items, now_dt=now_dt)` → `ZADD online:snapshot:timeline` (score=now) → `HSET online:snapshot:archive:{unix_ts}` → 滑窗裁剪 → `EXPIRE = calc_snapshot_archive_ttl(now_dt)`(11:00-11:30 动态 7200s 跨午休补偿)。(*§2 v6.3 重构 + §3 动态 TTL*)
4. **scheduler 阶段切换触发 savedata spawn**:`continuous_open` / `continuous_resume` 阶段 scheduler 调 `spawn_service("service_savedata_snapshot")` 子进程启动。(*`代码详细设计/scheduler_onlineData.md` §4 + `02_代码详细设计.md` §6.1*)
5. **子类基类初始化**:`SavedataSnapshot(SavedataDaemon)` 实例化 → `OnlineRedis()` + `connect_db()` → 读游标。(*`代码详细设计/service_savedata_loop.md` §3*)
6. **STREAM → SQLite 落盘**:`_do_persist()` → `persist_kind(r, kind="snapshot", trade_date=td)` → `persist_stream_kind(...)` → `XREAD > last_id COUNT 1000` → 批量 INSERT `snapshot_YYYYMMDD`。(*`core_persist_client.md` §3.2 + §5 游标持久化*)
7. **commit + 游标推进**:`conn.commit()`(业务表)→ `update_cursor(conn, ...)` 写 `online_stream_cursor` 表 → 再 `conn.commit()`。失败回滚 → 游标不变 → 下一轮重试同样 last_id,业务表 UNIQUE 约束防重复。(*`core_persist_client.md` §5*)

**完整链路**:同花顺 HTTP → TdxClient → OnlineRedis pipeline (STREAM+ZSET+HSET) → commit (timeline+archive) → scheduler 阶段切 → subprocess spawn savedata → STREAM XREAD 游标 → SQLite 批量 INSERT → 游标持久化 → 完成

**评判**:7 步清晰可走通,完整覆盖数据源 → Redis → STREAM → SQLite → 游标。✅
**小坑**:细节文档 `service_savedata_snapshot.md` §1 写的是 `persist_snapshot(r, trade_date)`,但实际上基类走的是 `persist_kind(r, kind="snapshot", trade_date)`(由 `service_savedata_loop.md` §3.2 `_do_persist` 决定)。不影响上手,但严格说有点不准。详见矛盾清单 C1。

---

## Q3:加新 kind `sector_quote` 的完整模板

**回答**(按 `04_QuickStart.md` §4.3 加新 kind 模板 21 处改动):

**A. 数据获取层**(1 处)
1. `core/watchlist_fetch.py`(或新建 `core/sector_quote_fetch.py`)加 `fetch_sector_quote(codes, ...)` 函数。§4.3 第 1 条

**A.0 数据源 → 客户端映射**(避免选错客户端):§4.3 第 1.5 条
- 数据源决定客户端。同花顺 → `TdxClient`;pytdx → `TdxHq_API`;KPL → `coreClient.kpl_api`。多数 kind 走同花顺。板块行情大概率同花顺或 KPL,先选数据源再选客户端。

**B. Redis 业务层**(1 处)
2. `core/redis_online.py` 加 `OnlineRedis.put_sector_quote` / `commit_sector_quote` / 若干 `get_sector_quote` 读方法。§4.3 第 2 条

**C. SQLite 落盘层**(3 处)
3. `core/sqlite_client.py` 加 `insert_sector_quote_batch` 函数 + 在 `_SCHEMA` 注册表 schema。§4.3 第 3 条
4. `core/persist_client.py` 加 `persist_sector_quote(r, conn, trade_date)` 函数 + 在 `persist_kind()` 路由表注册。§4.3 第 4 条
5. (如果是全表替换类)在 `core/sqlite_client.py` 加 `replace_sector_quote` 函数。§4.3 第 5 条

**D. AI Agent 工具层**(4 处,镜像 4 个文件)
6. `core/check_redis.py` 加 `check_sector_quote()`。§4.3 第 6 条
7. `core/query_redis.py` 加 `fetch_sector_quote_*()`。§4.3 第 7 条
8. `core/check_db.py` 加 `check_sector_quote_db()`。§4.3 第 8 条
9. `core/query_db.py` 加 `fetch_sector_quote_db()`。§4.3 第 9 条

**E. Service 层**(2 处)
10. 新建 `service/service_writeredis_sector_quote.py`,抄 `service_writeredis_auction.py` 模板,改 `KIND` / `INTERVAL` / 数据源。§4.3 第 10 条
11. 新建 `service/service_savedata_sector_quote.py`,继承 `SavedataDaemon`,`KIND = "sector_quote"`,`DEFAULT_INTERVAL = 900.0`。§4.3 第 11 条 + `代码详细设计/service_savedata_loop.md` §2 子类示例

**F. Scheduler 注册**(1 处)
12. `scheduler/scheduler_onlineData.py` 在 `WRITER_SERVICES` / `SAVEDATA_SERVICES` 字典 + `PHASES` 列表里加新服务(挂 `continuous_open` / `continuous_resume` 阶段)。§4.3 第 12 条

**G. 部署**(1 处)
13. `launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist && launchctl load ...`。§4.3 第 13 条(注意 plist 名见矛盾 C2)

**H. 测试**(2 处)
14. `test/test_checkredis_sector_quote.py` + `test/test_checkdb_sector_quote.py`(参考 `test_checkredis_zt.py`)。§4.3 第 14 条

**I. 文档**(6 处)
15. `代码详细设计/service_writeredis_sector_quote.md`(新)。§4.3 第 15 条
16. `代码详细设计/service_savedata_sector_quote.md`(新)。§4.3 第 16 条
17. `02_代码详细设计.md` §3.2 / §3.3 / §3.5 加新行。§4.3 第 17 条
18. `01_架构文档.md` §2.3 表格 + §4.1 频率表。§4.3 第 18 条
19. `03_数据详细设计.md` §2 加 schema(参考 §2.6 zt 模板)。§4.3 第 19 条
20. `04_QuickStart.md` §4.3 加 1 行例举新 kind。§4.3 第 20 条
21. 在 3 个 CHANGELOG 各追加 v1.X 节(`01_架构文档_CHANGELOG.md` / `02_代码详细设计_CHANGELOG.md` / `03_数据详细设计_CHANGELELOG.md`)。§4.3 第 21 条

**分布总结**:core 5 / service 2 / scheduler 1 / 部署 1 / 测试 2 / 文档 7 + CHANGELOG 3 = **21 处改动**(§4.3 末段已自结)。

**评判**:模板完整到 21 处都列了,完全可以照抄。✅
**可改进**:第 12 条说"挂 continuous_open / continuous_resume 阶段",但若 `sector_quote` 是低频全市场数据,可以放 `pre_market` 跑一次;模板没明确说"按需挂哪个阶段",但 `01_架构文档.md` §4.1 频率表提示了阶段归属。

---

## Q4:scheduler 阶段

**回答**:**scheduler 一共 9 个阶段**。

**9 个阶段名字**(按 `代码详细设计/scheduler_onlineData.md` §2 / `02_代码详细设计.md` §4.2):

| # | 阶段 | 时间窗 |
|---|---|---|
| 1 | `closed` | 15:35-次日 09:00 |
| 2 | `pre_market` | 09:00-09:15 |
| 3 | `auction_open` | 09:15-09:25 |
| 4 | `auction_collect` | 09:25-09:30 |
| 5 | `auction_pause` | 09:30-09:30(短过渡) |
| 6 | `continuous_open` | 09:30-11:30 |
| 7 | `lunch` | 11:30-13:00 |
| 8 | `continuous_resume` | 13:00-15:00 |
| 9 | `post_market` | 15:00-次日 03:00 |

**切到 lunch 的时间点**:**11:30**(continuous_open 阶段结束)。

**lunch 完全停止 writer 的原因**(*`代码详细设计/scheduler_onlineData.md` §2 注释 + §1 职责*):
- scheduler 在 11:30 切到 lunch 时**主动 kill** continuous_open 阶段的 **8 个 writeredis daemon**(snapshot/orderbook/minute/zt/break/anomaly/hot/limitperformance)
- 原因:A 股 11:30-13:00 午休,交易所停止撮合,**数据源没有新数据可拉**,writer 跑也是空转浪费资源
- 13:00 continuous_resume 时重新 spawn(daemon 内部 `while True`,不感知 lunch)

**lunch savedata 不停**:
- lunch 阶段 savedata **继续**跑(后台消化午休前积压的 STREAM 数据),9 个 savedata 全员后台落盘
- 这是"写入 × 落盘 二维分离"的核心体现(`01_架构文档.md` §5 原则 5)

**评判**:阶段名/数量/时间窗/切到 lunch 的原因都讲清楚了,文档内部一致(01 §2.2 + 02 §4.2 + scheduler_onlineData.md §2 三处一致)。✅

---

## Q5:`service_savedata_zt` 的 4 大属性

**回答**:

| 属性 | 值 | 依据 |
|---|---|---|
| **DEFAULT_INTERVAL** | `1800.0`(30 分钟) | `代码详细设计/service_savedata_zt.md` §1 明写;`代码详细设计/service_savedata_loop.md` §6 速查表"zt = 1800s" |
| **数据源** | **同花顺**(`fetch_limitup_pool`) | `01_架构文档.md` §2.3 writeredis_zt 行 + `代码详细设计/core_watchlist_fetch.md`(板块:`fetch_limitup_pool` 函数)。注意:这是**上游 writeredis_zt 的数据源**,savedata 本身不直接拉数据 |
| **父类** | `service.savedata_loop.SavedataDaemon` | `代码详细设计/service_savedata_loop.md` §2 子类示例 + `02_代码详细设计.md` §3.3 统一调用模式 + §3.4 其他 service 表 |
| **落盘函数** | `core.persist_client.persist_zt(r, trade_date=...)` | `代码详细设计/service_savedata_zt.md` §3 调用链 + `代码详细设计/core_persist_client.md` §3.3 `persist_zt` 签名 `(redis_client, *, trade_date: str) -> int`,从 `online:zt:stream` STREAM 读 |

**评判**:4 大属性都能从 docs 直接查到。✅
**注意**:
1. `service_savedata_zt.md` 本身没列"父类/SavedataDaemon"字样,要从基类 `service_savedata_loop.md` §1 + `02_代码详细设计.md` §3.3 推断。模板性弱了一点点。
2. `service_savedata_zt.md` §6 备注说"DEFAULT_INTERVAL = 1800s(代码真值,2026-09-14 体检校正)",可见是历史被纠正过的数字,这暗示其他 service 文件的数字也可能错 —— 不过本评估无法在不读代码的前提下验证。

---

## 总评分:**9 / 10**

**评分理由**:

- **Q1 模块定位**:1 句话能讲清(满分)。
- **Q2 snapshot 7 步链路**:清晰可走通,覆盖数据源→Redis→STREAM→SQLite→游标(扣 0.5 分因为 savedata 详细 doc 写 `persist_snapshot` 与基类 `persist_kind` 有出入)。
- **Q3 加新 kind 模板**:21 处改动列到每一条,可无脑照抄(满分,只是阶段挂载时机没说死,扣 0 分因为模板说"挂 continuous_open/resume",隐含假设该 kind 是盘中实时,若低频全市场需自行判断)。
- **Q4 scheduler 阶段**:9 个阶段完整一致,lunch 完全停止原因写得清晰(满分)。
- **Q5 4 大属性**:全部查到(扣 0.5 分因为 `service_savedata_zt.md` 自身没列父类,需要查基类才能补全)。

**从前几轮 7→8→9 趋势看,本轮保持在 9 分**:文档体系基本完美,AI 可无脑照抄;只扣 1 分因为存在 2 处明确的不一致(见下)。

**8 分升 9 分的契机在**:已修复(见 `代码详细设计/service_savedata_loop.md` §6 末段"2026-09-14 体检发现 §6 表周期数字全错 ... 已按 core/service_*.py 真实 DEFAULT_INTERVAL 校正")。
**9 分保 9 分**:仍有 2 处可改善的小不一致(下)。

---

## 跨文档矛盾清单(有据可查)

### C1: snapshot savedata 的落盘函数表述不一致

- **`代码详细设计/service_savedata_snapshot.md` §1 / §3 调用链**:写的是 `persist_snapshot(r, trade_date)`(具体函数名)。
- **`代码详细设计/service_savedata_loop.md` §3.2 `_do_persist`**:实际调的是 `persist_kind(r, kind=KIND, trade_date=td)`(通用路由)。
- **`代码详细设计/core_persist_client.md` §3.2 / §3.4**:`persist_kind` 内部路由到 `persist_stream_kind(kind="snapshot")`(`auction/snapshot/orderbook` 三个 STREAM 类共享同一通用函数)。
- **真值(由基类 + persist_client 综合推断)**:实际调用链是 `persist_kind(r, kind="snapshot", trade_date=...)` → `persist_stream_kind(...)`。snapshot 详细 doc 的 `persist_snapshot(r, ...)` 是**不存在的函数名**,可能是早期版本残留或简化描述。
- **影响**:低。AI 跟着基类模板抄不会出错;但只读 `service_savedata_snapshot.md` 会被误导。
- **同类可疑**:`service_savedata_auction.md` / `service_savedata_orderbook.md` 大概率也有同样问题(没逐个看)。

### C2: launchd plist 名字 2 处不一致

- **`04_QuickStart.md` §4.3 第 13 条**(部署重载命令):`~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist`
- **`04_QuickStart.md` §6.1 重启生产调度器**:`~/Library/LaunchAgents/com.tradingagent.onlineDataManager.plist`(同文件 §4.3 vs §6.1 内部矛盾)
- **`01_架构文档.md` §4.3 调度与运维**:`com.tradingagent.scheduler.onlineData.plist`
- **`代码详细设计/scheduler_onlineData.md` §6 启动方式**:`com.tradingagent.scheduler.onlineData.plist`
- **真值**:`com.tradingagent.scheduler.onlineData.plist`(3/4 处一致,§6.1 是孤本)
- **影响**:中。AI 照抄 §6.1 会 launchctl 失败一次才能发现错误。
- **建议**:把 §6.1 的 `com.tradingagent.onlineDataManager.plist` 改成 `com.tradingagent.scheduler.onlineData.plist`。

### C3: service 文件计数不一致(轻微)

- **`01_架构文档.md` §1.1 / §2.1**:service 层 **23 个文件**(10 writer + 10 savedata + savedata_loop + cleanredis + __init__=23,实际)
- **`02_代码详细设计.md` §1 分层链图**:写 `service_writeredis_*.py (11 个)` 和 `service_savedata_*.py (8 个)`(总 19,加基类+cleanredis=21)。**11 个 writeredis 是错的**(实际只有 10 个)。
- **`02_代码详细设计.md` §3.2 writeredis_详解**:**10 个**(正确)。
- **`02_代码详细设计.md` §3.3 savedata_详解**:**10 个**(正确)。
- **`02_代码详细设计.md` §3.5 末尾**:"writeredis_*(11 份)"(错);"savedata_*(10 份)"(对)。
- **`02_代码详细设计.md` §8 文档导航**:再次写"writeredis_*(11 份)"(错)。
- **`代码详细设计/`** 实际目录:`service_writeredis_*.md` 10 个 + `service_savedata_*.md` 10 个 + `service_savedata_loop.md` + `service_cleanredis_online.md` = 22 个 md,共 29 个 md。
- **真值**:**writeredis 共 10 个**(以 02 §3.2 + 目录实数为准)。
- **影响**:低。AI 跟着 02 §3.2 抄不会出错,但首次浏览 §1 ASCII 图会被骗。

### C4: savedata_watchlist 的落盘路径 2 处表述不一致(轻微)

- **`02_代码详细设计.md` §3.3 savedata_watchlist 行**:写 "kind=watchlist → `persist_client.persist_kind`"
- **`代码详细设计/core_persist_client.md` §3.4 路由表注释**:明确说 "watchlist 不在此路由表 — 因为 watchlist 走全表替换 `replace_watchlist`,由 `service_savedata_watchlist` 子类 override `_do_persist` 直接调,不走基类 `persist_kind`"
- **真值**:watchlist 走子类 override + `replace_watchlist`,**不走 `persist_kind`**(`core_persist_client.md` §3.4 是技术性更强的描述,更可信)。
- **影响**:低。AI 抄模板时按 §3.3 写会失败一次;但 `service_savedata_loop.md` §1 列的"10 个 savedata"实际只有 9 个继承基类,watchlist 是特殊。Q3 模板第 5 条已通过"全表替换类"提示到了。

### C5: §4.3 模板第 12 条说"PHASES 列表里加新服务"(轻微)

- **`04_QuickStart.md` §4.3 第 12 条**:让 AI 改 `scheduler_onlineData.py` 的 `WRITER_SERVICES` / `SAVEDATA_SERVICES` **字典** + `PHASES` **列表**。
- **`代码详细设计/scheduler_onlineData.md` §4**:说明 `WRITER_SERVICES` 和 `SAVEDATA_SERVICES` 是**字典**(kind → module),阶段表是矩阵,但**没有 `PHASES` 列表**这个数据结构(`PHASES` 在 §2 表格里展示但不一定叫 `PHASES` 变量)。
- **影响**:低。AI 抄模板会写错变量名,但很容易定位。

---

## 改进建议(优先级排序)

1. **[P1] 修 C2 plist 名矛盾**:`04_QuickStart.md` §6.1 的 plist 名应为 `com.tradingagent.scheduler.onlineData.plist`(5 秒搞定,但避免 AI 首次重启失败)。
2. **[P1] 修 C3 writeredis 11→10**:`02_代码详细设计.md` §1 ASCII 图 + §8 文档导航"11 份"改为"10 份"。
3. **[P2] 修 C1 persist_snapshot 名称**:`service_savedata_snapshot.md` §1 + §3 改为 `persist_kind(r, kind="snapshot", trade_date=td)`(与基类对齐);同类检查 `service_savedata_auction.md` / `service_savedata_orderbook.md`。
4. **[P2] 修 C4 watchlist 路由描述**:`02_代码详细设计.md` §3.3 watchlist 行改为"子类 override _do_persist,调 `replace_watchlist`,不走 persist_kind"。
5. **[P3] 增强 `service_savedata_zt.md`**:加一行"继承 SavedataDaemon,见 service_savedata_loop.md §2",省得 Q5 需要跨文档推断。
6. **[P3] Q3 模板第 12 条**:把"挂 continuous_open / continuous_resume 阶段"加一句"或 pre_market(若是日终全量拉取类)",覆盖低频全市场场景。
7. **[P3] 02 §4.2 阶段表 vs §4 矩阵**:统一一下"PHASES"这个变量名是否真存在;如果不是,把 §4.3 第 12 条"PHASES 列表"改成"§4.2 阶段表"。

---

## 评估者备注

- 本评估在 ~10 分钟内完成,完全靠 README 推荐的阅读路径,没看代码。
- 5 个测试题全部能答出,无任何一个需要"猜"。
- 矛盾清单里只有 C2 (plist) 是会让 AI 首次**操作失败**的;其余都是描述性偏差,不影响上手但累积会磨掉信任。
- 第 4 轮保持 9 分是合理的;距离 10 分只差那 2 处明确矛盾的修复。

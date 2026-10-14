# onlineDataManager 文档评估报告(第 5 轮)

> **评估者**:全新 AI agent,零代码上下文,只看 docs。
> **任务**:5 个测试题 + 总分 + 矛盾清单 + 改进建议。
> **评估日期**:2026-09-14
> **前几轮**:7 → 8 → 9 → 9 → ?(本轮)
> **本轮重点**:验证第 4 轮遗留 C1(`persist_xxx` 函数名)和 C5(`PHASES` 变量名)是否修干净。

---

## 阅读顺序

按 `README.md` §阅读顺序 走:**04_QuickStart → 01_架构文档 → 02_代码详细设计 → 代码详细设计/ 子目录 → 03_数据详细设计**。

本评估额外查了:
- `代码详细设计/service_writeredis_snapshot.md`(Q2 拉-写-落 链)
- `代码详细设计/service_savedata_snapshot.md` / `service_savedata_zt.md` / `service_savedata_minute.md` / `service_savedata_watchlist.md` / `service_savedata_auction.md`(Q2 + Q5 + C1 验证)
- `代码详细设计/service_savedata_loop.md`(Q5 基类 + DEFAULT_INTERVAL)
- `代码详细设计/scheduler_onlineData.md`(Q4 阶段 + C5 变量名验证)
- `代码详细设计/core_persist_client.md`(C1 验证:真实 `persist_kind` 路由)
- `03_数据详细设计.md` §2.3 snapshot 表结构
- `02_代码详细设计_CHANGELOG.md` v1.6(本轮修复记录,验证修复是否落到位)

---

## Q1:模块定位(一句话)

**回答**:`onlineDataManager` 是 A 股**实时行情数据采集与持久化模块**——交易时段从同花顺 / pytdx / KPL 多源拉数据 → 写 Redis(STREAM + ZSET + HSET + Window 四类结构)→ 通过 STREAM 游标机制批量落 SQLite 月度库,供下游 `policyStudy` 回测消费。

**依据**:
- `01_架构文档.md` §2.1 分层图(写入 × 落盘分离)+ §3.1 SQLite 月度库 + §3.3 Redis↔SQLite STREAM 桥梁
- `04_QuickStart.md` §1.1 推荐阅读顺序 / §5.1 写入/落盘分离原则 / §5.2 STREAM 游标 / §5.3 v3 双时间戳
- `01_架构文档.md` §4.4 数据流:`offline + online → policyStudy`
- `README.md` §关键约定 5 条:`写入/落盘分离` / `原子化` / `STREAM + 游标` / `v3 双时间戳` / `lu_time 永不丢`

**评判**:定位完全清晰,一句话能讲清楚。✅

---

## Q2:snapshot 数据完整链路(从数据源到 SQLite,7 步)

**回答**(以 `snapshot` kind 为例,从拉到存):

1. **数据源拉取**:`service_writeredis_snapshot` 调用 `TdxClient().fetch_snapshot_quotes(codes=...)`(同花顺客户端,`service_writeredis_snapshot.md` §5),watchlist 从 `r.get_watchlist()` 拿在线股票列表。
2. **pipeline 写 STREAM + HASH + Window**:`r.put_snapshots_batch(items)` 批量写入 `online:snapshot:stream`(STREAM, MAXLEN 200000)+ `online:snapshot:hash:{ts_code}`(HASH)+ `online:snapshot:window:{ts_code}`(ZSET, 滑动 30min)。(*`service_writeredis_snapshot.md` §5 调用链 + §6 Redis 数据结构*)
3. **一轮末尾 commit**:`r.commit_snapshots_batch(items, now_dt=now_dt)` → `ZADD online:snapshot:timeline`(score=now)→ `HSET online:snapshot:archive:{unix_ts}` → 滑窗裁剪 → `EXPIRE = calc_snapshot_archive_ttl(now_dt)`(11:00-11:30 动态 7200s 跨午休补偿)。(*`service_writeredis_snapshot.md` §2 v6.3 重构 + §3 动态 TTL*)
4. **scheduler 阶段切换触发 savedata spawn**:`continuous_open` / `continuous_resume` 阶段 scheduler 调 `spawn_service("service_savedata_snapshot")` 子进程启动。(*`scheduler_onlineData.md` §4 + `02_代码详细设计.md` §6.1*)
5. **子类基类初始化**:`SavedataSnapshot(SavedataDaemon)` 实例化 → `OnlineRedis()` + `connect_db()` → 读游标。(*`service_savedata_loop.md` §3*)
6. **STREAM → SQLite 落盘**:`_do_persist()` → `persist_kind(r, kind="snapshot", trade_date=td)` → `persist_stream_kind(...)` → `XREAD > last_id COUNT 1000` → 批量 INSERT `snapshot_YYYYMMDD`(schema 见 `03_数据详细设计.md` §2.3:PK = Redis STREAM id,字段含 data_timestamp + created_at 双时间戳)。(*`service_savedata_snapshot.md` §3 + `core_persist_client.md` §3.2 + §5 游标持久化*)
7. **commit + 游标推进**:`conn.commit()`(业务表)→ `update_cursor(conn, ...)` 写 `online_stream_cursor` 表 → 再 `conn.commit()`。失败回滚 → 游标不变 → 下一轮重试同样 last_id,业务表 UNIQUE 约束防重复。(*`core_persist_client.md` §5*)

**完整链路**:同花顺 HTTP → TdxClient → OnlineRedis pipeline (STREAM+ZSET+HSET) → commit (timeline+archive) → scheduler 阶段切 → subprocess spawn savedata → STREAM XREAD 游标 → SQLite 批量 INSERT → 游标持久化 → 完成

**评判**:7 步清晰可走通,完整覆盖数据源 → Redis → STREAM → SQLite → 游标。✅
**本轮改善**:第 4 轮 C1 (`service_savedata_snapshot.md` 写 `persist_snapshot(r, ...)` 不存在函数名)已修复为 `persist_kind(r, kind="snapshot", trade_date=trade_date)`(见 `service_savedata_snapshot.md` §1 + §3)。新基线 0 函数名错误。

---

## Q3:加新 kind `sector_quote` 的完整模板

**回答**(按 `04_QuickStart.md` §4.3 加新 kind 模板 21 处改动):

**A. 数据获取层**(1 处)
1. `core/watchlist_fetch.py`(或新建 `core/sector_quote_fetch.py`)加 `fetch_sector_quote(codes, ...)` 函数。§4.3 第 1 条

**A.0 数据源 → 客户端映射**(避免选错客户端):§4.3 第 1.5 条
- 数据源决定客户端。同花顺 → `TdxClient`;pytdx → `TdxHq_API`;KPL → `coreClient.kpl_api`。多数 kind 走同花顺。板块行情大概率同花顺或 KPL,先选数据源再选客户端。

**B. Redis 业务层**(1 处,通常要加多个方法)
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
12. `scheduler/scheduler_onlineData.py` 在 `WRITER_SERVICES` / `SAVEDATA_SERVICES` **字典**里加新服务(挂 `continuous_open` / `continuous_resume` 阶段),并在 9 阶段调度逻辑里 spawn/kill。§4.3 第 12 条(*本轮已修复:不再说 "PHASES 列表"*)

**G. 部署**(1 处)
13. `launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist && launchctl load ...`。§4.3 第 13 条(*本轮已修复:plist 名与 01/scheduler detail 完全一致*)

**H. 测试**(2 处)
14. `test/test_checkredis_sector_quote.py` + `test/test_checkdb_sector_quote.py`(参考 `test_checkredis_zt.py`)。§4.3 第 14 条

**I. 文档**(6 处)
15. `代码详细设计/service_writeredis_sector_quote.md`(新)。§4.3 第 15 条
16. `代码详细设计/service_savedata_sector_quote.md`(新)。§4.3 第 16 条
17. `02_代码详细设计.md` §3.2 / §3.3 / §3.5 加新行。§4.3 第 17 条
18. `01_架构文档.md` §2.3 表格 + §4.1 频率表。§4.3 第 18 条
19. `03_数据详细设计.md` §2 加 schema(参考 §2.6 zt 模板)。§4.3 第 19 条
20. `04_QuickStart.md` §4.3 加 1 行例举新 kind。§4.3 第 20 条
21. 在 3 个 CHANGELOG 各追加 v1.X 节。§4.3 第 21 条

**分布总结**:core 5 / service 2 / scheduler 1 / 部署 1 / 测试 2 / 文档 7 + CHANGELOG 3 = **21 处改动**(§4.3 末段已自结)。

**评判**:模板完整到 21 处都列了,完全可以照抄。✅
**本轮改善**:第 4 轮 C5 已修 — §4.3 F.12 现在用 "字典" + "9 阶段调度逻辑里 spawn/kill" 描述,与 `scheduler_onlineData.md §4` 真值完全对齐(`WRITER_SERVICES` / `SAVEDATA_SERVICES` 是字典,无 `PHASES` list 变量)。

---

## Q4:scheduler 阶段

**回答**:**scheduler 一共 9 个阶段**。

**9 个阶段名字**(按 `代码详细设计/scheduler_onlineData.md` §2 / `02_代码详细设计.md` §4.2 / `01_架构文档.md` §2.2,三处一致):

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

**切到 lunch 的时间点**:**11:30**(`continuous_open` 阶段结束)。

**lunch 完全停止 writer 的原因**(*`scheduler_onlineData.md` §1 职责 + §2 注释 + §4 写入/落盘分离*):
- scheduler 在 11:30 切到 lunch 时**主动 kill** continuous_open 阶段的 **8 个 writeredis daemon**(snapshot/orderbook/minute/zt/break/anomaly/hot/limitperformance)
- 原因:A 股 11:30-13:00 午休,交易所停止撮合,**数据源没有新数据可拉**,writer 跑也是空转浪费资源
- 13:00 continuous_resume 时重新 spawn(daemon 内部 `while True`,不感知 lunch)

**lunch savedata 不停**:
- lunch 阶段 savedata **继续**跑(后台消化午休前积压的 STREAM 数据),9 个 savedata 全员后台落盘
- 这是"写入 × 落盘 二维分离"(`WRITER_PHASES` × `SAVEDATA_PHASES`)的核心体现(*本轮改善:02 §4.2 已与 scheduler detail 完全对齐为 `WRITER_PHASES / SAVEDATA_PHASES`,无 PHASES 单变量*)

**评判**:阶段名/数量/时间窗/切到 lunch 的原因都讲清楚了,文档内部一致(01 §2.2 + 02 §4.2 + `scheduler_onlineData.md` §2 三处一致)。✅

---

## Q5:`service_savedata_zt` 的 4 大属性

**回答**:

| 属性 | 值 | 依据 |
|---|---|---|
| **DEFAULT_INTERVAL** | `1800.0`(30 分钟) | `代码详细设计/service_savedata_zt.md` §1 明写 `DEFAULT_INTERVAL = 1800s(代码真值,2026-09-14 体检校正)`;`service_savedata_loop.md` §6 速查表 "zt = 1800s";`01_架构文档.md` §4.1 频率表"zt 30min 落盘" |
| **数据源** | **同花顺**(`fetch_limitup_pool`) | `01_架构文档.md` §2.3 `service_writeredis_zt.py` 行 + §4.1 频率表(本轮已修:第 4 轮前曾标 "KPL",v1.2 修复后改 "同花顺");`02_代码详细设计.md` §3.2 表格。注意:这是**上游 writeredis_zt 的数据源**,savedata 本身不直接拉数据,只调 `core/watchlist_fetch.fetch_limitup_pool` 经 `redis_online.put_zt_pool` 落 STREAM |
| **父类** | `service.savedata_loop.SavedataDaemon` | `代码详细设计/service_savedata_loop.md` §1 设计要点 + §2 子类示例(10 个 savedata 都继承 `SavedataDaemon`)+ `02_代码详细设计.md` §3.3 统一调用模式(`class ServiceSavedataX(SavedataDaemon)`)+ §3.4 其他 service 表 |
| **落盘函数** | `core.persist_client.persist_kind(r, kind="zt", trade_date=...)`(路由到 `persist_zt` 内部 helper) | `代码详细设计/service_savedata_zt.md` §3 调用链(`persist_kind(r, kind="zt", trade_date=trade_date)`)+ `代码详细设计/core_persist_client.md` §3.3 列出 `persist_zt(r, *, trade_date: str) -> int`(内部 helper,`persist_kind` 路由到此处读 `online:zt:stream` STREAM) |

**评判**:4 大属性都能从 docs 直接查到,链路完整对齐。✅
**本轮改善**:第 4 轮 C1 已修 — `service_savedata_zt.md` §1 + §3 现在正确写 `persist_kind(r, kind="zt", trade_date=trade_date)`,与基类 + 路由层真值一致(9 个 STREAM 类 savedata md 全部对齐)。

---

## 总评分:**9.5 / 10**

**评分理由**:

- **Q1 模块定位**:1 句话能讲清(满分)。
- **Q2 snapshot 7 步链路**:清晰可走通,覆盖数据源→Redis→STREAM→SQLite→游标。第 4 轮 C1 函数名错误已修,完整 0 函数名错误。
- **Q3 加新 kind 模板**:21 处改动列到每一条,可无脑照抄。第 4 轮 C5(PHASES 列表)+ C2(plist 名)双修,模板现在与 scheduler 真值完全对齐。
- **Q4 scheduler 阶段**:9 个阶段完整一致,`WRITER_PHASES / SAVEDATA_PHASES` 二维变量名也对齐了(`02 §4.2` ↔ `scheduler_onlineData.md §4`),lunch 完全停止原因写得清晰。
- **Q5 4 大属性**:全部查到,链路 `service_savedata_zt.md` → `service_savedata_loop.md` → `core_persist_client.md` 三处交叉一致。

**从前几轮 7→8→9→9 趋势看,本轮 9.5 → 接近 10 分上限**:第 4 轮遗留的 2 处(共 19 处函数名 + 1 处变量名 + 1 处 plist 名)全部清完,文档体系基本完美,AI 可无脑照抄。扣 0.5 分因为还剩 3 处**极轻微**描述性瑕疵(见下)。

**距离 10/10 的最后 0.5 分**:3 处描述性不一致(累计影响微乎其微,只是文档洁癖问题)。

---

## 跨文档矛盾清单(有据可查)

### C1: `persist_xxx` 函数名错误 — **本轮已完全修复** ✅

- **旧问题**(第 4 轮):`service_savedata_*.md` × 9 个文件调用链写 `persist_xxx(r, trade_date)`(不存在的顶层函数名),实际是 `persist_kind(r, kind="xxx", trade_date=trade_date)` 走基类路由。
- **本轮验证**:
  - `service_savedata_{anomaly,auction,break,hot,limitperformance,orderbook,snapshot,zt}.md` §1 + §3 全部 19 处已改为 `persist_kind(r, kind="<kind>", trade_date=trade_date)` ✅
  - `service_savedata_minute.md` §3 已改为 `persist_kind(r, kind="minute", trade_date=trade_date)` ✅
  - `service_savedata_watchlist.md` §3 + §7 显式说明不走 `persist_kind`(子类 override `_do_persist` 直接调 `replace_watchlist`),与 `core_persist_client.md §3.4` 一致 ✅
  - `core_persist_client.md §3.3` 列出的 `persist_zt / persist_break / persist_anomaly / persist_hot / persist_limitperformance` 是**真实存在的内部 helper 函数**(`persist_kind` 路由表的目标),不是 savedata md 误用的入口。
- **影响**:0。完全修复,基线干净。
- **CHANGELOG 验证**:`02_代码详细设计_CHANGELOG.md` v1.6 记录"19 处修复",与实际一致。

### C5: PHASES 变量名 — **本轮已完全修复** ✅

- **旧问题**(第 4 轮):`04 §4.3 F.12` 写"PHASES 列表里加新服务",但 `scheduler_onlineData.py` 实际只有 `WRITER_SERVICES / SAVEDATA_SERVICES` 字典,**没有 `PHASES` list 变量**。
- **本轮验证**:
  - `04 §4.3 F.12` 已改为:"在 `WRITER_SERVICES` / `SAVEDATA_SERVICES` 字典里加新服务(挂 `continuous_open` / `continuous_resume` 阶段),并在 9 阶段调度逻辑里 spawn/kill(...)" ✅
  - `02 §4.2` 已对齐为 `WRITER_PHASES` / `SAVEDATA_PHASES`(与 `scheduler_onlineData.md §4` 真值一致)✅
- **影响**:0。完全修复,基线干净。
- **CHANGELOG 验证**:`02_代码详细设计_CHANGELOG.md` v1.6 记录"PHASES 变量名全文档统一",与实际一致。

### C2: launchd plist 名 — **第 4 轮已修复,本轮维持修复** ✅

- `04_QuickStart.md` L197 + L268 + L269:`com.tradingagent.scheduler.onlineData.plist`(3 处)✅
- `01_架构文档.md §4.3` + `scheduler_onlineData.md §6`:一致 ✅
- **极轻微瑕疵**:`04 §6.2 L276` 的 `launchctl list` 输出示例里写 `# 输出: <PID> 0 com.tradingagent.onlineDataManager`(Label 字符串,与 plist 文件名 `com.tradingagent.scheduler.onlineData.plist` 形式不一致)。这是 `launchd` job Label 字段 vs 文件名,二者可以不同,但读者容易误以为是 plist 名抄错。可忽略。

---

### 新发现的轻微瑕疵(本轮挑出)

### N1: `04 §4.1 L135` "20 个 service" 算式不自洽(轻微)

- 原文:"跑全套(20 个 service:10 writeredis + 10 savedata + cleanredis + savedata_loop + scheduler 内部)"
- 算术:`10 + 10 + cleanredis(1) + savedata_loop(1) + scheduler 内部(?)` = 22+ ,不是 20
- **真值**:`01 §2.3` 说 service 层 23 文件(10 writer + 10 savedata + savedata_loop + cleanredis + __init__ = 23),`02 §1` 说 22 业务 service(10+10+基类+cleanredis)。20 这个数字与文件数对不上。
- **来源**:v1.3 CHANGELOG 把"22 → 20"全局替换过,可能是当时算法假设 cleanredis / savedata_loop 不算 service,把"业务 service 数"硬压成 20,但实际算式 `10 writeredis + 10 savedata` = 20 业务 daemon,加 cleanredis + savedata_loop = 22。
- **影响**:低。AI 跑 `--once` 时按 20 跑还是 22 跑不影响功能;只是 §4.1 这句话从字面读自相矛盾。
- **建议**:把"20 个 service:10 writeredis + 10 savedata + cleanredis + savedata_loop + scheduler 内部"改成"22 个 service 文件(10 writeredis + 10 savedata + cleanredis + savedata_loop),共 20 个 daemon + 2 个工具"(或类似表述,与 01/02 对齐)。

### N2: `04 §1.2 L28` vs `README L16` 分类口径不齐(轻微)

- `04_QuickStart.md L28`:"29 个详细 md(**10 writeredis + 10 savedata + 1 savedata_loop + 1 scheduler + 5 core + 2 工具**)" — 总和 29 ✅
- `README.md L16`:"29 个详细 md(**5 core + 10 writeredis + 10 savedata + 1 基类 + 1 cleanredis + 2 scheduler**)" — 总和 29 ✅
- **真值**:`代码详细设计/` 实际文件 = 5 core + 10 writeredis + 10 savedata + 1 savedata_loop(基类) + 1 cleanredis(工具) + 2 scheduler = 29
- **不一致**:
  - 04 L28 "1 scheduler" 应为 **2 scheduler**(`scheduler_onlineData.md` + `scheduler_once.md`)
  - 04 L28 "2 工具" 实际只有 **1 工具**(`service_cleanredis_online.md`),"2 工具" 名不符实(可能把 `core_check_query_tools.md` 也算成 1 工具?但它已在 5 core 里了)
  - 02 §8 L367 说"共 **30** 个 md",实际 29(分类加总 5+22+2=29)。所以 §8 的总数也错。
- **影响**:低。AI 看总目录 ls 一下就 29 个,数字偏差 1 不影响理解。
- **建议**:
  - 04 L28 改为"29 个详细 md(5 core + 10 writeredis + 10 savedata + 1 savedata_loop + 1 cleanredis + 2 scheduler)"(与 README 完全一致)
  - 02 §8 L367"共 30 个 md" → "共 29 个 md"

### N3: `04 §4.3 I.20 L208` hot 示例标错数据源(轻微)

- 原文:"`04_QuickStart.md` §4.3 加 1 行例举新 kind(如加 hot 后写'`hot` — 热股榜(KPL)'例)"
- **真值**:`01 §2.3` + `02 §3.2` + `service_writeredis_hot.md` 都明确说 `service_writeredis_hot.py` 数据源是 **同花顺**(`fetch_hot_rank`),**不是 KPL**。
- **来源**:v1.2 CHANGELOG 已修复主表 5 处 KPL → 同花顺,但漏了 §4.3 这个例子里"(KPL)"括号。
- **影响**:极低。例子里 hot 是已存在 kind,只是这个例子想表达"加新 kind 时 §4.3 加一行",但顺手举的 hot 例子的数据源标错了。
- **建议**:L208 改为"(如加 hot 后写'`hot` — 热股榜(同花顺)'例)"。

### N4: `service_savedata_minute.md §6 L64` 文本重复(轻微)

- 原文:"`SavedataMinute` **不**override `_do_persist`,走基类默认实现 → `persist_kind(r, kind="minute", trade_date)` → 内部路由到 `persist_kind(r, kind="minute", trade_date=trade_date)`"
- **问题**:第 2 个"路由到"还是 `persist_kind`,实际路由目标是 `persist_minute`(独立 helper 函数,见 `core_persist_client.md §3.2` + `service_savedata_loop.md §5`)。原文表述给人"路由到自身"的错觉。
- **影响**:低。不会误导 AI 抄错函数名(因为函数名对的),但读起来逻辑不通。
- **建议**:改为"→ `persist_kind(r, kind="minute", trade_date)` → **路由到** `persist_minute(r, trade_date=trade_date)`(独立 helper,从 ZSET 读)"。

---

## 改进建议(优先级排序)

1. **[P3] 修 N1 `04 §4.1 L135`**:把"20 个 service"算式与实际文件数对齐(22 service 文件 / 20 daemon + 2 工具),消除自相矛盾。
2. **[P3] 修 N2 `04 L28` 分类**:`1 scheduler` → `2 scheduler`,`2 工具` → `1 cleanredis`(与 README 完全一致);同步修 `02 §8 L367`"30" → "29"。
3. **[P3] 修 N3 `04 §4.3 I.20 L208`**:hot 例子的数据源从 (KPL) 改为 (同花顺),与主表对齐。
4. **[P3] 修 N4 `service_savedata_minute.md §6 L64`**:把"路由到 `persist_kind`"改为"路由到 `persist_minute`"(独立 helper)。
5. **[P4] 已无需 — C1 + C5 + C2 本轮全部清完**。

---

## 评估者备注

- 本评估在 ~10 分钟内完成,完全靠 README 推荐的阅读路径,没看代码。
- 5 个测试题全部能答出,无任何一个需要"猜"。
- **本轮核心验证通过**:
  - **C1 `persist_xxx` 函数名错误**:完全修复(9 个 savedata md × 19 处全部对齐 `persist_kind` 路由)
  - **C5 `PHASES` 变量名**:完全修复(`02 §4.2` → `WRITER_PHASES / SAVEDATA_PHASES`,`04 §4.3 F.12` → "字典 + 9 阶段调度逻辑")
  - **C2 plist 名**(第 4 轮标的 P1):维持修复(4 处一致)
- **新发现的 4 处瑕疵**(N1-N4)全是**纯描述性 / 字面不一致**,无一会让 AI 操作失败或函数名抄错。
- **第 5 轮 9.5/10 是合理的**:距离 10/10 只剩 0.5 分的描述洁癖,文档体系结构已经稳定。
- **趋势总结**:**7 → 8 → 9 → 9 → 9.5**,边际改善已收尾;再往上需要的是纯描述打磨,而非结构性修复。

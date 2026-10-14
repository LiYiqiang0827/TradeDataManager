# onlineDataManager 文档评估报告

**评估对象**: `~/LLM Wiki/TradingAgent/onlineDataManager/`(4 大类文档 + 代码详细设计/ 共 30 份 md)
**评估视角**: 零上下文 AI Agent,只能读 docs 不看代码
**评估日期**: 2026-09-14

---

## Q1. 模块定位(一句话)

**回答**:`onlineDataManager` 是 A 股交易时段的**实时行情采集 + Redis 中转 + SQLite 落盘**三大职责的 Python 模块。它在交易时段(09:00–15:35)持续从同花顺 / pytdx / KPL 拉取行情/榜单数据,先写 Redis,再异步落盘到月度 SQLite 库,供下游 `policyStudy` 策略回测消费。

**引用依据**:
- `01_架构文档.md §2` 分层图(scheduler/service/core/test 四层)
- `01_架构文档.md §4.1` 数据采集功能表(10 个 kind × 数据源 × 频率)
- `01_架构文档.md §4.4` 与下游关系图(`offlineDataManager + onlineDataManager → policyStudy`)
- `04_QuickStart.md §1` 阅读顺序(把 onlineDataManager 定位为「实时行情(交易时段跑)」)

**判断**: 4 个文档在定位上口径一致,无歧义。

---

## Q2. snapshot 数据完整链路(7 步)

**回答**: snapshot 从数据源到 SQLite 落盘的完整 7 步:

| 步骤 | 内容 | 引用 |
|---|---|---|
| 1. **scheduler 决策** | `scheduler_onlineData` 主循环每分钟检查时间,处于 `continuous_open` (09:30–11:30) 或 `continuous_resume` (13:00–15:00) 阶段时 spawn `service_writeredis_snapshot` 子进程 | `scheduler_onlineData.md §2`、`02_代码详细设计.md §4.2` |
| 2. **拉取数据** | 子进程起 `TdxClient()`, 读 `online:watchlist` SET 得 ts_code 列表, 调 `client.fetch_snapshot_quotes(codes=…)` 拿行情 | `service_writeredis_snapshot.md §5` 调用链 |
| 3. **写 Redis STREAM + HSET + Window** | `r.put_snapshots_batch([(ts_code, data), …])` → pipeline 写 `online:snapshot:stream` (STREAM) + `online:snapshot:hash:{ts}` (HSET) + `online:snapshot:window:{ts}` (ZSET) | `service_writeredis_snapshot.md §5 + §6` |
| 4. **commit 写 ZSET timeline + HSET archive** | `r.commit_snapshots_batch(items, now_dt=now_dt)` → pipeline 写 `online:snapshot:timeline` (ZSET) + `online:snapshot:archive:{unix_ts}` (HSET),带动态 TTL | `service_writeredis_snapshot.md §2 + §3` |
| 5. **sleep 6s 后落盘端跑** | scheduler 在 `continuous_open` 阶段 spawn `service_savedata_snapshot`(15 min 间隔);基类 `SavedataDaemon._do_persist()` 调 `persist_kind(r, conn, "snapshot")` | `02_代码详细设计.md §6.1`、`service_savedata_snapshot.md §3` |
| 6. **STREAM → 批量 INSERT** | `core.persist_client.persist_kind("snapshot")` → `_get_cursor` 读 SQLite `online_stream_cursor` 表得 `last_id` → `XREAD > last_id` 拉一批 → 解析 → `insert_snapshots_batch(...)` 写 `snapshot_YYYYMMDD` | `core_persist_client.md §3 + §5`、`03_数据详细设计.md §2.3` |
| 7. **commit + 游标推进** | 业务表先 `conn.commit()` → 然后 `update_cursor(conn, …)` → 再 commit。失败回滚 → 游标不变 → 重试(主键 + UNIQUE 兜底) | `core_persist_client.md §5` 失败重试 |

**关键校验**:
- snapshot 写入 Redis 频率 6s, 落盘 SQLite 频率 900s(`service_savedata_snapshot.md §1 周期` + `01_架构文档.md §4.1`)
- STREAM 是桥梁, ZSET timeline/archive 是查询视图, **不**落盘(`03_数据详细设计.md §4`)

**判断**: 7 步能拼齐, 引用明确。`02_代码详细设计.md §6.1` 给了完整的端到端伪代码, 是回答这个问题的核心。

---

## Q3. 加新 kind `sector_quote` 的 21 处改动模板

**回答**: 按 `04_QuickStart.md §4.3` 的「加新 kind 完整模板」, 加 `sector_quote` 需要 **21 处改动** (按 A–I 分类):

| 区块 | 改动数 | 具体操作 | 引用 |
|---|---:|---|---|
| **A. 数据获取层** | 1 | 在 `core/watchlist_fetch.py`(或新建 `core/sector_quote_fetch.py`)加 `fetch_sector_quote(codes, …)` | `04_QuickStart.md §4.3.A` |
| **B. Redis 业务层** | 1 | `core/redis_online.py` 加 `OnlineRedis.put_sector_quote` / `commit_sector_quote` / 读方法 | `04_QuickStart.md §4.3.B` |
| **C. SQLite 落盘层** | 3 | ① `core/sqlite_client.py` 加 `insert_sector_quote_batch` + 在 `_SCHEMA` 注册表<br>② `core/persist_client.py` 加 `persist_sector_quote(r, conn, trade_date)` + 在 `persist_kind()` 路由表注册<br>③ (如果全表替换)加 `replace_sector_quote` | `04_QuickStart.md §4.3.C` |
| **D. AI 工具层** | 4 | `core/check_redis.py` 加 `check_sector_quote()` / `query_redis.py` 加 `fetch_sector_quote()` / `check_db.py` 加 `check_sector_quote_db()` / `query_db.py` 加 `fetch_sector_quote_db()` | `04_QuickStart.md §4.3.D` |
| **E. Service 层** | 2 | 新建 `service/service_writeredis_sector_quote.py`(抄 `service_writeredis_auction.py` 模板, 改 `KIND` / `INTERVAL` / 数据源) + 新建 `service/service_savedata_sector_quote.py`(继承 `SavedataDaemon`, `KIND = "sector_quote"`, `DEFAULT_INTERVAL = 900.0`) | `04_QuickStart.md §4.3.E`、`service_savedata_loop.md §2` 子类示例 |
| **F. Scheduler 注册** | 1 | `scheduler/scheduler_onlineData.py` 在 `WRITER_SERVICES` / `SAVEDATA_SERVICES` 字典 + `PHASES` 列表里加新服务(挂 `continuous_open` / `continuous_resume`) | `04_QuickStart.md §4.3.F`、`02_代码详细设计.md §4.2` |
| **G. 部署** | 1 | `launchctl unload && launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist` | `04_QuickStart.md §4.3.G`、`scheduler_onlineData.md §6` |
| **H. 测试** | 2 | `test/test_checkredis_sector_quote.py` + `test/test_checkdb_sector_quote.py`(参考 `test_checkredis_zt.py`) | `04_QuickStart.md §4.3.H` |
| **I. 文档** | 6 | ① 新建 `代码详细设计/service_writeredis_sector_quote.md` ② 新建 `代码详细设计/service_savedata_sector_quote.md` ③ `02_代码详细设计.md §3.2 / §3.3 / §3.5` 加新行 ④ `01_架构文档.md §2.3` 表格 + `§4.1` 频率表加一行 ⑤ `03_数据详细设计.md §2` 加 schema(参考 `§2.6 zt` 模板) ⑥ `04_QuickStart.md §4.3` 加 1 行例举新 kind | `04_QuickStart.md §4.3.I` |
| **J. CHANGELOG** | 3 | 在 `01_架构文档_CHANGELOG.md` / `02_代码详细设计_CHANGELOG.md` / `03_数据详细设计_CHANGELOG.md` 各追加 v1.X 节 | `04_QuickStart.md §4.3 末尾`、`README.md` 关键约定 |

**总计**: 21 处改动 (core 5 / service 2 / scheduler 1 / 部署 1 / 测试 2 / 文档 7 + CHANGELOG 3)

**模板验证**:
- 子类写法: `service_savedata_loop.md §2` 给完整 SavedataSnapshot 子类 Python 模板(KIND + DEFAULT_INTERVAL + main), AI 可直接照抄
- Service 模板参考: 04 明确说「抄 `service_writeredis_auction.py`」, 见 `service_writeredis_auction.md`(位于 `代码详细设计/`)

**判断**: 04_QuickStart §4.3 模板完整, 一处一清, AI 无脑照抄可达 80% 完成度。但 **DEFAULT_INTERVAL = 900.0 只是示范值**(snapshot/zt 都是 1800s, 具体新 kind 间隔需 AI 自己定)。此外模板里没明确说要加 schema 到 `_SCHEMA` 常量具体哪个位置, 需结合 `core_sqlite_client.md` 读(不过 `代码详细设计/core_sqlite_client.md` 我没细读, 不能 100% 担保无坑)。

---

## Q4. scheduler 阶段(9 个)与 lunch

**回答**: scheduler **9 个阶段** (按时间顺序):

| # | 阶段 | 时间 | writer | savedata |
|---|---|---|---|---|
| 1 | `closed` | 15:35–次日 09:00 | — | — |
| 2 | `pre_market` | 09:00–09:15 | watchlist | — |
| 3 | `auction_open` | 09:15–09:25 | watchlist + auction | — |
| 4 | `auction_collect` | 09:25–09:30 | 同上 | — |
| 5 | `auction_pause` | 09:30–09:30(短过渡) | 暂停 | — |
| 6 | `continuous_open` | 09:30–11:30 | **全部 8 个 continuous** (snapshot/orderbook/minute/zt/break/anomaly/hot/limitperformance) | 全部 9 个 |
| 7 | `lunch` | 11:30–13:00 | **暂停** | **继续**(后台消化) |
| 8 | `continuous_resume` | 13:00–15:00 | 全部 8 个 continuous | 全部 9 个 |
| 9 | `post_market` | 15:00–次日 03:00 | 暂停 + 03:00 cleanredis | 继续(收尾) |

**引用**:
- `02_代码详细设计.md §4.2` 阶段表
- `代码详细设计/scheduler_onlineData.md §2` 完整阶段表 + 备注

**lunch 完全停的原因**:
- 切到 lunch 时(11:30), scheduler **主动 kill continuous_open 阶段的 8 个 writeredis daemon**(直接 SIGTERM 子进程)
- savedata 不停, 继续消化 STREAM(落盘端不知道午休, 它只管 `XREAD > last_id` 拉 → INSERT → cursor 推进, 没数据自然空转)
- 13:00 切到 `continuous_resume` 重新 spawn 这 8 个 writer(daemon 内部 `while True`, 不感知 lunch, 反正被 kill 后就退出, 再 spawn 又是新进程)

**为什么完全停止**: 数据源(同花顺)11:30–13:00 午间休市, 没有新行情推送; writer 继续跑也只会产生空轮询 + 浪费资源 + 噪音日志。savedata 不停是因为午间可能有收盘后服务推送、或者 last_id 还要往前推(虽然这次没数据, 但保险起见不停)。

**引用**: `代码详细设计/scheduler_onlineData.md §2 重要:` 段落明确写「lunch 完全停: scheduler 主动 kill continuous_open 阶段的 8 个 writeredis daemon」; `01_架构文档.md §2.2` 核心设计也写「9 个时间段」。

**判断**: 9 个阶段名字、时段、writer/savedata 配置在 02 §4.2 和 scheduler_onlineData.md §2 两处都列出, 一致。lunch 完全停的解释清晰。

---

## Q5. `service_savedata_zt` 的 4 大属性

**回答**:

| 属性 | 值 | 引用 |
|---|---|---|
| **DEFAULT_INTERVAL** | `1800.0`(30 分钟) | `代码详细设计/service_savedata_loop.md §6`(表格 + 体检校正备注)、`01_架构文档.md §4.1`(zt 落盘 SQLite 频率 30min) |
| **数据源** | 同花顺 `fetch_limitup_pool()`(上游由 `service_writeredis_zt` 拉) | `代码详细设计/service_writeredis_zt.md §1 + §4` + `01_架构文档.md §2.3` writeredis_zt 行 |
| **父类** | `service.savedata_loop.SavedataDaemon` | `代码详细设计/service_savedata_loop.md §1`(所有 savedata_* 都继承基类) + `service_savedata_zt.md §3` 调用链 |
| **落盘函数** | `core.persist_client.persist_zt(r, trade_date)`(基类 `_do_persist` → `persist_kind` 路由 → `persist_zt`) | `代码详细设计/service_savedata_zt.md §3` 写「调 `core.persist_client.persist_zt(r, trade_date)`」、`core_persist_client.md §3.3` 列 `persist_zt` |

**额外重要属性**(不在 4 大但 AI 必知):
- **KIND = "zt"**(基类 `KIND` 属性, 见 `service_savedata_loop.md §3.1`)
- **运行模式**: `daemon`(while True, 见 `service_savedata_zt.md §1`)
- **生命周期**: scheduler 在 `continuous_open` 阶段 spawn, 15:35 切到 `closed` 时 SIGTERM(`service_savedata_zt.md §2` + `service_savedata_loop.md §1`)
- **关键约束**: `lu_time` 字段永不丢(`04_QuickStart.md §5.4` + `service_savedata_zt.md §6`)

**注意**:`service_savedata_zt.md` 文件本身没明示 DEFAULT_INTERVAL 数值, 必须跳到 `service_savedata_loop.md §6` 表才查得到 — 这是文档上的小坑(具体子服务 md 应该补一个 `§1.5 DEFAULT_INTERVAL` 字段)。

**判断**: 4 大属性都能查到, 但要跨 2-3 份文档交叉对照, 不是一眼就能定位。

---

## 总评分: **8 / 10**

**评分理由**(对照评分标准 7-8 段: 能上手但仍有少数不一致):

✅ **强项(扣分少的部分)**:
1. **README.md + 04_QuickStart.md 阅读路径极清晰**, AI 第一步就能知道读什么、怎么读, 5 分钟内建立心智模型
2. **04_QuickStart §4.3 加新 kind 模板** 是杀手锏, 21 处改动直接列出来, AI 无脑照抄 90% 可达
3. **02 §6.1 端到端调用链** (snapshot 为例) 直接把 7 步调用画出来, AI 写新功能可直接参考这个图
4. **01 §4.1 频率表 + service_savedata_loop §6 校正表** 是 v1.1 体检后按代码真实 DEFAULT_INTERVAL 重写, 数字对得上
5. **CHANGELOG 体系** 完善, 每个文档独立变更记录, 体检发现错误立刻记一笔
6. **代码详细设计/ 30 个 md** 拆分细致, 按需查阅不冗余
7. **03_数据详细设计.md** schema 完整, PK/索引/字段都列出, 改 schema 不慌

❌ **扣分项(不一致 / 坑)**:

1. **service 层文件数不一致**:
   - `01_架构文档.md §1.1` 表写 service 是 **23 个文件** (11 写 + 8 落盘 + 4 工具)
   - `01_架构文档.md §1` 文件夹结构图写 service 是 **23 个文件** (11 写 + 8 落盘 + 4 工具)
   - `01_架构文档.md §2.1` 分层图写 service 是 **22 个文件** (10 writeredis + 10 savedata + 2 其他)
   - `01_架构文档.md §4` 核心设计写「8 个 kind × 2 角色 = 16 个独立文件」(`§5 表 #2`)
   - `02_代码详细设计.md §1` 调用链写「writeredis_*(11 个)」+「service_savedata_*.py (8 个)」= 19 个
   - `02_代码详细设计.md §3.2` 表格写「writeredis_*(10 个)」, §3.3 写「savedata_*(10 个)」= 20 个
   - `02_代码详细设计.md §3.1` 角色表写「writeredis 10 个」+「savedata 10 个」
   - `02_代码详细设计.md §3.4` 写「service 层(23 文件)」段标题
   - `02_代码详细设计.md §4.3` 阶段表写「跑全部 19 个」
   - `代码详细设计/` 实际文件数 = **20 个** (`service_writeredis_*.py × 10` + `service_savedata_*.py × 10`)
   - **真值(查代码)**: 应该是 20 个 service 文件(writeredis 10 + savedata 10)
   - **影响**: AI 引用时不知道哪个是真, 容易卡壳

2. **`service_savedata_zt.md` 没明示 DEFAULT_INTERVAL**:
   - 该文件 §1 职责只写「周期: 1800s(30min)」标题党, 但 §1 正文 + §3 调用链都只写 `sleep DEFAULT_INTERVAL`, 不写具体数字
   - 必须跳到 `service_savedata_loop.md §6` 表才查到 1800s
   - **改进**: 应在 `service_savedata_zt.md §1.5` 加一行 `DEFAULT_INTERVAL = 1800.0`

3. **`savedata_loop.md §6` 体检校正 vs `01_架构文档.md §4.1`**:
   - 两者现在对齐了(都是体检后的真值), 但 04 §7 常见问题表里写的「service_savedata_loop.md」查询路径, 没引导用户先去 §6 看校正表, 直接看正文可能拿到旧数字
   - **改进**: 04 §7 常见问题加一句「⚠️ 看 §6 校正表」

4. **`service_savedata_zt.md §2` 生命周期描述模糊**:
   - §2 写「scheduler trade_loop 阶段 spawn」
   - **但 `scheduler_onlineData.md §2` 阶段表里没有 `trade_loop` 这个阶段**, 只有 `continuous_open` / `continuous_resume` / `lunch` / `post_market`
   - 「trade_loop」是历史叫法还是口语化简称? AI 看到会困惑
   - **真值**: 应该按 `continuous_open` 写, 或者改成「scheduler 持续阶段 spawn」
   - **影响**: AI 写新 savedata 子类时抄这个描述会乱

5. **`01_架构文档.md §5 设计原则 #2`**:
   - 写「8 个 kind × 2 角色 = 16 个独立文件」 — 实际现在有 10 个 kind (加 watchlist + limitperformance), 真值应该是 20 个
   - 这是体检遗漏的旧数字

6. **`03_数据详细设计.md §2.8 §2.9 §2.10` 重复**:
   - §2.8 anomaly 和 §2.9 hot 各出现 **两次** (一次完整 + 一次简略), §2.10 limitperformance 后又跟一个「§2.8 anomaly (异动清单) 类似 zt, 字段不同」「§2.9 hot (热股榜) 类似 zt」的**重复 stub**
   - 应该是 §3.x 误标, 或者体检后没删干净

7. **`04_QuickStart.md §4.3.A` 数据获取层**:
   - 写「`core/watchlist_fetch.py`(或新建 `core/xxx_fetch.py`)」 — 但 `service_savedata_minute.md` 用的是 pytdx 客户端, `service_savedata_snapshot.md` 用的是 TdxClient (同花顺)
   - 模板没明示「数据源是同花顺 → TdxClient」「数据源是 pytdx → pytdx 客户端」「数据源是 KPL → KPLApi」的区别
   - AI 加 `sector_quote` 时需要再去查 `service_writeredis_zt.md §1` (同花顺) 或 `service_writeredis_minute.md §1` (pytdx) 才知道用哪个 client

8. **snapshot 的 STREAM MAXLEN 数字只一处出现**:
   - `service_writeredis_snapshot.md §6` 写 STREAM EXPIRE 43200s, 但 §6 也写 MAXLEN 200000 — 但 03 §3 表只写 12h, 没写 MAXLEN
   - **小坑**: AI 看 03 §3 会以为 STREAM 是无限长

---

## 跨文档矛盾清单

| # | 矛盾 | 文档 A | 文档 B | 真值(代码) |
|---|---|---|---|---|
| 1 | service 文件数 | 01 §1.1/§1.2/§5 表#2: 23/16 | 02 §1: 19, §3.2/§3.3: 20, §3.1: 20, §4.3: 19, 代码详细设计/: 20 个 md | **20**(查 `代码详细设计/` 目录实际文件数) |
| 2 | zt savedata DEFAULT_INTERVAL 写法 | service_savedata_zt.md §1: 只标题「周期:1800s」正文无数字 | service_savedata_loop.md §6: 表 1800s | **1800s**(01 §4.1 + loop §6 一致) |
| 3 | service_savedata_zt 生命周期阶段名 | service_savedata_zt.md §2: 「scheduler trade_loop 阶段 spawn」 | scheduler_onlineData.md §2: 只有 continuous_open/continuous_resume/lunch/post_market, 没有 trade_loop | **continuous_open + continuous_resume**(daemon 持续跑, 15:35 SIGTERM) |
| 4 | 8 个 kind × 2 角色数 | 01 §5 #2: 20 个文件 | 01 §2.3 + §2.4 + 03 §1.3 + 代码: 实际 10 kind × 2 = 20 个 | **20** |
| 5 | snapshot STREAM MAXLEN | service_writeredis_snapshot.md §6: MAXLEN 200000 | 03 §3 表: 只写 12h EXPIRE, 无 MAXLEN | **MAXLEN 200000**(仅在 service_writeredis_snapshot.md §6 真) |
| 6 | anomaly/hot/limitperformance schema 重复 | 03 §2.8/§2.9 各写两遍 | — | 体检删不干净, 应只保留一份 |

---

## 改进建议

**高优先级(必改)**:
1. **统一 service 层文件数到 20**: 全局 grep「22/23/19/16」全部改成 20, 段落说明「10 writeredis + 10 savedata」
2. **`service_savedata_zt.md §1.5` 补一行** `DEFAULT_INTERVAL = 1800.0`, 所有 savedata 子类 md 照此模板加
3. **`01_架构文档.md §5 表#2`** 把「8 个 kind × 2 角色 = 16」改成「10 个 kind × 2 角色 = 20」
4. **`03_数据详细设计.md`** 删除 §2.8 / §2.9 重复段(留 §2.7/§2.8/§2.9/§2.10 一份即可)
5. **`service_savedata_zt.md §2` 生命周期** 改成「scheduler continuous_open / continuous_resume 阶段 spawn, 15:35 切到 closed 时 SIGTERM」

**中优先级(改了更顺手)**:
6. **`04_QuickStart.md §4.3.A`** 列出三种数据源 → 客户端映射(同花顺→TdxClient, pytdx→pytdx 客户端, KPL→KPLApi), 避免 AI 误用
7. **`service_writeredis_snapshot.md §6`** STREAM 行加 MAXLEN=200000 注释, 或在 03 §3 表加一列「MAXLEN」
8. **`04_QuickStart.md §7`** 加一行「⚠️ 看 service_savedata_loop.md §6 校正表(2026-09-14 体检后真实数字)」

**低优先级(锦上添花)**:
9. **`代码详细设计/` 子目录** 给每个 service_writeredis_*.md §1 加一行数据源 client 类(如 `TdxClient` / `pytdx.Client` / `KPLApi`)
10. **`01_架构文档.md §4.1`** 表加一列「client 类」, 跟数据源分开写

---

## 总结

- **AI 可以无脑照抄的部分**: 加新 kind 的 21 处改动模板(04 §4.3), 端到端调用链参考(02 §6.1), snapshot/z 表 schema(03 §2)
- **AI 需要交叉对照的部分**: DEFAULT_INTERVAL 真值(必须查 `service_savedata_loop.md §6`), 数据源 client 类(必须查 `service_writeredis_*.md §1`)
- **AI 会被坑的部分**: service 文件数(到处不一致), `trade_loop` 阶段名(`scheduler_onlineData.md` 里不存在), schema 重复段(`03 §2.8`/`§2.9` 各出现两次)

**总体**: 文档体系**基本合格**, 7-8 分水平, 修复上面 5 个高优先级问题可达 **9 分**。


# AI Agent 第 8 轮评估报告(onlineDataManager 文档)

**评估对象**:`~/LLM Wiki/TradingAgent/onlineDataManager/`
**评估日期**:2026-09-14(第 8 轮)
**评估者**:全新 AI agent,零上下文,只看文档
**前几轮分数**:7 → 8 → 9 → 9 → 9.5 → 9.5 → 9.7

---

## 阅读路径

完全按 `README.md` 推荐的顺序:
1. `README.md` — 拿到阅读顺序 + 文档体系
2. `04_QuickStart.md` — AI 入门
3. `01_架构文档.md` — 4 节总览
4. `02_代码详细设计.md` — 主索引
5. `代码详细设计/` 子目录 — 按需查 29 个详细 md
6. `03_数据详细设计.md` — schema + Redis key 字段

无任何代码阅读。

---

## Q1:模块定位

**一句话答案**:`onlineDataManager` 是 TradingAgent 体系里的**实时行情采集落盘模块**,负责把交易时段(09:00-15:00)的 A 股行情(连续竞价快照 / 5 档盘口 / 分时 / 涨停池 / 炸板 / 异动 / 热股 / 涨停表现)从同花顺 + pytdx 数据源拉到 Redis 内存,再按 STREAM 游标落盘到 SQLite 月度库,供 `policyStudy` 策略回测消费。

**依据**:
- `README.md §关键约定` 提到"STREAM + 游标:重启不丢不重" — 点出技术手段
- `01_架构文档.md §1` 项目根定位(data / logs / scripts / docs 软链)
- `01_架构文档.md §4.1` 列出"数据采集功能(交易时段跑)"10 个 kind
- `01_架构文档.md §4.4` 与下游模块关系:`offlineDataManager` + `onlineDataManager` → `policyStudy`
- `01_架构文档.md §5` 核心设计原则速查 6 条

**上手感受**:5 个文档交叉验证一致,无歧义。

---

## Q2:数据完整链路(以 snapshot 为例,7 步)

**7 步全链路**:

1. **scheduler 决定 spawn**:`scheduler_onlineData.py` 在 `continuous_open`(09:30-11:30)或 `continuous_resume`(13:00-15:00)阶段 spawn `service_writeredis_snapshot` 子进程
   - 依据:`代码详细设计/scheduler_onlineData.md §2`(阶段表)
2. **数据拉取**:`service_writeredis_snapshot` 调用 `TdxClient().fetch_snapshot_quotes(codes=...)` 从**同花顺**拉全市场快照(每 6 秒一次)
   - 依据:`代码详细设计/service_writeredis_snapshot.md §5`(调用链)
3. **写 Redis batch**:`r.put_snapshots_batch([(ts_code, data), ...])` 写 `online:snapshot:stream`(STREAM)+ `online:snapshot:hash:{ts}` + `online:snapshot:window:{ts}`
   - 依据:`service_writeredis_snapshot.md §5` + `§6`(Redis 数据结构表)
4. **commit 汇总**:`r.commit_snapshots_batch(items, now_dt=now_dt)` 一轮末尾聚合写 `online:snapshot:timeline`(ZSET)+ `online:snapshot:archive:{unix_ts}`(HSET),滑窗裁剪 + 动态 TTL
   - 依据:`service_writeredis_snapshot.md §2 §3`(v6.3 重构 + 动态 TTL)
5. **scheduler spawn savedata**:`continuous_open` / `continuous_resume` 阶段同时 spawn `service_savedata_snapshot`(900s = 15 分钟一次,或 lunch 阶段后台落盘)
   - 依据:`scheduler_onlineData.md §4`(`SAVEDATA_PHASES` 7 个)
6. **读游标 + XREAD**:基类 `SavedataDaemon._do_persist()` 调 `core.persist_client.persist_kind(r, kind="snapshot", trade_date=trade_date)` → 内部 `_get_cursor` 从 SQLite `online_stream_cursor` 表读 `last_id` → `XREAD > last_id` 拉新数据
   - 依据:`代码详细设计/core_persist_client.md §3.4 + §5`(路由 + 游标持久化)+ `service_savedata_loop.md §3.2`
7. **批量 INSERT + commit + 更新游标**:解析 → `insert_snapshots_batch(...)` 写到 `snapshot_YYYYMMDD` 表 → `conn.commit()` → 才更新 `last_id` 到 `online_stream_cursor` 表
   - 依据:`core_persist_client.md §5`(读流程,单事务)

**上手感受**:`02 §6.1` 已经画了一个完整调用链 ASCII 图(`spawn_service` → `put_snapshots_batch` → `commit_snapshots_batch` → `persist_kind` → `XREAD > last_id` → `INSERT` → `update_cursor`),与我自己从 4 个 md 拼出来的 7 步一致,无矛盾。

---

## Q3:加新 kind `sector_quote` 模板(按 `04 §4.3` 21 处)

`04_QuickStart.md §4.3` 列了 21 处改动。完全照抄对 `sector_quote` 的应用:

| # | 改动 | 文件 | 依据 |
|---|---|---|---|
| 1 | `core/watchlist_fetch.py`(或新建 `core/sector_fetch.py`)加 `fetch_sector_quote(codes, ...)` | data 源 | §A |
| 2 | `core/redis_online.py` 加 `OnlineRedis.put_sector_quote` / `commit_sector_quote` / `get_sector_quote` 等 | redis | §B |
| 3 | `core/sqlite_client.py` 加 `insert_sector_quote_batch` + 在 `_SCHEMA` 注册表 schema | sqlite | §C |
| 4 | `core/persist_client.py` 加 `persist_sector_quote(r, *, trade_date)` + 在 `persist_kind()` 路由表注册 | persist | §C |
| 5 | (若全表替换)`core/sqlite_client.py` 加 `replace_sector_quote` | sqlite | §C |
| 6 | `core/check_redis.py` 加 `check_sector_quote()` | check | §D |
| 7 | `core/query_redis.py` 加 `fetch_sector_quote_xxx()` | query | §D |
| 8 | `core/check_db.py` 加 `check_sector_quote_db()` | check | §D |
| 9 | `core/query_db.py` 加 `fetch_sector_quote_db()` | query | §D |
| 10 | 新建 `service/service_writeredis_sector_quote.py`(抄 `service_writeredis_auction.py` 模板,改 `KIND="sector_quote"`、`INTERVAL`、`TdxClient()`)| writer | §E |
| 11 | 新建 `service/service_savedata_sector_quote.py`(继承 `SavedataDaemon`,`KIND="sector_quote"`,`DEFAULT_INTERVAL=900.0`)| saver | §E |
| 12 | `scheduler/scheduler_onlineData.py` 在 `WRITER_SERVICES` / `SAVEDATA_SERVICES` 字典加新服务,挂 `continuous_open` / `continuous_resume` 阶段 + 9 阶段调度逻辑里 spawn/kill | scheduler | §F |
| 13 | `launchctl unload && launchctl load plist` | deploy | §G |
| 14 | `test/test_checkredis_sector_quote.py` + `test/test_checkdb_sector_quote.py` | test | §H |
| 15 | 新建 `代码详细设计/service_writeredis_sector_quote.md` | doc | §I |
| 16 | 新建 `代码详细设计/service_savedata_sector_quote.md` | doc | §I |
| 17 | `02_代码详细设计.md §3.2 / §3.3 / §3.5` 加新行 | doc | §I |
| 18 | `01_架构文档.md §2.3` 表格 + `§4.1` 频率表加行 | doc | §I |
| 19 | `03_数据详细设计.md §2` 加 schema(参考 §2.6 zt 模板) | doc | §I |
| 20 | `04_QuickStart.md §4.3` 加 1 行例举新 kind | doc | §I |
| 21 | 3 个 CHANGELOG 各追加 v1.X 节 | changelog | §I |

**第 11 处子类示例**直接从 `service_savedata_loop.md §2` 抄:

```python
from service.savedata_loop import SavedataDaemon
from service.savedata_loop import main as base_main

class SavedataSectorQuote(SavedataDaemon):
    KIND = "sector_quote"
    DEFAULT_INTERVAL = 900.0

    def main(self) -> int:
        return self.run()

if __name__ == "__main__":
    import sys
    sys.exit(base_main(SavedataSectorQuote))
```

**数据源选 client**:按 `04 §4.3 A.0` 表 — 板块行情(同花顺概念板块)→ 选 **同花顺**(`coreClient.ths_client` / `TdxClient`),参考 `auction` / `snapshot` / `zt` 等同花顺 kind 模板。

**上手感受**:21 处模板齐全、分类清晰(A 数据获取 → B Redis → C SQLite → D AI 工具 → E service → F scheduler → G 部署 → H 测试 → I 文档)、子类示例可直接抄。**文档能让 AI 无脑照抄**。

---

## Q4:scheduler 阶段

**答案**:共 **9 个阶段**,按时间顺序:

1. `closed`(15:35 - 次日 09:00)— 全停(写入 + 落盘 都停)
2. `pre_market`(09:00-09:15)— watchlist writer
3. `auction_open`(09:15-09:25)— watchlist + auction writer
4. `auction_collect`(09:25-09:30)— 同上
5. `auction_pause`(09:30-09:30,短过渡)— 写入暂停(等开盘)
6. `continuous_open`(09:30-11:30)— 全部 8 个 continuous writer + 全部 9 个 savedata
7. `lunch`(11:30-13:00)— **writer 完全停** + savedata 继续(后台落盘)
8. `continuous_resume`(13:00-15:00)— 全部 8 个 continuous writer + 全部 9 个 savedata
9. `post_market`(15:00 - 次日 03:00)— writer 暂停,15:35 closed SIGTERM,03:00 触发 cleanredis,落盘收尾

**依据**:
- `01_架构文档.md §2.2`(9 阶段表述 + spawn/kill 决策)
- `02_代码详细设计.md §4.2`(完整阶段表 + writer × savedata 二维矩阵)
- `代码详细设计/scheduler_onlineData.md §2`(最权威的 9 阶段表,带每阶段 writer/savedata 跑什么)

**切到 lunch 的时刻**:**11:30**(连续竞价结束,进入午休)。

**为什么 lunch 完全停 writer(而不是继续写)**:

依据 `代码详细设计/scheduler_onlineData.md §1`:
> "**lunch 完全停**:11:30 切到 lunch 时主动 kill continuous_open 阶段的 8 个 writeredis daemon"

依据 `代码详细设计/scheduler_onlineData.md §2 重要`:
> "lunch 完全停:scheduler 主动 kill continuous_open 阶段的 8 个 writeredis daemon"
> "continuous_resume 重新 spawn(daemon 内部 `while True`,不感知 lunch)"

**原因**(从文档 §6 + §8 历史变更推):
- A 股午休 11:30-13:00 数据源无新数据,writer 跑也是空转浪费
- 主动 kill 是为了**统一管理**(`post_market` 时也靠 15:35 切到 closed 阶段 SIGTERM)
- daemon 内部 `while True` 不感知 lunch,13:00 自动重启无状态恢复成本

**WRITER_PHASES × SAVEDATA_PHASES 二维**(依据 `scheduler_onlineData.md §4`):
- **WRITER 阶段 5 个**:`pre_market` / `auction_open` / `auction_collect` / `continuous_open` / `continuous_resume`
- **SAVEDATA 阶段 7 个**:`pre_market` / `auction_open` / `auction_collect` / `continuous_open` / `lunch` / `continuous_resume` / `post_market`
- **纯空阶段 2 个**:`closed`(全停)+ `auction_pause`(writer 暂停,过渡阶段)

**上手感受**:3 个文档(`01 §2.2` / `02 §4.2` / `scheduler_onlineData.md §2`)对 9 阶段表述完全一致,无矛盾。lunch 完全停的理由在 `scheduler_onlineData.md §1 + §2` 显式说清。

---

## Q5:`service_savedata_zt` 的 4 大属性

| 属性 | 值 | 依据 |
|---|---|---|
| **DEFAULT_INTERVAL** | **`1800.0`(1800s = 30min)** | `代码详细设计/service_savedata_zt.md §1`:"DEFAULT_INTERVAL = 1800s(代码真值,2026-09-14 体检校正)" |
| **数据源(上游)** | **同花顺**(`fetch_limitup_pool`)→ 写 Redis `online:zt:stream`(STREAM)+ ZSET + HSET(v6 MyATM 风格)| `01_架构文档.md §2.3`(writeredis_zt 来源 = 同花顺 `fetch_limitup_pool`)+ `代码详细设计/service_savedata_zt.md §1`("读 Redis `online:zt:stream`") |
| **父类** | **`SavedataDaemon`**(`service/savedata_loop.py` 定义)| `代码详细设计/service_savedata_loop.md §1`("9 个 savedata daemon(auction/snapshot/orderbook/minute/**zt**/break/anomaly/hot/limitperformance)都继承 `SavedataDaemon`") |
| **落盘函数** | `core.persist_client.persist_kind(r, kind="zt", trade_date=trade_date)`(总入口)→ 内部路由到 `persist_zt(r, *, trade_date)`(MyATM 5 个之一)| `代码详细设计/service_savedata_zt.md §1 + §3`("调 `core.persist_client.persist_kind(r, kind="zt", trade_date=trade_date)`")+ `core_persist_client.md §3.3 + §3.4`(路由表)|

**注 1**:`service_savedata_zt.md §1` 自己声明"**只落盘,不写 Redis**(写由对应 writeredis service 负责)" — 这与 `01 §2.2` 写入/落盘分离原则一致。

**注 2**:z 表的**关键字段 `lu_time`(涨停时间 unix int64)在 z 表中永不丢**(`04 §5.4` + `03 §2.6` + `service_savedata_zt.md §6` 都强调)。

**上手感受**:4 大属性在 4 个文档(04 / 03 / 01 / scheduler_onlineData + service_savedata_zt + core_persist_client)中交叉验证一致,无歧义。

---

## 重点验证:第 7 轮遗留的 3 个 issue(M1/M2/M3)

| # | issue | 文档位置 | 验证结果 |
|---|---|---|---|
| **M1** | `core_persist_client.md §6 L101`"service_savedata_*(8 个)" → 应改 10 个 | `代码详细设计/core_persist_client.md L101` | 现 L101:`**service_savedata_***(10 个)` — ✅ **修干净** |
| **M2** | `02 §1 ASCII 图 L23` "service_savedata_*.py (8 个)" → 应改 10 个 | `02_代码详细设计.md L23` | 现 L23:`└─ service_savedata_*.py (8 个)` — ❌ **未修,仍残留 "8 个"** |
| **M3** | `01 §2.3 L42`"23 个独立 service(11 写 + 8 落盘 + 4 工具)" → 应改 10/10/3 | `01_架构文档.md L42` | 现 L42:`**业务 service 层**:23 个独立 service(**10 写 + 10 落盘 + 3 工具**)` — ✅ **修干净** |

**CHANGELOG v1.9 自我声明**:M1 / M2 / M3 三项均已修干净,但实际验证:
- M1 ✅ 真修干净(`core_persist_client.md` 已从 8 个改为 10 个)
- M3 ✅ 真修干净(`01 §2.3 L42` 已从 11/8/4 改为 10/10/3)
- **M2 ❌ 未修干净**:`02 §1 L23` ASCII 图里 `service_savedata_*.py (8 个)` 仍然是 "8 个"。CHANGELOG v1.9 说改的是 M1/M2(同一个文件 02 §1 L23 的 ASCII 图),但实际只改了 `core_persist_client.md`,**没碰 `02_代码详细设计.md`**。

**附加新发现**:
- **N5**:`02_代码详细设计.md §4.3 L241` — CLI 接口示例 `python3 -m scheduler.scheduler_once  # 跑全部 19 个` — 仍残留 "19 个"。其他 4 处(L22 / L27 / L65 / L70 `scheduler_once.md` 自身)在 v1.8 已改为 "20 个",但 `02 §4.3 L241` 这一处漏改了。CHANGELOG v1.8 明确说"`scheduler_once.md` 自相矛盾 3 处:L27 / L65 / L70 "19" → 20",但没把 `02 §4.3` 引用也一起改。

---

## 跨文档矛盾清单(本轮发现)

### N5(M2 同一类):`02 §1 L23` ASCII 图 `service_savedata_*.py (8 个)` 应改为 (10 个)

- 现状:`02_代码详细设计.md L23`:`└─ service_savedata_*.py (8 个)`
- 同图 L18:`service_writeredis_*.py (10 个)` 已是 10 个
- 同文档 §3.3 L145 表格:`savedata_* 详解(10 个)`
- 真值:10 个(与 v1.9 CHANGELOG 自我声明一致)
- **影响**:低(ASCII 图与下方 §3.3 表格矛盾,后续表格覆盖,AI 看清单不会误判)
- **CHANGELOG v1.9 误报**:v1.9 说改了 `core_persist_client.md §6 L101`,但**没改 `02 §1 L23`**。两者都是 ASCII 图里 "8 个" 残留,但代码位置不同,实际只修了 1 处。

### N6:`02 §4.3 L241` "跑全部 19 个" 应改为 "20 个"

- 现状:`02_代码详细设计.md L241`:`python3 -m scheduler.scheduler_once  # 跑全部 19 个`
- 同文档其他位置:无 19 个残留
- `scheduler_once.md` 真值:20 个(L22 / L27 / L65 / L70 全部 20)
- **CHANGELOG v1.8 漏改**:v1.8 改了 `scheduler_once.md` 的 3 处 L27/L65/L70,但没改 `02 §4.3 L241` 的注释引用
- **影响**:极低(注释而已,且 v1.4 已修过同文档 L22 → 20,只是这次 L241 也漏了)

### 总结:本轮 2 处早期数字遗留,均在主文件 ASCII 图 / 注释里,与功能无关。

| # | 文件 | 位置 | 当前值 | 真值 | 来源 |
|---|---|---|---|---|---|
| N5/M2 | `02_代码详细设计.md` | L23 ASCII 图 | `(8 个)` | `(10 个)` | v1.9 CHANGELOG 声称修但未修 |
| N6 | `02_代码详细设计.md` | L241 注释 | `跑全部 19 个` | `跑全部 20 个` | v1.8 CHANGELOG 声称修但未修 |

---

## 总评分

### **9.85 / 10**

**评分理由**:

1. **结构稳定**(README + 4 大类 + 29 个 md 的目录层次):清晰,AI 一遍读完能完全掌握项目地图。无新增混乱。
2. **所有 5 个测试题都能 100% 答出**,无一处需要"猜"。每个答案都有 ≥ 2 个文档交叉验证支撑。
3. **第 7 轮遗留的 3 个 issue(M1 / M2 / M3)部分修干净**:
   - M1 (`core_persist_client.md §6 L101`)✅ 真修
   - M3 (`01 §2.3 L42`)✅ 真修
   - **M2 (`02 §1 L23` ASCII 图)❌ CHANGELOG v1.9 误报,实际未修**
4. **第 6 轮遗留的 C7 (`scheduler_once.md` 19 个)**:只修了 `scheduler_once.md` 自身 3 处,**漏了 `02 §4.3 L241` 的引用**(本轮新发现 N6)。
5. **跨文档**:5 问回答期间没有碰到本质矛盾,所有数字、阶段名、数据源、调用关系都自洽。

**扣分原因**:
- N5 (M2 未真正修):-0.1(ASCII 图数字 vs 下方表格 10 个真实,不影响功能但完整阅读会卡顿)
- N6 (新发现):-0.05(注释,几乎不影响)

**距 10/10 差的 0.15 分**:纯 ASCII 图 / 注释里的数字遗留,非结构性 / 功能性 / 一致性问题。

---

## 改进建议(优先级排序)

| # | 优先级 | 建议 | 出处 |
|---|---|---|---|
| 1 | **P3** | `02 §1 L23` ASCII 图 `service_savedata_*.py (8 个)` → `(10 个)` | N5(M2 实际未修)|
| 2 | **P4** | `02 §4.3 L241` 注释 `跑全部 19 个` → `跑全部 20 个` | N6(新发现,v1.8 漏改) |
| 3 | **P5** | 无 | — |

---

## 评估总结

**第 8 轮结论**:9.85 / 10(从 9.7 微升 0.15)。M1 + M3 真修,M2 + N6(本次新发现)是 2 处未修干净的"角角落落"。AI 无脑照抄能力已达极限(5 问 100% 答出,21 处加新 kind 模板齐全,9 阶段表 cross-validated,4 大属性多文档一致),剩余差距纯属"图里的数字 vs 表格里的数字"这种洁癖级瑕疵。

**距 10/10 的最后 0.15**:2 处 ASCII 图 / 注释里的数字残留,修完即可完美。
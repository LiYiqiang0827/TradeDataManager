# 01_架构文档 CHANGELOG

> **本体只保留最新内容**,变更记录走 CHANGELOG。

---

## v1.0 — 2026-09-14(初版)

- **新增**:01_架构文档.md
- **内容**:4 节总览(文件夹架构 + 代码架构 + 数据架构 + 功能列表)
- **依据**:用户拍板(2026-09-14 17:55)文档体系 4 大类设计
- **作者**:AI 助手(基于已存在的 `docs/ARCHITECTURE.md` v1.6 拆解重写)
- **删除**:旧 `docs/ARCHITECTURE.md` v1.6(1540 行) → 已归档到 `old/`

## 后续变更

## v1.1 — 2026-09-14 18:55(体检)

- **修改**:§4.1 数据采集功能表 — 频率列从单列(凭印象写)重写为 2 列(写入 Redis / 落盘 SQLite),10 个 kind 全部按代码真实 DEFAULT_INTERVAL 校正
  - watchlist:pre_auction 1 次 / 30min
  - auction:6s / --once
  - snapshot:6s / 15min
  - orderbook:10s / 15min
  - minute:60s / 15min
  - zt:30s / 30min
  - break:60s / 30min
  - anomaly:120s / 30min
  - hot:300s / 30min
  - limitperformance:30s / 15min
- **原因**:体检发现原表周期全部凭印象写,与各 service_writeredis_*.py / service_savedata_*.py 的 default interval 不符
- **影响**:文档与代码 100% 对应;生产 0 影响(plist 不变,daemon 配置不变)

---

## v1.4 — 2026-09-15(第 22 轮:doc ↔ code 全量一致性校正)

> **触发**:用户第 22 轮指令"再完整扫描一遍代码和文档,看看文档是否与代码一致,尤其是各个详细设计文档"。AI 用 `wc -l` + `grep -nE` 全量核对 31 份详细设计 + 4 份主文档 + 1 份指南,发现多处过期数据。

### 真值点修正清单(以代码实际为准)

**核心行数真值点**(以 `wc -l` 实际值,2026-09-15 14:15+ 校正):
- `redis_online.py`:**1377 → 1495**(+118)
- `persist_client.py`:**720 → 849**(+129)
- `sqlite_client.py`:**1213 → 1252**(+39)
- `check_redis.py`:674 → **675**(+1)
- `query_redis.py`:386 → **387**(+1)
- `check_db.py`:388 → **389**(+1)
- `query_db.py`:437 → **438**(+1)
- `scheduler_onlineData.py`:~640 → **643**(+3)
- `scheduler_once.py`:~260 → **250**(-10)
- `watchlist_fetch.py`:668 → **689**(+21)

**严重错误修正**:
1. **core_redis_online.md** §3.2 + §4:`set_watchlist` ❌ → `put_watchlist` ✅(实际方法名,AI 抄错会报 AttributeError)
2. **core_redis_online.md** §3:"80+ 方法" → **53 方法**(实际值)
3. **01 §2.1 + §2.3 service 层**:"25 个文件" → **24 个文件**(实际 10 writeredis + 10 savedata + 2 check + 1 savedata_loop + 1 cleanredis)
4. **01 §2.5 + 02 §5.3 test 段**:重复计数错误("12+1 个 checkredis" → **13 个 checkredis**,allkeys 已含)
5. **02 §3 标题**:"23 文件" → **24 文件**(与 01 §2.3 一致)
6. **03 §2 表字段数**:
   - snapshot:**12 → 14**(+2)
   - anomaly:**9 → 8**(-1)
   - hot:**10 → 14**(+4)
7. **03 §3 auction window TTL**:"5min(300s)=DEFAULT_WINDOW_TTL" ❌ → **1 天(86400s)=WINDOW_TTL_AUCTION**(全天窗口,不是滑窗)
8. **04 §1.2 文件树**:`scheduler_*.md (1 个)` → **(2 个)**(实际 scheduler_once.md + scheduler_onlineData.md)
9. **04 §4.1**:"23 个 service 文件" → **20 个 service 文件**(scheduler_once 实际跑 10 writer + 10 savedata,不含 check/loop/cleanredis)
10. **04 §3.4**:`check_redis_meta` ❌ → `check_meta` ✅(实际函数名)
11. **05 §11.1**:"service_writeredis_auction.md (145 行)" → **70 行**(实际 `wc -l`)

### 详细设计 doc 头部行数同步校正

8 份详细设计 doc 头部"行数"全部更新:
- core_redis_online.md:1377 → 1495
- core_persist_client.md:720 → 849
- core_sqlite_client.md:1213 → 1252
- core_watchlist_fetch.md:668 → 689
- core_check_query_tools.md:603 → 1889(4 文件合计)
- scheduler_once.md:249 → 250
- scheduler_onlineData.md:642 → 643
- service_savedata_loop.md:115 → 116

### 行号引用校正

- service_savedata_loop.md:`run` line 75-86 → **72-89**;`run_once` line 89-98 → **90-99**(代码多了导入行)

### 原因

用户第 22 轮明确指令"完整扫描一遍代码和文档,看看文档是否与代码一致,尤其是各个详细设计文档"。AI 在最近 21 轮反复修改了 31 份详细设计,但头部行数 / 函数签名 / 表字段数等"真值点"未同步,导致 doc 与 code 累计漂移 118~129 行(主要来自 v6.5~v6.7 三次大幅扩展)。AI 评估节奏:派 zero-context subagent 全量核对 → 修真值 → 重测 scheduler_once 20/20 → 追加 v1.x CHANGELOG(本节)。

### 影响

- **生产 0 影响**(plist / 调度阶段 / SQLite 路径未变,纯文档校正)
- **AI 加速**:未来 AI 读到详细设计 doc 不再被错误行数 / 错方法名误导,排查代码效率 ↑
- **复用价值**:本节校正清单 + 同步 4 份 CHANGELOG,沉淀为"doc ↔ code 真值点核对模板"

---

## 后续变更(模板)

```
## vX.Y — YYYY-MM-DD(变更简述)

- **新增/修改/删除**:...
- **原因**:...
- **影响**:...
```

---
## vX.Y — YYYY-MM-DD(变更简述)

- **新增/修改/删除**:...
- **原因**:...
- **影响**:...
```

## v1.2 — 2026-09-15(第 20 轮:check service 新增 — 架构图与清单更新)

> **触发**:v1.24 代码层新增 `service_check_redis.py` + `service_check_db.py`(v6.7),`01_架构文档.md` 同步更新文件清单 + 架构图。
> **改动**:
- **新增/修改/删除**:
  - `01_架构文档.md` §1 文件夹架构:`service/` 23 文件 → **25 文件**;新增 `service_check_*.log`
  - `01_架构文档.md` §1.1 service 子文件夹:数量 23 → 25(10 写 + 10 落盘 + 2 检查 + 3 工具)
  - `01_架构文档.md` §2.1 service 层架构图:22 文件 → **25 文件**;新增 check_* 行
  - `01_架构文档.md` §3 service 清单:新增 `**check_***(2 个,v6.7 新增)| 2 | CLI 工具...` 行
  - `01_架构文档.md` §3.4 其他工具表:check_redis.py + check_db.py 调用入口加 `或 python3 -m service.service_check_*`;新增 `service_check_*.py` 工具行
  - `01_架构文档.md` §3.2 core 模块清单:check_redis.py 行数 603 → **674**;watchlist_fetch.py 668 → **689**;check_db.py / check_redis.py 谁会调 + service_check_*
- **原因**:v6.7 新增 check service 必须反映在架构文档(否则 service 层架构图与实际文件数不一致 22 vs 25)
- **影响**:架构文档与代码现状完全对齐;新 AI session 读 01 能立即看到 check service 存在及位置

---

## v1.3 — 2026-09-15(顶部加 05 引用 + 05 指南创建)

- **新增/修改**:
  - `01_架构文档.md` 顶部"最新状态"后加一行:"加新实时监控落盘数据?→ 直接看 05_新增实时监控落盘数据指南.md(v1.0,2026-09-15 新增),不用通读这份架构文档"
- **原因**:用户第 21 轮指令"增加一个和架构文档平级的新文档,主要目的是指导一个 AI 该如何新增加一个实时监控落盘数据"。01 是总览文档,新顶级文档 05 是专项指南,必须在 01 顶部建立跳转,避免 AI 错误地把"加新实时监控管道"任务走到 01 总览里来。
- **影响**:AI 接到"加新实时监控数据"任务时,从 01 顶部直接跳 05,不再花时间读架构总览
- **联动**:新建 `05_新增实时监控落盘数据指南.md` + `05_新增实时监控落盘数据指南_CHANGELOG.md`

---

## v1.1 — 2026-09-16(snapshot_index kind 上线)

- **新增**:`01_架构文档.md` 顶部"最新状态"后加"v6.10 新增"一行,声明 `snapshot_index` kind 上线,总 kind 数 11 → 12
- **原因**:第 24 轮用户拍板"a 只用8个吧",snapshot_index 是第 12 个 kind,需在 01 总览文档体现
- **影响**:AI 进入新 session 读 01 时立即知道 snapshot_index 存在,不必查代码

---

## v1.5 — 2026-09-16 14:48+(v6.15 完整重构同步对齐)

**目的**:以代码 `scheduler_onlineData.py` v6.15 真值为准,同步主文档所有不一致点。

**改动**(以代码为准):

- §1 头部 — 新增 v6.15 重构标注;原 v6.10 头部保留;最新状态 v1.0 → **v1.1**
- §2.2 — 阶段数 8 → **10**;加 `idle_pre_morning` / `idle_pre_afternoon`;写入 09:29-11:31+13:00-15:01 / 落盘 09:30-11:46+13:00-15:16(晚 15 分钟)
- §2.3 — service 总数 24(10+10+2+2 → 10+9+2+3);writeredis 清单 10 → 9 活跃+1 停用;删 orderbook 加 snapshot_index;各 service 加 v6.15 频率标注(6 处调整);savedata 清单加 snapshot_index
- §2.5 — test 层 12+11 → 11+10;删 test_checkredis_orderbook.py / test_checkdb_orderbook.py;加 test_checkredis_snapshot_index.py / test_checkdb_snapshot_index.py
- §3.1 SQLite 表 — 11 张(v6.15 删 orderbook + 加 snapshot_index);orderbook 行删除
- §3.2 Redis 清单 — 删 orderbook + 加 snapshot_index 行
- §4.1 周期表 — 删 orderbook 行 + 加 snapshot_index 10s/次 + 6 处频率校准(snapshot_index 30→10s / minute 60→30s / zt 30→60s / hot 300→120s);写入窗 09:29-11:31+13:00-15:01 / 落盘窗 09:30-11:46+13:00-15:16 加注
- §4.3 — cleanredis 03:00 → 16:00 + 加注(用户已关注,待确认)
- §4.5 时序表 — 连续交易 09:30-11:30+13:00-15:00 → **09:29-11:31+13:00-15:01**;午休 11:30-13:00 → **11:31-12:59**;收盘 15:00-15:35 → **15:01-15:16**;post_savedata 15:35 之后 → **15:16 之后**
- §4.5 方法 A — 15:35 → **15:16**
- §5 原则表 — 8+8 阶段 → **10+10**

**代码依据**:

- `scheduler_onlineData.py` L 60-260 阶段定义(10 WRITER + 10 SAVEDATA)
- `service_writeredis_*` head interval_sec 9 个真值
- `WRITER_DAEMONS_MORNING_AFTERNOON` 列表 8 项(无 orderbook)
- `SAVEDATA_DAEMONS_MORNING_AFTERNOON` 列表 8 项(无 orderbook + 加 snapshot_index)
- `ONCE_TRIGGERS` 4 项真值

**作者**:AI 助手
**影响**:仅文档层,0 代码改动
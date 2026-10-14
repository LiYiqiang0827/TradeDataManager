# 04_QuickStart CHANGELOG

> **本体只保留最新内容**,变更记录走 CHANGELOG。

---

## v1.2 — 2026-09-15(第 20 轮:check service 新增 — 文件清单与排错表更新)

- **修改**:§1 文件清单 — `代码详细设计/` 29 个 → **31 个**;新增 `service_check_*.md (2 个,v6.7 新增)` 行
- **修改**:§7 常见问题排错表 — 新增 3 行:
  - "想一键看 Redis 全状态" → `service_check_redis.py --kind all`
  - "想一键看 SQLite 全状态" → `service_check_db.py --kind all --trade-date 20260915`
  - "想看 watchlist 三件套" → `service_check_redis.py --kind watchlist` 或 `--kind watchlist_three`
- **原因**:v6.7 新增 check service,§7 排错表必须指向新工具(否则 debug 仍只走 `from core.check_redis import check_*` 老路,不知道有统一 CLI)
- **影响**:任何 AI 排错"Redis 整体状态"/"SQLite 落盘状态"/"watchlist 三件套是否齐全"问题,都能从 §7 直接拿到 CLI 命令(无需读代码)

## v1.1 — 2026-09-15 00:34(plist 完整文档化)

- **新增**:§6.4 plist 完整结构(完整 XML + 9 字段逐项说明)
- **新增**:§6.5 plist 安装 / 编辑 / 验证(`plutil -lint` + `unload + load` + `launchctl print`)
- **新增**:§6.6 plist 常见故障(9 种症状-原因-解决映射表)
- **修改**:§6.3 看日志 — 改 launchd stdout/stderr 真路径(原写 `scheduler_onlineData.log` 错)
- **修改**:§7 表格 "scheduler 起不来" 改 launchd 路径
- **修改**:§8 相关文档加 `04 §6.4 - §6.6 plist 运维`链接
- **原因**:第 14 轮体检发现 04 §6 只有 unload+load 命令,**plist 文件本身 / 字段含义 / 安装 / 编辑 / 验证 / 故障表 全部缺失**,新增文档等于增加"第 5 大类",不合规;补到 §6 最合适
- **影响**:
  - 任何时候新人按 §6 部署 scheduler 都有完整 plist 真值可抄
  - plutil -lint 提前发现语法错误
  - 故障表 9 种覆盖了 2026-09 前所有的 launchd 报错场景
- **对照真值**:`~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist` 2026-09-15 cat 验证

---

## v1.0 — 2026-09-14(初版)

- **新增**:04_QuickStart.md(7.6KB)
- **内容**:
  - §1 AI 怎么读这套文档(推荐阅读顺序)
  - §2 模块功能速查
  - §3 常用 AI 调用(check_redis / query_redis / check_db / query_db)
  - §4 常用场景(调试 / 加新 kind / 验证)
  - §5 重要约定(写入/落盘分离 / STREAM 游标 / v3 双时间戳 / lu_time 永不丢 / encode-decode / pipeline / --once)
  - §6 调度器快速操作
  - §7 常见问题
  - §8 相关文档

## 后续变更

按以下格式追加:

```
## v1.X — YYYY-MM-DD

- **新增/修改/删除**:...
- **原因**:...
- **影响**:...
```

---

## v1.3 — 2026-09-15(新增 05_新增实时监控落盘数据指南 + 索引更新)

- **新增/修改**:
  - §1.1 推荐阅读顺序加第 6 条:"被要求'加新实时监控管道'时"指向 `05_新增实时监控落盘数据指南.md`
  - §1.2 文档体系树加 `05_新增实时监控落盘数据指南.md`(v1.0 新增)
  - §2 模块功能速查表加 2 行:加新实时监控 → 05 指南 / 加新 kind(通用)→ 04 §4.3
  - §4.3 末尾加 ⚠️ 注:"加新'实时监控'数据**不要走本节通用模板**,改走 [05_新增实时监控落盘数据指南.md](05_新增实时监控落盘数据指南.md)"
- **原因**:用户第 21 轮指令"下一步增加一个和架构文档平级的新文档,主要目的是指导一个 AI 该如何新增加一个实时监控落盘数据..."。04 §4.3 是通用 21 处模板,无法体现**实时监控管道专项设计**(三件套 / STREAM 落盘方法 / redis_online 接口规范 / check 工具完整链路)。
- **影响**:
  - 任何 AI 接到"加新实时监控数据"指令,从 04 §4.3 末尾警示直接跳到 05 指南,不再走错路径
  - 04 §4.3 通用模板保持纯净,继续负责"快照型 kind"+"单股 ZSET 型 kind"两类
  - 05 指南与 04 §4.3 **互不重叠**:05 覆盖实时监控管道(三件套 + STREAM 落盘 + redis_online + check + service + scheduler + 文档 14 步 checklist),04 §4.3 覆盖通用 21 处改动
- **联动**:新建 `05_新增实时监控落盘数据指南.md` + `05_新增实时监控落盘数据指南_CHANGELOG.md`

---

## v1.4 — 2026-09-15(第 22 轮:doc ↔ code 全量一致性校正)

> **触发**:用户第 22 轮指令"再完整扫描一遍代码和文档,看看文档是否与代码一致,尤其是各个详细设计文档"。AI 全量核对,发现 04 文档 2 处真值点错。

### 修正清单

**文件树 + scheduler_once 跑数量**:
- §1.2 文件树:`scheduler_*.md (1 个)` → **`(2 个)`**(实际 `scheduler_once.md` + `scheduler_onlineData.md` 都在子目录里)
- §4.1:"跑全套(23 个 service 文件)" → **20 个 service 文件 = 10 writeredis + 10 savedata**(不含 check/cleanredis/savedata_loop;这 3 个不是 scheduler_once 跑的对象)

**核心 API 名错**:
- §3.4 代码示例:`from core.check_redis import check_snapshot, check_redis_meta` → **`check_meta`**(实际函数名,不是 `check_redis_meta`)。AI 抄错会报 ImportError。

### 原因

用户第 22 轮明确指令"完整扫描一遍代码和文档,看看文档是否与代码一致,尤其是各个详细设计文档"。04 §1.2 / §4.1 / §3.4 是新 AI 第一次接触项目必读,但 v1.2 时漏校对了。

### 影响

- **生产 0 影响**(plist / 调度阶段 / SQLite 路径未变,纯文档校正)
- **新 AI 第一次跑** `python3 -m scheduler.scheduler_once` 不会再因为 §4.1 错误预期 "23 个 service"(实际跑 20 个,check 不在范围)
- **新 AI 第一次调** `check_meta()` 不再报 ImportError

---

## v1.5 — 2026-09-16 14:48+(v6.15 文档同步对齐)

> **触发**:用户原话"前面我们改了scheduler online里面的做法,请同步好对应的文档,要求文档与代码完全一致(以代码为准)"。04 涉及 v6.15 真值点全部校正。

### 修正清单

**文件树 md 数**(L30):
- `31 个详细 md(5 core + 10 writeredis + 10 savedata + 2 check + 1 savedata_loop + 1 cleanredis + 2 scheduler)` → **`29 个详细 md`(v6.15:5 core + **9 writeredis** + **9 savedata** + 2 check + 1 savedata_loop + 1 cleanredis + 2 scheduler)**

**数据源表**(L177/L185):
- `pytdx ... orderbook / minute(主力)` → **`minute(主力;orderbook 已停用 v6.15)`**
- `多数 kind 用同花顺,orderbook / minute 用 pytdx` → **`多数 kind 用同花顺,minute 用 pytdx;orderbook 已停用(v6.15)`**

**service 详细 md 数**(L33/L34):
- `service_writeredis_*.md (10 个)` → **`(9 活跃,v6.15 加 snapshot_index + 删 orderbook)`**
- `service_savedata_*.md (10 个)` → **`(9 活跃,v6.15 加 snapshot_index + 删 orderbook)`**

**排错表**(L459):
- `看是否 15:35 之前读的` → **`看是否 15:16 之前读的(v6.15 hard-clock)`**

### 影响

- **生产 0 影响**(plist / 调度阶段 / SQLite 路径未变,纯文档校正)
- **新 AI 第一次读 04 §1.2 看到 29 个 md** 知道 v6.15 真值
- **新 AI 看到 pytdx 行** 立即知道只有 minute(不要用 pytdx 加新 kind)

---

## v1.6 — 2026-09-16(目录结构整理:scripts/ + LLM_Wiki_Path.md 清理)

**触发**:用户原话"这些脚本文件为什么会出现在 LLM wiki 下面呀,如果没用就删除"+ "LLM_Wiki_Path.md 这个文档也不需要了"。

### 改动

**删除**:
- **顶层 `scripts/` 目录**(1 个 bash 脚本 + 8 个 morning_check 日志) — 排查无 plist / cron 引用,仅 wiki 文档体系自身的历史实验残留,删前备份到 `/tmp/wiki_scripts_backup_20260916_151832/`(保留 7 天可回滚)
- **`LLM_Wiki_Path.md`** — 内容(路径说明 + 软链机制 + 旧文档归档)已散落到 01-04 + 各 README 索引,冗余

**新建**(2026-09-16 15:19+):
- **`修改记录/`** 子目录 — 7 份 CHANGELOG 全部移到此处(本目录);含 `修改记录.md` README
- **`评估报告/`** 子目录 — 9 份 AI_Agent_评估报告移到此处;含 `评估报告.md` README

**修复**:
- **§1.2 文件树**:去掉 `LLM_Wiki_Path.md` 条目;新增 `修改记录/` + `评估报告/` 条目(2026-09-16 新建)

### 影响

- **生产 0 影响**(纯 wiki 目录整理)
- **文档体系更清爽**:6 主文档 + 3 子目录(`代码详细设计/` / `修改记录/` / `评估报告/`)+ `old/`(只读归档)
- **AI Agent 第一次接 onlineDataManager 任务**:直接看 `修改记录/README` 找到最新变更,看 `评估报告/README` 找到历史评估轨迹

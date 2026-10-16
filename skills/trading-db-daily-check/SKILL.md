---
name: trading-db-daily-check
description: 每日股票数据库自动体检 — offline/online scheduler 保活 + offline 表更新一致性 + online 落盘完整性,4 个 cron job(offline 09:15/22:00 拆开)通过飞书汇报。
version: 1.2
created: 2026-10-17
updated: 2026-10-17
---

# trading-db-daily-check

## 触发场景

- 用户说"检查 TradingDataManager 的 offline/online scheduler 是否在跑"
- 用户说"检查 offline 数据库是不是更新到今天了"
- 用户说"检查 online 数据库有没有断点"
- 用户说"跑一下每日股票数据库体检"
- 用户说"为什么 `kind_YYYYMMDD` 表不存在" — 配合下方"online 表缺失诊断"
- 任何 cron job 定时调用本 skill 的审计脚本时

## 三个审计脚本

skill 自带 3 个独立脚本,各自有详细自检能力:

1. **`hourly_live_check.py`** — Scheduler 保活/每 1h 跑
   - launchd 3 个服务 PID 检查: `com.tradingdatamanager.scheduler.{offlineData,onlineData}` + `com.tradingdatamanager.redis6380`
   - offline/online db 文件 mtime 新鲜度(分市场时段 + 非交易日不告警)
   - offline scheduler stderr log 最近写入时间(loguru 默认走 stderr)
   - redis 健康(通过 redis-cli PING,本地)
   - **正常 exit 0,异常 exit 1**

2. **`offline_daily_audit.py`** — 09:15 + 22:00 工作日各跑一次
   - 跨 4 个 db(`db_cn_basic/kpl/news/index`)逐表对照:
     - ctrl 表的 `key + max_date + updated_at`
     - 数据表的 `MAX(trade_date / cal_date / datetime)`
   - 三类区分:
     - 日增量表(参与滞后判定,>1 天滞后告警)
     - 静态表(basic/tradecal/index_basic)— 只看 ctrl_updated_at 是否 < 7 天
     - 派生表(week/month)— 无 ctrl,不参与判定
   - news 表按 `src` 而非 `key` 看 max_date,`fenghuang / yuncaijing` 卡在 09-10 是已知停更,不告警
   - A 股交易日历(从 basic db 读)对照期望最新交易日

3. **`online_daily_audit.py`** — 16:30 工作日跑一次
   - 10 个 kind(anomaly/auction/break/hot/limitperformance/minute/snapshot/snapshot_index/watchlist/zt)×最近 7 个交易日的表覆盖检查
   - 每个 kind 每天一张表 `kind_YYYYMMDD`,必须存在 + cnt > 0
   - cursor 表 `online_stream_cursor` 7+ stream 的 last_persist_ts 新鲜度
     - **工作日 9-16 时** 阈值 60 分钟(交易时段)
     - **周末/非交易时段** 阈值 24 小时(避免误报)
   - db 文件大小 sanity(当前月 < 1MB 告警)
   - **重要警告**:脚本检测到的"表不存在"未必是真丢数据 —— 可能是 `online_stream_timestamp_quarantine` 隔离。诊断流程见下方"online 表缺失诊断"。

## 项目位置与运行约定

- **skill 安装位置(两处副本互为同步,内容应完全一致)**:
  - `~/.hermes/skills/trading/trading-db-daily-check/`(hermes 加载,user 个人副本)
  - `~/TradingDataManager/skills/trading-db-daily-check/`(仓库内,git 跟踪,随代码提交)
  - **改完一处,记得同步另一处**
- 3 个脚本: `scripts/{hourly_live_check,offline_daily_audit,online_daily_audit}.py`
- 数据依赖:
  - `~/TradingDatabase/offline/db_cn_*.db`(4 个 offline db)
  - `~/TradingDatabase/online/online_data_YYYYMM.db`(按月归档)
- launchd 服务(被检查,非被启动):
  - `~/Library/LaunchAgents/com.tradingdatamanager.scheduler.{offlineData,onlineData}.plist`
  - `~/Library/LaunchAgents/com.tradingdatamanager.redis6380.plist`
- **关键认知**:`~/TradingDataManager/` 是独立 git 仓库(github.com/LiYiqiang0827/TradeDataManager),
  不是空目录也不是 symlink,plist 指向它是**正确**的;skill 副本跟着仓库走,运维更直观

### 两副本同步工作流

改完任何一份文件后,必须**双向同步 + git 提交**,不能只改一处:

1. **改主副本(hermes 加载的)**:编辑 `~/.hermes/skills/trading/trading-db-daily-check/`
2. **同步到仓库副本**:`cp -p <hermes-path>/<file> <repo-path>/<file>`(用 `cp -p` 保留时间戳便于审计)
3. **验证一致**:`for f in SKILL.md scripts/*.py references/*.md prompts/*.md; do diff -q <hermes>/$f <repo>/$f || echo "❌ $f"; done` —— 6 个文件应全部一致
4. **单独 commit**:`cd ~/TradingDataManager && git add skills/trading-db-daily-check/ && git commit -m "docs(skills): <改动描述>" && git push origin main`
5. **脚本逻辑改动**还要额外跑一次 sanity check 确认 hermes 副本能正常 exit 0(`python3 <hermes-path>/scripts/hourly_live_check.py --quiet-ok`)

逆方向同理(如果先在仓库副本改,也要 `cp` 回 hermes 副本)。

## 通用约定

- **不要**修改生产 plist / 数据 / launchd 配置(只读+审计)
- **凭证安全**:`~/TradingDataManager/.env` 含 `TUSHARE_TOKEN` 等,绝不打印到日志或飞书(出现时替换为 `[REDACTED]`)
- **退出码语义**:所有脚本 `0 = 全部正常`,`1 = 有异常`;Hermes 会保留运行状态并直接投递 stdout。当前 wrapper 不会自动给消息加 "⚠️" 前缀。
- **工作日判定**: `datetime.now().weekday() < 5` 是周一到周五;用户在最初要求"双休日不做",`hourly_live_check.py` 跑全天(系统级保活),但**offline / online 审计只在工作日跑**(用户在 cron 时间表里配置,见 SKILL 末尾"参考配置")

## 使用方法

### 直接运行单个脚本

```bash
# 1. 每小时保活
python3 ~/.hermes/skills/trading/trading-db-daily-check/scripts/hourly_live_check.py --quiet-ok

# 2. 早上 09:15 / 晚上 22:00 offline 审计
python3 ~/.hermes/skills/trading/trading-db-daily-check/scripts/offline_daily_audit.py --verbose

# 3. 下午 16:30 online 审计
python3 ~/.hermes/skills/trading/trading-db-daily-check/scripts/online_daily_audit.py
```

### 通过 cron job 调度 + 飞书发送

实际配置了 **4 个** cron job(offline 09:15 和 22:00 拆成两个),全部 `no_agent=true` + `script=<wrapper-name>` 模式 —— 脚本 stdout 即飞书消息,**零 token 消耗、零延迟**。

- `script` 字段必须是**纯文件名**(解析到 `~/.hermes/scripts/`),传绝对路径会直接报错。
- `deliver` 用 `feishu:<chat_id>` 格式;`no_agent=true` 时 prompt/skills 被忽略。
- 错误时脚本**必须** exit 非 0 + 输出完整报告;空 stdout 不发送(watchdog 模式)。

当前 4 个任务、wrapper 和 Hermes CLI 创建方式见 `references/cron-jobs.example.md`。创建前先用 `hermes cron list` 检查是否已存在，避免重复任务。

### 错误修复

| 报错 | 原因 | 修复 |
|---|---|---|
| `No such file or directory: '~/TradingDataManager/...'` | plist 跑了独立 git 仓库路径,**这是正确的** | 不要"修复"路径,skill 已经只读 PID 不看路径 |
| `FileNotFoundError: .../data/` | 同上 | 已加 ignore |
| online db mtime 误报"卡住" | 没分市场时段 | skill 已用 `is_market_hours` 判断 |
| cursor 周六告警 | 没分周判 | 阈值 24h(非交易日) |
| `cn_index_basic ctrl_max='all'` | 指数基本表全量刷,ctrl_max 是特殊标记 | 在脚本的 STATIC_TABLES 里,不参与滞后判定 |
| `tbl_cn_week/month` ctrl=None | 派生表无独立 ctrl | 标记 `is_derived`,不告警 |
| news `fenghuang/yuncaijing` 卡 09-10 | 已停更的数据源 | 已知白名单 `KNOWN_STALE_NEWS_SRCS` |
| cronjob create: `Script path must be relative to ~/.hermes/scripts/` | `script` 字段只认纯文件名,绝对路径会被拒 | 改传 `trading_db_xxx.sh`(wrapper 放 `~/.hermes/scripts/`) |
| `kind_YYYYMMDD` 表不存在但 cursor 正常 | 数据全在 `online_stream_timestamp_quarantine` 里 | 先查 quarantine 表的 `reason`,见下方诊断流程 |

### 已知白名单(2026-10-17 实战)

- **news src**: `fenghuang`, `yuncaijing`(停更 09-10)
- **静态表**(只看 ctrl_updated_at < 7 天,不看滞后):
  - `tbl_cn_basic`, `tbl_cn_tradecal`, `tbl_cn_index_basic`
- **派生表**(week/month):`is_derived=True`, 不告警

如果发现新的"已知异常",在脚本顶部对应的常量集合里加,不要新写判断逻辑。

## online 表缺失诊断(关键)

**`online_daily_audit.py` 报告 `kind_YYYYMMDD 表不存在`时,不是直接"数据丢失"**。下面 4 步是唯一标准诊断流程(不靠猜):

1. **查 quarantine 表**:
   ```sql
   sqlite3 ~/TradingDatabase/online/online_data_YYYYMM.db \
     "SELECT target_trade_date, reason, COUNT(*) FROM online_stream_timestamp_quarantine
      WHERE stream_key = 'online:<kind>:stream' AND target_trade_date = YYYYMMDD
      GROUP BY target_trade_date, reason"
   ```
   - 有 `missing_or_invalid_source_timestamp` → 数据被隔离,**字段命名错配**(`<kind>_timestamp` 缺失),见下方 snapshot_index 案例
   - 有 `source_date_mismatch` → 上游推的数据时间戳跨日,源数据问题
   - 无任何 quarantine 行 + 无 `kind_YYYYMMDD` 表 → 真丢数据(罕见,通常意味着 redis stream 也被清空)

2. **查 redis stream 长度**:`redis-cli -p 6380 XLEN online:<kind>:stream`
   - 非 0 + cursor 已推进 → 数据在 redis 里,persist 端有 bug
   - 0 + cursor 推进 → 数据已被读走但全进 quarantine

3. **查 cursor 状态**:`SELECT * FROM online_stream_cursor WHERE stream_key='online:<kind>:stream'`
   - `last_persist_ts` 在落盘窗口期(09:29-11:45 / 12:45-15:01)+ 无对应表 = **cursor 自欺欺人**;`persist_client.py` 在 `snapshots` 为空时仍推进 cursor 避免死循环。

4. **修复前确认影响范围**:对该 kind 的每个有问题的 trade_date,看 quarantine 行数 vs 同 kind 正常日的行数(数千级 → 全天失写;少数 → 部分失写)。

### snapshot_index 已知坑(2026-10-17 发现 + 已修)

- 根因:`persist_client.py` 用 `kind_ts_field = f"{kind}_timestamp"` 模板读 stream → 对 `snapshot_index` 要求 stream 里有 `snapshot_index_timestamp` 字段;`service_writeredis_snapshot_index.py` 实际写的是 `snapshot_timestamp`(沿用 snapshot 旧字段名)。
- 现象:**所有 snapshot_index stream 数据全被丢进 quarantine**,cursor 正常推进,`snapshot_index_YYYYMMDD` 表永远不建。诊断 4 步见上方。
- 修复策略(2026-10-17 实测):**双写兼容**,不重命名(避免破坏已落地的数据)。
  1. **写端**(`service_writeredis_snapshot_index.py` + `redis_online.py:put_snapshots_index_batch` + 单条兼容接口):推新名 `snapshot_index_timestamp`,同时保留 `snapshot_timestamp`;缺时间戳时使用 unix 毫秒兜底
  2. **读端**(`persist_client.py`):fallback list 加 `snapshot_timestamp`,让 quarantine 里的历史 payload 能被解析
  3. **补回**(`onlineDataManager/scripts/service/repair_<kind>_<YYYYMMDD>.py`):从 quarantine 表 `payload_json` 反序列化 → `INSERT OR IGNORE` 到缺失的 `kind_YYYYMMDD` 表
  4. **验证**:再跑 `online_daily_audit.py`,从 exit 1 → exit 0;`SELECT COUNT(*)` 应有 3800+ 行
- **同类问题复用**:任何 `online:<kind>:stream` 报 `kind_YYYYMMDD` 表不存在 + quarantine 有数千条 `missing_or_invalid_source_timestamp`,本质都是同一种"stream 字段名 <kind>_timestamp 错配"。先查 `service_writeredis_<kind>.py` 实际 setdefault 的字段名,再查 `persist_client.py` 的 `kind_ts_field` 期望,差在哪改哪,**双写兼容**永远比"重命名"安全。
- **写补回脚本的模板**:用 venv 的 python(因 `coreClient` 是 editable 安装),`from core import sqlite_client`,`payload_json` 反序列化的字段要按 `sqlite_client._KIND_COLUMNS[<kind>]` 的顺序对齐。

## 参考配置

当前 cron job 配置和 Hermes CLI 示例见 `references/cron-jobs.example.md`。

## 注意事项

- 本 skill **只读 + 审计**,不会修改任何 db / plist / scheduler 配置
- 出错不要 retry — 直接把异常发飞书,等用户决定怎么处理
- offline/online 两个 scheduler 之间**没有依赖关系**,但都依赖 redis(redis 死了两个 scheduler 都会爆,会被 hourly 检测到)
- cron job 的 `script` 字段:**纯文件名,不传绝对路径**(会被拒)
- quarantine 是诊断**真相**的唯一来源;cursor 上的"OK"不能信

# 04_QuickStart 文档

> **AI 入门指南 + 常用功能调用 + 常用场景**
> **受众**:AI Agent 第一次读这套文档 / 接手这个项目时

---

## §1 AI 怎么读这套文档

### 1.1 推荐阅读顺序

1. **本文件**(04_QuickStart.md) — 知道有什么 + 怎么用
2. **01_架构文档.md** — 知道整体结构(文件夹 + 模块分层 + 数据 + 功能列表)
3. **02_代码详细设计.md** — 知道每层接口 + 谁调谁
4. **`代码详细设计/`** 子目录 — 看具体文件怎么实现(按需)
5. **03_数据详细设计.md** — 查表 schema / Redis key(按需)
6. **被要求"加新实时监控管道"时**:→ [05_新增实时监控落盘数据指南.md](05_新增实时监控落盘数据指南.md)

### 1.2 文档体系

```
LLM Wiki/TradingAgent/onlineDataManager/   ← 权威源
├── 修改记录/                               ← 7 份 CHANGELOG 集合(2026-09-16 新建)
├── 评估报告/                               ← 9 份 AI_Agent_评估报告(2026-09-16 新建)
├── 01_架构文档.md                          ← 4 节总览
├── 02_代码详细设计.md                      ← 主索引 + 概述
├── 03_数据详细设计.md                      ← SQLite + Redis 字段
├── 04_QuickStart.md                       ← (本文件)
├── 05_新增实时监控落盘数据指南.md          ← 加新实时监控管道指南(v1.0 新增)
├── old/                                  ← 旧文档归档
└── 代码详细设计/                          ← **29 个**详细 md(v6.15:5 core + 9 writeredis + 9 savedata + 2 check + 1 savedata_loop + 1 cleanredis + 2 scheduler)
    ├── core_redis_online.md
    ├── ...
    ├── service_writeredis_*.md (**9 活跃**,v6.15 加 snapshot_index + 删 orderbook)
        ├── service_savedata_*.md (**9 活跃**,v6.15 加 snapshot_index + 删 orderbook)|
    ├── service_check_*.md (2 个,v6.7 新增)
    └── scheduler_*.md (2 个)
```

每个文档**独立 CHANGELOG**,本体只保留最新版。

## §2 模块功能速查

| 想做什么 | 看哪个文档 |
|---|---|
| **看整体架构** | 01_架构文档.md |
| **加新实时监控落盘数据** | **05_新增实时监控落盘数据指南.md**(v1.0 新增,本指南)|
| **加新 kind(通用)** | 04 §4.3 加新 kind 完整模板 |
| **看具体某文件怎么写** | 代码详细设计/<file>.md |
| **查 SQLite 表字段** | 03_数据详细设计.md §2 |
| **查 Redis key 类型** | 03_数据详细设计.md §3 |
| **看 scheduler 时间表** | 代码详细设计/scheduler_onlineData.md §2 |
| **测试 pipeline 是否 work** | 代码详细设计/scheduler_once.md |

## §3 常用 AI 调用

### 3.1 调试 Redis 状态

```python
from core.check_redis import check_snapshot, check_meta

# 看 snapshot 整体状态
print(check_snapshot())
# {
#   "timeline_size": 1,
#   "timeline_ttl": 43194,        # 动态计算
#   "latest_archive_size": 124,
#   "latest_archive_ttl": 1794,
#   "window_count": 124,
#   "stream_xlen": 200019,
#   ...
# }

# 看单股 window
print(check_snapshot_for_stock("000001.SZ"))
# {"window_size": 240, "latest": {...}, "ttl": 86399}
```

### 3.2 拉数据(从 Redis)

```python
from core.query_redis import (
    fetch_snapshot_archive_latest,        # 全市场最新 snapshot
    fetch_snapshot_window,                # 单股 window
    fetch_zt_archive_latest,              # 最新涨停
)

# 全市场最新 snapshot
archive = fetch_snapshot_archive_latest()
# {"000001.SZ": {open: ..., close: ..., ...}, ...}

# 单股最近 100 个 snapshot
window = fetch_snapshot_window("000001.SZ", idx_min=0, idx_max=100)
```

### 3.3 拉数据(从 SQLite)

```python
from core.query_db import fetch_snapshot, to_dataframe

# 当日全股 snapshot
rows = fetch_snapshot("snapshot", "20260914")
df = to_dataframe(rows)
print(df.head())

# 单股
rows = fetch_snapshot("snapshot", "20260914", ts_code="000001.SZ")
df = to_dataframe(rows, columns=["data_timestamp", "close", "volume"])
print(df.head())

# 当日涨停按时间排序
rows = fetch_snapshot("zt", "20260914")
df = to_dataframe(rows)
print(df.sort_values("lu_time").head(20))
```

### 3.4 看落盘状态

```python
from core.check_db import check_snapshot, check_cursors

# 当日 snapshot 表
print(check_snapshot("20260914"))
# {
#   "table": "snapshot_20260914",
#   "row_count": 150000,
#   "ts_count": 124,
#   "earliest_data_ts": "2026-09-14 09:30:01",
#   "latest_data_ts": "2026-09-14 15:00:00"
# }

# 所有游标
print(check_cursors())
```

## §4 常用场景

### 4.1 收盘后调试:"pipeline 是否 work?"

```bash
# 跑全套(20 个 service 文件 = 10 writeredis + 10 savedata,不含 check/cleanredis/savedata_loop)
# 注:check_redis / check_db 是 CLI 检查工具,不在 scheduler_once 跑
# savedata_loop 是基类(被所有 savedata_*.py 继承,不单独跑);cleanredis 由 scheduler_onlineData 在凌晨 spawn
python3 -m scheduler.scheduler_once

# 跑某个 kind
python3 -m scheduler.scheduler_once --only snapshot

# 跑某个 kind + skip 一些
python3 -m scheduler.scheduler_once --only writer --skip auction,break
```

### 4.2 验证 v6.3 改造 OK

```bash
# 跑 snapshot writer + savedata
python3 -m scheduler.scheduler_once --only snapshot

# 看 Redis snapshot 状态
python3 -c "from core.check_redis import check_snapshot; print(check_snapshot())"

# 看 SQLite snapshot 当日表
python3 -c "from core.check_db import check_snapshot; print(check_snapshot('20260914'))"
```

### 4.3 加新 kind(完整模板)

参考 `代码详细设计/service_savedata_loop.md` §2 子类示例。本节列出**完整 21 处**改动(避免漏掉查询/检查/测试/部署):

**A. 数据获取层**(1 处)
1. `core/watchlist_fetch.py`(或新建 `core/xxx_fetch.py`)加 `fetch_xxx(codes, ...)` 函数

**A.0 数据源 → 客户端映射**(避免加新 kind 时 client 选错):

| 数据源 | 客户端(实测)| 示例 kind |
|---|---|---|
| **同花顺 CLI**(`coreClient.ths_client`)| `from coreClient.ths_client import fetch_snapshots, fetch_auction_snapshots, ...`(函数,不是 class)| **auction / snapshot / zt / break / anomaly / hot** |
| **pytdx**(通达信本地)| `coreClient.tdx_client.TdxClient`(包装类,**不直接用 TdxHq_API**)| **minute**(主力;**orderbook 已停用** v6.15)|
| **KPL API**(开盘啦)| `coreClient.kpl_client.KPLClient`(**client 后缀,不是 api**)| **limitperformance**(主力,不是备用)|

> ⚠️ **2026-09-14 第 13 轮校正**(F-06):
> 1. **`TdxClient` 是 pytdx 的类**(`coreClient/tdx_client.py`),**不是同花顺**。同花顺走 `coreClient/ths_client.py`(模块 + 函数)。
> 2. `coreClient.kpl_api` **不存在**,真名是 `coreClient.kpl_client`(后缀 `client`)。
> 3. limitperformance **主力用 KPL 不是同花顺**(doc 之前列错)。

**加新 kind 前先选好数据源**,再选对应客户端。多数 kind 用同花顺,minute 用 pytdx;**orderbook 已停用**(v6.15)。

**B. Redis 业务层**(1 处,通常要加多个方法)
2. `core/redis_online.py` 加 `OnlineRedis.put_xxx` / `commit_xxx` / 若干 `get_xxx` 读方法

**C. SQLite 落盘层**(3 处)
3. `core/sqlite_client.py` 加 `insert_xxx_batch` 函数 + 在 `_SCHEMA` 注册表 schema
4. `core/persist_client.py` 加 `persist_xxx(redis_client, *, trade_date)` 函数(kwarg-only,**无 conn 参数**,conn 在函数内部 `connect(ym)`)+ 在 `persist_kind()` 路由表注册
5. (如果 watchlist 类全表替换)在 `core/sqlite_client.py` 加 `replace_xxx` 函数

**D. AI Agent 工具层**(4 处,check + query 镜像 4 个文件)
6. `core/check_redis.py` 加 `check_xxx()` 顶层函数
7. `core/query_redis.py` 加 `fetch_xxx_xxx()` 顶层函数
8. `core/check_db.py` 加 `check_xxx_db()` 顶层函数
9. `core/query_db.py` 加 `fetch_xxx_db()` 顶层函数

**E. Service 层**(2 处)
10. `service/service_writeredis_xxx.py`(新,抄 `service_writeredis_auction.py` 模板,改 `KIND` / `INTERVAL` / 数据源)
11. `service/service_savedata_xxx.py`(新,继承 `SavedataDaemon`,`KIND = "xxx"`,`DEFAULT_INTERVAL = 1800.0` = 30min — **MyATM kind 通用值**,特殊 kind 看 service_savedata_loop.md §6 DEFAULT_INTERVAL 表)

**F. Scheduler 注册**(2 处,变量名务必准确)
12. `scheduler/scheduler_onlineData.py` 在 **`WRITER_PHASE_SERVICES` / `SAVEDATA_PHASE_SERVICES`** 字典里加新服务(挂 `morning_savedata` / `afternoon_savedata` 阶段),并在 8 阶段调度逻辑里 spawn/kill(`closed / pre_open / watchlist / auction_writer / morning_writer / lunch / afternoon_writer / writer_stopped` 见 `代码详细设计/scheduler_onlineData.md §2`)
> ⚠️ **警告**:代码里**没有** `WRITER_SERVICES` / `SAVEDATA_SERVICES` / `PHASES` 这三个名字。**实际是** `WRITER_PHASE_SERVICES` / `SAVEDATA_PHASE_SERVICES`(完整名)。误抄会 `AttributeError`,scheduler 启不起来。

**G. 部署**(1 处)
13. 加载 plist 重载:`launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist && launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist`

**H. 测试**(2 处)
14. `test/test_checkredis_xxx.py` + `test/test_checkdb_xxx.py`(参考已有 `test_checkredis_zt.py`)

**I. 文档**(6 处)
15. `代码详细设计/service_writeredis_xxx.md`(新)
16. `代码详细设计/service_savedata_xxx.md`(新)
17. `02_代码详细设计.md` §3.2 / §3.3 / §3.5 加新行
18. `01_架构文档.md` §2.3 表格 + §4.1 频率表
19. `03_数据详细设计.md` §2 加 schema(参考 §2.6 zt 模板)
20. `04_QuickStart.md` §4.3 加 1 行例举新 kind(如加 hot 后写"`hot` — 热股榜(同花顺)"例)
21. 在 3 个 CHANGELOG 各追加 v1.X 节

**总结**:21 处改动,分布:core 5 / service 2 / scheduler 1 / 部署 1 / 测试 2 / 文档 7 + CHANGELOG 3

> **⚠️ 加新"实时监控"数据(如大单监控 / 资金流向 / 板块异动):不要走本节通用模板,改走 [05_新增实时监控落盘数据指南.md](05_新增实时监控落盘数据指南.md)。该指南覆盖三件套(stream + timeline + archive + window)+ STREAM 落盘方法 + redis_online 接口规范 + service/scheduler/check/文档 完整 14 步 checklist。**

### 4.4 看某个 kind 的所有 service

```bash
# 跑某个 kind 的全流程(writer + savedata)
python3 -m scheduler.scheduler_once --only zt

# Redis 状态
python3 -c "from core.check_redis import check_zt; print(check_zt())"

# SQLite 当日表
python3 -c "from core.check_db import check_zt; print(check_zt('20260914'))"
```

## §5 重要约定(必看)

### 5.1 写入/落盘分离

- `service_writeredis_*` **只写 Redis**
- `service_savedata_*` **只读 Redis → 写 SQLite**
- **绝不**在一个 service 里既写 Redis 又写 SQLite

### 5.2 STREAM + 游标

- 落盘走 STREAM(`XREAD > last_id`),游标持久化到 SQLite `online_stream_cursor` 表
- 重启不丢不重(commit 后才推进游标)

### 5.3 v3 双时间戳原则

每个落盘行都有:
- `data_timestamp`:数据时间(原始 ISO)
- `created_at`:落盘时间(本机 ISO)

绝不混淆。

### 5.4 lu_time 永不丢

涨停时间 `lu_time`(unix int64)在 zt/limitperformance 表里是**关键字段**。任何重构都不能丢这个字段。

### 5.5 统一 encode/decode

写 Redis 必须用 `self._encode(item)` / `self._decode(v)` 基类方法。**绝对不能用** `json.dumps` / `json.loads`。

### 5.6 Redis pipeline 限制

pipeline 不能 read 命令(如 `pipe.zrangebyscore`)。read 必须单独 `execute()`。

### 5.7 --once 模式

所有 service 都支持 `--once`(单次,跑完退出)。`scheduler_once` 用这个测端到端。

## §6 调度器快速操作

### 6.1 重启生产调度器

```bash
launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
```

### 6.2 看进程状态

```bash
launchctl list | grep onlineData
# 输出: <PID> 0 com.tradingagent.scheduler.onlineData   # 实际 Label,见 plist 文件
```

### 6.3 看日志

```bash
# scheduler 进程日志(launchd stdout/stderr 重定向,见 plist 的 StandardOutPath)
tail -f ~/TradingAgent/onlineDataManager/logs/scheduler.launchd.log
tail -f ~/TradingAgent/onlineDataManager/logs/scheduler.launchd.err.log

# service 进程日志(setup_logger 自动写,按天切)
ls -lt ~/TradingAgent/onlineDataManager/logs/ | head -20
tail -f ~/TradingAgent/onlineDataManager/logs/service_writeredis_snapshot_$(date +%Y%m%d).log
```

> 注:`setup_logger(name)` 会自动写 `logs/<name>_YYYYMMDD.log`(按天切),scheduler 自己**不**用 setup_logger,日志全靠 launchd 重定向。

### 6.4 plist 完整结构

**plist 文件位置**:`~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist`

> ⚠️ 必须放 `~/Library/LaunchAgents/`(用户域)而不是 `/Library/LaunchDaemons/`(系统域),否则需要 sudo 且不会随用户登录自动加载。

**完整 plist 内容**(2026-09-15 真值):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tradingagent.scheduler.onlineData</string>

    <key>ProgramArguments</key>
    <array>
        <string>/opt/anaconda3/bin/python3</string>
        <string>-m</string>
        <string>scheduler.scheduler_onlineData</string>
        <string>--daemon</string>
        <string>--scan-interval</string>
        <string>60</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/Users/nickzhang/TradingAgent/onlineDataManager/scripts</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/Users/nickzhang/.local/bin:/opt/anaconda3/bin:/opt/anaconda3/condabin:/usr/local/bin:/usr/bin:/bin</string>
        <key>PYTHONPATH</key>
        <string>/Users/nickzhang/TradingAgent/onlineDataManager/scripts</string>
    </dict>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>/Users/nickzhang/TradingAgent/onlineDataManager/logs/scheduler.launchd.log</string>

    <key>StandardErrorPath</key>
    <string>/Users/nickzhang/TradingAgent/onlineDataManager/logs/scheduler.launchd.err.log</string>
</dict>
</plist>
```

**逐字段说明**:

| Key | 值 | 说明 |
|---|---|---|
| `Label` | `com.tradingagent.scheduler.onlineData` | launchd 全局唯一 ID,`launchctl list` / `launchctl print` 都用它识别 |
| `ProgramArguments` | `[python3, -m, scheduler.scheduler_onlineData, --daemon, --scan-interval, 60]` | **必须用绝对路径**(`/opt/anaconda3/bin/python3`),不能写 `python3` / `python`(launchd 环境 PATH 极简) |
| `WorkingDirectory` | `~/TradingAgent/onlineDataManager/scripts` | 影响 `python -m scheduler.scheduler_onlineData` 模块解析路径;`PYTHONPATH` 已显式声明,这里再设一次保险 |
| `EnvironmentVariables.PATH` | 包含 `.local/bin + anaconda + usr/bin` | launchd 默认 PATH 不含 `.local/bin`,所以 uv 装的包 / `cliclick` 等命令找不到;**必须显式声明** |
| `EnvironmentVariables.PYTHONPATH` | `~/TradingAgent/onlineDataManager/scripts` | 让 `from core.xxx import ...` / `from service.xxx import ...` 能解析;不写会 `ModuleNotFoundError` |
| `RunAtLoad` | `<true/>` | 用户登录(`launchctl bootstrap gui/$(id -u)`)时立刻启动 |
| `KeepAlive` | `<true/>` | 进程退出后**立刻重启**(生产要求),不是 5 秒后再拉起 |
| `StandardOutPath` | `logs/scheduler.launchd.log` | scheduler 的 print / logger 输出;**目录必须事先存在**(`mkdir -p`),否则 launchd 会拒绝启动 |
| `StandardErrorPath` | `logs/scheduler.launchd.err.log` | 单独 stderr,跟 stdout 分文件便于排错 |

### 6.5 plist 安装 / 编辑 / 验证

**安装(一次性)**:

```bash
# 1. 确认 plist 文件已存在
ls -la ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist

# 2. 确认 logs/ 目录存在(launchd 不自动创建)
mkdir -p ~/TradingAgent/onlineDataManager/logs

# 3. 校验 plist 语法(plutil 是 macOS 自带)
plutil -lint ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
# 期望输出: OK

# 4. 加载(传统 unload + load 方式,2026-09-15 在用)
launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist 2>/dev/null
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
```

**编辑(改了 plist 后必须重载)**:

```bash
# 1. 用文本编辑器改(不要用 plist Editor / Xcode,会产生不可见字符)
nano ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
# 或
code ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist

# 2. 改完先 lint
plutil -lint ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist

# 3. 必走 unload + load(launchd 不监听 inotify,改完不会自动重读)
launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist

# 4. 验证生效
launchctl list | grep onlineData
# 期望: <PID> 0 com.tradingagent.scheduler.onlineData
```

**验证生效**:

```bash
# 1. launchctl list 看到进程
launchctl list | grep onlineData

# 2. launchctl print 看完整配置 + 上次退出码
launchctl print gui/$(id -u)/com.tradingagent.scheduler.onlineData 2>&1 | head -40

# 3. 看 stdout 日志确认有循环输出
tail -20 ~/TradingAgent/onlineDataManager/logs/scheduler.launchd.log

# 4. 看 service 子进程有被 spawn
ps aux | grep -E "service_(writeredis|savedata)" | grep -v grep | wc -l
# 期望: ≥ 5(开市时段有 writeredis 活跃 + savedata 待命)
```

### 6.6 plist 常见故障

| 症状 | 原因 | 解决 |
|---|---|---|
| `launchctl load` 报 `Could not find specified service` | Label 重复或拼错 | `launchctl list \| grep tradingagent` 看有没有遗留 Label;或检查 plist 内的 `<key>Label</key>` |
| `Could not find domain` | 用了 `bootout` / `bootstrap` 而非传统 `unload/load` | 改回传统 `unload + load`(2026-09-15 拍板) |
| 加载后进程秒退,scheduler.launchd.err.log 写 `ModuleNotFoundError: No module named 'core'` | `PYTHONPATH` 没生效 | plist 里加 `<key>PYTHONPATH</key>`,重 unload+load |
| 加载后进程秒退,scheduler.launchd.err.log 写 `FileNotFoundError: [Errno 2] No module named 'scheduler'` | `WorkingDirectory` 错或 PYTHONPATH 错 | 确认 plist 里 WorkingDirectory + PYTHONPATH 都指向 `~/TradingAgent/onlineDataManager/scripts` |
| 加载后 stderr 写 `Address already in use` | Redis 端口被占 | `lsof -i :6379` 看是不是有别的 redis 在跑;或改 `coreClient/redis_config.py` 端口 |
| 加载后日志有 `command not found: cliclick` | PATH 不含 `.local/bin` | plist 的 `EnvironmentVariables.PATH` 加 `:/Users/nickzhang/.local/bin` |
| `launchctl list` 看不到 Label | plist 文件不在 `~/Library/LaunchAgents/` | `mv` 过去;或用 `launchctl load -w <绝对路径>` 强制加载 |
| `KeepAlive` 起作用但 scheduler 自己退出循环了 | scheduler 内部 raise 未捕获异常 | 看 `scheduler.launchd.err.log` 的 traceback;常见是 data_pipeline API 临时挂了,会自动重连 |
| `RunAtLoad` 没生效 | 用户登录时 `~/Library/LaunchAgents` 还没扫 | `launchctl print gui/$(id -u)` 看 LoginItems;或用 `launchctl bootstrap` 替代 |

## §7 常见问题

| 问题 | 看哪里 |
|---|---|
| "scheduler 起不来" | logs/scheduler.launchd.log + scheduler.launchd.err.log |
| "Redis 数据不对" | check_redis.py + 看对应 service_writeredis_*.md |
| "SQLite 表不对" | check_db.py + 03_数据详细设计.md §2 |
| **"想一键看 Redis 全状态"** | **service_check_redis.py `--kind all`**(v6.7 新增) |
| **"想一键看 SQLite 全状态"** | **service_check_db.py `--kind all --trade-date 20260915`**(v6.7 新增) |
| **"想看 watchlist 三件套(stream + timeline + archive)是否齐全"** | **service_check_redis.py `--kind watchlist`** 或 `--kind watchlist_three`(单独三件套)|
| "某 service --once 卡住" | scheduler_once + minute --once bug fix(2026-09-14)|
| "加新字段" | 看 service_savedata_loop.md + 03 §2 + 改 3 个文件 |
| **"加新 kind 后启动报错 XXX"** | **回查 04 §4.3 21 处是否漏改(尤其是 `WRITER_PHASE_SERVICES` / `SAVEDATA_PHASE_SERVICES` 字典注册)+ 看对应 service_writeredis_*.md + service_savedata_*.md 模板** |
| "snapshot 数据有缺失 / 跟同花顺对不上" | check_redis.py 看 stream_xlen + check_db.py 看 row_count + 比 MAXLEN(参考 `01 §4.5 §3` 积压告警)|
| "policyStudy 读 snapshot_YYYYMMDD 没数据" | 看是否 **15:16** 之前读的(v6.15 hard-clock,参考 `01 §4.5 §1` 时序约定),或 `online_stream_cursor` last_id 是否落后太多 |

## §8 相关文档

- 架构:见 01_架构文档.md
- 代码细节:见 02_代码详细设计.md + `代码详细设计/` 子目录
- 数据字段:见 03_数据详细设计.md
- **plist 运维**:见 04 §6.4 - §6.6(完整结构 + 安装 + 故障表)

## §9 历史变更

- **2026-09-15 v1.1**:新增 §6.4-§6.6(plist 完整结构 + 安装 + 故障表 9 种)
- **2026-09-14 v1.0**:本文件即为初版
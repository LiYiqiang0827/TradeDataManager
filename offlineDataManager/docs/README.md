# offlineDataManager

`~/TradingAgent/offlineDataManager/` — A 股金融数据离线下载与本地化服务。从 MyATM 项目拆分而来(2026-09-10),只用 Tushare + 开盘啦 HTTP API,**4 个 SQLite 库**按主题分库存储(2026-09-15 加 `db_cn_index.db`)。

**目标读者**:AI Agent / 接手的开发者。
**用途**:在 5 分钟内搞清项目结构、数据在哪、怎么跑、怎么扩。

---

## 文档目录

| 文档 | 内容 | 何时读 |
|---|---|---|
| [QuickStart.md](QuickStart.md) | 5 分钟跑通:依赖、token、首次初始化、单 service 跑 | 第一次接触项目 |
| [00_架构文档.md](00_架构文档.md) | 整体架构、4 层数据流、调度链路、子进程模型、launchd 集成 | 理解"为什么这么写" |
| [01_数据详细设计文档.md](01_数据详细设计文档.md) | **4** 库 **31** 张表(含 4 个 ctrl 断点表)+ 27 张数据表 的完整 schema、字段语义、索引、ctrl 断点 | 写 SQL / 加表时 |
| [02_新增数据接入指南.md](02_新增数据接入指南.md) | **加新数据(港股/ETF/龙虎榜等)的端到端流程:改哪 7 处、文档同步清单、决策树** | 加新数据源时 |
| [03_调度时间表.md](03_调度时间表.md) | **每日 4 个 task 在哪个时间点跑、跑哪些 service、更新哪些表、写入哪个 DB** | 改调度 / 加新时间点时 |
| [题材研究数据库工具.md](题材研究数据库工具.md) | KPL涨停题材的DuckDB时序知识库、表结构、增量维护、查询接口、模型审计与Obsidian投影 | 查询或维护题材研究数据库时 |
| [新闻清洗压缩与LLM提炼工具.md](新闻清洗压缩与LLM提炼工具.md) | Major News 去重、事件聚类、行情结果过滤、Qwen结构化提炼与Obsidian投影 | 使用或维护每日新闻加工链时 |
| [新闻图数据库工具.md](新闻图数据库工具.md) | Major News 事件、评级、题材/实体关系的 DuckDB 归档，增量补录、查询接口与 Obsidian 重建 | 查询或维护新闻知识库时 |
| [代码详细设计/](代码详细设计/) | **40 个脚本**的逐个详细设计(职责、入口、依赖、关键代码) | 改具体文件前 |
| [修改记录/](修改记录/) | 代码/文档变更历史快照(背景、改动清单、验证、回滚) | 接手项目 / 还原历史 |
| [评估报告/](评估报告/) | 代码审查、一致性排查、性能评估报告 | 决策 / 架构改进参考 |

`代码详细设计/` 子目录每份对应一个脚本,共 **40 份**(跟代码 1:1 对应):

| 文档 | 对应脚本 |
|---|---|
| [代码详细设计/settings.md](代码详细设计/settings.md) | `scripts/config/settings.py` |
| [代码详细设计/offline_db_client.md](代码详细设计/offline_db_client.md) | `scripts/core/offline_db_client.py` |
| [代码详细设计/offline_downloader.md](代码详细设计/offline_downloader.md) | `scripts/core/offline_downloader.py` |
| [代码详细设计/scheduler_updateData.md](代码详细设计/scheduler_updateData.md) | `scripts/scheduler/scheduler_updateData.py` — launchd 常驻定时调度 |
| [代码详细设计/scheduler_once.md](代码详细设计/scheduler_once.md) | `scripts/scheduler/scheduler_once.py` — 一次性测试调度(2026-09-16 新增) |
| [代码详细设计/common.md](代码详细设计/common.md) | `scripts/service/common.py` |
| [代码详细设计/service_basic.md](代码详细设计/service_basic.md) | `scripts/service/service_basic.py` |
| [代码详细设计/service_tradecal.md](代码详细设计/service_tradecal.md) | `scripts/service/service_tradecal.py` |
| [代码详细设计/service_daily.md](代码详细设计/service_daily.md) | `scripts/service/service_daily.py` |
| [代码详细设计/service_adj_factor.md](代码详细设计/service_adj_factor.md) | `scripts/service/service_adj_factor.py` |
| [代码详细设计/service_week.md](代码详细设计/service_week.md) | `scripts/service/service_week.py` |
| [代码详细设计/service_month.md](代码详细设计/service_month.md) | `scripts/service/service_month.py` |
| [代码详细设计/service_kpl_list.md](代码详细设计/service_kpl_list.md) | `scripts/service/service_kpl_list.py` |
| [代码详细设计/service_kpl_concept_cons.md](代码详细设计/service_kpl_concept_cons.md) | `scripts/service/service_kpl_concept_cons.py` |
| [代码详细设计/service_kpl_limit_performance.md](代码详细设计/service_kpl_limit_performance.md) | `scripts/service/service_kpl_limit_performance.py` |
| [代码详细设计/service_news.md](代码详细设计/service_news.md) | `scripts/service/service_news.py` |
| [代码详细设计/service_major_news.md](代码详细设计/service_major_news.md) | `scripts/service/service_major_news.py` |
| [代码详细设计/service_cctv_news.md](代码详细设计/service_cctv_news.md) | `scripts/service/service_cctv_news.py` |
| [代码详细设计/service_intraday.md](代码详细设计/service_intraday.md) | `scripts/service/service_intraday.py` — 个股分钟 K(2026-09-17 新) |
| [代码详细设计/service_ticks.md](代码详细设计/service_ticks.md) | `scripts/service/service_ticks.py` — 个股分笔成交(2026-09-17 新) |
| [代码详细设计/service_intraday_index.md](代码详细设计/service_intraday_index.md) | `scripts/service/service_intraday_index.py` — 大盘指数分钟(2026-09-17 新) |
| [代码详细设计/basic_demo.md](代码详细设计/basic_demo.md) | `scripts/test/basic_demo.py` |
| [代码详细设计/kpl_demo.md](代码详细设计/kpl_demo.md) | `scripts/test/kpl_demo.py` |
| [代码详细设计/news_demo.md](代码详细设计/news_demo.md) | `scripts/test/news_demo.py` |
| [代码详细设计/validate_data.md](代码详细设计/validate_data.md) | `scripts/test/validate_data.py` |

---

## 1 分钟摘要

### 是什么
- **每天自动下载 A 股数据**到本地 SQLite(基本面 / 日 K / 复权因子 / 周月 K / 开盘啦涨停榜 / 涨停表现 / 题材成分 / 新闻)
- **提供 34 个只读 `get_*` 接口**给上层(在线服务、策略、查询)用,响应在毫秒级(含 `get_stk_limit` 涨跌停 / `get_suspend` 停复牌 / `get_top_list` 龙虎榜每日 / `get_top_inst` 龙虎榜机构 / `get_block_trade` 大宗 / `get_ggt_daily` 港股通 / `get_hsgt_top10` 沪深股通 / `get_limit_list` 涨跌停列表 / `get_moneyflow` 资金流向 / `get_margin` 融资融券汇总 / `get_margin_detail` 融资融券明细 / `get_cyq_perf` 筹码及胜率 / `get_daily_basic` 每日指标 / `get_index_basic` 指数基本信息 / `get_index_daily` 12 只指数日线行情,2026-09-15 加)
- **多 service 多进程**架构,launchd 拉起顶层 scheduler,scheduler 用 `subprocess.run` 同步 spawn 各 service

### 数据规模(2026-09-15 实测)

| DB | 大小 | 表数 | 关键数据行数 |
|---|---|---|---|
| `db_cn_basic.db` | 3.6 GB | 13 数据 + 1 ctrl | 日 K 1173 万 / adj_factor 1225 万 / daily_basic 1165 万 / stk_limit 1202 万 / moneyflow 1116 万 / margin_detail 639 万 / cyq_perf 798 万 / suspend 28 万 / week 249 万 / month 59 万 / margin 6408 / basic 5562 / tradecal 6360 |
| `db_cn_kpl.db` | 1.1 GB | 9 数据 + 1 ctrl | kpl_concept_cons 411 万 / kpl_list 18.5 万(涨停+炸板) / limit_list 16.5 万 / top_inst 163 万 / top_list 10 万 / block_trade 43 万 / hsgt_top10 4 万 / ggt_daily 2674 / kpl_limit_performance 1.2 万 |
| `db_cn_news.db` | 6.4 GB | 3 数据 + 1 ctrl | news 946 万 / major_news 146 万 / cctv 3.5 万 |
| `db_cn_index.db` | 8 MB | 2 数据 + 1 ctrl | index_daily 5.9 万(12 只指数,1993-2026) / index_basic 8000 |

### 关键事实
- **日 K 存的是不复权原始数据**;前复权在 `get_day(qfq=True)` 读时 join `tbl_cn_adj_factor` 计算
- **周月 K 是派生表**(`tbl_cn_day` × `adj_factor` 聚合),`tbl_cn_week` / `tbl_cn_month` 存的就是前复权
- **每个数据库一个 ctrl 表**(2026-09-15 重构,符合"每个 DB 都有自己的 ctrl"原则):
  - **db_cn_basic.db** → `tbl_basic_ctrl`(12 keys:cn_basic / cn_daily / cn_adj_factor / cn_tradecal_SSE / cn_tradecal_SZSE / cn_stk_limit / cn_suspend / cn_moneyflow / cn_margin / cn_margin_detail / cn_cyq_perf / cn_daily_basic)
  - **db_cn_kpl.db** → `tbl_kpl_ctrl`(9 keys:cn_kpl_list / cn_kpl_concept_cons / cn_kpl_limit_performance / cn_top_list / cn_top_inst / cn_block_trade / cn_ggt_daily / cn_hsgt_top10 / cn_limit_list)
  - **db_cn_index.db** → `tbl_index_ctrl`(2 keys:cn_index_basic / cn_index_daily)
  - **db_cn_news.db** → `tbl_news_ctrl`(11 src keys)
- **依赖链**: `daily → adj_factor → week → month`(`service_week` / `service_month` 启动时校验 `ctrl.cn_daily == ctrl.cn_adj_factor == today`,否则返回 rc=2 业务中止)
- **kpl 双源**:`kpl_list`(tushare,早上 09:05 task_morning_update 跑)+ `kpl_limit_performance`(kpl API 实时,16:30/20:00 跑),互补补 T+1 滞后

### 怎么跑

```bash
# === scheduler_updateData (生产调度,launchd 常驻) ===
# 看状态(不修改任何数据)
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --status

# 手动跑一次完整更新(慢,~75 分钟首次,~5 分钟增量)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --full

# 跑单个 service
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_daily

# launchd 常驻调度(每天 09:05 早上快速补 + 18:00/20:00/22:00 完整更新 + 16:30/20:00 kpl 涨停表现 + News 每 10 分钟)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --daemon

# === scheduler_once (一次性测试调度,2026-09-16 新增) ===
# 跑全部 27 个 service(按依赖顺序)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once

# 只跑某阶段
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only stage0

# 跑多个 service(空格分隔)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only kpl_list daily_basic index_daily

# 跑多个 service(逗号分隔)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only kpl_list,daily_basic,index_daily

# 只看计划,不实际跑(任何 --only / --skip 都能搭配)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --dry-run
```

详见 [QuickStart.md](QuickStart.md)。

---

## 项目位置与依赖

- **代码根目录**:`~/TradingAgent/offlineDataManager/`
- **运行时 Python**:`/opt/anaconda3/bin/python3`(3.12.7,带 tushare / pandas / loguru / schedule)
- **Tushare token**:`~/TradingAgent/coreClient/tushare_config.py`(由 `coreClient.tushare_client.TushareClient` 读取)
- **依赖声明**:`requirements.txt`(tushare >= 1.4 / pandas >= 2.0 / schedule >= 1.2 / loguru >= 0.7)
- **launchd plist**:`~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist`(Label=`com.tradingagent.scheduler.offlineData`)
- **旧文档备份**:无(2026-09-15 全部重写后旧的已删除)

---

## 与 MyATM / onlineDataManager 的关系

- **MyATM**(`~/myatm_redis/`):父项目,offlineDataManager 直接复用其 Tushare token、业务思路、多 service 架构
- **onlineDataManager**(`~/TradingAgent/onlineDataManager/`):并行项目,offlineDataManager 是它的**离线数据底座**。onlineDataManager 跑实时行情,offlineDataManager 跑盘后数据
- **两个项目都用 `coreClient/`**(在 `~/TradingAgent/coreClient/`):共享 `tushare_client` / `kpl_client` / 长连接服务

---

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-09-15 | 全量重写项目文档(从 8 篇旧 md 重构为 4 顶层 + 21 脚本级) |
| 2026-09-15 | 修 3 个真 bug(offline_downloader.show_status / replace_table 事务 / validate_data conn 错配) |
| 2026-09-16 | 加 `scheduler/scheduler_once.py` 一次性测试调度器(--only 支持多 service,2026-09-16 增强) |
| 2026-09-16 | 加 KPL/沪深股通/融资融券/指数日线/指数基本信息 等 16 个新 service 文档(21 → 37 份) |
| 2026-09-16 | 文档子目录改名:`code-detail/`(英文)→ `代码详细设计/`(中文,共 129 处链接全部更新,21 → 37 份) |
| 2026-09-15 | 删 6 处死代码(tbl_kpl_ctrl 表 + get_kpl_ctrl 函数 + 4 个空 __init__.py + 1 个未用常量 + 1 个未用导入 + 1 个未调用函数) |
| 2026-09-13 | 引入 `tbl_cn_kpl_limit_performance`(2026-09-15 改为读 `tbl_basic_ctrl` key=`cn_kpl_limit_performance`) |
| 2026-09-10 | 从 `~/cn_data` 合并到 `~/TradingAgent/offlineDataManager/` |

---

## 文档盲区(已知,但未在文档展开)

以下问题文档**没明确说明**,需要新 AI 走查代码或自行决策:

| # | 盲区 | 原因 | 应对 |
|---|---|---|---|
| W1 | TushareClient 完整方法清单 | TushareClient 在 `coreClient/` 外部依赖 | 见 `代码详细设计/offline_downloader.md` L190-199 列了所有方法 |
| W2 | scheduler 启动时立即跑 4 阶段,不管断点是否刚推进过 | 代码就是这么写的 | 详见 00_架构 L164-166(避免方法:launchd 重启前观察 cron 状态) |
| W3 | `tbl_cn_basic.exchange` / `market` 枚举对应不是任意组合 | 数据本身保证 | 详见 01_数据 L61-66 |
| W4 | YYYYMMDD 字符串排序 = 时间排序(代码隐式依赖) | 历史约定 | 详见 01_数据 L54 / L55 |
| W5 | `tbl_cn_kpl_list` 起始 2020-10-09(不是 2020-01-01) | tushare pro.kpl_list 接口上线时间 | 不可改,tushare 决定 |
| W6 | log rotation 备份保留几份 | loguru 默认行为 | 查 loguru 文档(`retention` 参数,默认 7 个) |
| W7 | DB 备份策略(11 GB 3 个库) | 项目层不涉及 | 用 `sqlite3 .backup` 命令(WAL 模式下不能用 cp),或 stop scheduler 后用 cp |
| W8 | SQLite TEXT 字段 1 GB 上限 | SQLite 默认 | 避免写超长文本,目前所有 TEXT 字段都在安全范围 |

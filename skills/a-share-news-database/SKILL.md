---
name: a-share-news-database
description: 查询、增量维护与验证 TradingAgent 的 Major News 新闻图数据库和每日新闻 Obsidian 文档。用于新闻事件、利好利空中性评级、原始报道溯源、题材或实体关系检索、历史补录及文档重建；不把模型题材标签视为已证实的炒作原因。
---

# A股 Major News 新闻数据库工具

事实源默认是 `/Users/nickzhang/TradingDatabase/offline/db_cn_news.db` 的 `tbl_major_news`。正式派生关系库默认是 `/Users/nickzhang/TradingDatabase/offline/db_major_news_events.duckdb`。可通过 `TRADING_DATABASE_ROOT` 覆盖数据库根目录。每日可读投影是 `~/Documents/每日新闻分析/每日新闻/YYYY-MM-DD.md`。维护代码位于 `/Users/nickzhang/TradingDataManager`。先阅读项目文档 `/Users/nickzhang/TradingDataManager/offlineDataManager/docs/新闻图数据库工具.md`，需要追查过滤、语义聚类或模型批次时再读同目录的 `新闻清洗压缩与LLM提炼工具.md`。

## 按任务操作

- 查询正式分析，优先用 `coreClient.data_provider` 的 `get_major_news_analysis`、`get_major_news_theme_edges`、`get_major_news_daily_summary`、`get_major_news_event_graph`。原始报道用既有 `get_major_news`。直接 SQL 主要用于审计和开发。
- 新增一日分析、历史回填、已有产物补录、状态检查与文档重建，使用 `PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python -m service.news_graph_tool` 的 `update`、`backfill`、`import`、`status`、`project` 子命令。进入仓库根目录运行；具体参数见项目文档或 `--help`。
- 大范围回填可用 `service.pipeline_major_news_backfill`：一日 Qwen 提炼与下一日清洗/向量化重叠，数据库写入及 Obsidian 发布仍串行。先验证原始库，再启动单个流水线实例；不可同时启动普通 `backfill` 处理同一日期范围。使用 `backfill_status_<起日>_<止日>.json` 检查已验收、失败和剩余日数，不把模型缓存当成正式完成。
- 历史回填与日常增量共用新闻事件库和文件锁。检查异常卡顿时，先看锁持有者、批次状态与 `pipeline_state.major_news_max_datetime`；历史日期处理不得把增量断点倒写到过去。故障续跑沿用同一日期范围和状态文件，先核对没有第二个批次实例。
- 历史回填前先检查原始 `tbl_major_news` 的逐日覆盖。缺日时用 `service.backfill_major_news_gaps --dry-run` 列出空白，再用本机 Tushare 凭据或 `--ssh-host macmini` 远端凭据分页补抓。已有日期也可能只抓到半天，用 `--refresh-existing` 逐日复核并补入遗漏报道；确认原始库完整后再启动模型分析批次。补抓不移动日常增量断点，远端 token 不应传回或写进日志。
- 需要检查一条新闻时，从语义事件 ID 查其规则事件、原始报道、模型评级、题材和实体边；确认新闻最早发布时间与来源，再引用其分析。

## 数据与判断边界

1. 每日新闻分析只处理 Major News。普通快讯和 CCTV 不计入本库当日分母。原始 SQLite 不因清洗删除新闻；规则排除项仍能审计。
2. 所有入选事件都有模型给出的利好、利空或中性评级与依据；只有高分事件有详细事实、题材与实体提取。方向评级说明对A股整体或直接相关行业的边际影响，不是涨跌预测。
3. 模型的新闻→题材边是 `related_to` 且 `model_unreviewed`，不能当成“题材启动原因”。只有在该日期已经存在、且名称或别名唯一匹配 KPL 题材时才引用 `theme_id`；未匹配或歧义标签保留文本和状态。ST、次新不进入可用题材关系。
4. 该分析目前是自然日盘后产物。做历史研究时明确截止日期，不要把当日完整新闻聚类和模型分析当作当日上午已知信息，也不要从后来的 KPL 别名反推旧新闻关系。
5. 日度更新先完成原始新闻采集，再运行 `update`。`import` 只归档已完整生成的产物；`project` 从正式数据库重建 Obsidian。重跑同日应替换该日派生记录，避免重复边。全面重建会影响下游图库和文档，应在完成重建后逐日补录并复核。

## 验收与汇报

分别验证原始库、清洗事件、图数据库、Obsidian 四层。至少核对某日原始数量、规则排除量、语义数量、入选数量、三类评级之和、图边与原始报道溯源，并打开当日 Markdown 核实数量和评级实际呈现。报告数据库入库与文档投影的实际状态，以及未匹配题材、证据不足或模型失败，不用“模型完成”代替“已写库/已同步”。

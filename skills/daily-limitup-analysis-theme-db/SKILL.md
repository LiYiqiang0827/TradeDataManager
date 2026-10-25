---
name: daily-limitup-analysis-theme-db
description: Analyze daily A-share limit-ups and update the theme graph.
license: UNLICENSED
metadata:
  hermes:
    tags: [finance, trading, limit-up, theme-graph]
    related_skills: [a-share-theme-database, trading-db-daily-check]
---

# 每日涨停分析和题材库更新

## When to Use

Use for daily or historical A-share limit-up attribution, report gap-filling, and updating the Mac mini theme-date-stock SQLite/Neo4j graph.

## 用途与数据边界

本 skill 把每日涨停归因报告、证据包、SQLite 图关系库和 Neo4j 图数据库作为一条可复核流程维护。

- **历史事实源**：`~/TradingDataBase/offline/db_theme_graph.duckdb`。只读提取 `fact_limit_event`、`rel_limit_theme`、`dim_theme` 和 `fact_theme_daily`；其中开盘啦字段是行情/标签事实，不是涨停原因结论。
- **当日涨停补数源**：`~/TradingDataBase/offline/db_cn_kpl.db:tbl_cn_kpl_limit_performance`。这是 `KPLClient` 的同日 `HomeDingPan/DailyLimitPerformance` 实时接口落库，包含主题材、涨停原因、板数、封板时间；`tbl_cn_kpl_list` 走 Tushare `pro.kpl_list`，通常次日早上才更新，不能据其当晚缺行认定没有涨停。
- **按日期选择开盘啦数据源（强制规则）**：目标交易日是当前日期时，开盘啦涨停表现/题材/原因数据一律读取 `db_cn_kpl.db:tbl_cn_kpl_limit_performance`，不得等待或改读 Tushare `tbl_cn_kpl_list`。目标日期早于当前日期时，历史研究以 DuckDB 的 `fact_limit_event`、`rel_limit_theme` 等历史事实表为准；不要用当前实时表覆盖历史截面。当前日期的表没有合格快照时，按每日流程检查后补抓或报告缺数，不能静默切到另一数据源、前一交易日或解释成零涨停。
- **归因库与报告**：`~/TradingDataBase/theme_graph/`，SQLite 为 `topic_limitup_graph.sqlite3`，逐日 Markdown 为 `每日涨停原因/YYYYMMDD.md`，原始分析输入/行情包为 `每日涨停原因/分析输入包/YYYYMMDD.json`，必要时的外部来源快照放 `每日涨停原因/证据包/`。
- **维护代码和提示词**：`~/TradingDataManager/theme_graph/`。Neo4j API 正式类在 `~/TradingDataManager/coreClient/neo4j_client.py`。
- **图数据库服务数据**：`~/TradingDataBase/theme_graph/neo4j/`。Neo4j Community 的数据库是由 store 文件、事务日志和配置共同构成的目录；不要把它当成一个 SQLite 单文件复制。服务安装目录仍在 `~/Applications/Neo4jCommunity`。
- **分工**：DuckDB 保存开盘啦事实题材时间序列；本主题图 SQLite/Neo4j 保存人工整理的每日题材摘要和个股涨停归因。不要把模型推断写回 DuckDB 事实表。

## 每日/历史分析流程

### 1. 确定分析日期和股票全集

从 DuckDB 按 `tag='涨停'` 查询真实交易日和完整股票清单；不要根据日历推测交易日，也不要把炸板股加入涨停归因范围。对已有日报先比对日期与行数，只分析缺日或用户要求重做的日期。

每日 21:00 的同日复盘按以下顺序取数：先检查 SSE 交易日历；非交易日直接跳过。对交易日，先读取 offlineDataManager 已定时维护的同花顺 KPL 快照，不要例行重复请求 API。scheduler 在 16:30 和 20:00 执行 `task_kpl_limit_performance`，数据写入 `db_cn_kpl.db:tbl_cn_kpl_limit_performance`。确认目标 `trade_date` 有非空数据、`snap_ts` 为当日收盘后时间且股票代码唯一后，直接生成 packet。只有快照缺失、日期错配、收盘前或校验不完整时，才运行独立 KPL 客户端刷新脚本作为补救（不要调用会初始化无关 Tushare 客户端的批处理 service），然后复核写入结果：

```bash
~/TradingDataManager/.venv/bin/python ~/TradingDataManager/theme_graph/scripts/refresh_same_day_kpl_snapshot.py --trade-date YYYYMMDD
~/TradingDataManager/.venv/bin/python ~/TradingDataManager/theme_graph/scripts/build_same_day_kpl_packet.py \
  --db ~/TradingDataBase/offline/db_cn_kpl.db \
  --out ~/TradingDataBase/theme_graph/每日涨停原因/分析输入包 \
  --trade-date YYYYMMDD
```

该 packet 将注明 `source_kind=same_day_kpl_api` 和快照时间；主题统计只是 API 主标签计数，不等同 DuckDB 规范题材宽度/梯队/热度。若 API 刷新失败，可只在确认同一交易日已有收盘后快照时继续并标注快照时间；若没有同日行、交易日不符或来源时间缺失，停止分析并报告缺数，绝不能沿用前一交易日数据或把空包解释成“没有涨停”。历史补录仍使用下面的 DuckDB packet 构建方式。

历史分析在 Mac mini 生成 DuckDB 只读输入包：

```bash
~/TradingDataManager/.venv/bin/python \
  ~/TradingDataManager/theme_graph/scripts/build_daily_limitup_packet.py \
  --db ~/TradingDataBase/offline/db_theme_graph.duckdb \
  --out ~/TradingDataBase/theme_graph/每日涨停原因/分析输入包 \
  --start YYYYMMDD --end YYYYMMDD
```

输入包必须保留源代码、名称、原始涨停状态、连续板数、首次/最后封板时间、原始题材标签、成交字段、关系库规范题材标签，以及当天的题材宽度、梯队、高度和热度字段。对空值原样保留，不从自然语言状态反推缺失字段。题材日宽度与报告中每只股票唯一主归属的章节人数是不同口径。

### 2. 研究与写报告

读取 [`references/limit_attribution_v3_2.md`](references/limit_attribution_v3_2.md)，按交易日顺序研究。每个交易日写 `每日涨停原因/YYYYMMDD.md`，每只涨停股恰好出现一次，按 packet 的 `lu_desc` 唯一分到主章节；章节按实际股票数降序，题材内按连续板数降序、首封时间升序。另保留 `theme_raw` 和规范标签，不能把它们当成公司事实。

报告包括：分析截点和口径、当日市场叙事（事实与推断分开）、题材章节摘要、完整个股表格、来源标题/原文 URL/发布时间、反证或未兑现项、信心、传闻/猜测复核。无可靠信源时明确检索缺口、下调信心，不编来源或业务关系。

时点规则：

1. 仅使用分析日当天及此前原始发布时间可确认的信息。次日及更晚发布的报道、公告、复盘、网页修订或后来股价结果不得用于该日归因。
2. 当日盘后报道可描述“盘后市场如何解释当日”，不能倒写为封板前催化；只有独立证据表明事件/消息已在封板前发生或传播，才可据此讨论催化。
3. 市场传闻和猜测可以记录，但需标注未证实、原始传播来源是否找到、可核发布时间、公司回应及来源局限。不得把猜测写成事实。
4. 连板股可承接前一交易日已写明的叙事假设；必须补当日新信息、盘面变化或反证，不能把前日叙事伪装为当日新增催化。区分连续板和窗口内多次涨停。
5. 按 prompt 的强信号规则深挖高板、一字、快速封板、放量和核心股。来源只支持市场在讲某叙事时，可以作市场归因，但不能声称商业兑现。

每日日报开头写明截至收盘的股票总数和数据源；所有章节表格的股票行数相加必须等于输入包 `limitup_stock_count`。

**归因质量门槛**：不能把“资金交易题材标签/方向扩散”批量复制成每只股票的涨停原因。相邻股票不得复用同一因果模板；普通股低信心映射也需写出该公司的具体公开线索及未核环节；每只股票的兑现/反证栏不得为空，未核实也要说明缺少什么证据。结构校验通过只表示行情覆盖正确，不代表归因合格。每个题材章节需点出当日具体事件或市场叙事及盘面证据；每只高板、一字、快速封板/核心股需查到个股桥梁、公司事项或更有解释力的替代故事。普通股若只找到行情标签，结论必须写成低信心的标签映射/情绪扩散推断，并指出未核实处；不得写成已查明催化。盘后复盘可以做市场叙事证据，但不能替代原始事件或个股桥梁。批量完稿时还要搜索并改写重复的模板因果句；每个题材章节需引用可定位的当日共性叙事，若没有则明示检索缺口。来源必须核原始发布时间，不以抓取/更新时间代替。行情包相对链接必须按最终日报目录解析为 分析输入包/YYYYMMDD.json。

### 3. 报告验收

对每份报告逐一核对。可用随 skill 提供的结构校验器检查日期、完整代码覆盖、重复/多余股票、名称、原始状态、连板数和封板时间：

```bash
~/TradingDataManager/.venv/bin/python \
  ~/TradingDataManager/theme_graph/scripts/validate_daily_report.py \
  --packet ~/TradingDataBase/theme_graph/每日涨停原因/分析输入包/YYYYMMDD.json \
  --report ~/TradingDataBase/theme_graph/每日涨停原因/YYYYMMDD.md
```

此外人工检查来源 URL、发布时间、题材叙事与传闻标注。结构校验通过不代表因果归因已核实。


- 股票代码和名称与输入包一致，个股无遗漏、无重复、无额外股票；
- 状态、连板数和封板时间与 packet 一致，未知字段仍为未知；
- 主标签章节唯一归属，章节人数和排序正确；
- 来源 URL 可打开且发布时间不晚于分析日；盘后材料角色标注正确；
- 传闻、推断、事实与兑现程度表达清楚；连板叙事引用前文存在且没有日期倒置；
- Markdown 结构符合 `daily_limitup_graph/import_report.py` 解析约定：题材标题为 `## 一、题材名` 格式，股票表格第一列为 `股票`，并含“涨停状态”“为什么涨停”“个股与故事的桥梁”“兑现程度/反证”“信心”“资料来源”等列。

不通过验收的日期不导入 SQLite。

### 4. 写入 SQLite 图谱

报告通过验收后，在 Mac mini 逐份导入：

```bash
cd ~/TradingDataManager
PYTHONPATH="$PWD" .venv/bin/python -m theme_graph.daily_limitup_graph.cli \
  --db ~/TradingDataBase/theme_graph/topic_limitup_graph.sqlite3 \
  import-report ~/TradingDataBase/theme_graph/每日涨停原因/YYYYMMDD.md \
  --source-uri ~/TradingDataBase/theme_graph/每日涨停原因/YYYYMMDD.md
```

重复导入同一日期会按该日当前报告重建该日题材/股票图视图；如需更新报告，先校验替换内容再重导入。SQLite 保留报告来源、逐题材摘要、连板梯队和个股原因。导入后核对 `PRAGMA integrity_check`、外键、导入日期、题材日期数和个股边数；数目必须与日报章节和表格一致。

### 5. 同步 Neo4j 并验收

从通过 SQLite 完整性检查的数据库导出快照，再调用幂等 Neo4j 导入器：

```bash
~/TradingDataManager/.venv/bin/python \
  ~/TradingDataManager/theme_graph/scripts/export_sqlite_snapshot.py \
  --db ~/TradingDataBase/theme_graph/topic_limitup_graph.sqlite3 \
  --out ~/TradingDataBase/theme_graph/neo4j/import/sqlite_snapshot_YYYYMMDD.json

cd ~/TradingDataManager
PYTHONPATH="$PWD" .venv/bin/python -m theme_graph.neo4j.import_sqlite_snapshot \
  ~/TradingDataBase/theme_graph/neo4j/import/sqlite_snapshot_YYYYMMDD.json
```

Neo4j 导入后检查 Browser/Bolt 可用、日期集合一致，并逐日比较题材日快照数和 `ATTRIBUTES_LIMITUP` 边数。汇报输入报告数、SQLite 导入数和 Neo4j 验收数；不能仅凭脚本返回成功就宣称数据一致。

## 可用脚本和提示词

- `scripts/build_daily_limitup_packet.py`：从 Mac mini DuckDB 只读生成历史日期输入包。
- `scripts/refresh_same_day_kpl_snapshot.py`：仅当 offlineDataManager 的同日快照缺失/不合格时作为补救；直接逐板数调用同花顺 KPL 实时 API，避免无关 Tushare 初始化；核验 SSE 日期、五个板数端点、源时间和股票代码后缀，再以 `(trade_date, ts_code)` upsert 并检查行数。
- `scripts/build_same_day_kpl_packet.py`：从同花顺 KPL API 已落库快照生成 21:00 同日输入包；缺失时失败，不推断为零涨停。
- `daily_limitup_graph/import_report.py`：Markdown 日报写入 SQLite 图表。
- `scripts/validate_daily_report.py`：对照 packet 验证报告股票覆盖和关键行情字段。
- `scripts/export_sqlite_snapshot.py`：生成 Neo4j 全量/增量幂等导入快照。
- `neo4j/import_sqlite_snapshot.py`：通过 Neo4j 白名单 upsert API 导入并核对计数。
- `references/limit_attribution_v3_2.md`：每日题材和个股归因规范（与 TradingDataManager 提示词源同步）。
- `coreClient/neo4j_client.py`：Neo4j 基础读写 API。

## 只读常用查询

```python
from coreClient.neo4j_client import ThemeGraphNeo4jStore
with ThemeGraphNeo4jStore() as graph:
    days = graph.list_topic_days("20260701", "20261016")
    detail = graph.get_topic_day("20260901", "农业")
    history = graph.get_topic_history("农业")
    stocks = graph.get_stock_attributions("20260901")
```

## 安装与发现

唯一维护源是 `~/TradingDataManager/skills/daily-limitup-analysis-theme-db/SKILL.md`。`~/TradingDataManager/theme_graph/SKILL.md` 与 `~/.hermes/skills/daily-limitup-analysis-theme-db/` 指向这份 skill。更新时修改维护源，避免多份提示词造成漂移。

## 汇报标准

最终说明实际完成了哪些日期、报告数和涨停股行数；SQLite/Neo4j 各自日期、题材日和边数；验证命令结果；数据源缺日或报告证据不足之处。没有逐日核对的范围明确标为未完成。

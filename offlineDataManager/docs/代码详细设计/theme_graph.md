# 开盘啦题材时序知识库

## 目标与边界

题材知识库第一版只读取 `db_cn_kpl.db / tbl_cn_kpl_list`，把每日涨停归因、题材热度、涨停梯队和炒作周期保存成可按历史时点查询的结构。开盘啦是主源，同花顺只作为参考证据，不能静默覆盖开盘啦。

第一版股票范围严格限定为**涨停股**。`tbl_cn_kpl_concept_cons` 含大量没有涨停的普通概念成分，因此完全不参与第一版构建：

- 不读取或复制数百万条全量成分快照，不把普通概念成分写入 `dim_stock`；
- `get_theme_members()` 返回实际由开盘啦主归因到该题材的涨停股；
- 热度、宽度、梯队、核心候选和 Obsidian 页面都按涨停股计算；
- “涨幅大于 7%”等扩展口径以后作为独立筛选层增加，不改变当前口径。

本模块不把市场高度核心等同于产业核心。涨停归因描述当日市场为何交易，公司主营、订单、收入和利润受益由证据与模型另行判断。

## 数据分层

主库默认是 `offlineDataManager/data/db_theme_graph.duckdb`。

1. 原始/可追溯事实：`fact_limit_event`、`source_day_manifest`。
2. 稳定实体：`dim_theme`、`theme_alias`、`dim_stock`。
3. 时序关系：`rel_limit_theme`。
4. 程序化派生：`fact_theme_daily`、`fact_theme_episode`。
5. 模型与人工复核：`llm_analysis`、`source_conflict`。
6. 构建审计：`graph_build_run`、`source_checkpoint`、`dirty_entity_queue`。

每日主归因来自 `tbl_cn_kpl_list.lu_desc`；`theme` 字段拆为辅助标签。题材对应股票由历史涨停事件累积得到，不使用全量概念成分池。schema 中预留的 membership 表在第一版保持为空，只有未来明确扩展时才启用。

## 稳定 ID 与时间

- KPL list 只有题材名称，因此稳定 ID 为 `T-NAME-{name sha1}`。
- 涨停事件 ID 由交易日、股票和榜单状态生成。

所有历史接口使用 `as_of` 截断。每日热度和生命周期状态按日期正序生成，只使用当日及以前记录；周期的最终结束日属于事后分段字段，不能当作当时已知判断。

## 热度与生命周期候选

热度满分 100，当前程序化组成是：

- 涨停宽度 25；
- 梯队积分 35（首板 1、二板 2.5、三板 5、四板 8、五板 12，之后每板加 4）；
- 最大高度 15；
- 封板率 10；
- 最近五个市场日持续性 10；
- 10:00 前封板占比 5。

阶段标签包括试探、启动、发酵、加速、高潮、分歧、延续、退潮。它们是可复核候选标签，不是已经校准的预测真值。当前周期切分允许中间最多一个无活跃记录的市场日；催化变化和二波关系仍需新闻证据与人工/模型复核。

## 增量与回填

```bash
# 首次只构建 2026 年；会同时投影到本地 Obsidian
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py \
  --mode backfill --start-date 20260101 --end-date 20261231

# 每日增量：从断点回看 7 天，内容哈希不变的日期直接跳过
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py --mode incremental

# 修订某一天
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py \
  --trade-date 20260924 --force

# 完全重建指定范围
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py \
  --mode rebuild --start-date 20260101 --end-date 20260924
```

`rebuild` 会删除目标 DuckDB 后重建；`backfill` 可将 2025、2024 或更早数据追加进同一版本化结构。没有断点时，自动增量只构建源库最新年份；更早历史必须显式 backfill。历史回填应按时间正序执行，不能用 2026 的别名或新闻结论倒填过去。服务使用文件锁避免两个 DuckDB 写进程并发。调度器在 KPL 主事实更新后自动运行增量服务。

## 统一查询接口

```python
from coreClient.data_provider import (
    get_theme_profile, get_theme_members, get_theme_daily, get_market_theme_review, get_theme_analyses,
    get_stock_theme_history,
)

profile = get_theme_profile(name="机器人", as_of="20260924")
leaders = get_theme_members(profile["theme_id"], as_of="20260924")
timeline = get_theme_daily(profile["theme_id"], "20260101", "20260924")
market_review = get_market_theme_review("20260924")
analyses = get_theme_analyses(theme_id=profile["theme_id"])
semiconductor_children = get_theme_taxonomy("半导体")
stock_history = get_stock_theme_history("000001.SZ")
```

`get_theme_members(..., historical=True)` 返回逐次主归因涨停事件；默认模式按股票聚合首次涨停、最近涨停、涨停日数与最高板。
`get_theme_analyses(...)` 返回周期归因、证据、审计状态、模型和提示词版本；默认每个周期每类任务只取最新版本。后续研究必须先查此接口，再决定是否补充检索。

## 一级与二级题材

一级题材用于组织知识库文件夹，开盘啦 `lu_desc` 原名作为二级题材保留，不因归类而合并事实。当前 `level1-v1` 将 234 个主归因题材完整映射到 30 个有效一级题材。每个二级题材只有一个主一级归属，同时可在 `secondary_level1_json` 保存最多两个交叉归属。用户明确的电子布、MLCC、电阻电容等归入半导体；模型初分不能覆盖硬规则。AI 应用、软件与数字经济、科技生态与平台分别统计，商业航天与低空经济也分别统计，避免在生命周期分析中混用不同驱动。

配置与映射位于：

- `policyStudy/policy/题材涨停研究/theme_graph/taxonomy/level1_v1.json`
- `policyStudy/policy/题材涨停研究/theme_graph/taxonomy/theme_mapping_v1.json`

重新生成模型草案并写库：

```bash
cd policyStudy/policy/题材涨停研究/theme_graph
../../../../.venv/bin/python build_taxonomy.py --generate
```

日常只应用已经复核的映射时省略 `--generate`。未映射的新题材进入“待归类”文件夹，不会被模型静默并入旧题材。

## Obsidian 投影

默认目录为 `~/Documents/Obsidian Vault/A股题材知识库`，可用 `TRADING_AGENT_THEME_VAULT_ROOT` 修改。`01_题材` 下每个一级题材是一个文件夹，包含一级总览和所属二级题材页；每日复盘中的题材链接会指向对应文件夹。`06_每日复盘` 的人工入口嵌入自动生成页；自动页包含截至当日的题材情绪分、主线结构、热门题材前十与龙一至龙三。3,330 只历史涨停股暂不批量建档，研究到具体股票时再按需创建；题材周期不建立独立文档，始终作为所属题材页面内的周期表和时间线。周期表直接显示最新大模型归因、审计状态和置信度；表格下保留阶段摘要、催化时间线、市场核心、新闻/行情证据索引及未解决问题。`needs_review` 结果允许进入自动生成区并醒目标注为待复核，但不能冒充人工确认结论。渲染器只在人工文件不存在时创建骨架，后续不会覆盖人工研究内容。

每日复盘规则位于 `core/theme_daily_review.py`。前三题材热度按50%/30%/20%合成，再按截至当日最近120个交易日的历史百分位映射到0—100；结构分类另看题材宽度集中度、一级题材聚合、封板质量、持续性和前日排名迁移。龙头候选只取当日主归因涨停股，先按板高分层，同板高内再按近20个市场日涨停频次、封板时间、封单/自由流通盘和成交额确定龙一至龙三。ST板块、ST摘帽、次新股及一级分类“ST与次新”全链路排除。

## 本地模型分工

标准提示词和运行器位于 `policyStudy/policy/题材涨停研究/theme_graph`：

- Qwen vLLM：批量关系分类、题材归一、涨停归因复核；
- Qwen vLLM：优先承担新闻逐条筛选和催化结果审计，利用严格JSON与较快吞吐；
- MiniMax/Hermes：承担筛选后长文本催化综合和周期总结，必须经过Qwen审计关；
- Codex：模式设计、难例裁决、评估和最终方法审查。

模型输出写入 `llm_analysis`，保留 provider、model、prompt_version、input_hash、证据 ID、置信度与复核状态。模型不能直接改写事实表。

```bash
cd policyStudy/policy/题材涨停研究/theme_graph
../../../../.venv/bin/python evaluate_models.py --provider qwen-vllm
../../../../.venv/bin/python evaluate_models.py --provider minimax-hermes
```

当前种子集只有两个边界案例，只用于验证 JSON 合同和“市场核心/产业核心”区分，不可报告为模型准确率。正式比较需扩展为分层金标集，覆盖直接主营、控股子公司、参股、基金间接投资、客户供应商、送样、量产、传闻和否认。

题材催化使用 `build_catalyst_packet.py` 和 `analyze_episode_catalysts.py`。程序先按题材周期构建历史时间窗，新闻只按来源时间进入候选；Qwen逐条筛选，MiniMax综合主因与竞争解释，Qwen再检查时间倒置、证据引用和因果等级。盘后行情稿只能作为 `late_explanation/background_only`，不能倒写为盘中触发。全部输出默认 `needs_review`，可投影到 Obsidian 自动生成区作为研究底稿，但必须保留状态和模型版本。

单周期分析和联网补证在成功写库后自动刷新 Obsidian；批量任务只在整批完成后刷新一次。渲染顺序固定，无数据变化时第二次运行 `files_written=0`。

若本地新闻库得出 `no_reliable_reason`、只有盘后解释或关键启动/高潮日仍无直接证据，则进入联网补证流程。检索范围限定在该周期及其前置窗口；每条外部证据必须保存原始 URL、网页标题、来源、发布时间、抓取时间和对应行情日期，并重新经过 Qwen 时序/引用审计后写入 `llm_analysis`。搜索摘要不能单独作为归因证据。联网仍无结果时保留“未找到可靠归因”，不得用常识补写。

新闻候选中“某股涨停、板块拉升、概念异动”属于行情结果稿。检索排序会对这类标题降权，筛选模型将其标为 `market_recap`；没有新增独立事件的异动稿最多作为市场响应或盘后解释，不能进入主因证据。若异动稿引用了政策、公告、发射、价格变化等独立事件，必须拆开事件事实与行情结果，并按事件最早可见时间判断因果。

## 验证

```bash
PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python -m pytest -q \
  offlineDataManager/scripts/core/test_theme_graph_store.py
```

测试覆盖首次构建、重复运行幂等、源数据修订、历史时点、成员区间以及 Obsidian 人工内容保护。

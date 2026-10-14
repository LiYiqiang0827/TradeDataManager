# Major News 清洗、压缩与 LLM 提炼工具

## 1. 目标与边界

工具把 `db_cn_news.db` 中的 `tbl_major_news` 加工成可供题材研究、每日复盘和催化检索直接使用的新闻事件。每日新闻分析不再处理 `tbl_news` 普通快讯；原始 SQLite 新闻仍全部保留。去重、过滤和模型结论写入可重建的 DuckDB 派生层。

处理链分为四层：

1. **原始层**：`tbl_major_news`；`tbl_news` 和 `tbl_cctv_news` 继续采集但不进入每日新闻分析；
2. **确定性清洗层**：文本标准化、同源精确去重、跨源精确去重、36小时近重复聚类；
3. **规则过滤层**：剔除A股个股、指数、板块涨跌结果稿、成交额播报和涨停复盘模板，保留政策、公告、商品价格、海外指数、供需与产能事实；
4. **LLM提炼层**：Qwen批量判断研究价值，输出重要性、类别、一句话摘要、关键事实、相关题材、实体，以及对A股整体或主要相关行业的利好/利空/中性评级与依据。

行情结果稿仍保留在事件成员表，并带有排除原因；它们不会进入默认有效新闻结果，也不会被物理删除。

## 2. 文件与运行入口

| 用途 | 路径 |
|---|---|
| 原始新闻库 | `offlineDataManager/data/db_cn_news.db` |
| 派生事件库 | `offlineDataManager/data/db_major_news_events.duckdb` |
| 清洗和事件聚类 | `offlineDataManager/scripts/core/news_event_store.py` |
| LLM提炼 | `offlineDataManager/scripts/core/news_llm_refiner.py` |
| 提炼提示词 | `offlineDataManager/scripts/core/prompts/news_refinement_v2.md` |
| 清洗服务 | `offlineDataManager/scripts/service/service_news_events.py` |
| 每日完整服务 | `offlineDataManager/scripts/service/service_daily_major_news_analysis.py` |
| 旧逐事件LLM服务 | `offlineDataManager/scripts/service/service_news_llm.py`（保留作审计，不再由调度器调用） |
| 统一查询 | `coreClient/data_provider.py` |

`service_major_news` 更新完成后调用确定性清洗服务；`service_news` 只更新原始普通快讯。清洗服务和每日分析服务共用 `.news_events.lock`，避免同一 DuckDB 在分析时被改写。模型运行期间原始 SQLite 仍可更新，下一轮 Major News 清洗会按断点补齐。

常驻调度器每天05:10异步启动前一自然日的完整 Major News 分析：规则事件、BGE-M3语义聚类、reranker/Qwen轻筛、最高分事件深提炼、Obsidian投影。修改调度代码后需要按项目原有方式重启常驻调度器才会加载新任务。

## 3. 数据库结构

### `fact_news_event`

一行代表一个去重后的事件。保存首次/最后发布时间、代表新闻、来源集合、原始成员数、精确/近重复数、过滤类别、是否排除及规则版本。

### `rel_news_event_member`

保存每一条原始 Major News 到事件的映射。`duplicate_kind` 为：

- `representative`：新事件首条；
- `same_source_exact`：同源内容完全相同；
- `cross_source_exact`：不同来源内容完全相同；
- `near_duplicate`：标题语义高度相似且数值签名一致。

数值签名用于保护“铜价涨2%”与“铜价涨3%”等连续更新，不把不同价格或涨跌幅错误合并。

### `fact_news_insight`

每个确定性有效事件对应一条最新模型判断。字段包括 `llm_keep`、`importance_score`、`category`、`summary`、`key_facts_json`、`themes_json`、`entities_json`、影响周期、新颖度、模型、输入哈希与 `run_id`。

模型只看到当时事件文本。输入哈希和提示词版本相同时自动跳过；新闻代表文本或提示词改变时会重新处理。

默认每批40个事件。高召回提示词的输出较长，实测100条一批可能使JSON在生成末尾被截断；40条批次可稳定通过机器校验，成功批次立即落库，失败后续跑只处理尚未成功的输入。

### `news_llm_run` 与 `pipeline_state`

`news_llm_run` 记录旧逐事件模型批次，供审计使用；快速级联的中间结果按日期写入 `outputs/major_news_analysis/`。`pipeline_state` 保存 Major News 最新处理时间、数据源类型和规则版本，支持一天回看窗口的增量续跑。

`pipeline_state` 同时保存清洗规则版本。代码规则升级后，增量任务会拒绝把不同版本混在同一派生库中，并要求显式使用 `--rebuild`；重建会清空对应的旧LLM结论，避免摘要引用已经变化的事件簇。

每日LLM统计只计算当前 `NEWS_LLM_PROMPT_VERSION`。提示词升级期间，旧版本结论可以暂留供审计，但不会与新版本样本混算为“当日完成”。

## 4. 过滤原则

### 默认剔除

- A股个股快速拉升、跳水、涨停、跌停等价格结果；
- A股指数、板块、概念的盘中涨跌播报；
- 两市/ETF成交额播报、午评、收评、涨停复盘；
- “概念联动N连板、背后逻辑揭晓”等先有股价再拼接原因的模板稿。

### 默认保留

- 法规政策、正式公告、中标、并购、投产、停产、减产、价格调整；
- 宏观、产业、供需、库存、产销量等可核验数据；
- 铜、铝、原油、黄金、农产品等商品价格事实；
- 美股等海外主要市场事实；
- 标题先陈述独立事实、随后提及市场反应的新闻。

规则只是高召回的第一道门。最终研究价值由LLM根据“是否包含能改变预期的新事实”进一步压缩。

## 5. 使用方法

```bash
cd ~/TradingAgent

# 重建一个日期范围的 Major News 确定性事件层
PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python \
  -m service.service_news_events \
  --start-date 20260901 --end-date 20260930 --rebuild

# Major News 日常增量（按断点回看一天）
PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python \
  -m service.service_news_events

# 完整生成某日分析并写入 Obsidian；相同输入自动续跑/复用
PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python \
  -m service.service_daily_major_news_analysis --trade-date 20260924

# 验证提示词时可做稳定抽样，不必先跑完整日
THEME_VLLM_TRANSPORT=ssh \
THEME_VLLM_MODEL=/home/sysadmin/data/disk02/jc/models/qwen3.8-27b-fp8 \
PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python \
  -m service.service_news_llm \
  --start-date 20260924 --end-date 20260924 \
  --limit 200 --sample --sample-seed validation-20260928 --force

# 用Hermes MiniMax提炼
PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python \
  -m service.service_news_llm \
  --start-date 20260924 --end-date 20260924 --provider minimax-hermes
```

统一查询接口：

```python
from coreClient.data_provider import (
    get_news_events,
    get_news_event_daily_stats,
    get_news_insights,
    get_news_llm_daily_stats,
)

events = get_news_events("20260924", "20260924")
insights = get_news_insights("20260924", "20260924", min_importance=60)
stats = get_news_event_daily_stats("20260901", "20260930")
llm_stats = get_news_llm_daily_stats("20260901", "20260930")
```

## 6. 每日质量检查

每天至少检查：

- 原始量、事件量、确定性有效量、LLM有效量是否偏离近期分位数；
- `same_source_exact`、`cross_source_exact`、`near_duplicate`占比是否突变；
- 随机抽查被剔除结果稿和保留事实各20条；
- 数值不同的同主题快讯是否被错误合并；
- LLM批次是否完整，是否存在 `failed` 运行或未覆盖事件；
- 所有模型摘要能否追溯到 `event_id` 和原始成员。

LLM有效新闻量只能在 `llm_processed_events == deterministic_events` 时用于正式统计；部分覆盖只能视为进度，不能外推为当日完成结果。

统计同时给出三个使用层级：`llm_effective_events` 为35分以上的事实底座，`material_events` 为45分以上的日常研究材料，`high_importance_events` 为60分以上的复盘优先项。下游不应把“35分事实”误写成交易建议。

## 7. Major News 语义事件快速级联

每日生产链采用分层级联流程：

1. 规则事件用服务器上的 `BAAI/bge-m3` 生成中文向量；
2. 向量只负责召回疑似重复事件，`BAAI/bge-reranker-v2-m3` 对事件对做严格复核；
3. 对 Major News 全量语义事件做重排；日量低于1,000时不丢弃孤立新闻；
4. 候选按大块并行交给Qwen，模型只返回ID、分数和类别；
5. 对最高分的少量事件生成摘要、关键事实、题材、实体、影响周期和方向评级；其余入选事件由轻量批次补齐利好/利空/中性评级及依据；
6. 将全部入选事件及图关系归档到 DuckDB，再从数据库投影到独立 Obsidian Vault。

试验入口：

| 用途 | 路径 |
|---|---|
| 语义聚类 | `offlineDataManager/scripts/experiments/news_event_cluster_pilot.py` |
| 完整级联基线 | `offlineDataManager/scripts/experiments/news_event_cascade_pilot.py` |
| 传播度快速级联 | `offlineDataManager/scripts/experiments/news_event_fast_pilot.py` |
| 新闻图归档与查询 | `offlineDataManager/scripts/core/major_news_graph_store.py`、`offlineDataManager/scripts/service/news_graph_tool.py` |
| Obsidian投影 | `offlineDataManager/scripts/service/project_daily_news_to_obsidian.py` |
| 每日输出 | `outputs/major_news_analysis/YYYYMMDD/` |
| 每日新闻Vault | `~/Documents/每日新闻分析/` |

2026-09-24 的实测输入为820条原始 Major News，规则层形成792个事件并排除11个行情结果事件；781个有效规则事件合并为765个语义事件。Qwen轻筛保留283个，对最高60个做深提炼。向量与聚类约8.6秒，正常冷启动完整日约4至5分钟。一次模型枚举格式异常使首次试跑续跑40条；归一化修复后不会再因该类展示字段中止整批。

因为 Major News 日量通常低于1,000，当前默认让所有规则有效事件进入Qwen轻筛，不再使用普通快讯方案中可能漏掉重大孤立新闻的800条激进预筛。多数 Major News 记录只有标题、正文为空，因此摘要和关键事实只能以标题为证据；需要全文证据的研究仍应回查原始URL或后续新闻来源。

生成每日文档（须先完成当日图数据库导入）：

```bash
.venv/bin/python offlineDataManager/scripts/service/project_daily_news_to_obsidian.py \
  --trade-date 20260924 --mode fast
```

文档默认写入 `~/Documents/每日新闻分析/每日新闻/YYYY-MM-DD.md`，并更新Vault根目录的 `首页.md`。同一日期重复运行会幂等覆盖当天文档。

图数据库的增量、补录、查询和文档重建方法见 [新闻图数据库工具.md](新闻图数据库工具.md)。上述 2026-09-24 数量是早期试跑快照；真实入库数量以 `fact_major_news_daily_analysis` 当前记录为准。

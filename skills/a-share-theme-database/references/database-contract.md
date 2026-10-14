# 查询与数据合同

## 核心路径

- 项目：`/Users/nickzhang/TradingAgent`
- DuckDB主库：`offlineDataManager/data/db_theme_graph.duckdb`
- KPL源库：`offlineDataManager/data/db_cn_kpl.db`
- 新闻源库：`offlineDataManager/data/db_cn_news.db`
- 构建器：`offlineDataManager/scripts/core/theme_graph_store.py`
- 服务：`offlineDataManager/scripts/service/service_theme_graph.py`
- 渲染器：`offlineDataManager/scripts/core/theme_vault.py`
- 查询入口：`coreClient/data_provider.py`
- Obsidian：`~/Documents/Obsidian Vault/A股题材知识库`

环境变量 `TRADING_AGENT_THEME_GRAPH_DB_PATH` 和 `TRADING_AGENT_THEME_VAULT_ROOT` 可以覆盖默认路径。执行前读取实际配置。

## 核心表

| 表 | 作用 | 关键主键 |
|---|---|---|
| `dim_theme` | 主题材、辅助标签和事件分支 | `theme_id` |
| `theme_alias` | 题材别名与有效期 | `theme_id, alias, source, valid_from` |
| `dim_theme_taxonomy` | 一级、二级分类 | `theme_id` |
| `dim_stock` | 至少实际涨停过一次的股票 | `ts_code` |
| `fact_limit_event` | 涨停和炸板事件 | `event_id` |
| `rel_limit_theme` | 事件到主、辅助题材的关系 | `event_id, theme_id, attribution_role` |
| `fact_theme_daily` | 宽度、高度、梯队、热度和阶段 | `trade_date, theme_id` |
| `fact_theme_episode` | 炒作周期 | `episode_id` |
| `llm_analysis` | 模型归因、审计、摘要和证据 | `analysis_id`及版本唯一键 |
| `source_day_manifest` | 每日源行数与内容哈希 | `source_name, trade_date` |
| `source_checkpoint` | 增量断点 | `source_name` |
| `graph_build_run` | 构建运行记录 | `run_id` |
| `dirty_entity_queue` | 待重算与已处理实体 | 复合键 |

`fact_membership_snapshot` 和 `rel_theme_stock_membership` 是未来普通成分扩展层，当前应为空。

## 查询接口

```python
from coreClient.data_provider import (
    get_theme_profile,
    get_theme_members,
    get_theme_daily,
    get_market_theme_review,
    get_market_theme_review_series,
    get_theme_cycle_data,
    get_theme_analyses,
    get_theme_taxonomy,
    get_stock_theme_history,
)
```

- `get_theme_profile(theme_id=None, name=None, as_of=None)`：题材身份、截至时点的最近状态和周期。
- `get_theme_members(theme_id, as_of=None, historical=False)`：历史主归因涨停股；`historical=True` 返回逐次事件。
- `get_theme_daily(theme_id=None, start_date=None, end_date=None)`：日度宽度、高度、热度和阶段。
- `get_market_theme_review(trade_date=None, top_n=10, leader_count=3)`：指定日题材情绪、热门题材、结构标签和龙一至龙三；为空时取库内最新交易日。
- `get_market_theme_review_series(start_date=None, end_date=None, top_n=10, leader_count=3)`：批量返回连续交易日的全市场题材复盘序列。
- `get_theme_cycle_data(theme_id=None, name=None, start_date=None, end_date=None, as_of=None)`：返回单题材周期图所需的完整交易日、日度状态、周期、个股事件、龙头和催化分析。
- `get_theme_analyses(theme_id=None, name=None, episode_id=None, latest=True)`：归因、证据、审计和模型版本。
- `get_theme_taxonomy(level1_name=None)`：一级、二级题材映射。
- `get_stock_theme_history(ts_code)`：个股历史主、辅助题材归因。

## 历史时点

- 题材研究指定日期时，`get_theme_profile` 和 `get_theme_members` 必须传 `as_of`。
- `get_theme_daily` 必须限制 `end_date`。
- `get_market_theme_review` 在历史研究中必须传 `trade_date`；内部只读取该日及以前事实。
- 周期的 `end_date` 是事后分段字段；历史判断只使用截至当时可见的日度记录。
- 模型分析的 `created_at` 晚于历史决策时点时，只能作为事后知识，不得伪装为当时信息。

## 维护流程

在项目根目录执行：

```bash
# 每日增量，默认回看7天
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py --mode incremental

# 指定日强制修复
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py \
  --trade-date 20260924 --force

# 历史回填
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py \
  --mode backfill --start-date 20250101 --end-date 20251231

# 只更新数据库，不渲染Obsidian
.venv/bin/python offlineDataManager/scripts/service/service_theme_graph.py \
  --mode incremental --skip-vault
```

不要在普通维护中执行 `--mode rebuild`。重建会删除目标数据库；需要先决定如何保留 `llm_analysis` 等研究成果。

## 只读审计

```python
import duckdb

con = duckdb.connect(
    "/Users/nickzhang/TradingAgent/offlineDataManager/data/db_theme_graph.duckdb",
    read_only=True,
)
```

优先检查：

```sql
SELECT * FROM source_checkpoint;

SELECT run_id, mode, start_date, end_date, status, error,
       started_at, completed_at
FROM graph_build_run
ORDER BY started_at DESC
LIMIT 10;

SELECT status, COUNT(*)
FROM dirty_entity_queue
GROUP BY status;

SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*)
FROM fact_limit_event;

SELECT tag, COUNT(*), COUNT(DISTINCT ts_code)
FROM fact_limit_event
GROUP BY tag;

SELECT attribution_role, COUNT(*)
FROM rel_limit_theme
GROUP BY attribution_role;
```

## 验收

### 数据库

- 源库最大交易日与检查点符合预期；
- 最新构建运行成功且范围正确；
- 内容未变化时重复增量为幂等；
- 脏队列没有异常长期 pending；
- 实际涨停数、炸板数和主归因关系数与 KPL 源一致；
- 历史 `as_of` 查询不返回未来记录；
- membership预留表仍为空，除非用户明确批准扩展口径。

### 模型分析

- 研究前已经查询并复用现有 `llm_analysis`；
- 模型结果没有覆盖事实表；
- 证据可追溯到原文，发布时间不晚于所解释的行情节点；
- 搜索摘要和纯异动稿没有单独充当原因；
- 结果保留竞争解释、置信度、复核状态和未解决项。

### Obsidian

- 目标题材页存在于正确一级题材文件夹；
- 周期归因已进入题材页的炒作周期表；
- 自动区与人工研究区都保留；
- 无数据库变化时二次渲染不应重复写文件；
- 报告模型完成、数据库写入、Obsidian写入各自数量。

### 每日题材复盘

- 情绪分在0—100，返回日期与请求日期一致；
- ST板块、ST摘帽、次新股和一级分类ST与次新没有进入统计；
- 热门题材、结构证据和龙头候选只使用截止日可见事实；
- 龙一至龙三遵循板高硬优先，同板高内再比较人气、封板、封单和成交；
- 生成页与接口结果一致；模型盲审一致率只作为规则审计，不报告为真实准确率。

# 每日题材复盘

## 定位与入口

每日题材复盘是题材知识库的可回测派生层。它从 DuckDB 中截至指定交易日可见的题材日度事实和主归因涨停事件生成结果，不新增或改写事实表。

统一入口：

```python
from coreClient.data_provider import get_market_theme_review

review = get_market_theme_review("20260924", top_n=10, leader_count=3)
```

`trade_date` 为空时取题材库最新交易日。历史研究必须显式传入日期；接口只读取该日及以前数据。实现位于 `offlineDataManager/scripts/core/theme_daily_review.py`，方法版本为 `theme-market-review-v2`。

## 结果合同

顶层字段包括：

- `trade_date`、`method_version`、`point_in_time`；
- `theme_sentiment_score`、`theme_sentiment_level`、`top3_heat_composite`、`history_window_sessions`；
- `structure`：代码、中文标签、置信等级、摘要和可复核证据；
- `hot_themes`：热度排名、宽度、炸板、高度、封板率、持续性、阶段、前日排名变化和龙头梯队；
- `excluded_scope`：全链路排除项。

五种结构枚举：

| code | 含义 |
|---|---|
| `clear_single_mainline` | 主线明确 |
| `multiple_mainlines` | 多主线 |
| `mainline_replacement` | 主线替换 |
| `scattered_no_mainline` | 热点分散、无明确主线 |
| `cold_no_mainline` | 热点低迷、无主线 |

## 计算口径

1. 当日前三题材热度按 50%/30%/20% 合成原始值。
2. 情绪分是原始值在截至当日最近最多120个交易日中的百分位，映射到0—100。80、60、40、20分别划分高热、偏高、中性、偏低和低迷。历史窗口不足120日时必须报告实际窗口，不能把分数与完整历史样本直接比较。
3. 市场结构另看题材宽度、板高、封板质量、五日持续性、集中度和前日排名迁移。多主线先按一级题材聚合，防止同一产业的多个分支被误判为多条主线；同一一级题材内部轮动不算主线替换。
4. 龙头候选仅来自当日 `primary` 主归因涨停股。板高使用互不重叠的硬优先级；同板高内按近20个市场日涨停频次40%、封板时间20%、封单/自由流通盘20%、成交额20%排序，最多输出龙一、龙二、龙三，并保留高度核心、先锋、容量核心、封单核心和人气核心等角色。
5. `ST板块`、`ST摘帽`、`次新股` 和一级分类 `ST与次新` 不进入情绪分母、热门排名、结构判断或龙头候选。

情绪分回答“前三题材相对自身历史是否热”，结构标签回答“热点是否聚合成可持续主线”。高情绪可以对应热点分散，低情绪也不排除局部题材有高度股。

## Obsidian投影

`ThemeVaultRenderer.render()` 会生成：

- `_generated/daily/<YYYYMMDD>__dashboard.md`：情绪、结构、热门题材前十、龙一至龙三、完整热度表和方法说明；
- `06_每日复盘/<YYYYMMDD>.md`：人工入口，嵌入对应自动页面。

人工入口只在不存在时创建，后续渲染不得覆盖人工研究内容。无数据变化时再次渲染应为幂等，验收时报告 `files_written`、`files_unchanged` 和错误。

## 周期图

连续区间查询使用：

```python
from coreClient.data_provider import get_market_theme_review_series, get_theme_cycle_data

market = get_market_theme_review_series("20260105", "20260924")
theme = get_theme_cycle_data(name="商业航天", start_date="20260401", end_date="20260731")
```

全市场图：

```bash
.venv/bin/python plot/plot_market_theme_cycle.py \
  --start-date 20260105 --end-date 20260924
```

单题材图：

```bash
.venv/bin/python plot/plot_single_theme_cycle.py 商业航天 \
  --start-date 20260401 --end-date 20260731
```

全市场输出情绪周期和一级题材轮动两张图；单题材输出生命周期和核心股演化两张图。默认保存到 `plot/tmp_savepic/theme_cycle`。历史回放传 `--as-of`，不得在历史可见图中绘制截止日之后生成的模型归因或最终周期边界。

## 验证

基础测试：

```bash
PYTHONPATH=offlineDataManager/scripts:. .venv/bin/python -m pytest -q \
  offlineDataManager/scripts/core/test_theme_graph_store.py
```

盲审脚本：

```bash
.venv/bin/python policyStudy/policy/market_review/evaluate_theme_structure.py \
  --dates 20260904 20260916 20260924 --providers both
```

盲审输入只包含当日和前一交易日派生事实，不含程序标签或未来信息。Qwen/MiniMax与程序的一致率只用于发现边界案例，不能称为真实市场准确率。真实准确率需要预先定义人工金标，或用冻结的当日判断和后续结果建立独立评分。

每日验收至少检查：

- 返回日期与请求日期一致，情绪分位于0—100；
- `point_in_time=true`，查询未读取截止日后的事实；
- 排除题材没有进入热门题材或龙头候选；
- 龙一板高不低于同题材龙二、龙三；
- 同一级题材分支轮动没有误报为主线替换；
- Obsidian生成页与接口的分数、结构和排名一致；
- 报告程序规则版本，并把模型一致率与真实准确率明确区分。

# 题材知识库模型任务

这里存放可以交给本地 Qwen vLLM 或 Hermes/MiniMax 批量执行的标准化提示词、运行器和金标评估。结构化市场事实由 `offlineDataManager/scripts/core/theme_graph_store.py` 生成，模型只写审计表 `llm_analysis`。

五类任务：

1. `stock_exposure`：股票与题材的市场关系和产业关系。
2. `theme_normalization`：题材同义、上下级和阶段分支。
3. `catalyst_extraction`：新闻/公告催化抽取。
4. `limit_attribution`：当日涨停归因复核。
5. `episode_summary`：行情段总结。

催化分析采用有界四段流程：

1. Python 从新闻库按行情窗口、题材检索词和启动股名称构建候选包；
2. Qwen vLLM 执行 `catalyst_screening`，逐条保留、排除并识别转载；
3. Hermes/MiniMax 执行 `catalyst_extraction`，结合行情宽度、高度和涨停时间归纳主因、竞争解释与叙事演化；
4. Qwen 执行 `catalyst_audit`，检查引用、时间倒置、因果过度和类型错误，MiniMax再生成周期摘要。

所有模型结论默认是待复核草案。模型只写 `llm_analysis`，不能改动题材事实表，也不能直接写进 Obsidian 人工结论；渲染器会把最新审计结果投影到题材页的自动生成区，在周期表内显示归因并明确标记复核状态。

后续研究先通过 `coreClient.data_provider.get_theme_analyses()` 读取已有周期归因和证据。只有新周期、源数据修订，或已有结果为 `no_reliable_reason` / 明确留有关键证据缺口时才重新检索。本地新闻仍无法归因时可联网补证，但必须保存 URL、来源、发布时间和抓取时间，经过同样的时序审计后才能写回；搜索摘要本身不是证据。

个股异动、涨停和板块拉升新闻先视为行情结果。候选排序对纯异动稿降权，Qwen必须输出 `evidence_nature=market_recap`；这类稿件可验证市场响应，不能单独解释启动。只有稿内可独立核验的原始事件才进入催化判断，且应继续寻找公告、政策或事件原始来源。

每个输入证据必须带 `evidence_id` 和历史可见时点 `valid_at`。提示词要求严格 JSON；运行器会检查必填字段并缓存输入哈希。种子评估集只含两个范例，不能代表真实准确率。

单周期运行示例：

```bash
PYTHONPATH=offlineDataManager/scripts:. /Users/nickzhang/TradingAgent/.venv/bin/python \
  theme_graph/analyze_episode_catalysts.py \
  --episode-id EP-T-NAME-75A8F7584A2C2C-20260414-03 \
  --compare-provider qwen-vllm
```

运行产物位于 `outputs/theme_catalyst_analysis/<episode_id>/`，包括候选新闻、新闻全文、筛选结果、双模型催化判断、审计和周期摘要。

本地新闻无法形成可靠归因时，把已打开并核验原文的外部证据写成 JSON（字段至少含 `url/title/source/published_at/observed_at/matched_focus_date/content`），再运行：

```bash
PYTHONPATH=offlineDataManager/scripts:. /Users/nickzhang/TradingAgent/.venv/bin/python \
  theme_graph/reanalyze_with_external_evidence.py \
  --episode-id <EPISODE_ID> --evidence-file <WEB_EVIDENCE.json>
```

外部证据会先由 Qwen 单独筛选，再与本地证据合并，经过 MiniMax 综合、Qwen 审计和周期摘要后写回 `llm_analysis`。外部筛选子步骤只保存到产物目录，不覆盖数据库里的完整候选集筛选版本。

批量联网补证由 `research_external_evidence_batch.py` 执行。默认先在 IMA 共享知识库“前瞻研报团队（每日更新）”按题材名检索时间窗口内的研报标题，把结果保存为 `09_ima_research_leads.json`，再交给 Hermes/MiniMax 追溯并打开原始网页。IMA 标题和摘要只能帮助发现事件，不能直接进入证据；最终 `10_web_evidence.json` 仍须包含可访问的原文 URL、发布时间和事实摘要。IMA 配额耗尽或共享文件无法读取时自动继续 Web 检索，并在缓存中记录状态供次日续跑。

```bash
PYTHONPATH=offlineDataManager/scripts:. /Users/nickzhang/TradingAgent/.venv/bin/python \
  theme_graph/research_external_evidence_batch.py \
  --episode-id <EPISODE_ID> --render-every 1
```

模型分工保持固定：MiniMax 使用 Web 工具找原始事件与完成归因，Qwen 执行证据筛选和时序/引用审计，MiniMax生成周期摘要。批次支持已有证据、已有原始模型输出和完整外部分析结果的分层复用，避免重复消耗模型；`--force` 才会强制重新研究。证据发布时间晚于错误匹配日时，程序只允许把它重映射到其后的首个关键日；若已没有可解释的后续关键日，则剔除该条，不能倒置因果。

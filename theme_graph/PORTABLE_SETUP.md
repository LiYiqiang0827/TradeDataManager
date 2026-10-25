# 每日题材摘要 / 涨停归因：完整链路与新机部署

## 两个代码仓库

- 数据接口、结构、生成与入库：[TradeDataManager](https://github.com/LiYiqiang0827/TradeDataManager)，本地目录建议叫 `TradingDataManager`。
- 网页：[TradingAlertSystem](https://github.com/LiYiqiang0827/TradingAlertSystem)，入口 `limitup-dashboard/server.py`。
- 本次配套版本：两个仓库的 `theme-graph-chain-v1` 标签；也可使用包含该标签的 main。
- 数据不在 Git 内。正式数据放 `$TRADING_DATABASE_ROOT`，包含 `offline/` 行情和 `theme_graph/` 分析成果。

这次补交的是 Mac mini 实际运行的 `coreClient/neo4j_client.py`、日报解析/SQLite、Neo4j 导入器、当前提示词及 skill。新增的可移植配置和合成示例用于验证这条链路，不复制本机密码、真实日报或正式库。

## 数据流与结构

```text
历史 DuckDB / 当日 KPL performance SQLite
    -> 分析输入包（行情事实） + 同日及以前的新闻证据 + 前几日日报
    -> agent 按 skill 研究，或既有 Qwen/Hermes 接口生成候选 Markdown 日报
    -> 结构校验 + 人工证据审核
    -> daily_limitup_graph SQLite（可追溯权威分析快照）
    -> JSON 表快照 -> Neo4j
    -> dashboard 的每日/题材/个股页面、天梯悬浮框
```

开盘啦标签不是 AI 归因；Neo4j 里没有的原因不由网页现场生成。历史分析从 `offline/db_theme_graph.duckdb` 读取；同日晚复盘从 `offline/db_cn_kpl.db:tbl_cn_kpl_limit_performance` 读取，不能等通常次日才到的 `tbl_cn_kpl_list`。网页的单股主归属仍以 performance 的主题材为准。旧日报的复合章节摘要不自动冒充另一题材的摘要。

| Neo4j 对象 | 业务键 / 内容 | SQLite 对应 |
| --- | --- | --- |
| `Topic` | `topic_id`，名称、原章节、归一化状态 | `topic_nodes` |
| `TradeDate` | `trade_date`，YYYYMMDD | `date_nodes` |
| `TopicDate` | `topic_date_id`，承接题材-日期上下文 | `topic_date_nodes` |
| `Topic-[:HAS_DAILY_SNAPSHOT]->TradeDate` | 催化、强度、梯队 JSON、高度、宽度、资金概览 | `topic_date_summary_edges` |
| `TopicDate-[:ABOUT_TOPIC]->Topic` / `TopicDate-[:FOR_DATE]->TradeDate` | 上下文关联 | 由题材日期行派生 |
| `Stock` | `stock_code`，如 600000.SH | `stock_nodes` |
| `TopicDate-[:ATTRIBUTES_LIMITUP]->Stock` | 涨停原因、题材桥梁、反证、信心、板数、时间、来源 | `topic_date_stock_edges` |
| `SourceDocument` / `ImportRun` / `FROM_SOURCE` | 原文 SHA256、来源、导入历史 | `source_documents` / `import_runs` |

SQLite DDL：`theme_graph/daily_limitup_graph/schema.sql`。
Neo4j DDL：`coreClient/neo4j_schema.cypher`，由 `ThemeGraphNeo4jStore.ensure_schema()` 幂等创建唯一约束及查询索引。无数值强度或资金信息时保持缺失，不用定性文本编造净流入。

## 环境与安装

要求 Python >=3.10（本次验证 Python 3.12）、Neo4j Community（验证 2026.09.0）、driver `neo4j>=6,<7`。依赖见根目录 `pyproject.toml` 的 `graph` extra；Redis 可选，未安装时网页回退本地 SQLite 缓存。演示无需 Tushare/KPL 令牌、Hermes 或模型服务。

```bash
mkdir -p ~/trading-dev
cd ~/trading-dev
git clone https://github.com/LiYiqiang0827/TradeDataManager.git TradingDataManager
git clone https://github.com/LiYiqiang0827/TradingAlertSystem.git TradingAlertSystem
git -C TradingDataManager checkout theme-graph-chain-v1
git -C TradingAlertSystem checkout theme-graph-chain-v1
export TRADING_DATA_MANAGER_ROOT="$PWD/TradingDataManager"
export ALERT_ROOT="$PWD/TradingAlertSystem"
python3 -m venv "$TRADING_DATA_MANAGER_ROOT/.venv"
export TRADING_PYTHON="$TRADING_DATA_MANAGER_ROOT/.venv/bin/python"
"$TRADING_PYTHON" -m pip install -e "$TRADING_DATA_MANAGER_ROOT[graph]"
```

若 GitHub 是私有仓库，先使用本人有权限的 Git 登录方式。配置模板：`theme_graph/.env.example`，没有凭据；编辑为自己的绝对路径后手动 `. /path/to/private.env`。Python 不会自动加载 `.env`。

## 最小可复现示例（隔离实例）

示例是明确标注的合成行情和报告，日期 20261015、20261016。代码用两个真实格式的代码测试代码后缀处理，但名称、行情、题材和原因均为演示数据，不是真实市场研究。没有分钟/指数历史时相关图表为空，本例的验收对象是摘要、归因、日期与页面链路。

1. 新建隔离 Neo4j。已有 Neo4j 时可用一个单独空实例。也提供下述 Docker Compose 入口；Mac mini 实验实际使用独立原生实例，同版本数据库，未改正式库。

```bash
# 用交互输入提供自己的私密密码，不要写进 Git。
read -r -s TRADING_THEME_NEO4J_PASSWORD
export TRADING_THEME_NEO4J_PASSWORD
# 确保输入密码长度符合 Neo4j 要求，然后启动；首次需等待 Bolt 就绪。
docker compose -f "$TRADING_DATA_MANAGER_ROOT/theme_graph/neo4j/compose.yaml" up -d
export TRADING_THEME_NEO4J_URI="bolt://127.0.0.1:17687"
export TRADING_THEME_NEO4J_USERNAME="neo4j"
export TRADING_THEME_NEO4J_DATABASE="neo4j"
```

不用 Docker 时自行安装 Neo4j Community、启用认证、设定密码，设置上面的 URI。也可用 `TRADING_THEME_NEO4J_PASSWORD_FILE=/absolute/private/password-file`，权限建议 0600。驱动密码优先级为显式构造参数、环境密码、指定密码文件；保留旧 Mac mini trial 文件作为兼容兜底，不要求别的机器有这个文件。

2. 生成输入、校验日报、SQLite 入库、导出并导入 Neo4j，重复导入确认无重复，并实际查询和更新：

```bash
# 必须是一个尚不存在的目录；程序会拒绝已有目录和非空 Neo4j。
export TRADING_DATABASE_ROOT="$HOME/trading-demo-data"
"$TRADING_PYTHON" "$TRADING_DATA_MANAGER_ROOT/theme_graph/examples/run_demo.py" \
  --root "$TRADING_DATABASE_ROOT"
```

预期：2 个日期、1 个题材、2 个题材日期、2 个股票、3 条归因、2 条来源边；SQLite integrity=ok；末尾 status=passed。流程复用生产 packet 构建器、日报解析器、图 API、快照导入器，没有 mock Neo4j，也不调用付费模型。

3. 启动实际网页与 API：

```bash
export TRADINGALERT_HOST=127.0.0.1
export TRADINGALERT_PORT=8765
# Redis 可选；不需要先安装或启动。
cd "$ALERT_ROOT/limitup-dashboard"
"$TRADING_PYTHON" server.py
```

打开 `http://127.0.0.1:8765/?date=20261016`，点击“示例题材A”，催化与题材逻辑应出现“演示催化摘要”；点击“演示甲”，涨停原因应出现“演示甲涨停原因”。另一个终端在相同环境运行：

```bash
"$TRADING_PYTHON" "$ALERT_ROOT/limitup-dashboard/tests/smoke_theme_chain.py" \
  --url http://127.0.0.1:8765
```

网页读取只需 Neo4j 的读权限；建约束/入库另需写权限。正式市场数据需要自己的合法来源与采集配置，示例不提供供应商账号或正式数据副本。

## 正式生成与更新

维护源 skill：`skills/daily-limitup-analysis-theme-db/SKILL.md`。`theme_graph/SKILL.md` 是相对链接；skill 的 `references/`、`scripts/` 等也用仓库内相对链接，克隆后可直接解析。可让具备检索能力的 agent 依照 skill 执行。

生产脚本分工：

- `theme_graph/scripts/build_daily_limitup_packet.py`：历史 DuckDB 输入包，`--db --out --start --end`。
- `theme_graph/scripts/build_same_day_kpl_packet.py`：当日已落库 performance 输入包，`--db --out --trade-date`。
- `refresh_same_day_kpl_snapshot.py`：仅当同日快照缺失或不合格时补抓；需私有 KPL 配置，不是新机演示前置。
- `generate_daily_report.py`：新加的便携入口，复用当前 v3.2 提示词以及既有 `theme_graph.model_runner.call_qwen/call_hermes` 传输。默认只构建任务提示词；真实生成需显式选 provider，只写候选日报，不自动入库。

```bash
export DAY=20261016
export REPORTS="$TRADING_DATABASE_ROOT/theme_graph/每日涨停原因"
export PACKET="$REPORTS/分析输入包/$DAY.json"
# --evidence 可提供 JSON 数组，各项有 published_at（带时区）、title、url、text。
# 程序拒绝晚于目标日的信息；盘后解释不能倒写为封板前催化。
"$TRADING_PYTHON" "$TRADING_DATA_MANAGER_ROOT/theme_graph/scripts/generate_daily_report.py" \
  --packet "$PACKET" --previous-report "$REPORTS/20261015.md" \
  --prompt-out "$TRADING_DATABASE_ROOT/theme_graph/tasks/$DAY.txt"
# 使用已配置的 vLLM 时追加：
# --provider qwen --base-url http://127.0.0.1:8002/v1 --model YOUR_MODEL \
# --out "$TRADING_DATABASE_ROOT/theme_graph/drafts/$DAY.md"
# Hermes/MiniMax：--provider hermes --hermes-provider minimax-cn --model YOUR_MODEL --out ...
```

模型接口没有自带网页检索。证据应由研究者/具备检索工具的 agent 提供；缺证据必须写检索缺口，不能声称已检索。`--previous-report` 必须早于目标日，允许连板承接前日叙事并补当天信息。`--provider prompt` 的成功不代表调用过模型。真实生成的能力/质量取决于所选模型和证据；结构校验不等于归因可信。

人工完成时点、来源、事实/推断/传闻和个股桥梁审核后，使用规范 `YYYYMMDD.md`：

```bash
"$TRADING_PYTHON" "$TRADING_DATA_MANAGER_ROOT/theme_graph/scripts/validate_daily_report.py" \
  --packet "$PACKET" --report "$REPORTS/$DAY.md"
"$TRADING_PYTHON" -m theme_graph.daily_limitup_graph.cli \
  --db "$TRADING_DATABASE_ROOT/theme_graph/topic_limitup_graph.sqlite3" \
  import-report "$REPORTS/$DAY.md"
"$TRADING_PYTHON" -m theme_graph.daily_limitup_graph.cli \
  --db "$TRADING_DATABASE_ROOT/theme_graph/topic_limitup_graph.sqlite3" integrity
"$TRADING_PYTHON" "$TRADING_DATA_MANAGER_ROOT/theme_graph/scripts/export_sqlite_snapshot.py" \
  --db "$TRADING_DATABASE_ROOT/theme_graph/topic_limitup_graph.sqlite3" \
  --out "$TRADING_DATABASE_ROOT/theme_graph/neo4j/import/snapshot.json"
"$TRADING_PYTHON" -m theme_graph.neo4j.import_sqlite_snapshot \
  "$TRADING_DATABASE_ROOT/theme_graph/neo4j/import/snapshot.json"
```

导出的是完整 SQLite 快照。默认导入为 MERGE，不清理其它记录；重复同一快照幂等。若修改了某日报、删除/改名题材或减少股票，须先备份并在维护窗口加 `--replace-dates`：仅替换输入完整快照中日期的当前图视图，保留其它日期和来源/导入历史，防止旧关系残留。该导入器按多笔查询写入，不承诺整个快照原子切换；中途中断可从 SQLite 全量快照重跑。成功时核对各类总数与逐日归因数，数量不符会报错，不宣称同步完成。

## 测试与常见问题

```bash
cd "$TRADING_DATA_MANAGER_ROOT"
"$TRADING_PYTHON" -m unittest discover -s tests -p test_theme_graph_delivery.py -v
```

- `No module named coreClient.neo4j_client`：确认使用上面的配套版本、正确 Python，且安装 `-e '.[graph]'`；不要把旧 TradingAgent 的 coreClient 放在 sys.path 前面。
- 缺 `neo4j` driver：安装 graph extra。
- 连接拒绝/认证失败：检查 Neo4j 已启动及 URI/密码文件；配置模板不含实际密码。
- `/api/dates` 为空：网页日期由市场 SQLite 表驱动，只有 Neo4j 分析不能代替行情库。先跑合成示例或采集自己的行情。
- 摘要存在但题材显示无摘要：只有日报主题与 KPL 主标签完全一致才继承摘要，复合/不同题材章节需重新归一整理，不能自动乱配。
- 实盘/分钟图空：本例不提供真实分钟历史，不能用合成分钟数据声称补齐正式行情。

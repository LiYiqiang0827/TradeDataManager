# Neo4j题材归因数据库接口

这里是 TradingDataManager 对 Mac mini Neo4j 题材归因库的兼容导出和导入脚本；操作类的正式文件在 `coreClient/neo4j_client.py`。它与现有 DuckDB `theme_graph_store.py` 职责不同：DuckDB 继续维护开盘啦时序题材事实，Neo4j 接口维护人工复盘形成的题材日快照、个股涨停归因和来源关联。

## 连接

Neo4j Community trial 默认监听本机 `127.0.0.1:7687`，数据库名为 `neo4j`。密码读取优先级：

1. `TRADING_THEME_NEO4J_PASSWORD`
2. `TRADING_THEME_NEO4J_PASSWORD_FILE`
3. `~/Applications/Neo4jCommunity/.trial-password`

也可设置 `TRADING_THEME_NEO4J_URI`、`TRADING_THEME_NEO4J_USERNAME` 和 `TRADING_THEME_NEO4J_DATABASE`。密码不写入仓库。

## API

```python
from theme_graph.neo4j import ThemeGraphNeo4jStore

with ThemeGraphNeo4jStore() as graph:
    days = graph.list_topic_days("20261015", "20261016")
    detail = graph.get_topic_day("20261016", "TOPIC-1FB3509A02C59F9A")
    history = graph.get_topic_history("低空经济与飞行汽车")
    attributions = graph.get_stock_attributions("20261016", "603197.SH")
```

`list_topic_days(date, date)` 可按某日查看全部题材及涨停归因数量；`get_topic_day(date, topic_id_or_name)` 返回该题材当天的摘要和股票原因；`get_topic_history(topic_id_or_name)` 列出题材所有有记录的日期，并附每日摘要与股票原因。题材可用稳定 ID 或准确名称查询。

写入或更新使用 `upsert_topic_day(...)` 和 `upsert_stock_attribution(...)`。题材日摘要保存在 `Topic-[:HAS_DAILY_SNAPSHOT]->TradeDate` 关系上；个股原因保存在 `TopicDate-[:ATTRIBUTES_LIMITUP]->Stock` 关系上。Upsert按业务键合并，不删除其它日期、题材或个股数据。允许写入的字段列在 `coreClient/neo4j_client.py` 的属性白名单中，调用端不能拼接任意属性名。

## 依赖

通过 `pip install -e '.[graph]'` 安装 Neo4j Python driver。图数据与事务日志使用 Neo4j 实例配置的目录；正式数据位于 `~/TradingDataBase/theme_graph/neo4j/`。新机请按 PORTABLE_SETUP 建立隔离目录。

## SQLite快照迁移

`import_sqlite_snapshot.py` 将 `daily_limitup_graph` SQLite 的表快照幂等写入 Neo4j，保留题材、日期、题材日期、个股、来源文档及导入批次 ID。SQLite 与 Neo4j 数据统一位于 `~/TradingDataBase/theme_graph/`；迁移快照放在数据库目录的 Neo4j `import/` 子目录。该脚本通过现有 `ThemeGraphNeo4jStore` upsert API 写入题材日和个股归因，并创建 `FROM_SOURCE` 来源边；重复执行不会重复创建相同业务键。


新机部署、DDL、隔离实例和完整示例见 [../PORTABLE_SETUP.md](../PORTABLE_SETUP.md)。导入器创建约束；修订后的完整快照可显式使用 `--replace-dates` 同步当前日期视图。

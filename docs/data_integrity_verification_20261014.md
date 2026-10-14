# TradingAgent → TradingDataManager / TradingDatabase 迁移验收设计与初始只读检查

日期：2026-10-14（Asia/Shanghai）

## 目的、边界与当前结论

本报告最初用于建立迁移验收分母、契约与分层检查；下文保留初始只读盘点，再补充冻结源和迁移后验收结果。本审计没有执行业务数据写入；迁移agent执行了服务停写、路径切换和Redis恢复。

**最终状态（2026-10-14）：**迁移已完成，冻结源与目标的限定范围验收全部通过：18/18 provider API 输出指纹相同；两个月在线 SQLite 的完整 schema 与全部逐表行数相同，指定近期 minute/snapshot/snapshot_index/auction 及 stream cursor 指纹相同；Redis 308 个 key 类型和值 hash 相同；三只股票最近完整周/月的 origin/qfq 四表语义样本与日线聚合一致。该结论只证明下文列明的迁移一致性检查，不等于全库逐行重算或所有市场数据均无内容缺口。迁移前已发现的在线跨日污染与 20260922 分钟分区缺失仍单独列为源内容问题。

第一轮盘点时，真实主仓与worktree各有147项dirty；本报告没有修改TradingAgent业务代码。源库当时仍在旧路径。其后数据agent按同卷原位rename完成TradingDatabase迁移，最终结果见本报告“迁移后验收结果”。

存储文件盘点发现离线 SQLite 库存在 `-wal`/`-shm` 侧文件，另有 DuckDB 锁文件；在线 2026-10 SQLite 文件的修改时间接近检查时点。`ps` 进程枚举返回 `Operation not permitted`。为避免影响可能活跃的 worker，本轮没有跑长查询、SQLite `integrity_check`、DuckDB 查询或完整 Parquet 扫描；下文标记为未知的事项不能视为通过。

## 初始检查结果

### 仓库与数据文件

- 当前真实仓库和 worktree 的 `git status --short` 均为 147 项；根 README 的 199 个历史断言不作为本次证据。
- `offlineDataManager/data` 包含 4 个主要 SQLite：`db_cn_basic.db`、`db_cn_kpl.db`、`db_cn_news.db`、`db_cn_index.db`；另有 `db_fifteenMinute.db` DuckDB、`db_theme_graph.duckdb`、`db_major_news_events.duckdb`、旧 `db_news_events.duckdb`、旧十五分钟库 `fifteenMinute/fifteen_min.duckdb`、一分钟 DuckDB catalog、policy 库及 1 分钟 Parquet。
- `oneMinute/parquet/schema_v2` 有 428 个 `trade_date=*` 分区目录。本轮没有对全部分区读数据或验证目录日期是否连续。
- `onlineDataManager/data` 有 `online_data_202609.db`、`online_data_202610.db`；后者 schema 有 91 张表。目录还未迁入 TradingDatabase。
- 找不到历史线上审计报告 `outputs/online_data_audit/20261009/online_data_audit_report_20261009.md` 和 JSON。历史审计记忆只能作为风险线索，不能当作本次结果。

### SQLite schema 元数据（immutable、只读、只读取 sqlite_master/PRAGMA table_info）

本次以 Python `sqlite3` URI `mode=ro&immutable=1` 查看现有主文件，不读取 WAL 中的最新提交，也不跑数据行扫描。此方式适用于确认表名/列，不代表当前 WAL 快照上的结构/内容已完全核验。

| 库 | 实际表数 | 观察 |
|---|---:|---|
| `db_cn_basic.db` | 16 | `tbl_basic_ctrl`、13 类基础业务表及 `tbl_cn_week_origin`/`tbl_cn_month_origin`；`tbl_cn_basic` 有 18 列（含 `act_ent_type`、`act_name`、`area`） |
| `db_cn_kpl.db` | 10 | 9 张业务表及 `tbl_kpl_ctrl` |
| `db_cn_news.db` | 4 | 新闻 3 表及 `tbl_news_ctrl` |
| `db_cn_index.db` | 3 | 指数 2 表及 `tbl_index_ctrl` |
| `online_data_202610.db` | 91 | 分日业务表、`online_stream_cursor`；细分类型需由验收工具自动按表名前缀统计 |

离线四库合计 33 张 SQLite 表。现有数据字典 `offlineDataManager/docs/01_数据详细设计文档.md` 标注 31 张、basic 14 张且其 basic 字段仍是 15 列，与当前实际 schema 的 33 张 / 16 张 / 18 列不一致；需在迁移验收时把版本化 schema 作为比较基准，并确认这两张 period origin 表及新增 basic 字段的迁移映射。不要以 README 中旧表数作为目标分母。

## 可重复验收矩阵

每个迁移阶段应保留同一冻结源快照的清单：迁移时间、源/目标绝对路径、文件大小、SQLite `user_version`/schema SQL、表清单、逐表行数、主键/唯一键定义、索引、缺失/新增表、内容摘要。源数据库若 WAL 活跃，先用受支持的 SQLite backup API 将一致性快照写入隔离临时盘，且要校验 backup 快照；不要直接复制主 DB 文件而漏掉 WAL。读快照和写目标都必须与 worker 隔离。

| 层 | 覆盖分母与可重复方法 | 接受条件 | 失败归因/状态 |
|---|---|---|---|
| 路径和文件映射 | 对主仓/目标树递归枚举所有 `*.db/*.sqlite/*.duckdb/*.parquet`、WAL/SHM、catalog、lock；生成相对路径清单 | 每个源文件有且仅有一个声明的目标或显式排除理由；不能把 DuckDB 扩展名 `.db` 当 SQLite | 当前目标目录不存在，**迁移未验** |
| SQLite schema | 离线 4 库所有 33 张表；线上月库所有 91 张表，另一个月库完整表集；逐表比较类型、列顺序/NULL/默认值/PK、索引、触发器、视图、外键、AUTOINCREMENT、collation、SQLite pragma | schema 版本有明确、可解释变更；迁移后查询 API可读；逐表主键/唯一键约束一致 | 先以源冻结快照定基线；目标缺列/错类型/PK丢失为迁移回归；已在源快照存在的缺陷标为迁移前问题 |
| SQLite 行数据与语义 | 每库、每表逐表总行数；所有复合主键重复数；按日期/股票分桶计数；对规范化排序行计算稳定 hash；较大表按主键 key-range 分块比较；抽取 NULL、非法值与边界值清单 | 行数相等，键集合/字段值相等；如做类型规范化必须保留 NULL、整数/浮点、文本、时区和精度语义 | 逐表/hash 对不上先分块定位；同源已有坏值算 baseline，目标新增/变值算迁移回归 |
| 日线 / daily_basic / adj_factor | `tbl_cn_day`、`tbl_cn_daily_basic`、`tbl_cn_adj_factor` 全量；按交易日、ts_code 计行数；覆盖日期首末、全历史每日；复合键唯一；OHLC正值与 high≥max(open,close)、low≤min(open,close)，vol/amount域；daily_basic列值/百分比量纲；因子有限且正、按代码日期排序 | 源目标逐行一致；日线、指标、因子无错库错列；因子日期为该证券实际交易日，关联 coverage 不漏股票 | 源端自身异常通过基线分类，不把重新下载值当作迁移比对基准 |
| 周/月复权与不复权 | 周/月期表 `tbl_cn_week/month` 与 `tbl_cn_week_origin/month_origin` 全量；交易周/自然月边界、跨年 ISO 周、首根 `pre_close`、停牌/缺因子；对照源日线 + adj_factor 重算 | origin OHLCVA 与原始日线聚合一致；qfq/origin 同键同覆盖；缺失因子处理与源API一致。当前 `get_day(qfq=True)` 源码和旧数据字典都将OHLC价格乘以 `adj_factor[日]/adj_factor[最新]`、将 vol 除以该因子，amount不变；这是当前接口行为，成交量是否应继续调节需按用户认可的研究契约另行确认。迁移本身保留现状，验收中不以通用复权公式改写/判错；如调整量口径，应另开语义变更及真实成交量研究确认。 | 先记录旧库四张 period 表的基线；如果源缺失或公式错误已存在，标迁移前；目标差异归迁移转换 |
| 一分钟 Parquet + catalog | 当前 schema_v2 的 428 个 trade-date 目录；对所有 parquet footer/列 schema/row-group 行数/文件可解码性全量扫；按交易日 hash/行数/股票数/重复 `(ts_code,datetime)`；比较 catalog；样本日全量解码 | 日期分区路径与记录 `trade_date` 一致；时间只能 09:31–11:30 / 13:01–15:00（每证券每有效日240个时点，实际停牌/新股/缺失单独解释）；PK无重复；OHLCVA 与空值/数值域正确；catalog覆盖与文件一致 | 428 是文件分母，不是交易日完整性结论；报告明确断档日期/缺stock，不把目录数当完整 |
| 15 分钟 DuckDB / TDX | 主库 `db_fifteenMinute.db`、表/索引/约束及每股票日期；源码契约 `TIMES` 16 个端点：09:45–11:30、13:15–15:00；源 CSV 旧库仅作为迁移对照（若纳入范围）。对全部 `(code,date)` 样本行和 hash 对照 | 每有效股票日16根，时间严格等于契约；OHLCVA与 adj_factor 分列语义；数据库虽扩展名 `.db` 引擎仍为 DuckDB；同步状态无超前、写事务数据与状态原子 | 源库截至时点的缺 bar / 错因子是前置质量问题；迁移行值和 sync 状态改变归迁移 |
| KPL | `db_cn_kpl.db` 9业务表+ctrl，全量行数、复合键、日期分桶；`kpl_list`/`limit_performance`/龙虎榜/成员的日期一致性、JSON/文本时间字段和保留 NULL；检查每个 source 表，不按 service 数量替代 | 表/字段及行值不丢；ctrl不能因历史补录倒退；主题/股票代码连接键稳定 | 确认源快照里已有的缺页/错误，按源目标差分归因 |
| 新闻及新闻事件图 | `db_cn_news.db` 3事实表+ctrl；DuckDB `db_major_news_events.duckdb` 事件、证据/引用、事件实体关系、来源id。对新闻原文 md5、唯一键、时间/时区、图实体/边ID及关联引用逐条比对 | 原文和来源可追溯；事件图节点/边/证据关联不孤立；不得以重跑LLM或重建代替内容对比 | 旧 `db_news_events.duckdb` 是不是弃用来源须声明；不要两套事件库静默合并 |
| 题材图 | `db_theme_graph.duckdb` 全表数据、关系、别名/有效期/人工审计，源日 KPL 事实关联；统计实体/事实/关系/日热度/周期/LLM审计各表 | 历史有效期和来源事实一致；变更日期、人工状态、证据引用不丢；相同源事实重放幂等 | 被源 KPL 已有错误的关联和迁移造成的数据变更分开；需保留人工研究成果 |
| 在线 minute | 2 个月 SQLite、所有 `minute_YYYYMMDD` 分表（本月 schema 盘点中的 11 表）；全表行数、trade_date时间戳对应、主键唯一、按 `(code,date)` session覆盖、单日分钟索引从 09:31 起连续至15:00（连续交易时段明确午休间断）、行情域；找跨日污染、超范围日期、停市日行 | 每个表名日期与行 `trade_date` 和 datetime 日期相同；不接受跨日串入；单代码时间戳不重复，索引值契约一致 | 本次源与目标近期样本均观察到10/13、10/14跨日行，具体数量及归因见冻结源和最终验收结果；完整两月所有分钟值未全量逐行检查 |
| 在线 snapshot / auction | 每天 `snapshot*`、`auction*` 等完整分母；时间字段与 trade_date；覆盖证券数分布、重复/缺漏、auction 阶段/时间段、每证券记录数、`turnover` 与单位/价格/量的语义关系，`data_status` 枚举和布尔值；SQLite与Redis抽样 key逐值核对 | 状态字段语义显式枚举；集合覆盖、时序、turnover单位与Redis生产者定义吻合；禁止把空/None转 bool 后误标；Redis键/TTL/序列化一致 | 历史线上修复报告遗漏 `auction.turnover` 的迁移覆盖，是必须新增的旧 schema 输入，不作为已证实本次故障 |
| Redis 与 scheduler 生命周期 | 不触生产Redis。用隔离 Redis 临时实例/假时钟/假子进程验证所有 key family、过期和游标；scheduler启动、pidfile/registry、PID reuse、陈旧记录清理、SIGTERM+进程组退出、重启恢复、跨分钟重试；模拟正常退出/崩溃/权限拒绝/相同PID不同starttime | 所有 writer 子进程退出可证实；registry与PID/starttime/command一致；僵尸/陈旧注册可恢复；错误分钟可重试；已有任务仍活着时不得启动重复进程；Redis/月表/归档幂等 | 旧报告仅 128 onlineData checks+21 core tests通过仍遗留 PID reuse、stale registry、进程组退出、cross-minute retry、auction.turnover等覆盖缺口；一律重新验证，旧测试不作通过证据 |
| Provider / 上层 API | `coreClient.data_provider` 30 个接口的源选择、读路由、列类型、排序、过滤、空结果语义；offline strictly `database_only`；将迁移前后相同 query 参数结果 DataFrame规范化hash比较 | 接口输出保持语义；strict offline绝不走网络；默认 fallback另行标注；qfq等参数实际有效 | API失败若源端旧契约已有记录另报；provider指向新库错误属迁移 |

## 日线/周期及分钟的固定契约

- 一分钟导入代码定义连续交易时间为每股每天 240 个标签：09:31–11:30 计 120 根、13:01–15:00 计 120 根。午休没有 bar；time_idx 必须对应上述顺序，不能按日历分钟连续跨午休。
- TDX 原生 15 分钟保存的是 16 个 end-label：09:45,10:00,...,11:30 与 13:15,13:30,...,15:00。不能误用 bar open time 或从 09:30开始的 CSV 标签。
- 15m DuckDB 文档称原始 OHLCVA 不复权，`adj_factor` 独立保存供调用时按需前复权；对错误或缺失因子做修复是内容更新，必须单独获得迁移前源快照、更新清单和审计 diff，不能混入路径迁移验收。
- 周月同时保留 qfq 与 origin 两个视图/表，完整源数据需按同一交易日历和固定截止日对比；重算后 `pre_close`、qfq基准、volume缩放和边界覆盖都需可复现。

## 迁移前后归因协议

1. 当前可用盘空间19GiB不足以复制36.8GiB源数据，不做全库backup副本。写入方停止后记录原位源文件与WAL/SHM的inode、大小、schema、行数和哈希；按本机cutover规程同盘原位rename，检查目标 inode/哈希保持一致、未遗留未处理WAL。
2. 迁移仅改变路径或数据库实现时，任何字段值、键集合、时序、行数差异默认列为迁移差异，直到明确证明为类型/排序的无损规范化。
3. 在迁移开始前源库就存在、且源 snapshot 本身有证据的异常，标“迁移前已存在”；迁移后数据新增/丢失/改写、比源快照更差，标“迁移回归”；无法冻结同一源时点的跨日在线差异标“无法归因”，不推测。
4. 对 expected-source/outcome 之外的上游错误/代码写错归因，需要追踪具体 writer/parser/scheduler，不能只看 schema/值差异做结论。
5. 所有验收输出用隔离目录保存；不修复生产数据、不把被发现的问题静默清理。

## 本次隔离测试

使用真实主仓 `.venv`，worktree 代码路径，禁用 bytecode 与 pytest cache，运行只使用 SQLite `:memory:` 或 pytest `tmp_path` 的三组单元测试：

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.:offlineDataManager/scripts /Users/nickzhang/TradingAgent/.venv/bin/python -m pytest -p no:cacheprovider offlineDataManager/scripts/core/test_tdx_one_min_store.py offlineDataManager/scripts/core/test_tdx_fifteen_min_store.py offlineDataManager/scripts/core/test_week_month_origin.py -q`

结果：`41 passed in 11.62s`。测试覆盖的是 worktree 当前代码的 TDX分钟隔离存储/边界、15m DuckDB隔离写读/因子修复、周月 qfq/origin 聚合等单元契约；不能证明真实 TradingDatabase 内容正确、不能证明 SQLite 数据搬迁或在线 Redis/scheduler 生命周期，也不能证明完整 Parquet 分区正确。

## 待完成/未知事项与时间估计

- **未知：**独立目标 TradingDatabase目录未出现；迁移是否正在其他位置生成目标，本轮不知。
- **未知：**进程清单被 OS 权限拒绝；无法确认生产 writer 当前是否活跃。WAL/SHM、近期mtime表明存在活动或较近期连接可能性，故长数据扫描已延期。
- **未知：**4个离线SQLite数据内容一致性、约2.5GB十五分钟DuckDB内容、428个一分钟分区全量读/校验、在线所有历史月份及Redis状态均未做语义验收。
- **已发现文档差异：**离线数据字典的31张表/`tbl_cn_basic` 15列与只读 schema 元数据的33张/18列不符；需确认文档更新是否在dirty worktree里，之后以冻结版本做schema contract。
- **预计完整验收耗时：**目标库存在且可安全快照后，schema + SQLite逐表扫描约10–30分钟；一分钟Parquet全量 footer/行扫描约10–40分钟（外置盘I/O影响较大）；DuckDB逐表扫描和图数据库比较约5–20分钟；隔离在线生命周期/Redis fake服务集成验证约10–30分钟。整体约35–120分钟，取决于外置盘和快照大小。此为运行前估算，不是已测耗时。

## 本次依据

本次实际只读代码与文档：`README.md`、`offlineDataManager/config/settings.py`、`offlineDataManager/docs/01_数据详细设计文档.md`、`offlineDataManager/scripts/core/tdx_one_min_store.py`、`offlineDataManager/scripts/core/tdx_fifteen_min_store.py`、相关三份隔离测试。历史 2026-10-09 onlineData audit artifacts 在记忆记录位置未找到；其记忆总结仅用来确保复核跨日污染、语义值、PID/registry生命周期和 `auction.turnover`，不代表本次状态。


## 第二轮只读内容抽样（2026-10-14）

进程核验使用获准的只读 `ps`/`lsof`：PID 19735 是 `service_month`（22:22启动），PID 53008 是 `scheduler.scheduler_updateData --daemon`；19735 持有 `db_cn_basic.db` 与约 901.7MB WAL，并打开KPL/news/index主库及空WAL。Redis PID 22730 在 `127.0.0.1:6379`，持有 AOF 增量约37.7MB且有多个客户端连接。本机当前生产 writer 是本机进程，迁移前应只安排本机cutover，不假设远端 worker共享卷。

本轮没有使用 immutable 内容查询。在线月库第一次常规 `mode=ro` 在默认 sandbox 因目录/共享内存侧文件限制失败；通过获准提权重试常规 `mode=ro` 后读成功，命令未发出任何写 SQL。若这次连接创建了 SHM，需以随后文件 stat/lsof 检查记录；主库数据内容未写。样本覆盖月库 `minute_20261012/13/14` 与相应 `auction_*`，三个是近期完整交易日：

| 表 | 行数 | 代码数 | 表名日期≠datetime日期 | 重复(code,datetime) | 表内时间范围 |
|---|---:|---:|---:|---:|---|
| minute_20261012 | 20,880 | 87 | 0 | 0 | 10/12 09:31–15:00 |
| minute_20261013 | 27,600 | 115 | **20,880** | 0 | 10/12 09:31–10/13 15:00 |
| minute_20261014 | 28,800 | 120 | **15,120** | 0 | 10/13 09:31–10/14 15:00 |

这确认样本中 10/13 表混入前日 87×240 条、10/14 表混入前日63×240条。每代码去重后都是240根、表内时段检查未发现盘外点，但表名/trade_date与datetime的跨日矛盾是**当前源库真实异常**。它在迁移前源端已观察到，因此若同样字节原位迁移仍存在，归类为迁移前问题；若迁移后增加/减少则另外归因迁移。初始 immutable 查询得到的值和此次常规 RO 查询完全吻合，但只有此处常规 RO 结果作为当前证据。

三张竞价表每张均2000行、覆盖63–97代码、`data_status`全非NULL；但 `turnover` **2000/2000均为NULL**，时间集中09:15:03–09:18:41。需要追代码和上游字段来源确认字段契约/生产写入，不先把 NULL判为迁移缺列。竞价审计必须显式纳入 `turnover` 的源列及目标列。

离线 basic 使用常规 `mode=ro` 短查询成功（有并发 writer；各语句短事务）：SSE和SZSE日历各 3,208 行，20180101–20261014，开市2,128、休市1,080；近三日10/12–14均为开市；BSE当前无记录。这个与旧数据字典所述“BSE实际上未拉到”相符，覆盖缺失不是本次迁移造成，但属于源库能力/coverage限制。全表min/max只做了一次窄列端点扫描，日线、adj_factor、daily_basic都为20150105–20261014；不据此推断所有交易日连续。10/12–14日线每日至少5,561条、OHLC域异常数均为0；daily_basic对应代码数与日线相同；adj_factor各日5,573/5,574/5,574行，NULL/非正因子数为0。代码 qfq成交量行为见上节，是源接口兼容面，真实成交量语义仍需确认。

一分钟Parquet全量读取了最近三完整交易日的3个文件，而非全历史：每天代码数5,561/5,561/5,563，行数1,334,640/1,334,640/1,335,120；每只代码均恰好240个独立datetime。`trade_date`/datetime日期错配、盘外时间、OHLCVA/adj_factor域错误都为0。每天首尾09:31和15:00、time_idx 0–239。最早分区 `20250102` 与最晚 `20261014` 仅读footer、schema和首3行；没有声称端点全日正确。catalog DuckDB read-only：`one_min_ingest_catalog` 428行，范围20250102–20261014，全部 `imported`；10/12–14 每日catalog行数、代码数、240 bars及schema_version与Parquet一致。这是第一轮目录盘点时的状态；迁移后目录内容见最终验收段。

十五分钟 DuckDB 以 `read_only=True` 对近三完整交易日做时间/行数/域查询：库表为 `fifteen_min_bars/fifteen_min_days/fifteen_min_meta/fifteen_min_sync`，全库41,319,844条、5,225代码、范围20240902–20261014。近三日每股原本应有16个 TDX end-label；52xx代码里每天分别有12/13/11代码为15根，**逐个缺的全是11:30**。这36个股票日都在 basic `tbl_cn_suspend` 标记 `suspend_type='S'`，`tbl_cn_day` 当日无行，一分钟Parquet也无该代码当日行；因此该抽样不像有交易的OHLC漏迁。接受条件改为：对全日停牌证券允许无分钟记录，并要求与源证券状态/逐日manifest一致；不能把“每个上市代码必定16根”当硬规则。现有15分钟行的日期、time_idx到时刻映射、OHLCVA域和因子有效性在这三日异常数为0。

## API查询指纹（迁移前后可重跑）

对 `coreClient/data_provider.py` 做AST盘点为 **49个顶层 `get_*`接口**，不是 README 的30接口旧数字。查询指纹计划已单独保存为 [api_fingerprint_manifest.json](data_integrity_verification_20261014/api_fingerprint_manifest.json)，含全部49名称/分类和18个固定样例（calendar、日线原始/qfq、daily_basic、adj_factor、周/月原始/qfq、1m/15m、policy minute、KPL及news）。有实体ID依赖的theme/news graph接口待从源端只读清单选出现存ID，再补显式参数，不用空ID制造“空结果等于通过”。所有支持source选择的查询指定 `database_only`，避免外网fallback；1m/15m使用显式 data_root。

实际在writer停后、cutover前对源端运行manifest，cutover后对目标执行同样参数。指纹存每个返回值列名/列顺序/dtype/index元数据，保留所有列（包括snap_ts）、所有行/NULL和时间偏移；主SHA256按API返回顺序序列化，浮点不舍入、字符串原样保留。

已知迁移前源端差异：在线minute跨日数据；竞价 `turnover` 全NULL；BSE日历覆盖为空。仍未知：在线完整历史两个月所有表、Redis key数据与TTL、图数据库逐表数据、15m全历史逐日coverage、KPL/news现行所有内容。

## 第三轮当前内容核验补记（2026-10-14，后续补记优先于初始计划中的“未检查”）

### 日历与日线全历史日期覆盖

SSE 开市日历的分母为 2,128 天，范围 20180102–20261014（calendar 表整体范围 20180101–20261014；20180101 为闭市）。日线、daily_basic、adj_factor 均按 `GROUP BY trade_date` 扫描了全历史日期分桶，再与该日历集合对比：

| 数据 | 表内分桶日期数 | 日历开市日缺失 | 表内非开市日期 | 现有 check_database 邻日阈值异常 |
|---|---:|---:|---:|---:|
| `tbl_cn_day` | 2,128 | 0 | 0 | 0 |
| `tbl_cn_daily_basic` | 2,128 | 0 | 0 | 0 |
| `tbl_cn_adj_factor` | 2,130 | 0 | 2 | 0 |

三表最早数据日均为 20150105，最晚 20261014；本地交易日历仅从 2018 年开始，因此 2015–2017 数据的开市日完整性未知，不能将其判为完整。`adj_factor` 多出的两个日期为 20210920、20210921，两交易所当日均闭市，日线/daily_basic 无记录，但因子表各有一行，且都属于 `689009.SH`。这是源端可观察到的闭市日期常数因子行；更新器按自然日查询能解释其来源，不把它单独判为错误，迁移时按源快照原样对照。`check_database.py` 的阈值按相邻开市日数据量的 90% 判定；该合同覆盖的 2,128 个开市日均无异常。它不自动报告“额外闭市日”的因子行，因此需要独立检查日历集合差。

### 一分钟 Parquet 全部分区 footer 与日期目录

本轮打开了全部 428 个 `part-*.parquet` footer，没有读全历史 5.6 亿行。428/428 footer 可解析、0 文件错误、总元数据行数 560,819,760、4,472 row groups；目录日期 428 个，20250102–20261014，与 catalog 日期集合一致。与 SSE 开市日历在相同覆盖范围比较，缺少开市日分区 `20260922` 一天，没有额外闭市日目录。此前已全量扫描最近三日三文件行值；其余 425 个文件仅 footer，不能据此宣称所有行内容已验证。

全部 Parquet 实际有两种 Arrow schema：321 文件 `datetime: timestamp[us]`，范围 20250102–20260506；107 文件 `datetime: timestamp[ns]`，范围 20260507–20261014；其他字段/顺序一致。文件均可被当前 Arrow 解析，但这是迁移前 schema 精度分段差异，迁移后必须逐文件保持或由明确的规范化合同解释，不能只比较 catalog 声明的 schema_version。

### 15 分钟历史日期分母与异常缺 bar 核验

十五分钟 `fifteen_min_days` ledger 有 509 个不同交易日，范围 20240902–20261014，与 SSE 日历相同区间的 509 个开市日一一对应，没有缺日/额外闭市日。基础 bars 全库窄聚合扫描一次得到 41,319,844 行，5,225 只证券。最近三日 20261012/13/14 的 12/13/11 个证券只有15根而非16根，且每只都恰好缺 11:30。这36个证券日逐一在 `tbl_cn_suspend` 有 `suspend_type='S'`，当日日线和1分钟数据均没有该证券，因此证据支持全日停牌解释。验收以逐日证券状态/实际有交易证券集为分母，不要求全市场每一证券每天都有16根。

### KPL、主题图和新闻图最新截面

KPL 控制表与事实表最近日期核对结果：`cn_block_trade`、`cn_hsgt_top10`、`cn_kpl_concept_cons`、`cn_kpl_limit_performance`、`cn_limit_list`、`cn_top_inst`、`cn_top_list` 最新均为 20261014；`cn_ggt_daily` 控制到20261014但事实表最大日期为20261013；`cn_kpl_list` 控制/事实最大日期均为20261013。前者记为 control/fact 一日差，须结合接口空日和重试语义判断；后者属于最新可见发布日期，不直接判漏数。

主题图 DuckDB `db_theme_graph.duckdb` 有 17 表。`fact_limit_event` 为20,670行/185日期/3,709代码，`fact_theme_daily` 为10,366行/185日期/604题材，二者日期范围均20260105–20261013。`source_day_manifest` 185日/20,670行、`source_checkpoint` `kpl_list` max/last_complete 均20261013。最近 build 于2026-10-14 14:54:31完成、status=`success`、as_of=20261013。图中 `fact_membership_snapshot` 为0行且 `rel_theme_stock_membership` 也为0行；这是可观察的源端当前状态，需查主题图产品接口与生成器对两张成员表的预期。它可能是设计上未维护的表，但迁移不能把“表在”当作成员图内容通过。其他图事实/关系有内容，后续 API 指纹应分别记录空成员接口及非空事件—主题接口。

`db_major_news_events.duckdb` 最新 raw/event 数据可到2026-10-14：`fact_news_event` 406,302行、事件日范围2026-01-01–10-14、max `updated_at` 21:43:15；`rel_news_event_member` 572,178行。语义事件表和每日分析表都截至2026-10-13，语义事件382,808行、284日期；每日分析284行、最新10/13；pipeline_state 的 `major_news_max_datetime=2026-10-14 21:34:04`、更新21:53:17。raw前进一天而语义/每日分析滞后一天，可能是业务加工节奏；按各层自己的checkpoint验收并记录滞后，不把raw max日期冒充语义层最新日期。新闻来源控制有 `fenghuang` 与 `yuncaijing` 停在2026-09-10，其他主要来源截至10/14；需列为源端 stale-source 状态并判断是否仍属启用来源。

### 只读访问副作用与实时 worker

在线 `online_data_202610.db` 使用 SQLite 普通 `mode=ro` 成功短查。首次默认 sandbox 的 RO 失败后，获准提权重试；检查后见 `.db-shm` 32,768 bytes、`.db-wal` 0 bytes。该 SHM 是只读连接为 WAL 协调创建的共享内存侧文件，不含 SQL 业务数据写入；迁移文件清单应单独识别 sidecar，不能把它算成迁移内容。此轮没有停止 PID 19735/53008 或 Redis PID 22730，writer 状态如前述仍在运行，因此全部内容结果是运行中源端时点观察，不构成跨库同一时点快照。

## 验收判定更新

本报告开头“初始检查结果”中的未检查列表是第一轮当时状态；以上第二、三轮补记覆盖相应项。当前确证的源端基线异常至少包括：online minute 表名日期与datetime跨日错位；auction 三日样本 `turnover` 全 NULL；`adj_factor` 两个闭市日额外行；1分钟 Parquet 缺20260922开市日分区；Parquet datetime 微秒/纳秒两种单位；Parquet us/ns精度差异（实测统一读取API跨20260507兼容，且与TDX切换日一致）；主题图成员表为空（文档明确为未来扩展预留）；新闻语义层比raw层滞后一日（分层pipeline checkpoint）；部分新闻来源控制停滞（是否仍启用待核实）。这些均按源状态/既定合同保持和对照，不在缺少证据时判为迁移失败。当前可判源端实质缺口是online minute跨日错位及1分钟缺20260922开市分区；auction `turnover` 全NULL仍需核producer契约。迁移前已观察到的异常不能归责迁移；迁移后只有相对冻结源清单新增的差异才算迁移回归。在线跨日/auction 内容属于活跃库抽样，停写冻结后还应复查同一分区，以确认cutover基准是否仍一致。

未验证：所有SQLite完整性检查、所有分钟/15分钟全历史行值、scheduler重启/退出行为、49个API全部返回指纹、KPL/news主题实体图全量关系。单次元数据 footer 扫描耗时约2.6秒；十五分钟单次全库轻量聚合与分组覆盖、DuckDB图库只读核验均在数秒内完成。不要重复全历史5亿行级扫描。

## 合同复核与迁移后快验准备（2026-10-14）

### 先按既有合同解释几处差异

- `datetime` 的 us/ns 切点为 20260507，和 `tdx_one_min_store.py` 中 `TDX_ONE_MIN_START = "20260507"`、服务文档“该日起以 TDX 原生 OHLC 为主”的来源边界完全一致。小样本调用统一 `get_oneMin` 分别查 20260506、20260507 和跨两天区间：单日各240行，合并480行；所有返回 dtype 均统一为 `datetime64[us]`，其他列dtype相同；时间09:31–15:00，`time_idx` 0–239，主键无重复。故当前确认统一 API 在边界兼容；底层文件精度差异是来源版本元数据，作为搬迁一致性合同保留并核对，不单独判迁移缺陷。
- 主题图文档 `offlineDataManager/docs/题材研究数据库工具.md` 明确写明 `fact_membership_snapshot` 与 `rel_theme_stock_membership` 是未来普通成分扩展，当前保持为空，不能用于涨停宽度计算。其0行是有文档合同的既定状态；迁移只需保持表结构/空值及现行 `get_theme_members` 行为。当前正式涨停归因走 `fact_limit_event` + `rel_limit_theme`，不能把成员表空当作图损坏。
- `major_news_max_datetime` 推进至10/14、raw事件最大日10/14，而语义事件/每日分析最大日10/13，是分层流水线阶段的各自检查点。迁移应逐层比较行数、checkpoint 和状态；只要源/目标相同，不将正常一日加工滞后独立判为迁移故障。
- 689009.SH 是九号公司（SSE、20201029上市），不是与交易所日历不同的 CDR 特殊交易日个例。20210920/21 SSE/SZSE日历均闭市，日线无行；相邻三日因子均为1.0，因子表在这两闭市日各有一行。更新器 `update_adj_factor` 的现行实现按自然日循环调用 `pro.adj_factor(trade_date=...)`，没有先用交易日历过滤，因此这两行可由当前源端更新路径自然产生。它们是不交易日上的冗余常数因子记录，不应据此声称个股特殊交易时段或迁移回归；以保持原样和 API因子读取结果为迁移合同。若未来要清洗，应作为单独数据治理变更。

### 缺少 20260922 一分钟分区的恢复规模与分 phase 方案

这天为SSE开市日，附近 Parquet 分区/catalog也缺失，不是休市日应删分区的情形。basic 日线当日有5,554只证券、5,554行；按240根完整交易时段估算待回补约 **1,332,960行**。2026年9月其余20个分钟文件共322.35MB，单文件大小15.84–16.62MB、中位数16.03MB；据此该日正式 Parquet 约16MB，额外需要约同等规模 staging 空间及catalog增量。历史日实际活跃清单应从原始日线/状态与发行日期合成交叉取得，不能直接用今天的 L/P 状态代替。

迁移阶段只核验缺口并记录在交接单，不下载、不回填。迁后数据修复单独开 phase：先由用户/主任务明确批准对这一个开市日做本地 TDX 修复；用 `service_oneMinute.py --trade-date 20260922` 的全市场单日模式、显式TDM output/catalog/stage路径、保留staging，先校验代码日清单、每只有效交易证券240 bars、09:31–15:00、OHLCVA域、调整因子、无重复和源主机；确认失败数为0后再原子 finalize，最后重跑该日footer、catalog与API查询，并把修复前迁移验收结果和修复后数据治理结果分开报告。使用 `--limit` 或 `--ts-codes` 的模式只进入测试 staging、不替换正式分区；不要把这些冒烟参数误当全市场正式补齐。该方案当前没有执行任何网络读取或磁盘写入。

### 停写窗口18条统一API指纹

`api_fingerprint_manifest.json` 的18条固定 `query_cases` 是本次快验集合；同文件的 `graph_api_cases` 是218个候选调用生成配置，不属于这次停写窗口18条。已在冻结源与迁移目标使用相同参数执行，保留完整列/索引/dtype/值与顺序hash。只读执行器 [run_api_fingerprints.py](data_integrity_verification_20261014/run_api_fingerprints.py) 已用于冻结源与target查询。它通过 `--repo-root` 与显式 `--database-root` 选择代码及数据库树；所有普通数据API固定 `database_only`，1m接口天然仅本地Parquet，15m只读DuckDB。18条分别为日历、原始/qfq日线、daily_basic、adj_factor、原始/qfq周线与月线、1m、15m、policy minute、3类KPL、普通新闻/major news/CCTV news。大范围只有单只股票周/月两年数据，在线新闻限制200行，日线只3天；已在结果中记录每条 `elapsed_seconds`。

### 在线 writer 生命周期与快照冻结步骤

对 TDM 当前 `scheduler_onlineData.py` 的只读代码核对：writer/savedata 子 PID 仅保存在 `_DAEMON_PIDS` 进程内字典，不落持久化PID registry；phase清理给进程组发 `SIGTERM` 后即从字典删除，没有wait/join。scheduler启动清理用 `pgrep -f service.service_` 发SIGTERM并固定sleep 2秒。`scheduler_updateData` 的更新任务用同步 `subprocess.run`，调度daemon自身持续常驻。因此迁移停写门必须由维护任务/LaunchAgent层暂停调度器和所有writer，随后单独用进程树与 `lsof` 确认所有SQLite/DuckDB/Parquet writer退出、WAL不再变化/句柄关闭，再取源指纹。不能仅凭scheduler进程仍存活、PID字典已删、2秒sleep或sidecar大小判定冻结成功。验收的记录项包括父PID/子PID/命令行、打开的源文件、停写确认时间、最后一次WAL大小与mtime、重新启动后目标路径的lsof映射。此处是验收门设计；我没有停止或启动服务。

### 停写窗口脚本与耗时预估

停写窗口执行了18条API源/目标指纹和两月online结构/计数/抽样值指纹；迁移后的配套脚本、manifest、source/target receipt与比较结果保存在 `data_integrity_verification_20261014/`。采用只读查询；实际每case耗时与窗口时间已记录在receipt。报告和配套指纹/脚本已保存到 `TradingDataManager/docs/data_integrity_verification_20261014.md` 及同名子目录。

Redis只读指纹工具 [run_redis_readonly_fingerprint.py](data_integrity_verification_20261014/run_redis_readonly_fingerprint.py) 只执行SCAN、TYPE、PTTL、DUMP、DBSIZE、INFO；输出key名hash、值dump hash、类型与TTL，不输出key原文，也不发写命令。source/target Redis key类型和值hash完全相同；TTL比较按采样时间校正。

线上 SQLite 指纹执行器先通过 `/private/tmp` 两表fixture冒烟，随后在冻结源和目标各执行了一次；fixture已删除。

为避免迁移前的统一 provider 误读/创建空目标库，18-query runner 在 import 前要求显式 `--database-root` 已存在；已在tmp建好 `source_database_root/{offline,online}` 两个只读目标符号链接，分别指向旧TradingAgent生产数据目录。冻结源基线使用 `--repo-root /Users/nickzhang/TradingDataManager --database-root data_integrity_verification_20261014/source_database_root`；迁后使用同一个repo root、`--database-root /Users/nickzhang/TradingDatabase`。runner拒绝自动创建不存在的根目录。另加 [compare_redis_fingerprints.py](data_integrity_verification_20261014/compare_redis_fingerprints.py)，将Redis value/type与key存在性做硬比较，并按停写窗口耗时校正PTTL自然衰减；已过期key只有在此前TTL不大于窗口耗时时归为预期过期。

线上指纹脚本支持按月库分别运行：对 `online_data_202609.db` 与 `online_data_202610.db` 各输出全表结构/逐表行数；近期行hash分别选月末三日与10/12–14。[compare_online_fingerprints.py](data_integrity_verification_20261014/compare_online_fingerprints.py) 将逐表行数、全库schema/index/FK hash和所选四类表全行指纹对比，并逐项列出差异。近期某些kind/date原本没有表时会单独记在 `selected_tables_absent`，只和源侧预期集合比，不自动判为失败。源和目标库均已访问并对比；详细结果见最终验收表。

TDM代码ready后，静态导入 `/Users/nickzhang/TradingDataManager/coreClient/data_provider.py` 并以 `inspect.signature` 对18条query manifest的全部API名/kwargs做了只读预检，18/18均有匹配签名；没有调用数据库。特殊新闻时间窗API要求 `source='database_only'`，manifest已满足。记录此为调用参数预检，不代替冻结数据查询。

### 冻结源窗口指纹与周/月语义抽样（2026-10-14）

data agent确认本机 launchd writers 已卸载，17个已确认归属的 online worker 已退出；源离线/在线库 `lsof` 无打开句柄，Redis PID 22730 保持运行。冻结窗口只做只读查询，没有数据库写入、数据刷新或Redis写命令。API、在线库和Redis执行器及结果均保存在 `data_integrity_verification_20261014/`，target ready后必须用相同manifest、日期集和比较脚本重跑。

- 18条统一 provider API 均 `status=ok`（18/18），输出含shape、列/dtype/index、顺序行hash及耗时，见 `api_fingerprint_source.json`。耗时最大为最近日历查询19.137秒；日线、周月、分钟、KPL和新闻样本均返回。空结果仍按空结果hash留档，不因接口返回成功就断言业务覆盖充分。
- `online_data_202609.db`：112表，4,126,658行，schema hash `f4069739…f1c3a00`；`online_data_202610.db`：91表，2,523,873行，schema hash `ccfa2721…b9e5a30`。两库仅在确认无writer、无WAL/SHM sidecar后用immutable指纹；各选3个近月日期，hash minute/snapshot/snapshot_index/auction及stream cursor，共13个候选指纹。完整值见 `online_fingerprint_source_202609.json` 与 `online_fingerprint_source_202610.json`。
- Redis `DBSIZE=308`，逐key保存类型、PTTL与DUMP哈希指纹，见 `redis_fingerprint_source.json`。Redis持久目录随后一并迁至 `TradingDatabase/online/redis`；target比较对预期TTL自然衰减做时间校正，不把到期key误判为迁移差异。
- 周/月真实内容小样本：对 `000001.SZ`、`600000.SH`、`300750.SZ`，以日线API聚合出的最近3个完整周和3个完整月，分别对比 `get_week/get_month` 的 origin/qfq 行。4组每组9行，OHLC、vol、amount均无差异。qfq的聚合输入是项目自身 `get_day(qfq=True)` 输出；未套用外部成交量复权公式。日线到20261014，完整周截止20261009、完整月截止20260930。四张period表各自最大日期20261014；周表各2,511,637行/5,832代码，月表各598,960行/5,832代码。`tbl_basic_ctrl` 只返回 `cn_daily` 与 `cn_adj_factor`，没有 `cn_week`、`cn_month` 记录；表中周期数据仍有10/14行，因此需把控制状态缺项与服务超时日志作为源端状态保留，不能将表数据旧鲜度误报为迁移损失。明细见 `period_semantic_source.json`。

上述冻结源指纹是迁移一致性基准，不表示生产数据语义全部正确。10/13、10/14在线minute跨日污染和20260922一分钟分区缺失仍是迁移前观察到的源端内容问题；迁后相同则属保持源状态，变化则按差分定位。在线快照样本中 `auction.turnover` 全NULL的producer契约仍待确认。

## 迁移后验收结果（2026-10-14）

数据agent确认目标树 `/Users/nickzhang/TradingDatabase/{offline,online}` 已就绪；target指纹开始前检查到offline/online scheduler仍unloaded，唯一匹配到的服务是Redis PID 32888。目标两月SQLite文件无WAL/SHM sidecar，故使用immutable只读；API和周期抽样均用 `source='database_only'`。本审计的指纹查询没有业务写入；迁移agent负责停写、checkpoint、原位目录切换和Redis恢复。

cutover log记录同卷目录 `os.rename`（device `16777234`）：offline目录inode `233754524` 原样成为 `TradingDatabase/offline`，online目录inode `233756979` 原样成为 `TradingDatabase/online`，Redis目录inode `233960264` 原样成为 `TradingDatabase/online/redis`。这是目录身份/原位移动证据，不是每个SQLite、DuckDB、Parquet文件分别计算的完整文件hash manifest；本次没有宣称逐文件哈希全量PASS。

| 验收面 | 分母/抽样 | 结果 |
|---|---|---|
| provider API | manifest固定18 case，源与target各18 | 18/18结果状态及schema/dtype/index/行数/有序值hash完全相同；见 `api_fingerprint_comparison.json` |
| Online 202609 | 全库112表、4,126,658行；近期三日4种表+cursor共13个hash候选 | schema hash、全部表count、指定hash与absent-table清单均相同；见 `online_comparison_202609.json` |
| Online 202610 | 全库91表、2,523,873行；近期三日4种表+cursor共13个hash候选 | schema hash、全部表count、指定hash与absent-table清单均相同；见 `online_comparison_202610.json` |
| Redis | 冻结源/target各308 key | key集合、type、DUMP hash相同；PTTL以3秒容差校正约662.172秒间隔后308/308通过。默认2秒容差曾把5个key标为TTL偏差，5项均只超约11毫秒，原因是整秒elapsed时间与毫秒PTTL取样不同步；未发现新key、丢失key或值变化。见 `redis_fingerprint_comparison.json` |
| 周/月语义 | 3只股票；完整周截止20261009、完整月截止20260930；origin/qfq四表各9行 | 源、target都与同模式日线API聚合吻合；4个period表行数/代码数/日期边界和ctrl记录一致。见 `period_semantic_source.json`、`period_semantic_target.json` |

迁移后的内容正确性结论分为两层：**所测迁移一致性 PASS**；**全库内容完整性没有由本次有限指纹完全证明**。offline四个SQLite和各DuckDB/Parquet大文件没有做全量行hash；靠冻结源/目标API小指纹与文件移动审计证明有限语义和路径切换。全Parquet footer与全历史日期目录检查是在源端完成，target未重新解码428个分区。因此不能把本报告的迁移PASS扩写成“所有分钟记录均正确”。

已确证且迁移前存在的源端问题仍保持为独立事项：online `minute_20261013` 表有20,880条前一日记录、`minute_20261014` 表有15,120条前一日记录；一分钟Parquet缺少开市日分区 `20260922`，预计约1,332,960 bars/约16MB。不得在迁移通过结论中静默修复或将其归因迁移。三日auction样本 `turnover` 2000/2000均NULL，producer契约待确认。周/月表源端control缺 `cn_week`/`cn_month` 两项，但四张数据表及抽样内容均到20261014且目标与源一致；这属于源control状态，不是迁移差异。源日志中的 `service_month` 于22:52:59因1800秒subprocess timeout失败；实际四张周期表已有10/14截止记录，勿以失败日志推定周期表内容落后。

可重跑脚本、manifest、源/目标receipt与比较JSON均存于 `data_integrity_verification_20261014/`。主要执行命令形态如下，源/target只替换 `--database-root`，在线库只替换DB路径及日期：

```sh
RUN_DIR=/private/tmp/trading_restructure_20261015/run_YYYYMMDD_HHMMSS
mkdir -p "$RUN_DIR"
PYTHONDONTWRITEBYTECODE=1 /Users/nickzhang/TradingAgent/.venv/bin/python /Users/nickzhang/TradingDataManager/docs/data_integrity_verification_20261014/run_api_fingerprints.py --repo-root /Users/nickzhang/TradingDataManager --database-root /Users/nickzhang/TradingDatabase --manifest /Users/nickzhang/TradingDataManager/docs/data_integrity_verification_20261014/api_fingerprint_manifest.json --label target-rerun --output "$RUN_DIR/api_fingerprint_target.json"
```

每次重跑应新建 `run_YYYYMMDD_HHMMSS` 输出目录，不覆盖本次封存的 source/target receipts。以上比较必须保留manifest固定参数和同一代码版本；redis TTL比较应使用两次采样时间差，并保留3秒容差的解释。历史完整性扫描应按本报告的分母复核，勿重复扫描5亿分钟行来替代迁移文件一致性证据。

### service_month超时后的完整周期表审计（2026-10-15）

查询前只读确认offline scheduler PID 33462处于daemon状态，`lsof +D /Users/nickzhang/TradingDatabase/offline` 未发现打开的数据库句柄；目标basic DB无 `-wal`/`-shm`，使用immutable只读查询。没有启动service或重建。

代码核对：`service_month.main()`只检查 `tbl_basic_ctrl.cn_daily` 与 `cn_adj_factor` 相同且不早于最近收盘交易日，然后调用 `CNDataDown.update_month()`；该service及 `rebuild_period_tables()` 没有写入 `cn_month`/`cn_week` control key。周期重建按5,832个code全历史处理，两张结果先写私有staging SQLite；只有全部code处理成功之后，才在一个事务内成对 DELETE+INSERT qfq/origin 两表并COMMIT，任何中断会ROLLBACK并保留上一对表。因此control缺两个派生表key是当前代码合同，不是本次重建失败证据。

`service_month.log` 的同一日期流水显示：18:46开始的首轮于19:12:15完成，两表各重建598,960行；20:15开始的第二轮于20:29:46完成，同样各598,960行；22:22:58第三轮全量重算，直到22:52:43只完成4,160/5,832只股票 staging，22:52:59被scheduler的1800秒timeout终止。日志没有第三轮pair commit；目标月表 `snap_ts` 最大值仍为20:29:37。故22点是当天第三次全量重复刷新中途失败，前两次已经成功覆盖当天数据。

对4张周期表全量键与日线最新日期做了bounded-memory只读检查（receipt `period_full_audit_target.json`，耗时151.867秒）：周qfq/origin键差异0/0，月qfq/origin键差异0/0；每张表覆盖的5,832代码均与 `tbl_cn_day` 代码集合一致，缺码0、额外code 0、period max date与该code原始日线max date不一致0。逐code最早日线max为20150429，最晚为20261014。

前复权基准复核显示当前因子没有使旧月线价格过期：全量598,960条月qfq `close` 均与相同期间结束日原始日线close、期间adj_factor及该代码当前最新非空adj_factor重算相符；缺日线0、缺因子0、价格差异0，最大绝对浮点差2.28e-13。另对所有5,832只股票的最新周qfq close按当前日线和factor核对，缺行及差异也均为0。当前因子最新快照时间不晚于2026-10-14 18:00:40；周表 `snap_ts` 最大22:20:39、月表最大20:29:37。

**判定：无需补跑 `service_month`。**其当天前两轮成功，第三轮失败在成对表提交之前；全量period键、每代码最新日期、所有历史月qfq收盘价格基准均已与当前日线/因子核对通过。日志、表内快照、事务代码和只读扫描相互印证。该结论不要求此刻启动周期服务；后续按正常调度继续即可。

本次实际代码根为 `/Users/nickzhang/TradingDataManager`，数据库根为 `/Users/nickzhang/TradingDatabase`。新CLI case在默认sandbox下因本地路径权限报 `DatabaseError`；相同 `day_raw_recent` 三行、`source='database_only'` 查询在获准的只读提权调用下成功，判为sandbox访问边界，不是目标库/API缺陷。

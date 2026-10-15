# TradingDataManager 数据完整性审计 — 2026-10-15

## 审计边界和状态

本报告把结构、内部数值域、来源交叉核对和研究语义分开记录。通过行数、键或SQLite/DuckDB完整性检查不等于行情值已被外部来源证明正确。当前本机离线库逐表审计仍在进行；没有把未扫描表或缺少来源证据的内容写成PASS。在线分钟事务已在冻结窗口完成本机修复并按受控离线增量交给部署流程；Mac mini服务与合并验收状态见迁移计划和部署回执。

主要可复核状态文件在 `/private/tmp/trading_data_audit_20261015/`，核心日线profile与续跑交叉表结果见 `core_daily_tables_audit.json`、`core_daily_key_coverage_duckdb.json`。持久化后的TDX 2026-09-22来源证据在 `offline/oneMinute/quality_audit/20260922/`。

## 统一1分钟Parquet

当前canonical包含429个交易日、562,152,720行，日期覆盖2025-01-02至2026-10-14。分母由原有428个分区的完整结构/数值扫描，加2026-09-22独立全量采集、单独验收后晋升的1,332,960行、5,554代码（每只240根）组成；不能把这两次扫描描述成一次对429分区的全量扫描。原428个分区均可解码，日期与分钟索引匹配，常规交易时段和重复键检查通过；新9/22分区有独立的keys、sidecar、payload hash及日线例外验收。与更新后的SSE交易日历对照，429个开市日均有主分区。原428树数值补扫未发现NaN/±Inf、负价格、非法OHLC关系、负/小数股数或负amount；9/22另按单日manifest验收。该结果证明所检查文件的内部一致性，不代表所有价格和成交额均已对外部逐笔真值核验。

在107个原有分区中，已将4,580,454个精确的 `2^-127` TDX amount解码哨兵修正为 `0.0`。修复只匹配该精确值；除amount列外的Arrow值hash、物理schema、metadata和行数通过逐文件校验，manifest与DuckDB catalog尺寸已更新。107份原分区及配套manifest/catalog保存在权限为0700的私有回退档案中。事后以归档逐行逻辑重建核对107/107通过；当时未保存每个源文件修复前的SHA，因此报告不声称有逐源SHA预存证明。工具后续版本已加入源读前/读后SHA与备份SHA相等检查、备份冲突拒绝及回滚fixture。

解码哨兵是已确认的零值修正，属于信息性记录。另有24,667个原有bar的存储值满足 `vol=0 AND amount>0`，2026-09-22新增251个同类bar；合计24,918行。它们的OHLC和原始volume/amount均保留，作为量化/单位待核的软质量警告，不能仅因该情况排除价格研究，也不能据此宣称成交量精确。`get_oneMin_quality`可从已存质量sidecar或当前canonical值派生查询，并区分原始来源payload和 `historical_stored_values`。缺少sidecar且无日级审计时返回unknown，不会把未审计解释为0条异常。

2026-09-22整日结构导入状态为 `ok`，但接受清单明确带有来源聚合例外：4只股票首根09:31 open与日线open不同；2只北交所股票的日线成交量/成交额与分钟聚合存在大幅单位/来源差异；另有9只股票的分钟amount聚合与TDX category-9日线超过float32误差界。值未自动改写。质量API按日期、股票附上例外计数、过滤后的代码详情、相对证据路径和SHA；详情仅在证据文件SHA与manifest相符时标记verified。9/22证据在结构成功之外仍保留 `accepted_with_known_exceptions_query_follow_up_pending` 状态。

## 在线SQLite分钟

旧在线分钟分时源曾把无日期的时间序列按采集当天合成日期，且源decoder产生无法解释的巨大正值。新采集端改用TDX原生1分钟bars，并把精确volume/amount零值哨兵解码为0；`amount>0, vol=0`保留bar与来源质量标记，非法OHLC、非正价格、负量和非有限值仍拒绝。历史旧表没有通过改日期修复；对可用日期从可信canonical OHLC重建，修复时间写入created_at，历史fetch时间未知则保持NULL。

2026-09月本机修复回执覆盖12个日期，逐日源与写后SQLite行hash、code数及行数匹配；9/22为52,800行、220只股票，冻结可见集合中 `002860.SZ` 未从TDX返回，状态未知，未声称完整。2026-10-15本机写入与stage逐行hash完全匹配：14,043行、119只股票；其中118只各119行（09:31–11:29），`600825.SH`仅有09:31一根，237个预期时点缺口保留为source缺口，不补造。SQLite `integrity_check=ok`。Mac mini 2026-09库已按业务键晋升，12张分钟表逐字段双向差集为0；7,588个Mini优先payload冲突另留未解决ledger。Mini 2026-10的 `snapshot_index_20261001` 已隔离3,002条与9/30逐字段相同的跨日旧Redis回灌记录，并保留同库备份/隔离表；3,840条真正时间戳为10/1的闭市响应仍在活动表并有day ledger。receipt核到备份6,842行=活动3,840行+隔离3,002行，双向EXCEPT为0；10/15活动表3,096行，事务前后max timestamp/created_at未变。对应验收receipt分别在Mini `online/archive/merge_202609_bizkey_20261015/promotion_receipt.json` 和 `online/archive/snapshot_index_20261001/snapshot_index_20261001_quarantine_receipt.json`。在线服务最新恢复状态仍以deployment实时回执为准。

早期只读基线发现23张在线minute表中17张有跨日期记录，共403,680/662,428行；本机历史重建不代表当时在线采集链路正常。snapshot、auction、watchlist等其他在线family按采集状态与交易日语义分别审核，不因分钟表结论推断通过；`auction.turnover`仍有已报告的NULL/来源合同问题。

## 旧SQLite分钟与tick数据

本机`policy_minute.db`的 `tbl_minute` 有10,558,320行、161个交易日、43,993个代码日；结构、240个time_idx、日期/交易时段、OHLC正值与非负volume检查通过。其字段来源是旧无时间戳分时，量单位经抽样与canonical全OHLC分钟对照约为“手”而非新native源的“股”；抽样price及volume也存在源值差异。未平移time_idx、未把历史volume乘100、未覆盖旧记录。已将旧stock和index分钟写入入口fail-closed，避免新单位或未验证的股票源混入旧表；旧表保留查询用途并须带来源/单位限制。

控制表中有133个 `tdx_empty` 代码日，尚未逐一与上市、停牌和可验证数据源核对，不能直接计为缺失或正常空值。历史 `created_at` 缺值只记录为来源时间未知，不伪造采集时间。

Mac mini原 `policy_ticks.db` v1历史已保护为 `offline/legacy/policy_ticks_v1.db`（30,507,851行，2026-01-05至2026-08-31，PK为code/date/seqId），并与本地v2库分开。已提供显式只读 `get_ticks_legacy` API；它要求限定交易日或完整起止范围，原样保留raw方向码，并注明datetime只有分钟精度、`seqId`尚未证明是交易所成交序号。该mini文件的字段/行数范围已做静态审核，但完整逐行语义与新v2的统一迁移仍未完成。配套legacy `policy_minute_v1.db`约1.37GB也保留在legacy路径，尚未完成同等级内容审计。本机未挂载mini legacy目录时API明确报文件缺失，不回退到小型v2库。

## 离线SQLite核心日线表

本机canonical `db_cn_basic.db`的SSE/SZSE交易日历覆盖2015-01-01至2026-12-31，共8,766行、每市场4,383行。除先前补齐的2015–17与2026-10-15至12-31官方日历外，本次按官方Tushare回执增加了2026-09-20 SSE/SZSE两行：`is_open=0, pretrade_date=20260918`；增量回执为 `tradecal_local_patch_20260920.json`。这是calendar-only更新，不是完整行情更新。日线审计工具 `tools/data_checks/audit_core_daily_tables.py` 对2015-01-01至2026-10-14使用 `check_database(auto_repair=False, regenerate_derived=False)`，并执行schema、主键、NULL、numeric storage type、finite、域、开市日和跨表键对照。profile已完成；最初SQLite跨表JOIN进入持续随机I/O后，保留三表profile并停止该只读进程，改用分块键→临时DuckDB完成四项anti-join。所有邻日行数比例候选都只是调查线索，不会自动重下载。

已扫到的表级结果：

| 表 | 行数 | SSE开市日分母 | 邻日候选 | 已执行域检查 |
|---|---:|---:|---|---|
| `tbl_cn_day` | 11,820,330 | 2,860 | 2015-07-08、2015-07-10、2015-07-13 | OHLC关系及价格、负vol/amount违规为0 |
| `tbl_cn_daily_basic` | 11,729,968 | 2,860 | 同上 | close非正、share/market value为负违规为0 |
| `tbl_cn_adj_factor` | 12,333,582 | 2,860 | 无 | 非正factor违规为0 |

三表numeric非有限数均为0。日线 `pre_close/change/pct_chg` 各有224个NULL，最大 `pct_chg=200400.0`；尚未完成与IPO首日/原始上游字段的逐行定性，不截断极值。每日指标财务估值列存在大量NULL，PE/PB/PS等负值可能合法，已避免把它们当通用负值违规。2015-07-08/10/13候选处在股灾期间；停牌表需要进一步按代码日核验，暂不视为真实缺失。

全历史键比较通过分块流式读取源SQLite、将三张表的`ts_code,trade_date`键装入1GB上限的临时DuckDB后完成，临时键库不属于生产数据。结果为：日线键缺`daily_basic` 90,362条，日线键缺`adj_factor` 0条，`daily_basic`键缺日线0条，`adj_factor`键缺日线513,252条。差异样本按日期/代码集中在特定时期，包含历史北交所代码映射和2015年7月等日期；在完成代码变更、上市/停牌及provider来源语义核对前，这些是未解释的键差异，不等同已证实损坏或缺行。原SQLite JOIN尝试被中断、三表profile未重跑；流式续跑来源和四项结果记在上述JSON receipt。

`db_cn_basic.db`共16表的结构/行数清单与SQLite `quick_check=ok`已完成；全库所有表的家族特定内容审计仍未完成。其他本机SQLite的已完成行数清点：`db_cn_kpl.db` 10表、`db_cn_news.db` 4表、`policy_minute.db` 4表。上述行数清点不替代所有表的NULL、来源、状态和经济语义审核；`policy_ticks.db`、`db_cn_index.db`及mini独有legacy库仍需逐表补齐审核证据。

## DuckDB与其余未完成项

独立只读审计覆盖7个DuckDB文件、63个表、103,812,018行：重复主键、主键NULL、非法日期、非法JSON和闭市日记录计数均为0；共有551,410个NULL。原汇总862条孤儿候选全部来自 `fact_limit_event(ts_code)->dim_stock(ts_code)`：active中404行/340代码，frozen中458行/368代码，均只含炸板事件；炸板代码不要求有 `dim_stock` 项。审计确认真正缺少stock身份的涨停行=0、其他未知tag=0，因此真实missing/unknown关系为0；其余已声明外键关系孤儿均为0，`dim_theme.parent_theme_id`的691个NULL是可空根节点。Derived active 10,420个代码日组、frozen 13,478组的limitup/blown计数对账均为0差异；热度分数状态机未回放。具体关系计数见 `/private/tmp/trading_data_audit_20261015/duckdb_tables_audit_v2.json`。15分钟等衍生日线聚合的一致性样本与表范围另见DuckDB审计receipt。

尚未完成的范围包括：其余offline SQLite全部表的逐表finite/NULL/域/PK/时间/状态语义；mini legacy两个大库的完整审计；非分钟在线snapshot、auction状态值及其采集频率/闭市语义；133个 `tdx_empty` 代码日核验；离线week/month所有OHLCV完整聚合一致性；源外部价格真值的全范围验证。没有把这些项目并入已通过的内部结构结果。

## 可重复工具和回执

- `tools/data_checks/audit_parquet_partitions.py` 与 `audit_parquet_numeric_supplement.py`：全分区结构/数值审计。
- `tools/data_checks/correct_tdx_zero_amount_sentinel.py`：仅精确哨兵替换；新备份路径先比对源读前/读后SHA和已有备份SHA，冲突即停止。
- `tools/data_checks/audit_core_daily_tables.py`：核心日线/日指标/复权因子只读逐日覆盖及NULL/域/日历profile。
- `tools/data_checks/audit_core_daily_key_coverage_duckdb.py`：以有界DuckDB临时键库分块续跑跨表key anti-join，不重复扫描/计算数值profile。
- 详细运行回执、分区来源清单和修复证据位于 `/private/tmp/trading_data_audit_20261015/` 与canonical `offline/oneMinute/quality_audit/`；其中临时状态文件只在扫描完整后才适合作为最终记录。

# TradingDataManager 数据完整性审计 — 2026-10-15

## 审计边界和状态

本报告把结构、内部数值域、来源交叉核对和研究语义分开记录。通过行数、键或SQLite/DuckDB完整性检查不等于行情值已被外部来源证明正确。本机 SQLite 审计覆盖32张表：v2逐表profile完整，v3只复扫`db_cn_news.db:tbl_news.datetime`并沿用其余31张profile；另有核心日线及周/月按独立工具审计。已完成内部审计不代表外部行情真值、历史来源语义或未匹配记录已获证明。在线分钟、事件时间与日终落库结果见本报告和收盘验收记录 `docs/macmini_close_acceptance_20261015.md`。

主要可复核状态文件在 `/private/tmp/trading_data_audit_20261015/`，核心日线profile与续跑交叉表结果见 `core_daily_tables_audit.json`、`core_daily_key_coverage_duckdb.json`。持久化后的TDX 2026-09-22来源证据在 `offline/oneMinute/quality_audit/20260922/`。

## 统一1分钟Parquet

当前canonical包含429个交易日、562,152,720行，日期覆盖2025-01-02至2026-10-14。分母由原有428个分区的完整结构/数值扫描，加2026-09-22独立全量采集、单独验收后晋升的1,332,960行、5,554代码（每只240根）组成；不能把这两次扫描描述成一次对429分区的全量扫描。原428个分区均可解码，日期与分钟索引匹配，常规交易时段和重复键检查通过；新9/22分区有独立的keys、sidecar、payload hash及日线例外验收。与更新后的SSE交易日历对照，429个开市日均有主分区。原428树数值补扫未发现NaN/±Inf、负价格、非法OHLC关系、负/小数股数或负amount；9/22另按单日manifest验收。该结果证明所检查文件的内部一致性，不代表所有价格和成交额均已对外部逐笔真值核验。

本次复核严格限定canonical物理路径 `offline/oneMinute/parquet/schema_v2/year=*/month=*/trade_date=*/part-*.parquet`：429个唯一分区与429份trade-date manifest的 `target_file` resolve路径和size全部一一对应（missing/extra/duplicate/size mismatch均0）；不把quality_audit归档或`_quality` sidecar计入主分区。仅投影每个bar的 `(ts_code,trade_date)` 后，429分区共562,152,720行、2,342,303个不同code-day。与日线2015-01-02至2026-10-14对应范围比较，原代码键差异为日线缺minute 26、minute无日线4,406；4,406个额外code-day尚未逐项定性，不直接判为污染或有效交易。26个缺同码分钟均为 `300114.SZ` 的2025-01-02至02-14；当日canonical分钟分区均有 `302132.SZ` 的240根。深交所2025-02-15公告显示该公司2025-02-17启用302132、T-1前代码为300114。26日旧代码日线与新代码分钟聚合的close一致26/26、high一致23/26、low一致25/26；首分钟open与日线开盘存在会话口径差异，volume约有手/股单位差、amount约有千元/元单位差。此处只作为历史代码别名覆盖证据，不改主表stored code，也不宣称所有字段等价。逐日证据见 `one_min_code_alias_300114_302132.json`。

在107个原有分区中，已将4,580,454个精确的 `2^-127` TDX amount解码哨兵修正为 `0.0`。修复只匹配该精确值；除amount列外的Arrow值hash、物理schema、metadata和行数通过逐文件校验，manifest与DuckDB catalog尺寸已更新。107份原分区及配套manifest/catalog保存在权限为0700的私有回退档案中。事后以归档逐行逻辑重建核对107/107通过；当时未保存每个源文件修复前的SHA，因此报告不声称有逐源SHA预存证明。工具后续版本已加入源读前/读后SHA与备份SHA相等检查、备份冲突拒绝及回滚fixture。

解码哨兵是已确认的零值修正，属于信息性记录。另有24,667个原有bar的存储值满足 `vol=0 AND amount>0`，2026-09-22新增251个同类bar；合计24,918行。它们的OHLC和原始volume/amount均保留，作为量化/单位待核的软质量警告，不能仅因该情况排除价格研究，也不能据此宣称成交量精确。`get_oneMin_quality`可从已存质量sidecar或当前canonical值派生查询，并区分原始来源payload和 `historical_stored_values`。缺少sidecar且无日级审计时返回unknown，不会把未审计解释为0条异常。

2026-09-22整日结构导入状态为 `ok`，但接受清单明确带有来源聚合例外：4只股票首根09:31 open与日线open不同；2只北交所股票的日线成交量/成交额与分钟聚合存在大幅单位/来源差异；另有9只股票的分钟amount聚合与TDX category-9日线超过float32误差界。值未自动改写。质量API按日期、股票附上例外计数、过滤后的代码详情、相对证据路径和SHA；详情仅在证据文件SHA与manifest相符时标记verified。9/22证据在结构成功之外仍保留 `accepted_with_known_exceptions_query_follow_up_pending` 状态。

## 在线SQLite分钟

旧在线分钟分时源曾把无日期的时间序列按采集当天合成日期，且源decoder产生无法解释的巨大正值。新采集端改用TDX原生1分钟bars，并把精确volume/amount零值哨兵解码为0；`amount>0, vol=0`保留bar与来源质量标记，非法OHLC、非正价格、负量和非有限值仍拒绝。历史旧表没有通过改日期修复；对可用日期从可信canonical OHLC重建，修复时间写入created_at，历史fetch时间未知则保持NULL。

2026-09月本机修复回执覆盖12个日期，逐日源与写后SQLite行hash、code数及行数匹配；9/22为52,800行、220只股票，冻结可见集合中 `002860.SZ` 未从TDX返回，状态未知，未声称完整。2026-10-15本机写入与stage逐行hash完全匹配：14,043行、119只股票；其中118只各119行（09:31–11:29），`600825.SH`仅有09:31一根，237个预期时点缺口保留为source缺口，不补造。SQLite `integrity_check=ok`。Mac mini 2026-09库已按业务键晋升，12张分钟表逐字段双向差集为0；7,588个Mini优先payload冲突另留未解决ledger。Mini 2026-10的 `snapshot_index_20261001` 已隔离3,002条与9/30逐字段相同的跨日旧Redis回灌记录，并保留同库备份/隔离表；3,840条真正时间戳为10/1的闭市响应仍在活动表并有day ledger。receipt核到备份6,842行=活动3,840行+隔离3,002行，双向EXCEPT为0；10/15活动表3,096行，事务前后max timestamp/created_at未变。对应验收receipt分别在Mini `online/archive/merge_202609_bizkey_20261015/promotion_receipt.json` 和 `online/archive/snapshot_index_20261001/snapshot_index_20261001_quarantine_receipt.json`。在线服务最新恢复状态仍以deployment实时回执为准。事件流修复详情：2,313条zt秒时间和1,728条anomaly毫秒时间已从raw值规范化；anomaly中1,395条规范化，331条与既有同键payload完全相同的副本留档去重，另2条tag_name不同的记录隔离为未解决冲突。zt布尔标志的10/15源匹配5,000条已修复，仍有6,591条无保留源覆盖；包括该6,591条在内，当前记录范围内340,179行的`is_st/is_new`不能作为历史ST事实。无历史来源的其余15个受影响交易日仍保留原值并标不可信。回执和API警示见`docs/online_zt_flag_quality_20261015.md`及Mini私有治理目录，不将其当前状态扩展为未审日期的永久结论。2026-10-15收盘受管验收确认73只午后观察股的15:00分钟bar均自动落库，`time_idx=239`，与独立Redis逐键相符；SQLite最终写入窗口为15:16:13.961–15:16:14.052，无人工补落。详情见`docs/macmini_close_acceptance_20261015.md`。竞价缺口仍未补采，旧6379 raw未按用户最新指示导出。

早期只读基线发现23张在线minute表中17张有跨日期记录，共403,680/662,428行；本机历史重建不代表当时在线采集链路正常。snapshot、auction、watchlist等其他在线family按采集状态与交易日语义分别审核，不因分钟表结论推断通过；`auction.turnover`仍有已报告的NULL/来源合同问题。

## 旧SQLite分钟与tick数据

本机`policy_minute.db`的 `tbl_minute` 有10,558,320行、161个交易日、43,993个代码日；结构、240个time_idx、日期/交易时段、OHLC正值与非负volume检查通过。其字段来源是旧无时间戳分时，量单位经抽样与canonical全OHLC分钟对照约为“手”而非新native源的“股”；抽样price及volume也存在源值差异。未平移time_idx、未把历史volume乘100、未覆盖旧记录。已将旧stock和index分钟写入入口fail-closed，避免新单位或未验证的股票源混入旧表；旧表保留查询用途并须带来源/单位限制。

控制表中有133个 `tdx_empty` 代码日，尚未逐一与上市、停牌和可验证数据源核对，不能直接计为缺失或正常空值。历史 `created_at` 缺值只记录为来源时间未知，不伪造采集时间。

Mac mini原 `policy_ticks.db` v1历史已保护为 `offline/legacy/policy_ticks_v1.db`（30,507,851行、3,176代码、160日期，2026-01-05至2026-08-31，PK为code/date/seqId），并与本地v2库分开。只读全表域/控制表审计中，code/date/time字段、price/vol基础域均无空值或非法值，13,136个控制代码日与数据双向一致；24,489行落在09:25–15:00常规时段之外（111行09:15–09:24，24,378行15:05–15:30），需按扩展时段/集合竞价来源继续解释，原样保留。方向码原值包括0/1/2/5/8，不因代码5/8过滤；`seqId`顺序下datetime倒序为0，但这只验证存储排序关系，不能证明它是交易所真实成交序号。已提供显式只读 `get_ticks_legacy` API；它要求限定交易日或完整起止范围，原样保留方向码，并在attrs标记“内部域和控制表已审、来源完整性及seq语义未验证”。配套legacy `policy_minute_v1.db`约1.37GB也保留在legacy路径，完整表审计有8,326,802股票分钟行、3,241代码、34,696代码日，控制表双向匹配；所有已存code-days time_idx在0–239、240根顺序及基础价格/数量域通过。唯一少于240根的代码日为`000006.SZ/20260908`仅2根（缺238，来源原因未知）。`tbl_minute_index`有192,000行、800个指数代码日，控制表匹配、索引顺序与基础域检查通过。完整JSON见`/private/tmp/legacy_semantic_audit.json`。本机未挂载mini legacy目录时API明确报文件缺失，不回退到小型v2库。

## 离线SQLite核心日线表

本机canonical `db_cn_basic.db`的SSE/SZSE交易日历覆盖2015-01-01至2026-12-31，共8,766行、每市场4,383行。除先前补齐的2015–17与2026-10-15至12-31官方日历外，本次按官方Tushare回执增加了2026-09-20 SSE/SZSE两行：`is_open=0, pretrade_date=20260918`；增量回执为 `tradecal_local_patch_20260920.json`。这是calendar-only更新，不是完整行情更新。日线审计工具 `tools/data_checks/audit_core_daily_tables.py` 对2015-01-01至2026-10-14使用 `check_database(auto_repair=False, regenerate_derived=False)`，并执行schema、主键、NULL、numeric storage type、finite、域、开市日和跨表键对照。profile已完成；最初SQLite跨表JOIN进入持续随机I/O后，保留三表profile并停止该只读进程，改用分块键→临时DuckDB完成四项anti-join。所有邻日行数比例候选都只是调查线索，不会自动重下载。

已扫到的表级结果：

| 表 | 行数 | SSE开市日分母 | 邻日候选 | 已执行域检查 |
|---|---:|---:|---|---|
| `tbl_cn_day` | 11,820,330 | 2,860 | 2015-07-08、2015-07-10、2015-07-13 | OHLC关系及价格、负vol/amount违规为0 |
| `tbl_cn_daily_basic` | 11,729,968 | 2,860 | 同上 | close非正、share/market value为负违规为0 |
| `tbl_cn_adj_factor` | 12,333,582 | 2,860 | 无 | 非正factor违规为0 |

三表numeric非有限数均为0。日线 `pre_close/change/pct_chg` 各有224个NULL，最大 `pct_chg=200400.0`；尚未完成与IPO首日/原始上游字段的逐行定性，不截断极值。每日指标财务估值列存在大量NULL，PE/PB/PS等负值可能合法，已避免把它们当通用负值违规。2015-07-08/10/13候选处在股灾期间；停牌表需要进一步按代码日核验，暂不视为真实缺失。

全历史键比较通过分块流式读取源SQLite、将三张表的`ts_code,trade_date`键装入1GB上限的临时DuckDB后完成，临时键库不属于生产数据。结果为：日线键缺`daily_basic` 90,362条，日线键缺`adj_factor` 0条，`daily_basic`键缺日线0条，`adj_factor`键缺日线513,252条。差异样本按日期/代码集中在特定时期，包含仍未取得可验证历史代码映射关系的北交所候选和2015年7月等日期；在完成代码变更、上市/停牌及provider来源语义核对前，这些是未解释的键差异，不等同已证实损坏或缺行。原SQLite JOIN尝试被中断、三表profile未重跑；流式续跑来源和四项结果记在上述JSON receipt。

SQLite复合审计v2包含`db_cn_basic.db`、`db_cn_kpl.db`、`db_cn_news.db`、`db_cn_index.db`和本机空的`policy_ticks.db`，共32张表；各表schema/键/NULL/存储类型/数值域/适用时间字段及控制关系的内部profile已完成。`db_cn_basic.db`共16表；其中核心日线相关结果另见上文和`audit_core_daily_tables.py`。v3仅更新新闻表时间字段，其余31个profile从完整v2基线保留。Mini独有legacy tick/minute/index三表也完成内部域/控制表审计，但来源完整性、真实成交序号和少数不满240根代码日仍未由外部来源证明。

SQLite审计器v3只重扫了 `db_cn_news.db:tbl_news.datetime`，旧v2其余表profile保持原样（32张表中仅该字段更新）。新闻表9,578,880行的datetime均非NULL，事件日期2,479组，范围2020-01-01至2026-10-15；9,269,913行为合法完整ISO秒时间戳，308,967行为合法16字符ISO分钟时间戳。对分钟时间戳另检查真实公历日期（含闰年）及小时00–23、分钟00–59；无非法或未知格式。回执顶层 `status=partial` 表示本次只执行了timestamp-only更新；其 `audit_provenance.composite_coverage_status=complete` 记录由完整v2 32表profile加v3新闻时间字段组成的完整覆盖，且声明仅1表重扫、31表沿用。基线v2回执SHA-256为 `b61a0dac412c9fa2a793b2e4ae7eb6ca20050d96dd15f374eb27344eac4bb2e4`。v3回执为 `/private/tmp/trading_data_audit_20261015/sqlite_semantic_audit_v3.json`，持久副本在 `~/.local/share/TradingDataManager/data_governance/20261015/audits/sqlite_semantic_audit_v3.json`。此检查只说明存储时间戳格式/范围，不验证新闻内容或上游时间真值。

`tbl_cn_limit_list`有1,063条记录落在SSE闭市日，保留原始源记录，不把它们直接当作有效交易日涨停事件。只读调用现有Tushare接入源对2026-10-01查询返回73行、唯一返回日期为2026-10-01，schema为18列；实现检查显示本地updater传入请求日期范围且按source返回的 `trade_date` 写入，没有本地覆盖日期的逻辑。当前证据无法确认该接入请求走原生接口还是配置代理，也无法解释闭市日返回行的源语义；因此仅记录为“现有接入源在闭市日仍返回数据”，不声称交易所事实，也不删除或改写这些历史行。

## 周/月衍生日线聚合

只读工具 `tools/data_checks/audit_week_month_period_tables.py` 按无`ORDER BY`的SQLite顺序投影，将日线、复权因子与四张实际期表分块载入1GB限制的DuckDB，再按生产代码合同重聚合并逐键对照。周按ISO year/week，月按YYYYMM；原始表与前复权表都核对OHLC、pre_close、change、pct_chg、vol、amount，`snap_ts`不参与。目标数据库和期表均未写入。

四表key missing/extra均为0；周origin和qfq各2,511,637行，月origin和qfq各598,960行。除pct_chg的舍入实现外，其余字段严格误差计数均为0。DuckDB round严格比较仍有pct_chg差异94/59/39/29（周origin/qfq、月origin/qfq）；将相同close/pre_close输入按生产pandas/NumPy `round(...,4)` ties-to-even重新计算全部共同键后，四表均0差异、NULL parity 0，确认这些是半舍入规则差异。最近周/月为截至最新日线的未结束周期，按现有已存日线行对比。运行回执为 `week_month_formal_tool_replay.json`，临时投影与首轮完整读取证据为 `period_compare.duckdb` 和 `week_month_full_compare.json`；工具的跨年ISO周、因子变化、价格调整、量反向调整及前期close fixture为3项通过。

## DuckDB与剩余语义限制

独立只读审计覆盖7个DuckDB文件、63个表、103,812,018行：重复主键、主键NULL、非法日期、非法JSON和闭市日记录计数均为0；共有551,410个NULL。原汇总862条孤儿候选全部来自 `fact_limit_event(ts_code)->dim_stock(ts_code)`：active中404行/340代码，frozen中458行/368代码，均只含炸板事件；炸板代码不要求有 `dim_stock` 项。审计确认真正缺少stock身份的涨停行=0、其他未知tag=0，因此真实missing/unknown关系为0；其余已声明外键关系孤儿均为0，`dim_theme.parent_theme_id`的691个NULL是可空根节点。Derived active 10,420个代码日组、frozen 13,478组的limitup/blown计数对账均为0差异；热度分数状态机未回放。具体关系计数见 `/private/tmp/trading_data_audit_20261015/duckdb_tables_audit_v2.json`。15分钟等衍生日线聚合的一致性样本与表范围另见DuckDB审计receipt。

尚未完成的范围是来源或业务语义核验，而非上述32张SQLite表的内部profile：非分钟在线snapshot的采集频率/闭市语义；auction当日final覆盖和历史来源缺口；133个`tdx_empty`代码日核验；1m额外4,406个code-day的状态/来源解释；部分证券上市/更名/停牌区间与历史标记；以及外部行情真值的全范围比对。没有把这些项目并入已完成的内部结构结果。

## 可重复工具和回执

- `tools/data_checks/audit_parquet_partitions.py` 与 `audit_parquet_numeric_supplement.py`：全分区结构/数值审计。
- `tools/data_checks/correct_tdx_zero_amount_sentinel.py`：仅精确哨兵替换；新备份路径先比对源读前/读后SHA和已有备份SHA，冲突即停止。
- `tools/data_checks/audit_core_daily_tables.py`：核心日线/日指标/复权因子只读逐日覆盖及NULL/域/日历profile。
- `tools/data_checks/audit_core_daily_key_coverage_duckdb.py`：以有界DuckDB临时键库分块续跑跨表key anti-join，不重复扫描/计算数值profile。
- `tools/data_checks/audit_one_min_code_day_coverage.py`：校验canonical主分区与manifest路径/size，并以仅code/date投影对照日线代码日。
- `tools/data_checks/audit_week_month_period_tables.py`：只读比较四张周/月origin/qfq表的全键与全OHLCV派生结果。
- 详细运行回执、分区来源清单和修复证据位于 `/private/tmp/trading_data_audit_20261015/` 与canonical `offline/oneMinute/quality_audit/`；其中临时状态文件只在扫描完整后才适合作为最终记录。

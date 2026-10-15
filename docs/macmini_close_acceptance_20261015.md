# Mac mini 数据服务收盘验收 — 2026-10-15

## 结论

今天73只午后观察股的原生15:00分钟线已经自动进入SQLite，时间、价格和成交量与独立Redis 6380逐键相符。当前保留的8类非空事件流已经消费至尾部；采集与落盘子进程已退出，只保留在线、离线调度器父进程。Mac mini为唯一数据服务主机，本机不恢复采集。

数据库包含已标注的历史来源限制和缺口，因此本次结论是“完成迁移与今天收盘落库验收，保留明确质量限制”，不是所有历史数据的外部真值认证。

## 实际证据

| 检查 | 实际结果 |
|---|---|
| 正式路径 | `/Users/nickzhang/TradingDataManager`；`/Users/nickzhang/TradingDatabase/offline`、`online` |
| Redis | 新数据服务独立使用6380；原共享6379未停止或修改 |
| 最后分钟健康轮 | 15:05:49，watchlist=73、written=73、empty=0、failed=0 |
| 自动收盘落库 | 73根15:00 bar，`time_idx=239`；SQLite `created_at` 为15:16:13.961—15:16:14.052，无人工补落 |
| 今日分钟表 | 22,921行、119个曾观察代码；午后收盘验收分母为当时73只，不是119只或全市场 |
| 当前流尾部 | snapshot、zt、break、anomaly、hot、watchlist、limitperformance、snapshot_index的SQLite cursor均与6380当前tail一致 |
| 竞价 | 新6380当前无auction stream，不能与tail比较；保留旧游标和早段1,000条覆盖缺口，不通过重置游标制造通过 |
| 受管退出 | writer在15:06窗口结束，saver在15:16后结束；最终修复版本受管重启后为单一在线parent，无watchlist/writer/saver孤儿 |
| 本机自启 | 原始`com.tradingagent.scheduler.offlineData`、`com.tradingagent.scheduler.onlineData`及`com.tradingagent.redis`均unloaded；UID503的`print-disabled`明确三项均disabled |
| 数据接口 | 本机SSH Arrow真实查询Mini正式日线、1分钟、15分钟及legacy tick；`database_only`，质量元信息保留 |

## 本次发现及处理

1. 在线分钟源改用TDX原生bar，日期、常规时段和已完成bar校验前置。旧decoder的错日和非法值从受保护原件回溯，不通过改日期制造行情。9月22日新增全市场分钟分区5,554只、1,332,960根，每只240根；已知跨源量额差异保留。
2. 107个Parquet分区中4,580,454个精确TDX零解码哨兵改为0；非amount列、schema及行数保持，原件可回退。普通正amount/零volume的24,918根保留源值和质量标记。
3. 最终flush由单批消费改为分页drain，并补watchlist。第一轮集成虽有77项测试通过，根agent真实字段review仍发现原始epoch覆盖已规范化时间的回归；修复后增加真实extractor/SQLite测试，81项在线测试通过。测试通过和真实落库分别验收。
4. 本次flush引入的2,313条zt秒时间、1,728条anomaly毫秒时间已完整备份后修正。anomaly中1,395条规范化；331条与既有规范键内容完全相同的副本归档去重；2条tag_name不同的raw侧进入冲突隔离，原规范ISO侧保留。两侧可回溯，2组冲突仍未解决，不算作同值。
5. 历史zt曾将字符串`False`按Python truth值写成1。已修复编码解析；仅对当前6380源与代码、事件时间精确匹配的5,000条恢复两flag为0。其余340,179条历史记录的`is_st/is_new`保留原存值并标不可信，不能以这些1认定真实ST或次新，不能用当前名单回填历史。
6. 9月在线库按实际业务唯一键合并，恢复2,157,745条本机独有记录，7,588条同键不同payload保留两侧证据并标未解决。10月1日3,002条已确证旧Redis错日指数副本经单独授权隔离；其余3,840条为闭市响应快照，不当作新成交。
7. 查询入口按真实表结构修复：minute采用实际`data_timestamp`过滤、`datetime/time_idx`排序，snapshot_index使用`snapshot_timestamp`，落盘时间用`created_at`；DataFrame与list查询复用底层合同。zt查询保留返回类型并提供明确的历史flag质量提示。查询修复后的完整在线测试为86项通过，不以fixture代替生产库只读调用。
8. Mini正式融资融券明细中1条接口错误提示占位行已保存完整原行与schema后隔离，事务内重核全列，变化数严格为1，修后目标数0、`quick_check=ok`。下载端新增该API专用响应校验，合法NULL指标保留；坏页拒写且不推进月水位，25项定向测试通过。未借此删除其它来源未定性的闭市记录。

事件行修复未修改任何分钟bar或STREAM cursor。修后zt/anomaly的raw epoch时间均为0，`quick_check=ok`。完整原行、schema、哈希、冲突和事务结果保存在Mini私有治理目录的 `event_time_flag_repair_20261015T162020+0800/`。

融资融券单行回退档案位于同一治理根目录的 `margin_placeholder_repair_2026-10-15T162935+0800/`，完整行/schema归档SHA-256为`8141c56e5eb85636ff78ae061aa0465ca5ae494b7d5ad332d3a96568325e4d40`。本机静态迁移镜像保留旧审计基线，不当作持续更新的正式库。

## 离线审计与边界

- 离线迁移冻结基线871文件、37,359,424,248字节逐文件SHA一致；恢复更新后的Mini可合法变化，不能继续宣称两机当前字节相同。
- canonical 1分钟429日、562,152,720根已完成分区及代码日覆盖；4,406个分钟有/日线无的代码日未定性。26个300114历史代码日存在302132分钟别名覆盖证据，未改物理代码，也未宣称所有字段一致。
- 三张核心日线表35,883,880行完成内部数值与跨表键检查。四张周/月原始及前复权表6,221,194行按生产NumPy舍入规则重算，全键和各字段无差异。
- 其余32张SQLite表有完整v2逐表审计；快讯时间另做v3单列补扫，308,967条为合法分钟精度时间，不补造秒数。7个DuckDB、63表、103,812,018行有结构、域和已定义关系检查。Mini独有legacy tick/minute另有全表内部审计，源完整性与序号含义保留限制。
- 闭市日limit_list有1,063条来源日期矛盾。现有Tushare接入源单查10月1日仍返回73条同日记录，本地未覆写源日期；保留原值与证据，不能当作该日新交易事件。历史代码、停牌、个别上市首日字段及源量额差异仍需按研究需要核验。

## 尚未验证的运行范围

今天13:00—13:07首次成功采集前有真实缺口；后补历史bar不等于当时已实时采到。当前流尾与cursor一致不证明此前MAXLEN裁剪从未丢失未消费消息。

按用户最后指示，旧6379竞价原始记录没有导出或恢复，未改TTL。下一交易日竞价final阶段和18:00首次完整离线更新尚未实际发生；14:35的新闻增量更新已成功，但不能代替这两个运行验收。

详细方法和每表分母见TradingDataManager的 `docs/data_integrity_audit_20261015.md`；停写、发布、回退和收盘时间线见 `docs/macmini_data_services_plan_20261015.md`。正式回执保存在Mini `~/.local/share/TradingDataManager/data_governance/20261015/` 及 `.runtime/deploy_receipts/`，数据库和私有回执不上传GitHub。

# TradingDataManager发布、数据修复与Mac mini服务迁移

日期：2026-10-15，北京时间。负责人：主agent；执行：3个GPT-6 Luna子agent。

## 最终目标

- 数据工具发布到用户指定仓库 `git@github.com:LiYiqiang0827/TradeDataManager.git`；本机目录继续叫 `TradingDataManager`。
- TradingAgent继续使用原仓库，保留MrWu、MrLi及原有研究改动。
- 修复可复核的数据缺口与错误；建立每库每表的完整性、语义正确性清单，重点检查online。
- Mac mini使用最新代码和完整数据库，成为唯一数据采集、维护与数据库服务主机。本机保留研究用途，停止数据库服务及其自动重启。

## 分工与写入约束

| 执行者 | 职责 | 边界 |
|---|---|---|
| repository agent（research_inventory） | 两仓发布、Git唯一提交者、兼容接口、部署/skill文档 | 不操作生产数据库或服务；不force push |
| repair agent（data_inventory） | 所有库/表审计、原因确认、可重复检查与修复代码、数据修复账本 | 生产写入须在根agent确认的停写窗口；其他时间只读或隔离测试 |
| deployment agent（validation_design） | SSH预检、Mac mini环境、分批续传、部署、独立online验收 | 不覆盖远端独有数据，不reset远端改动，停启顺序由根agent确认 |
| 主agent | 范围与来源判断、冲突协调、切换决策、监控与最终验收 | 不把指纹一致或测试通过当作市场数据全部正确 |

代码修改按文件归属协调；Git index只有repository agent写。不同agent不得同时操作同一个生产库、Redis或scheduler。

## 阶段与接受条件

1. **发布与盘点。** 核对远端refs、两端dirty文件、SSH用户、空间、服务归属、数据库和日历范围；排除数据库、Parquet、JPG、私有配置及凭据。先正常上传TDM，TradingAgent提交本次可克隆迁移结构，后续修复再追加提交。
2. **逐表审计与隔离复现。** 每个SQLite/DuckDB实际表均有检查结果及分母；检查键、NULL、数值域、时间/交易日/分钟标签、状态字段、派生与图引用。所有Parquet分区读取与缺口统计，未全行验证必须说明。重点复核online所有日期，而非仅最新两天。
3. **修复并保存原始证据。** 优先20260922一分钟缺分区、online错日行、非法值及状态；竞价turnover先追踪原接口契约。不能通过改日期或填0制造行情；来源不可恢复时隔离错误记录并明确未恢复缺口。每次修复记录原行/文件、范围、来源、事务、前后结果和回退方式。
4. **数据传输与服务切换。** Mac mini原库与独有近期数据先保护；初始传输只入staging，不视为有效快照。最终停写、checkpoint/flush、增量传输和指纹对照后才启用。当前为交易日盘中，优先午休或收盘窗口；保留单一正确采集链路，禁止旧/新实例双写同库。
5. **验收与关闭本机。** Mac mini从两个仓库的发布commit运行，数据库路径唯一；offline查询、online真实时间/状态/Redis与SQLite对应、进程所有权、停止/重启恢复、增量更新均可复核。确认远端主服务健康后卸载并禁用本机三个LaunchAgent及确认的子worker，防止重启后恢复本机采集。研究端只读使用和按需数据获取方式另写说明。

## 证据和风险分类

检查结果分为：通过、修复后通过、可靠来源无法恢复、确实缺口、未验证。每张表列出行数、日期范围、异常数量、检查方法、适用契约和修复来源；区分原始数据、模型派生、弃用库及冻结研究快照。停牌、源端非更新日、预留空表、实时未完成交易日不能直接判缺失。

Mac mini可能拥有本机没有的近期在线记录，不能用本机副本直接覆盖。数据同步清单、源目标校验、受保护备份、服务停启记录保存在运行目录或数据库维护档案；代码库只存脚本和不含秘密的报告。未进行真实下一交易日采集时，不宣称已验证下一交易日全链路。

## 当前进度

- 2026-10-15 10:53：已分派三个Luna执行者；本机online仍有活动worker，尚未停写或修写生产数据。开始代码发布、逐表审计和Mac mini预检。
- 迁移前已知问题：20260922一分钟分区缺失；10/13、10/14在线minute有错日行；auction turnover空值的来源语义待确认。
- 初次发布：TDM的origin已按用户指定地址配置并正常上传，初始发布commit为`839ca13`。
- 扩大审计发现：23张online分钟表共662428行，其中17张表有403680行错日；23张表均有负价格/负量，当前10/15也出现。此为运行中初查，修复前须冻结重计。
- 只读同连接源复现：旧`get_minute_time_data`的整组价格/成交量解码不可信，不只是个别负值；TDX原生1m bars末价与quote一致，按股统计的原生volume合计与quote手数×100对应，amount同为元。决定替换实时及历史minute入口，先核全日与现有Parquet一致、未完成bar及分钟端点；不能通过过滤负值就宣称修好。
- Mac mini数据初传进入独立staging并标记未冻结/不可用于服务。远端dirty文件51项共6.6GB已另存并逐文件校验；其较大的tick历史必须保留，不能被本机28KB的空/小库覆盖。
- Mac mini原Homebrew Redis当前连接来自TradingAgent，但不能证明其他应用未来不会共用；新TDM使用独立端口/目录，旧实例及文件保留，不flushall。
- 初传完成：10:59:55–11:35:37，35分42秒，发送约39.08GB；rsync exit24因活跃Redis rewrite时旧AOF消失，SQLite/Parquet也未经冻结。暂存数据不得作为正式有效快照或直接恢复Redis。
- 11:35以后自动审批曾拒绝停止Mac mini现役采集服务；用户随后明确授权按计划范围停写、切换和恢复。Deployment 已继续受控freeze；本计划更新时仍等待其报告writer清零、checkpoint与source fingerprint READY，之后才能执行正式数据切换或生产修复事务。此前本机scheduler标签已卸载但曾有孤儿worker；Mac mini原scheduler及worker的清理状态以deployment最终回执为准。隔离源核对、定向测试和临时staging可继续。
- 源解码补证：真实包的volume/amount uint32为0时，pytdx误解为精确`2^-127`，批准对这一已证明哨兵纠为0；全Parquet发现amount哨兵4580454行，另24667行是volume=0而普通正amount。后一类可能涉及源端量化/零股精度，不能擅自改字段或删除整根OHLC；需保留可信价格、原始字段和可查询的质量异常记录。
- 原生9/22三只股票样本证实SH/SZ/BJ都有返回；强拒绝zero-volume/nonzero-amount会丢BJ一根bar，样本719/720暂不完整，不得promotion。待质量标记方案完成后重验。

完成记录和最终分母以执行回执更新，不能仅凭本计划判定完成。

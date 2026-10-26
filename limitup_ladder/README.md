# 离线涨停天梯节点库

代码归 TradingDataManager；数据库默认位于 `~/TradingDatabase/offline/limitup_ladder.db`。
`TRADING_DATABASE_ROOT` 或 `TRADING_LADDER_DATABASE` 可覆盖数据路径。仅依赖 Python 标准库；
更新服务沿用项目现有 loguru 依赖，不调用行情下载、Neo4j 或模型接口。

## 数据与题材时点

- `nodes` 保存每日真实涨停、未晋级和首板破板节点，`original_theme` 永远保留节点当日 KPL 分类。
- `edges` 保存实际晋级、几天几板的断板续连、停牌复牌及十个交易日内再次涨停的连接。
- `followups` 保存节点下一个非整日停牌交易日及行情，读取历史截止日时才决定是否可显示失败/停牌结果。
- `theme_history` 以连板链为单位保存题材的生效日期、最高板节点和题材；查询只使用 `effective_date <= end_date` 的版本。
  后续重新首板的续接关系不合并两条连板链。节点读取结果同时含 `originalTheme`、截止日适用的 `theme`、`themePeakDate/Board`。
- `trade_dates` 是小型数据日期目录；`daily_stats` 保存每日数量。
- 不把未来晋级结果或未来题材写成历史节点的唯一属性。不同结束日期可同时查询，互不污染。

## 构建与更新

```sh
cd ~/TradingDataManager
.venv/bin/python -m limitup_ladder.build
# 明确的历史修订：从修订日期开始检查，并重算受影响的后续截面
.venv/bin/python -m limitup_ladder.build --start-date 20261015
# 全历史原始日线/涨跌停价/停牌事实审计
.venv/bin/python -m limitup_ladder.build --audit
```

首次回填所有可用日期。之后，全部历史 KPL 涨停事实参与内容指纹检查，原始日线、涨跌停价、
停牌和日历默认检查最近七个交易日；显式历史回填传入 start-date，全历史修订审计使用 --audit。
与涨停路径无关的资金流或估值表更新不会改变生成版本。

更新钩子位于 `offlineDataManager/scripts/service/ladder_update_hook.py`，在以下服务成功提交后触发：
`service_kpl_limit_performance`、`service_kpl_list`、`service_limit_list`、`service_daily`、
`service_stk_limit`、`service_suspend`。完整更新和早间任务结束也兜底调用 `service_limitup_ladder`。
所有自动调用与手工调用共用文件锁。被另一构建占用时排队，取得锁后重新检查最新源事实；无变化不发布新版本。

每个日期先验证 KPL 股票池、昨日涨停候选的当日日线/涨跌停价和整日停牌覆盖。
未齐的新日期标为待构建，原有已发布日期若覆盖退化则拒绝替换。构建与题材版本更新在同一事务中提交，
WAL 允许网页继续读取上一版本。源事实在构建期间变动时回滚，下一次上游完成钩子重试。
历史修订会重算修订日期之后的所有日期，处理跨较长停牌及连板链的依赖。

## 读取

```python
from coreClient.limitup_ladder import LimitupLadderStore
store = LimitupLadderStore()
print(store.metadata())
window = store.read('20261023', sessions=20)
print(window['sealedStocks'][0]['originalTheme'])
print(window['sealedStocks'][0]['theme'])
```

只读模式不会在缺文件时创建空数据库。TradingAlert 优先读取此库，Redis 和浏览器按已发布版本缓存。
原始行情更新与派生发布之间继续保留上一版本，未收盘和闭市后尚未离线发布的当天通过 online 叠加。
当天只动态计算一日节点，与离线历史合并；数据库不可用时沿用旧原始行情算法兜底。

旧算法来自实际运行 dashboard 的 `limitup_ladder_data`，抽取到 `calculator.py` 后保持板数、
换题材、断板和停牌规则。网页读取不执行此历史计算器；它只用于离线更新及实时当天。

## 运维

- 备份使用 SQLite backup API，不能只复制正在写入的主文件而遗漏 WAL。
- 数据问题可用 `PRAGMA quick_check` 和 metadata 中 blocked_days 检查；原始数据库不被此程序修改。
- 修改路径判断规则后需提升 `ALGORITHM_VERSION` 并重建；接口仍保留原字段以兼容现有页面。

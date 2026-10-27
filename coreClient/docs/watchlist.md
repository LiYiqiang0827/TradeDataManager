# 自选分组存储与缓存

`coreClient.watchlist.WatchlistStore` 独立于行情原始表，默认持久库为
`TradingDatabase/offline/watchlists.db`。首次启动创建 SQLite WAL 表结构，不联网、不写行情表。

```python
from coreClient.watchlist import WatchlistStore
import redis

cache = redis.Redis(host="127.0.0.1", port=6379, db=2, decode_responses=True)
store = WatchlistStore(redis_client=cache)
store.refresh()  # 服务启动时把 SQLite 快照预载至 Redis
snapshot = store.snapshot()  # 正常读取直接命中 Redis
```

- `create_group(name, stock=None)`：按规范化名称幂等创建；可传 `stock={"code": ..., "name": ...}`，同一事务中加入当前股票，并保留其他分组。
- `set_groups(code, name, group_ids, expected_version)`：以完整分组集合更新股票；取消全部传 `[]`。版本由 `snapshot["stocks"][code]["version"]` 获取，未保存股票为 0。
- 多窗口/多设备同时编辑同股时，过期版本抛出 `WatchlistConflict`，调用方重新读取并请用户重选；不会静默覆盖。
- SQLite 提交后发布 Redis 快照，Lua 比较 generation/revision，迟到的读取不能覆盖更新的快照。
- Redis 不可用时 SQLite 仍保存修改，当前 store 标记缓存需重建，恢复后重新发布。
- Redis 键 `tradingalert:watchlists:v1:<路径哈希>`，保留七天；验证库与正式库不会共用同一键。
- 表为 `watchlist_group`、`watchlist_stock`、`watchlist_member`、`watchlist_meta`，股票与分组为多对多关系，分组数量不影响历史交易日期。

网页采用自己的 `TRADING_WATCHLIST_DB` 可选环境变量传入替代路径，默认仍为正式 offline 文件。
生产数据库和凭据均不应提交 Git。新机器需要更新本模块及 TradingAlertSystem 的 watchlists.js / server.py。

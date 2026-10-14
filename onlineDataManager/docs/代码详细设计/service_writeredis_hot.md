# service/service_writeredis_hot.py 详细设计

> **热股榜写入 service** — **v6.15:120s/轮**

> **v6.15 更新(2026-09-16)**:频率 300s → **120s**(用户最新统一规定,普通 kind hot/anomaly 频率)
> **数据源**:`同花顺 fetch_hot_stock(period='hour')`
> **周期**:`300s/轮`
> **批量**:`全市场`
> **v6 特殊性**:`v6 MyATM 风格,archive EXPIRE 1200s`

---

## §1 职责

- 拉取 `同花顺 fetch_hot_stock(period='hour')` 数据
- watchlist = 在线 `online:watchlist` SET(由 `service_writeredis_watchlist` 维护)
- 写 Redis `online:hot:*` 一整套(详见 §4)
- **只写 Redis,不落盘**(落盘由 `service_savedata_hot` 独立 daemon)

## §1.5 设计意图(2026-09-15 第 18 轮体检补)


### §1.5. 设计意图(2026-09-15 第 18 轮体检补)

#### §1.5..1 业务背景
- **hot** 是 热股榜(同花顺小时榜)——1 小时内涨幅/资金流入最活跃的股票集合
- 不复用其它 kind 的业务动机:热股是动态排序(按 rank),不复用涨停/异动(事件级)

#### §1.5..2 存储结构
- **STREAM `online:hot:stream`**:每次榜单 tick 进 STREAM
- **timeline ZSET `online:hot:timeline`**(43200s):MyATM 索引
- **archive HSET `online:hot:archive:{unix_ts}`**(1200s/20min):全市场榜单聚合,跟 anomaly 一致

#### §1.5..3 TTL / 周期 推导
- **archive TTL = 1200s(20min)**:热股榜短时变化快,20min 内可观察排名变化趋势
- **timeline TTL = 43200s(12h)**:标准
- **STREAM TTL = 21600s(6h)**:标准
- **writeredis 周期 300s(5min)**:比 zt/break 长 5 倍,反映「榜单更新本身慢」的业务特性

#### §1.5..4 v5 / v6 MyATM 改造(2026-09-14)
- 改造前:用 pool SET
- 改造后(v6):删 pool SET,改 MyATM 风格
- 改造动机:同 zt/break(SET 不支持按 field 查询)


---

## §2 生命周期

scheduler 在 09:25 auction 阶段末尾 spawn:
- 一轮:拉数据 + 写 Redis
- 之后 sleep `interval` 秒等下一轮
- 15:35 切到 closed 时 SIGTERM 杀掉

## §3 调用链

```
service_writeredis_hot
  └─ OnlineRedis()
  └─ r.get_watchlist()                  # 读 online:watchlist
  └─ <数据源>.fetch_xxx(codes=...)
  └─ r.put_hot_rank(hot_list, data_timestamp=now_unix)   # **批量 pipeline,不是 put_hot**(方法名是 put_hot_rank)
  └─ (MyATM 风格无 commit,EXPIRE 在 put_hot_rank 内部处理)
  └─ sleep 300s
```

## §4 Redis 数据结构

| key | 类型 | EXPIRE | 备注 |
|---|---|---:|---|
| `online:hot:archive:{unix_ts}` | HSET | 1200s |  |
| `online:hot:timeline` | ZSET | 43200s(12h) = `HOT_TIMELINE_TTL` |  |
| `online:hot:stream` | STREAM | **21600s(6h)**(= `STREAM_TTL` = `DEFAULT_STREAM_TTL` = `3600*6`)|  |

> ⚠️ doc 早期写 STREAM EXPIRE "1d" 错,v3 改 6h。
> ⚠️ 真值方法名 `put_hot_rank`,doc 早期写 `put_hot` 错。

## §5 启动方式

```bash
python3 -m service.service_writeredis_hot --interval <N>
python3 -m service.service_writeredis_hot --once      # 单次(测试用)
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_hot** | 读 STREAM `online:hot:stream` 落盘 |

## §7 历史变更

- **2026-09-11 新建** 
- **2026-09-14 v6** 改 MyATM 风格,删除 rank LIST

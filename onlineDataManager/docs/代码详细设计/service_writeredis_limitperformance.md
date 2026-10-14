# service/service_writeredis_limitperformance.py 详细设计

> **涨停表现详情写入 service**
> **数据源**:`KPLClient.fetch_realtime_limit_performance()(apphwhq 实时盯盘 host)`
> **周期**:`30s/轮`
> **批量**:`全市场`
> **v6 特殊性**:`v5 新建 + v6 改 MyATM 风格`

---

## §1 职责

- 拉取 `KPLClient.fetch_realtime_limit_performance()(apphwhq 实时盯盘 host)` 数据
- watchlist = 在线 `online:watchlist` SET(由 `service_writeredis_watchlist` 维护)
- 写 Redis `online:limitperformance:*` 一整套(详见 §4)
- **只写 Redis,不落盘**(落盘由 `service_savedata_limitperformance` 独立 daemon)

## §1.5 设计意图(2026-09-15 第 18 轮体检补)


### §1.5. 设计意图(2026-09-15 第 18 轮体检补)

#### §1.5..1 业务背景
- **limitperformance** 是 涨停表现详情——涨停后多久开板/回封/封单变化,2026-09-14 v5 新增(同花顺无此接口,只有 KPL apphwhq)
- 不复用其它 kind 的业务动机:涨停表现是涨停后的连续 tick,不复用 zt(zt 是涨停瞬时)

#### §1.5..2 存储结构
- **STREAM `online:limitperformance:stream`**:每条涨停表现 tick 进 STREAM
- **timeline ZSET `online:limitperformance:timeline`**(43200s):MyATM 索引
- **archive HSET `online:limitperformance:archive:{unix_ts}`**(300s/5min):全市场涨停表现聚合

#### §1.5..3 TTL / 周期 推导
- **archive TTL = 300s(5min)**:涨停表现 30s/轮,数据量大,5min archive 够下游看趋势
- **timeline TTL = 43200s(12h)**:标准
- **STREAM TTL = 21600s(6h)**:标准
- **writeredis 周期 30s**:与 zt 一致(涨停表现跟涨停事件强相关)
- **STREAM MAXLEN=5000**:与其它低频 kind 一致

#### §1.5..4 v5 / v6 MyATM 改造(2026-09-14)
- **v5 新增**(2026-09-14):用 KPL apphwhq 接口(同花顺无)
- **v6 改 MyATM 风格**:同 zt/break/hot/anomy,删 pool SET 改 timeline + archive


---

## §2 生命周期

scheduler 在 09:25 auction 阶段末尾 spawn:
- 一轮:拉数据 + 写 Redis
- 之后 sleep `interval` 秒等下一轮
- 15:35 切到 closed 时 SIGTERM 杀掉

## §3 调用链

```
service_writeredis_limitperformance
  └─ OnlineRedis()
  └─ r.get_watchlist()                  # 读 online:watchlist
  └─ <数据源>.fetch_xxx(codes=...)
  └─ r.put_limitperformance(lp_list, *, data_timestamp=None)   # **批量 pipeline + kwarg-only**(data_timestamp 是 keyword-only,必传或 None)
  └─ (MyATM 风格无 commit,EXPIRE 在 put_limitperformance 内部处理)
  └─ sleep 30s
```

## §4 Redis 数据结构

| key | 类型 | EXPIRE | 备注 |
|---|---|---:|---|
| `online:limitperformance:archive:{unix_ts}` | HSET | 300s |  |
| `online:limitperformance:timeline` | ZSET | 43200s(12h) = `LIMITPERFORMANCE_TIMELINE_TTL` |  |
| `online:limitperformance:stream` | STREAM | **21600s(6h)**(= `STREAM_TTL` = `DEFAULT_STREAM_TTL` = `3600*6`)|  |

> ⚠️ doc 早期写 STREAM EXPIRE "1d" 错,v3 改 6h。
> ⚠️ 真值签名 `put_limitperformance(lp_list, *, data_timestamp=None)`(keyword-only),doc 早期写 `(ts_code, data)` 错。

## §5 启动方式

```bash
python3 -m service.service_writeredis_limitperformance --interval <N>
python3 -m service.service_writeredis_limitperformance --once      # 单次(测试用)
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_limitperformance** | 读 STREAM `online:limitperformance:stream` 落盘 |

## §7 历史变更

- **2026-09-14 v5 新建** 新增 service
- **2026-09-14 v6** 改 MyATM 风格

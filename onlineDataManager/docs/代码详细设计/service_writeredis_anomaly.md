# service/service_writeredis_anomaly.py 详细设计

> **异动清单写入 service**
> **数据源**:`同花顺 fetch_anomaly_list()`
> **周期**:`120s/轮`
> **批量**:`全市场`
> **v6 特殊性**:`v6 MyATM 风格,archive EXPIRE 1200s(用户拍板 20 分钟)`

---

## §1 职责

- 拉取 `同花顺 fetch_anomaly_list()` 数据
- watchlist = 在线 `online:watchlist` SET(由 `service_writeredis_watchlist` 维护)
- 写 Redis `online:anomaly:*` 一整套(详见 §4)
- **只写 Redis,不落盘**(落盘由 `service_savedata_anomaly` 独立 daemon)

## §1.5 设计意图(2026-09-15 第 18 轮体检补)


### §1.5. 设计意图(2026-09-15 第 18 轮体检补)

#### §1.5..1 业务背景
- **anomaly** 是 异动清单(同花顺 keyword_list)——盘中异动事件(快速拉升/跳水/封板)实时监控
- 不复用其它 kind 的业务动机:异动是事件级,不复用 snapshot(连续 tick)

#### §1.5..2 存储结构
- **STREAM `online:anomaly:stream`**:每条异动事件进 STREAM,savedata 靠 XREAD > cursor 读
- **timeline ZSET `online:anomaly:timeline`**(43200s):MyATM 索引
- **archive HSET `online:anomaly:archive:{unix_ts}`**(1200s/20min):全市场聚合,比 zt 长 4 倍因为异动事件可跨多分钟观察
- **anomaly_keywords 长表**(`anomaly_keywords_YYYYMMDD`):同花顺 keyword_list 拆 keyword_idx 到独立长表,避免宽表臃肿

#### §1.5..3 TTL / 周期 推导
- **archive TTL = 1200s(20min)**:异动事件可跨多分钟延续(快速拉升 1-2min + 回稳 10min),20min 够下游观察完整窗口
- **timeline TTL = 43200s(12h)**:业务日内 12h 全覆盖
- **STREAM TTL = 21600s(6h)**:标准 `STREAM_TTL`
- **writeredis 周期 120s**:盘中持续新异动;落盘周期 1800s(30min)节奏不对称,防丢+防雪崩

#### §1.5..4 v5 / v6 MyATM 改造(2026-09-14)
- 改造前:v1/v2 用 LIST 存全市场异动 list
- 改造后(v6):删 pool SET,改 MyATM 风格(STREAM + timeline + archive 三件套)
- 改造动机:SET 不支持按 field 查询 + 历史快照需要 timeline 索引
- **双表设计**:同花顺 `keyword_list` 拆 keyword_idx 到 `anomaly_keywords_YYYYMMDD` 长表,主表 8 列(不含 keywords JSON),避免宽表写入性能


---

## §2 生命周期

scheduler 在 09:25 auction 阶段末尾 spawn:
- 一轮:拉数据 + 写 Redis
- 之后 sleep `interval` 秒等下一轮
- 15:35 切到 closed 时 SIGTERM 杀掉

## §3 调用链

```
service_writeredis_anomaly
  └─ OnlineRedis()
  └─ r.get_watchlist()                  # 读 online:watchlist
  └─ <数据源>.fetch_xxx(codes=...)
  └─ r.put_anomaly(anomaly_list, data_timestamp=now_unix)   # **批量 pipeline(参数名虽然叫 anomaly 但语义是 list[dict],不是单条 ts_code)**
  └─ (MyATM 风格无 commit,EXPIRE 在 put_anomaly 内部处理)
  └─ sleep 120s
```

## §4 Redis 数据结构

| key | 类型 | EXPIRE | 备注 |
|---|---|---:|---|
| `online:anomaly:archive:{unix_ts}` | HSET | 1200s |  |
| `online:anomaly:timeline` | ZSET | 43200s(12h) = `ANOMALY_TIMELINE_TTL` |  |
| `online:anomaly:stream` | STREAM | **21600s(6h)**(= `STREAM_TTL` = `DEFAULT_STREAM_TTL` = `3600*6`)|  |

> ⚠️ doc 早期写 STREAM EXPIRE "1d" 错,v3 改 6h。
> ⚠️ 真值方法名 `put_anomaly(anomaly_list, data_timestamp=...)`,doc 早期写 `put_anomaly(ts_code, data)` 错(参数语义是 list)。

## §5 启动方式

```bash
python3 -m service.service_writeredis_anomaly --interval <N>
python3 -m service.service_writeredis_anomaly --once      # 单次(测试用)
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_anomaly** | 读 STREAM `online:anomaly:stream` 落盘 |

## §7 历史变更

- **2026-09-11 新建** 30s/轮
- **2026-09-14 v6** 改 120s/轮 + MyATM 风格,删除 list LIST

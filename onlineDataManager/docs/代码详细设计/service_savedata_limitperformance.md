# service/service_savedata_limitperformance.py 详细设计

> **limitperformance 涨停表现落盘 daemon**
> **运行模式**:`daemon`
> **周期**:`900s(15min)`

---

## §1 职责

- 读 Redis `online:limitperformance:stream (STREAM)`
- 调 `core.persist_client.persist_kind(r, kind="limitperformance", trade_date=trade_date)`
- 落盘 SQLite 表 `limitperformance_YYYYMMDD`(注意无 `_` 分隔,详见 03 §2.10 备注)
- **DEFAULT_INTERVAL** = `900s`(代码真值,2026-09-14 体检校正)
- **只落盘,不写 Redis**(写由对应 writeredis service 负责)


## §2 设计意图(2026-09-15 第 18 轮体检补)

### §2.1 业务背景
- limitperformance 是 涨停表现详情(KPL apphwhq 接口)——涨停后多久开板/回封/封单变化,2026-09-14 v5 新增,因为同花顺无此接口
- 不复用其它 kind 的业务动机:涨停表现是涨停后的连续 tick,不复用 zt(zt 是涨停瞬时)

### §2.2 存储结构
- **STREAM `online:limitperformance:stream`**:每条 tick 进 STREAM,savedata 靠 XREAD > last_id cursor 增量读
- **timeline ZSET**(`43200s (12h)`):MyATM v6 改造后加的快照索引,支持「某时刻全市场 limitperformance 快照」查询
- **archive HSET**(`300s (5min)`):全市场聚合 `limitperformance:archive:{unix_ts}`,savedata 兜底时 N→1 一次取齐
- **window ZSET**(`(limitperformance 无单股 window)`):每只股近 N 个 tick 滑窗,下游实时渲染

### §2.3 TTL 推导
- **archive TTL = 300s (5min)**:涨停表现 30s/轮,数据量大,5min archive 够下游看趋势
- **timeline TTL = 43200s(12h)**:业务日内 12h 全覆盖 + 跨日兜底
- **STREAM TTL = 21600s(6h)** = `STREAM_TTL`:标准 `DEFAULT_STREAM_TTL`,不是特殊
- **window TTL = (limitperformance 无单股 window)**:涨停表现事件级而非 tick 级

### §2.4 落盘节奏
- **DEFAULT_INTERVAL = 900**(15min):涨停表现 30s/轮 × 480 轮/天 ≈ 23k 数据量大,15min 兜底节约 SQLite
- **daemon 模式**(非 `--once`):因为盘中持续有新 limitperformance 事件,需要持续消费
- **失败重试语义**:`savedata_loop.py:60-73` 包裹 try/except,**daemon 永不退出**;**STREAM cursor 持久化**到 SQLite `online_stream_cursor`,**重启不丢**

### §2.5 双时间戳原则
- **`limitperformance_timestamp`**(数据时间):limitperformance_timestamp 来源同花顺数据源头 wall-clock,**用户最高优先级约束**——复盘时区分「数据几点产生」与「系统几点记录」
- **`created_at`**(落盘时间):SQLite DEFAULT `datetime('now','localtime')` 自动加

### §2.6 特殊字段(lu_time 永不丢)
- 无涨停时间字段
## §2 生命周期

**scheduler morning_savedata / afternoon_savedata 阶段 spawn,15:35 closed 阶段 SIGTERM**

## §3 调用链

```
service_savedata_limitperformance
  └─ OnlineRedis()
  └─ (无独立 connect_db 子类层调用)
  └─ SavedataDaemon._do_persist()
     └─ persist_kind(r, kind="limitperformance", trade_date=trade_date)        # ← core.persist_client
     └─ INSERT INTO limitperformance_YYYYMMDD ...
  └─ sleep DEFAULT_INTERVAL                  # 仅 daemon 模式
```

## §4 表结构

```sql
-- limitperformance_YYYYMMDD
-- (详细 schema 见 03_数据详细设计.md §2.10)
```

## §5 启动方式

```bash
python3 -m service.service_savedata_limitperformance             # daemon
python3 -m service.service_savedata_limitperformance --once      # 单次
```

## §6 备注

**PK: UNIQUE(ts_code, limitperformance_timestamp)**

## §7 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_writeredis_limitperformance** | 上游,写 Redis |
| **scheduler_onlineData.py** | spawn 触发 |

## §8 历史变更

- **2026-09-14 v5 新建** 新增 service

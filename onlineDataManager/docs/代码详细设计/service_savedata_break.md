# service/service_savedata_break.py 详细设计

> **break 炸板池落盘 daemon**
> **运行模式**:`daemon`
> **周期**:`1800s(30min)`

---

## §1 职责

- 读 Redis `online:break:stream (STREAM)`
- 调 `core.persist_client.persist_kind(r, kind="break", trade_date=trade_date)`
- 落盘 SQLite 表 `break_YYYYMMDD`
- **DEFAULT_INTERVAL** = `1800s`(代码真值,2026-09-14 体检校正)
- **只落盘,不写 Redis**(写由对应 writeredis service 负责)


## §2 设计意图(2026-09-15 第 18 轮体检补)

### §2.1 业务背景
- break 是 炸板池——涨停板打开瞬间的股票集合,用于监控盘中开板风险
- 不复用其它 kind 的业务动机:炸板事件比涨停低频,单独 kind 便于 sent 报警 + 独立落盘

### §2.2 存储结构
- **STREAM `online:break:stream`**:每条 tick 进 STREAM,savedata 靠 XREAD > last_id cursor 增量读
- **timeline ZSET**(`43200s (12h)`):MyATM v6 改造后加的快照索引,支持「某时刻全市场 break 快照」查询
- **archive HSET**(`300s (5min)`):全市场聚合 `break:archive:{unix_ts}`,savedata 兜底时 N→1 一次取齐
- **window ZSET**(`(break 无单股 window,只有 timeline + archive + stream)`):每只股近 N 个 tick 滑窗,下游实时渲染

### §2.3 TTL 推导
- **archive TTL = 300s (5min)**:炸板是瞬时事件,5min 内必变化,archive 只给下游秒级预警
- **timeline TTL = 43200s(12h)**:业务日内 12h 全覆盖 + 跨日兜底
- **STREAM TTL = 21600s(6h)** = `STREAM_TTL`:标准 `DEFAULT_STREAM_TTL`,不是特殊
- **window TTL = (break 无单股 window,只有 timeline + archive + stream)**:无单股滑窗需求(炸板是事件级而非 tick 级)

### §2.4 落盘节奏
- **DEFAULT_INTERVAL = 1800**(30min):炸板事件低频,30min 兜底足够复盘
- **daemon 模式**(非 `--once`):因为盘中持续有新 break 事件,需要持续消费
- **失败重试语义**:`savedata_loop.py:60-73` 包裹 try/except,**daemon 永不退出**;**STREAM cursor 持久化**到 SQLite `online_stream_cursor`,**重启不丢**

### §2.5 双时间戳原则
- **`break_timestamp`**(数据时间):break_timestamp 来源同花顺数据源头 wall-clock,**用户最高优先级约束**——复盘时区分「数据几点产生」与「系统几点记录」
- **`created_at`**(落盘时间):SQLite DEFAULT `datetime('now','localtime')` 自动加

### §2.6 特殊字段(lu_time 永不丢)
- 炸板不涉及涨停时间 lu_time(zt 才用)
## §2 生命周期

**scheduler morning_savedata / afternoon_savedata 阶段 spawn,15:35 closed 阶段 SIGTERM**

## §3 调用链

```
service_savedata_break
  └─ OnlineRedis()
  └─ (无独立 connect_db 子类层调用)
  └─ SavedataDaemon._do_persist()
     └─ persist_kind(r, kind="break", trade_date=trade_date)        # ← core.persist_client
     └─ INSERT INTO break_YYYYMMDD ...
  └─ sleep DEFAULT_INTERVAL                  # 仅 daemon 模式
```

## §4 表结构

```sql
-- break_YYYYMMDD
-- (详细 schema 见 03_数据详细设计.md §<X>)
```

## §5 启动方式

```bash
python3 -m service.service_savedata_break             # daemon
python3 -m service.service_savedata_break --once      # 单次
```

## §6 备注

基类 SavedataDaemon 流程

## §7 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_writeredis_break** | 上游,写 Redis |
| **scheduler_onlineData.py** | spawn 触发 |

## §8 历史变更

- **2026-09-11 新建** 新增 service

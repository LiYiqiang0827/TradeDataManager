# service/service_cleanredis_online.py 详细设计

> **每日 Redis 清理 service**(**09:00 开盘前 + 03:00 凌晨清残留**,各跑一次)
> **清理范围**:只清 `online:` 前缀 + 清 SQLite 的 `online_stream_cursor` 表
> ✅ **生产真值**(**2026-09-16 用户拍板**):**09:00 开盘前 + 03:00 凌晨清残留**,各跑一次。**默认 3 点,不再回 16:00**(原 16:00 是 2026-09-14 调试前的老值)。

---

## §1 职责

- **每天 09:00(开盘前):清空所有 `online:*` key(STREAM / ZSET / HSET / LIST / SET)
- **每天 03:00(凌晨清残留):同上(2026-09-16 拍板为生产真值,不再回 16:00)**
- 同时清 SQLite 的 `online_stream_cursor` 表(STREAM ID 在 Redis flush 后失效,从 0 重读)

**不清理**:
- `online:*` 之外的其他 namespace(`kpl:` `ths:` 等其他系统的不动)
- 11 个 kind 的 SQLite 历史表(那是数据,要保留)

## §2 调用链(2026-09-14 校正)

> **⚠️ 警告**:实际代码里**没有** `OnlineRedis.clean_online_keys()` 这个 method。清理逻辑是**模块内局部函数** `clean_redis_online(r, log)`,不挂在 OnlineRedis 类上。

```
service_cleanredis_online --once
  └─ OnlineRedis()                           # 只用 r.r.scan + r.r.delete
  └─ clean_redis_online(r, log)              # 模块内函数(不是 method!)
  └─ ONLINE_DATA_ROOT (Path, 从 sqlite_client 导入)
  └─ clean_sqlite_cursor(log)                # 遍历所有月份 db 清 online_stream_cursor 表

## §3 启动方式

**只能 `--once` 模式**(scheduler 触发,不 daemon):

```bash
python3 -m service.service_cleanredis_online --once
```

scheduler **每天 09:00 + 03:00 各 spawn 一次**(`ONCE_TRIGGERS = [(9, 0, "service_cleanredis_online", []), (3, 0, "service_cleanredis_online", [])]`,2026-09-14 改 16:00→03:00)。

## §4 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **scheduler_onlineData.py** | 03:00 各 spawn 一次 |
| **scheduler_once.py** | 加 `--only cleanredis` / `--skip cleanredis` |

## §5 历史变更

- **2026-09-11 重构**:只清理 online: namespace(原来是 all,误删了 sqlite_test 库的 sample 数据)
- **2026-09-14**:加 SQLite 游标清理(STREAM flush 后游标失效)
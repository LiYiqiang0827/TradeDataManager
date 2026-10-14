# scheduler/scheduler_onlineData.py 详细设计

> **onlineDataManager 顶层调度器**(launchd KeepAlive 唯一顶层进程)
> **行数**:687 行(`wc -l`,2026-09-16 v6.15 重构后校正)
> **阶段**:WRITER × SAVEDATA 各 **10 个阶段**(`WRITER_PHASES` / `SAVEDATA_PHASES` 真值,v6.15 加 `idle_pre_morning` / `idle_pre_afternoon`)+ 4 个一次性触发(`ONCE_TRIGGERS`,v6.15 加 `(15, 15)` `service_savedata_auction --once`)|
> ⚠️ **2026-09-14 第 13+ 轮校正**:本节 §2-§4 重写为**以代码真值为主**,旧 doc 业务语义作为"业务语义别名"附录列出。**不要按旧 doc 业务语义名写新代码**(代码里完全没有 `pre_market / auction_open / auction_collect / continuous_open / continuous_resume / post_market / auction_pause` 这 7 个阶段名)。
| **v6.15 重构(2026-09-16)**:用户最新统一指令:
- **写盘**:auction 09:14-09:26;watchlist 09:10 一次性;**普通 kind 8 种**(snapshot / snapshot_index / minute / limitperformance / zt / break / hot / anomaly)统一 09:29-11:31 + 12:59-15:01
- **落盘**:auction 09:29 + 15:15 双落;watchlist 09:30-11:46 + 13:00-15:16(每 15 分钟);**普通 kind 8 种**统一 09:30-11:46 + 13:00-15:16(每 15 分钟)
- **orderbook 完全停止抓取**(service 文件保留,scheduler 不再 spawn)
- **新加 2 个 idle 阶段**:`idle_pre_morning`(09:26-09:29 写 / 09:26-09:30 落)和 `idle_pre_afternoon`(12:59-13:00)
- **频率变更**:snapshot_index 30s → 10s;minute 60s → 30s;zt 30s → 60s;hot 300s → 120s;其它不变
- **WRITER_PHASES / SAVEDATA_PHASES** 从各 8 阶段 → 各 10 阶段(加 idle_pre_*)

---

## §1 职责

- launchd 启动后**唯一顶层进程**(KeepAlive 监控对象)
- 全天运行,**每分钟检查时间** → 判断当前阶段 → spawn 对应 service 子进程
- 子进程跑完一轮后由 scheduler 决定下一步
- **lunch 写盘停 / 落盘继续**:11:31 切到 `lunch` 时主动 kill `morning_writer` 阶段的 **8 个 writeredis daemon**(snapshot / snapshot_index / minute / limitperformance / zt / break / hot / anomaly,v6.15);savedata 在 `morning_savedata` 阶段继续跑到 11:46 收尾

## §2 阶段(WRITER × SAVEDATA 各 **10 个**,v6.15 加 idle_pre_morning / idle_pre_afternoon)

### 2.1 code 真值(`WRITER_PHASES` / `SAVEDATA_PHASES` 数组)

| WRITER_PHASES | SAVEDATA_PHASES | 时段 | 备注 |
|---|---|---|---|
| `pre_open` | `pre_open` | 09:00-09:10 | 空 |
| `watchlist` | `watchlist` | 09:10-09:14 | 写 + 落都跑,一次性 |
| `auction_writer` | `auction_savedata` | 09:14-09:26 | 写=auction 抓取;落=等 09:29 --once |
| `idle_pre_morning` | `idle_pre_morning` | 09:26-09:29(写)/ 09:26-09:30(落) | **新加 v6.15** — 空窗 |
| `morning_writer` | `morning_savedata` | 09:29-11:31(写)/ 09:30-11:46(落) | **新窗口 v6.15**;写 = 8 个普通 kind;落 = 8 个普通 + watchlist |
| `lunch` | `morning_savedata` | 11:31-11:46 | **写停 / 落盘继续收尾 15 分钟**(v6.15 新逻辑) |
| `lunch` | `savedata_paused` | 11:46-12:59 | **都停** |
| `idle_pre_afternoon` | `idle_pre_afternoon` | 12:59-13:00 | **新加 v6.15** — 空窗 |
| `afternoon_writer` | `afternoon_savedata` | 13:00-15:01(写)/ 13:00-15:16(落) | 同 morning |
| `writer_stopped` | `afternoon_savedata` | 15:01-15:16 | **写停 / 落盘继续收尾 15 分钟** |
| `writer_stopped` | `post_savedata` | 15:16-16:00 | **都停** |
| `closed` | `closed` | 16:00-次日 | — |

> **实测代码**(`scheduler_onlineData.py` L90-94):
> ```python
> WRITER_PHASES = ["pre_open", "watchlist", "auction_writer", "idle_pre_morning",
>                  "morning_writer", "lunch", "idle_pre_afternoon",
>                  "afternoon_writer", "writer_stopped", "closed"]
> SAVEDATA_PHASES = ["pre_open", "watchlist", "auction_savedata",
>                    "idle_pre_morning",
>                    "morning_savedata", "savedata_paused",
>                    "idle_pre_afternoon",
>                    "afternoon_savedata", "post_savedata", "closed"]
> ```

### 2.2 阶段字典(`WRITER_PHASE_SERVICES` / `SAVEDATA_PHASE_SERVICES`)

**关键**(2026-09-14 第 13 轮校正):
- 字典名是 **`WRITER_PHASE_SERVICES` / `SAVEDATA_PHASE_SERVICES`**(有 `_PHASE_` 中缀)
- **`WRITER_SERVICES` / `SAVEDATA_SERVICES` / `WRITER_PHASES` / `SAVEDATA_PHASES` 这 4 个名字不存在**(误抄会 AttributeError)
- 10 个 phase **key**(SAVEDATA) 都用 `_savedata` 后缀,不要混用

| phase key | WRITER 跑的服务 | SAVEDATA 跑的服务 |
|---|---|---|
| `pre_open` | — | — |
| `watchlist` | service_writeredis_watchlist | service_savedata_watchlist |
| `auction_writer` / `auction_savedata` | **v6.15:仅** service_writeredis_auction(原 limitperformance 已下放 morning) | service_savedata_auction(--once 09:29 + 15:15 触发) |
| `idle_pre_morning` | — | — |
| `morning_writer` / `morning_savedata` | **v6.15:8 个**(删 orderbook):snapshot / snapshot_index / minute / limitperformance / zt / break / anomaly / hot | **v6.15:9 个**(删 orderbook,加 snapshot_index):snapshot / snapshot_index / minute / limitperformance / zt / break / anomaly / hot + watchlist |
| `lunch` / `savedata_paused` | — | — |
| `afternoon_writer` / `afternoon_savedata` | 同 morning 8 个 | 同 morning 9 个 |
| `writer_stopped` / `post_savedata` | — | — |
| `closed` | — | — |

### 2.3 业务语义别名(旧 doc 术语 ↔ code 真值)

| 旧 doc 业务语义 | code 阶段(WRITER)| code 阶段(SAVEDATA)|
|---|---|---|
| `pre_market` | `pre_open` → `watchlist` | `pre_open` |
| `auction_open` | `auction_writer` | `auction_savedata` |
| `auction_collect` | `auction_writer` | `auction_savedata` |
| `continuous_open` | `morning_writer` | `morning_savedata` |
| `lunch` | `lunch` | `savedata_paused` |
| `continuous_resume` | `afternoon_writer` | `afternoon_savedata` |
| `post_market` | `writer_stopped` | `post_savedata` |
| `auction_pause` | **(已删除,09:30 短暂过渡,不专门占一阶段)** | — |

> **警告**:`auction_pause` 这个阶段名**代码里不存在**,只在 doc 历史里出现过。第 9 轮修订时已经删除。**新人不要再用这个名字**。

## §3 一次性触发(主循环每分钟检查时间点)

| 时间点 | 触发 |
|---|---|
| **09:00** | `service_cleanredis_online --once`(开盘前清空 Redis) |
| **09:29** | `service_savedata_auction --once`(auction 数据 09:14-09:26,v6.15 落盘)|
| **15:15** | `service_savedata_auction --once`(v6.15 用户最新规定:auction 兜底补落)|
| **03:00** | `service_cleanredis_online --once`(**生产真值:凌晨 03:00 清残留,2026-09-16 用户拍板默认 3 点不再回 16:00**)|

> **真值**(`scheduler_onlineData.py` L297-302 `ONCE_TRIGGERS`):`(9,0)` cleanredis / `(9,29)` savedata_auction / `(15,15)` savedata_auction / `(3,0)` cleanredis。

## §4 lunch / 收盘 处理(v6.15 重构)

- **idle_pre_morning(09:26-09:29 写 / 09:26-09:30 落)**:scheduler 不 spawn 任何 service(空窗过渡)
- **morning_writer(09:29-11:31)**:spawn 8 个普通写入 daemon
- **lunch 11:31**:scheduler 主动 kill morning_writer 的 8 个 writeredis daemon;落盘继续到 11:46(morning_savedata 收尾 15 分钟)
- **savedata_paused(11:46-12:59)**:scheduler kill 8 个 savedata daemon
- **idle_pre_afternoon(12:59-13:00)**:scheduler 不 spawn 任何 service(空窗过渡)
- **afternoon_writer(13:00-15:01)**:重新 spawn 8 个 writeredis(`while True` 内部不感知 lunch)
- **writer_stopped(15:01)**:scheduler kill 8 个 writeredis daemon;落盘继续到 15:16(afternoon_savedata 收尾 15 分钟)
- **post_savedata(15:16)**:scheduler kill 8 个 savedata daemon;03:00 触发 cleanredis

## §5 调用链

```
scheduler_onlineData (主循环,每分钟一次)
  ├─ check_time()                                # 当前时间 → 阶段
  ├─ if 阶段切换:
  │   ├─ kill 旧阶段的 service 子进程
  │   └─ spawn 新阶段的 service 子进程
  ├─ if 一次性触发时间点:
  │   └─ subprocess.Popen(["python3", "-m", "service.xxx", "--once"])
  └─ sleep 60 秒
```

## §6 启动方式

通过 launchd plist 启动(`~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist`):
```xml
<key>KeepAlive</key><true/>
<key>RunAtLoad</key><true/>
```

重启:
```bash
launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.onlineData.plist
```

## §7 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **launchd** | 父进程,KeepAlive |
| **service_writeredis_*** / **service_savedata_*** | 子进程 |
| **service_cleanredis_online** | 一次性触发 |

## §8 历史变更

- **2026-09-11 v2 重构**:从单文件逻辑改为 9 阶段 + 写入/落盘分离
- **2026-09-11**:lunch 完全停
- **2026-09-12 v3**:watchlist 独立成 09:10-09:14 一阶段(阶段名 `watchlist`,非 `pre_auction`)
- **2026-09-14 v5**:**WRITER_PHASE_SERVICES.auction_writer 新增 `service_writeredis_limitperformance`**(涨停表现详情),`WRITER_DAEMONS_MORNING_AFTERNOON` 从 7 个扩到 8 个
- **2026-09-14 v6.2**:auction 加 timeline/archive,scheduler 不动
- **2026-09-14 v6.3**:snapshot 改名(从 realtime → snapshot),scheduler line 154 已同步
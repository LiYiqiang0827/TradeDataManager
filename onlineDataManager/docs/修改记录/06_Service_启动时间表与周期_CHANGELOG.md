# 06_Service_启动时间表与周期 CHANGELOG

> **本体只保留最新内容**,变更记录走 CHANGELOG。

---

## v1.0 — 2026-09-15(初版)

- **新增**:`06_Service_启动时间表与周期.md`
- **触发**:用户原话"你能写一个和架构文平级的文档,详细描述 scheduler 里面各个 write redis 的启动时间段和 save data 的启动时间段,每个数据的拉取周期和落盘周期,包括 watchlist,还有 clean redis 等等"
- **内容**:
  - §0 一图流完整时间表(写入 09:10-11:40 + 12:55-15:10,落盘 09:10-12:10 + 12:55-15:35)
  - §1 阶段定义(8 个 writer + 8 个 savedata + weekend)
  - §2 Writer Service 全清单(10 个,含 1 个一次性 watchlist)
  - §3 Savedata Service 全清单(10 个,含 2 个 ONCE_TRIGGER)
  - §4 一次性 Trigger(4 个:09:00 clean / 09:29 auction / 15:10 auction / 03:00 clean)
  - §5 汇总速查表(Writer 周期 / Savedata 周期 / Trigger / Check 4 张表)
  - §6 关键不变量(踩过的坑)
  - §7 排障指引(3 个常见问题)
- **依据**:`scheduler_onlineData.py` L 60-260 阶段定义 + `WRITER_PHASE_SERVICES` + `SAVEDATA_PHASE_SERVICES` + `ONCE_TRIGGERS` + 10 个 `service_writeredis_*.py` head 周期真值 + 10 个 `service_savedata_*.py` DEFAULT_INTERVAL 真值 + 2026-09-15 v6.8 DEFAULT_DRAIN 新增
- **作者**:AI 助手
- **影响**:文档层新增,0 代码改动;现有 5 份顶级文档(01-05)不变

---

## v1.1 — 2026-09-15(双重保险 + 时间窗对齐,5 处)

- **触发**:用户发现 v1.0 §2.2 / §2.4 对 auction 和 snapshot 的时间窗描述不准确 — **auction writer 应在 9:26 停**(不是写到 11:40 跟其他 writer 一样)、**snapshot writer 应在 11:40-12:55 期间停**(lunch 阶段 + 内部 `is_trading_window` 双重保险)。原 v1.0 把 auction 关闭阶段写成"lunch 11:40"是错的。
- **修改**(06 主文档,5 处):
  - **§0 一图流**:新增"🔒 双重保险 — writer 内部时间窗"小节,说明 `is_auction_window()`=09:15-09:25 + `is_trading_window()`=09:30-15:00 是 daemon 内部兜底,savedata 无内部窗口只看游标
  - **§2.2 writeredis_auction**:关闭阶段修正为 `morning_writer`(09:26)— scheduler 阶段切换时 kill,加 `🔒 内部时间窗` 行,改备注强调"auction writer 在 09:26 就停止拉数据并写 Redis"
  - **§2.4 writeredis_snapshot**:加 `🔒 内部时间窗` 行(`is_trading_window`=09:30-15:00),改备注说"双重保险:scheduler kill + is_trading_window 11:31-12:59"
  - **§5.1 Writer 汇总表**:新增"内部时间窗(`is_*_window`)"列,展示哪些 writer 有内部窗口、值多少
  - **§6 关键不变量**:新增 3 条
    - "🔒 双重保险(关键)" — writer 内部窗口兜底,savedata 无内部窗口
    - "auction writer 9:26 就停" — 实际停机 = min(9:26 scheduler kill, 9:25 is_auction_window 退出) = 9:26
    - "snapshot writer 11:40-12:55 停" — 实际停机 = min(11:40 scheduler kill, 11:31 is_trading_window 退出) = 11:40
  - **§7.4 新增**:排障小节"auction 9:26 真的停了吗?snapshot 11:40-12:55 真的停了吗?"— 给 3 个 ps / log / redis-cli 验证命令 + 双重保险停机时间真值
- **代码依据**:
  - `scheduler_onlineData.py` L 154-167 `WRITER_DAEMONS_MORNING_AFTERNOON` 不含 auction(仅 morning_writer 阶段挂 7 个非 auction writer)→ 9:26 切到 morning_writer 时 scheduler kill auction daemon
  - `service_writeredis_auction.py` `is_auction_window()` = `(9, 15) <= (hh, mm) <= (9, 25)` — 9:25 后只 sleep 不拉数据
  - `service_writeredis_snapshot.py` `is_trading_window()` = `(9, 30) <= (hh, mm) <= (15, 0)` — 11:31-12:59 期间返回 False,只 sleep 不拉数据
- **作者**:AI 助手
- **影响**:仅文档层修正,0 代码改动;`scheduler_onlineData.py` / 10 个 writeredis service / 10 个 savedata service 全部代码不变

---

## v1.2 — 2026-09-16(v6.15 完整重构对齐)

**目的**:对齐 `scheduler_onlineData.py` v6.15 全部真值(阶段数 8→10、统一窗口、删 orderbook、加 snapshot_index 落盘、6 个 service 频率调整)。

**改动**(以代码为准):

- §0 一图流 — **时间窗全部校准**:11:40→11:31 / 12:55→12:59 / 15:10→15:01 / 12:10→11:46 / 15:35→15:16;daemon 数 7→8(加 snapshot_index 落盘);加 `idle_pre_morning` (09:26-09:29 写/09:26-09:30 落) + `idle_pre_afternoon` (12:59-13:00)
- §1.1 写入阶段 — **8 → 10 阶段**(加 `idle_pre_morning` / `idle_pre_afternoon`);`morning_writer` 09:26-11:40 → **09:29-11:31**;`lunch` 11:40-12:55 → **11:31-12:59**;`afternoon_writer` 12:55-15:10 → **13:00-15:01**;`writer_stopped` 15:10+ → 15:01+
- §1.2 落盘阶段 — **8 → 10 阶段**;`morning_savedata` 09:26-12:10 → **09:30-11:46**;`savedata_paused` 12:10-12:55 → **11:46-12:59**;`afternoon_savedata` 12:55-15:35 → **13:00-15:16**;加 `post_savedata` 15:16-16:00
- §1.3 — 新增"写入/落盘时间窗分离 v6.15 关键"图,落盘比写盘晚 15 分钟
- §2.2 auction — 周期 / 阶段全部对齐(已对)+ 加注 09:26 scheduler kill 触发 idle_pre_morning
- §2.3 limitperformance — **v6.15 起不再在 auction_writer 阶段启动**(只在 morning/afternoon_writer)
- §2.4 snapshot — `morning_writer` 启动 09:26→**09:29** / `lunch` 关闭 11:40→**11:31** / `writer_stopped` 关闭 15:10→**15:01** / 数据源 pytdx/HithinkCLI → **同花顺 fetch_snapshots** / 实际有效拉数据 11:40→**11:31** + 12:55→**13:00**
- **🆕 §2.5 snapshot_index** — v6.10 接入,v6.15 频率 30s→**10s**
- §2.6 minute — 周期 60s→**30s**(v6.15)
- §2.7 zt — 周期 30s→**60s**(v6.15)
- §2.10 hot — 周期 300s→**120s**(v6.15)
- **🔴 §2.11 orderbook — v6.15 停用**:scheduler 不再 spawn;service 文件保留
- §3.1 watchlist — 启动阶段 `morning_savedata` 09:26→**09:30** / `afternoon_savedata` 12:55→**13:00** / `savedata_paused` 12:10→**11:46** / `post_savedata` 15:35→**15:16**
- §3.2 auction trigger — **15:10 → 15:15**(v6.15 用户拍板)
- §3.10 limitperformance — **v6.15 起不再在 auction_savedata 阶段挂**(下放回 morning/afternoon_savedata)
- **🆕 §3.4 snapshot_index** — v6.10 接入代码,v6.15 正式纳入 scheduler 落盘阶段
- **🔴 §3.11 orderbook — v6.15 停用**
- §5.1 writer 表 — 删 orderbook + 加 snapshot_index + 6 处频率调整
- §5.2 savedata 表 — 删 orderbook + 加 snapshot_index
- §6 不变量 — 新增"8 普通 kind 共用 morning/afternoon 阶段"
- §7 排障 — 全部时间点校准;§7.4 双重保险停机时间全部对齐 v6.15(11:31 / 13:00 / 15:01)
- §8 引用 — 加 v6.7/v6.8/v6.10/v6.15 版本标注

**重大变更**:

- 阶段数 8 → 10(加 idle_pre_morning / idle_pre_afternoon)
- auction 兜底落盘 15:10 → 15:15
- cleanredis **生产真值 03:00**(2026-09-16 用户拍板:默认 3 点不再回 16:00;原 16:00 已废弃)
- 删 orderbook(完全停用)
- 加 snapshot_index 落盘(2 个 service 同步上线)
- 6 个 service 频率调整(snapshot_index 30→10s / minute 60→30s / zt 30→60s / hot 300→120s / 其余不变)

**代码依据**:

- `scheduler_onlineData.py` L 60-260 阶段定义(10 个 WRITER + 10 个 SAVEDATA)+ L 180-230 PHASE_SERVICES + L 296-301 ONCE_TRIGGERS
- `service_writeredis_snapshot_index.py` head interval_sec = 10(v6.15 原 30s)
- `service_writeredis_minute.py` head interval_sec = 30(v6.15 原 60s)
- `service_writeredis_zt.py` head interval_sec = 60(v6.15 原 30s)
- `service_writeredis_hot.py` head interval_sec = 120(v6.15 原 300s)
- `service_writeredis_orderbook.py` 与 `service_savedata_orderbook.py` v6.15 停用标记
- `WRITER_DAEMONS_MORNING_AFTERNOON` 与 `SAVEDATA_DAEMONS_MORNING_AFTERNOON` 列表真值(8 个普通 kind,不含 orderbook)

**作者**:AI 助手
**影响**:仅文档层修正 + 1 处代码注释(scheduler.py 头 docstring 可选);`scheduler_onlineData.py` 主体代码 v6.15 已在线运行

## 后续变更

- **v1.3(2026-09-16 14:48+)**:cleanredis 生产真值拍板为 03:00
  - **触发**:用户原话"不用改回下午 16 点 以后就默认 3 点"
  - **改动**:06_Service_启动时间表与周期.md §3.4 L376 + §5.3 L420 + 代码详细设计/scheduler_onlineData.md §3 L96 + 代码详细设计/service_cleanredis_online.md L5/L12 + 01_架构文档.md §4.3 + 02_代码详细设计.md §3.4 + scheduler_onlineData.py L29/L86/L300 + 06_CHANGELOG §重大变更项 全部从"调试期"标注改为"生产真值:03:00"
  - **生产 scheduler daemon 无需重启**(ONCE_TRIGGERS 时间点 (3, 0) 无变化,只是注释语义翻转)

# scheduler/scheduler_once.py 详细设计

> **一次性测试调度器**(2026-09-14 新增)
> **不入 plist**,纯 CLI 工具,用于任何时间都能测 pipeline
| **行数**:250 行(`wc -l`,2026-09-15 校正)(`wc -l`)
| **v6.10 更新(2026-09-16)**:`ALL_WRITERS` 11 → 12(`snapshot_index`)+ `ALL_SAVEDATA` 11 → 12。`--only` choices 加 `snapshot_index`。当前跑 22/22 OK(2026-09-16 00:25 跑通)。

---

## §1 设计动机

- **收盘后调试**:`scheduler_onlineData` 主要在交易时段跑(写入 09:14-15:10、落盘 09:14-15:35),收盘后用 `scheduler_once` 测试
- **周末/假期手工测试**:不靠时间也能跑
- **CI smoke test**(将来)

## §2 跑哪些 service

| 阶段 | 默认 | 全套 |
|---|---|---|
| **WRITER** | 10 个 | watchlist / auction / snapshot / orderbook / minute / zt / break / anomaly / hot / limitperformance |
| **SAVEDATA** | 10 个 | watchlist / auction / snapshot / orderbook / minute / zt / break / anomaly / hot / limitperformance |

**默认全部 20 个 service**(writer 10 + savedata 10)。

## §3 CLI 接口

```bash
# 跑全部(writer + savedata 共 20 个)
python3 -m scheduler.scheduler_once

# 只跑 writer
python3 -m scheduler.scheduler_once --only writer

# 只跑 savedata
python3 -m scheduler.scheduler_once --only savedata

# 只跑某个 kind(单 writer + 单 savedata)
python3 -m scheduler.scheduler_once --only snapshot
python3 -m scheduler.scheduler_once --only zt
python3 -m scheduler.scheduler_once --only watchlist

# 跳过某些 kind(逗号分隔)
python3 -m scheduler.scheduler_once --skip auction,break,anomaly,hot,limitperformance

# 组合
python3 -m scheduler.scheduler_once --only writer --skip auction,break
```

## §4 输出:summary 表

每跑完一个 service 输出:

| KIND | ROLE | RC | ELAPSED | STATUS |
|---|---|---|---|---|
| watchlist | WRITER | 0 | 0.1s | OK |
| snapshot | WRITER | 0 | 1.1s | OK |
| orderbook | WRITER | 0 | 1.2s | OK |
| minute | WRITER | 0 | 4.3s | OK |
| zt | WRITER | 0 | 0.3s | OK |
| watchlist | SAVEDATA | 0 | 0.1s | OK |
| snapshot | SAVEDATA | 0 | 0.2s | OK |
| orderbook | SAVEDATA | 0 | 2.3s | OK |
| minute | SAVEDATA | 0 | 0.2s | OK |
| zt | SAVEDATA | 0 | 0.1s | OK |

**合计 10/10 OK / 10.1s**(完整版 20 个:理论 timeout 上限 ~37 分钟(写入 ~28min,落盘 ~9min);实际正常情况下各 service 几十秒就跑完,实测 20-300s)

末尾打印(参考 `scheduler_once.py:206-218` 实测输出):
```
=========================================================================
scheduler_once 完工 @ <ts>  耗时 <sec>s
=========================================================================

KIND                 ROLE         RC   ELAPSED  STATUS
----------------------------------------------------------------------
watchlist            WRITER        0      0.1s  OK
snapshot             WRITER        0      1.1s  OK
... (每条一行,按 ALL_WRITERS + ALL_SAVEDATA 顺序)
----------------------------------------------------------------------
合计: 20 ok / 0 fail / 20 total
=========================================================================
```

> ⚠️ **真值**(`scheduler_once.py:206-218`):代码只输出 `KIND / ROLE / RC / ELAPSED / STATUS` + `合计: X ok / Y fail / Z total`,**没有** `TIMEOUT` 和 `SKIP` 字段(失败统一显示 `FAIL`,不区分超时/异常)。

## §5 设计要点

- **顺序执行**(不是并行):保持测试可观察性,一次看一个
- **超时保护**(按 kind 而异,**不是统一 30s**):
  - **WRITER**:`watchlist` = **60s**,其他 9 个 writer = **180s**
  - **SAVEDATA**:`watchlist` / `auction` = **30s**,其他 8 个 = **60s**
  - 防止 `--once` 死循环(如 2026-09-14 minute bug)
- **失败不中断**:一个 service 失败,继续跑下一个,最后看 SUMMARY 表判断
- **不入 plist**:纯 CLI 工具,launchd 不管它

## §6 端到端验证(2026-09-14 17:30)

`--skip auction,break,anomaly,hot,limitperformance` 跑 5 kind:
- **10/10 全 OK / 10.1s**
- 详情见 §4

## §7 调用链

```
scheduler_once.main(args)
  ├─ filter_services(args.only, args.skip) → [(name, role, timeout), ...]
  ├─ for (name, role, timeout) in services:
  │   ├─ start = time.time()
  │   ├─ rc = spawn_service(name, ["--once"], log=log, timeout_sec=timeout)   ← 从 scheduler_onlineData 导入
  │   │           └─ 内部:--once 走 subprocess.run(timeout=timeout_sec, stdout=DEVNULL, stderr=DEVNULL) 同步模式
  │   ├─ elapsed = time.time() - start
  │   └─ record (name, role, rc, elapsed, "OK" if rc == 0 else "FAIL")
  └─ print SUMMARY table (KIND / ROLE / RC / ELAPSED / STATUS + 合计 X ok / Y fail / Z total)
```

> ⚠️ **真值**(`scheduler_once.py:151`):实际调用的是 `spawn_service`(从 `scheduler_onlineData` 导入,L60),**不是**直接的 `subprocess.Popen`;`spawn_service` 内部 `--once` 走 `subprocess.run` 而非 `Popen`(`scheduler_onlineData.py:316`);timeout 取自 `svc["timeout"]`,**不**是写死 30s。

## §8 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **开发/调试时手工跑** | 直接调 |
| **AI Agent 测 pipeline** | 调,看 SUMMARY |

## §9 历史变更

- **2026-09-14 新建**:本文件即为初版
- **2026-09-14**:发现并修复 minute --once 死循环(配合本工具)
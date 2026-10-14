"""
scheduler/scheduler_once.py
============================

**一次性测试调度器**(2026-09-14 新增)— 不管当前什么时间,都能按顺序跑所有
service 的 --once 模式,用来:
- 测试 Redis 写入是否 ok
- 测试 SQLite 落盘是否 ok
- 测试整套 pipeline 端到端是否 work

适用场景:
- 收盘后调试(原 scheduler_onlineData 只能 9:00-15:10 跑 service)
- 周末 / 假期手工测试
- CI smoke test(将来)

用法:
    # 跑全部(writer + savedata 共 19 个 service)
    python3 -m scheduler.scheduler_once

    # 只跑 writer
    python3 -m scheduler.scheduler_once --only writer

    # 只跑 savedata
    python3 -m scheduler.scheduler_once --only savedata

    # 只跑某个 kind(单 writer + 单 savedata)
    python3 -m scheduler.scheduler_once --only snapshot
    python3 -m scheduler.scheduler_once --only zt
    python3 -m scheduler.scheduler_once --only watchlist

    # 跳过某些(逗号分隔 kind 名)
    python3 -m scheduler.scheduler_once --skip auction,break

设计:
- **复用** scheduler_onlineData.spawn_service()(不重新实现子进程逻辑)
- **顺序执行**(一个一个跑完再下一个)— 简单直观
- writered 跑完后**立即跑**对应 savedata(数据已落 Redis,直接落盘)
- watchlist 是其他 service 的前置(其他都依赖 watchlist),所以第一个跑
- auction 是 09:15-09:25 集合竞价数据,收盘后跑也会 fetch,只是拿不到新数据(降级 ok)

注意:
- 这不是生产工具,生产用 scheduler_onlineData(launchd 持有)
- 这个脚本只在手工调试 / 端到端测试时用
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent  # scripts/
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(SERVICE_DIR))  # 让 import scheduler_onlineData 工作

from scheduler.scheduler_onlineData import spawn_service  # noqa: E402


# ============================================================
# 测试 service 清单(2026-09-14 v6.3)
# ============================================================
# 顺序原则:
#   1. watchlist 必须先跑(其他 service 依赖 watchlist)
#   2. auction 接着(集合竞价独立,跟其他 service 不冲突)
#   3. 7 个常规写入 + limitperformance(快照/盘口/分时/涨停...)
#   4. 写入跑完后**立即**落盘(数据刚进 Redis,游标会被推进)
#
# KPL 慢超时:KPL fetch 接口单次可能 60-120s,默认给 180s
# savedata 极快(本地 SQLite),默认 60s

# 全 12 个 writer(去掉 watchlist 单独提)
# v6.10 (2026-09-16) 新增 snapshot_index(8 只指数,30s/轮)
ALL_WRITERS: list[dict] = [
    {"name": "service_writeredis_watchlist",        "kind": "watchlist",      "timeout": 60},
    {"name": "service_writeredis_auction",          "kind": "auction",        "timeout": 180},
    {"name": "service_writeredis_snapshot",         "kind": "snapshot",       "timeout": 180},
    {"name": "service_writeredis_snapshot_index",   "kind": "snapshot_index","timeout": 180},    # 2026-09-16 v6.10 新增
    {"name": "service_writeredis_orderbook",        "kind": "orderbook",      "timeout": 180},
    {"name": "service_writeredis_minute",           "kind": "minute",         "timeout": 180},
    {"name": "service_writeredis_zt",               "kind": "zt",             "timeout": 180},
    {"name": "service_writeredis_break",            "kind": "break",          "timeout": 180},
    {"name": "service_writeredis_anomaly",          "kind": "anomaly",        "timeout": 180},
    {"name": "service_writeredis_hot",              "kind": "hot",            "timeout": 180},
    {"name": "service_writeredis_limitperformance", "kind": "limitperformance","timeout": 180},
]

# 11 个 savedata(watchlist 落盘 + 10 个常规落盘)
# 注:auction savedata 已有 --once,虽然 scheduler 主流程不常单独跑,但作为完整性也包含
# v6.10 (2026-09-16) 新增 snapshot_index savedata
ALL_SAVEDATA: list[dict] = [
    {"name": "service_savedata_watchlist",          "kind": "watchlist",      "timeout": 30},
    {"name": "service_savedata_auction",            "kind": "auction",        "timeout": 30},
    {"name": "service_savedata_snapshot",           "kind": "snapshot",       "timeout": 60},
    {"name": "service_savedata_snapshot_index",     "kind": "snapshot_index","timeout": 60},    # 2026-09-16 v6.10 新增
    {"name": "service_savedata_orderbook",          "kind": "orderbook",      "timeout": 60},
    {"name": "service_savedata_minute",             "kind": "minute",         "timeout": 60},
    {"name": "service_savedata_zt",                 "kind": "zt",             "timeout": 60},
    {"name": "service_savedata_break",              "kind": "break",          "timeout": 60},
    {"name": "service_savedata_anomaly",            "kind": "anomaly",        "timeout": 60},
    {"name": "service_savedata_hot",                "kind": "hot",            "timeout": 60},
    {"name": "service_savedata_limitperformance",   "kind": "limitperformance","timeout": 60},
]

# 按 kind 索引(给 --only snapshot 用)
_WRITERS_BY_KIND = {svc["kind"]: svc for svc in ALL_WRITERS}
_SAVEDATA_BY_KIND = {svc["kind"]: svc for svc in ALL_SAVEDATA}


def setup_logger(name: str) -> logging.Logger:
    """跟 scheduler_onlineData 一致的 logger(单文件输出,不写 logs/)"""
    log = logging.getLogger(name)
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter(
        "[%(asctime)s] %(name)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    log.addHandler(h)
    return log


def filter_services(
    services: list[dict],
    only_kind: str | None,
    skip_kinds: set[str],
) -> list[dict]:
    """根据 --only / --skip 过滤 service 清单"""
    if only_kind:
        # --only <kind>: 单 kind 模式
        if only_kind == "writer":
            return services  # 全部 writer
        if only_kind == "savedata":
            return services  # 调用方传的是 ALL_SAVEDATA
        # 单 kind: 仅返回该 kind 的 service
        return [s for s in services if s["kind"] == only_kind]
    if skip_kinds:
        return [s for s in services if s["kind"] not in skip_kinds]
    return services


def run_one_service(svc: dict, role: str, log: logging.Logger) -> tuple[str, int, float]:
    """跑一个 service --once,返回 (kind, rc, elapsed_sec)"""
    name = svc["name"]
    kind = svc["kind"]
    timeout = svc["timeout"]
    args = ["--once"]
    t0 = time.time()
    log.info(f"━━━ [{role}] {name} --once (timeout={timeout}s) ━━━")
    rc = spawn_service(name, args, log=log, timeout_sec=timeout)
    elapsed = time.time() - t0
    status = "OK" if rc == 0 else f"FAIL(rc={rc})"
    log.info(f"━━━ [{role}] {name} {status} 耗时 {elapsed:.1f}s ━━━\n")
    return (kind, rc, elapsed)


def run_once(
    log: logging.Logger,
    only_kind: str | None = None,
    skip_kinds: set[str] | None = None,
) -> int:
    """主流程:按顺序跑 writer → savedata,打印 summary"""
    skip_kinds = skip_kinds or set()
    start_ts = datetime.now()
    log.info("=" * 70)
    log.info(f"scheduler_once 启动 @ {start_ts.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  only={only_kind}  skip={sorted(skip_kinds)}")
    log.info("=" * 70)

    # 决定跑哪些
    writers = filter_services(ALL_WRITERS, only_kind, skip_kinds)
    savedata = filter_services(ALL_SAVEDATA, only_kind, skip_kinds)

    if not writers and not savedata:
        log.warning("过滤后没有任何 service 可跑,退出")
        return 1

    log.info(f"计划: {len(writers)} 个 writer + {len(savedata)} 个 savedata")
    log.info("")

    results: list[tuple[str, str, int, float]] = []  # (kind, role, rc, elapsed)

    # 1. 写入端
    for svc in writers:
        kind, rc, elapsed = run_one_service(svc, "WRITER", log)
        results.append((kind, "WRITER", rc, elapsed))

    log.info("")
    log.info("─" * 70)
    log.info("写入端跑完, 接下来跑 savedata")
    log.info("─" * 70)
    log.info("")

    # 2. 落盘端
    for svc in savedata:
        kind, rc, elapsed = run_one_service(svc, "SAVEDATA", log)
        results.append((kind, "SAVEDATA", rc, elapsed))

    # 3. Summary
    end_ts = datetime.now()
    total_sec = (end_ts - start_ts).total_seconds()
    ok_count = sum(1 for _, _, rc, _ in results if rc == 0)
    fail_count = len(results) - ok_count

    log.info("")
    log.info("=" * 70)
    log.info(f"scheduler_once 完工 @ {end_ts.strftime('%Y-%m-%d %H:%M:%S')}  耗时 {total_sec:.1f}s")
    log.info("=" * 70)
    log.info("")
    log.info(f"{'KIND':<20s} {'ROLE':<10s} {'RC':>4s}  {'ELAPSED':>8s}  STATUS")
    log.info("-" * 70)
    for kind, role, rc, elapsed in results:
        status = "OK" if rc == 0 else "FAIL"
        log.info(f"{kind:<20s} {role:<10s} {rc:>4d}  {elapsed:>7.1f}s  {status}")
    log.info("-" * 70)
    log.info(f"合计: {ok_count} ok / {fail_count} fail / {len(results)} total")
    log.info("=" * 70)

    return 0 if fail_count == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="scheduler_once: 一次性按顺序跑全部 service --once(调试用,不管时间)",
    )
    parser.add_argument(
        "--only",
        choices=["writer", "savedata",
                 "watchlist", "auction", "snapshot", "snapshot_index", "orderbook", "minute",    # 2026-09-16 v6.10 + snapshot_index
                 "zt", "break", "anomaly", "hot", "limitperformance"],
        help="只跑某类(writer/savedata)或某 kind",
    )
    parser.add_argument(
        "--skip",
        type=str,
        default="",
        help="跳过的 kind(逗号分隔):auction,break,anomaly",
    )
    args = parser.parse_args()

    log = setup_logger("scheduler_once")
    log.info(f"启动 scheduler_once args={vars(args)}")

    skip_kinds = set(k.strip() for k in args.skip.split(",") if k.strip())
    return run_once(log, only_kind=args.only, skip_kinds=skip_kinds)


if __name__ == "__main__":
    sys.exit(main())
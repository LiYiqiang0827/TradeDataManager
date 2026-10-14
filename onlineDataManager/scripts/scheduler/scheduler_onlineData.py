"""
scheduler/scheduler_onlineData.py
===================================

onlineDataManager 顶层调度器(2026-09-11 重构后,MyATM 多 service 模式)。

设计:
- launchd 启动后唯一顶层进程(也是 KeepAlive 监控对象)
- 全天运行,按当前时间判断阶段 → spawn 对应 service 子进程
- service 跑完一轮后由 scheduler 决定下一步

阶段(2026-09-16 v6.15 重构,统一窗口 + 写/落盘分离):
  - 09:00-09:10  pre_open(空)
  - 09:10-09:14  watchlist(写 + 落盘都跑,一次性)
  - 09:14-09:26  auction_writer(auction 抓取)
  - 09:26-09:29  idle_pre_morning(空窗)
  - 09:29-11:31  morning_writer(8 个普通写入 daemon)
  - 11:31-11:46  lunch / morning_savedata(写入停,落盘继续收尾)
  - 11:46-12:59  lunch / savedata_paused(都停)
  - 12:59-13:00  idle_pre_afternoon(空窗)
  - 13:00-15:01  afternoon_writer(8 个普通写入 daemon)
  - 15:01-15:16  writer_stopped / afternoon_savedata(写入停,落盘收尾)
  - 15:16-16:00  closed(全停)

一次性触发(scheduler 主循环每分钟检查时间点):
  - 09:00   service_cleanredis_online --once
  - 09:29   service_savedata_auction --once(auction 数据 09:14-09:26)
  - 15:15   service_savedata_auction --once(v6.15 用户最新规定:auction 兜底补落)
  - 03:00   service_cleanredis_online --once(凌晨清残留,2026-09-16 用户拍板:生产真值 03:00,非 16:00)

启动:
    launchd plist: -m scheduler.scheduler_onlineData --daemon
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import logging
import os
import signal
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent  # scripts/
ONLINE_DATA_ROOT = SCRIPTS_DIR.parent  # onlineDataManager/

sys.path.insert(0, str(SCRIPTS_DIR))

from core.redis_online import OnlineRedis                   # noqa: E402

# ============================================================
# 阶段定义(2026-09-16 v6.15 重构,统一窗口 + 写/落盘分离)
# ============================================================
# 写入 service(writeredis_*)和落盘 service(savedata_*)的时间窗不一样
#
# 特殊 kind(独立时间窗):
#   - watchlist  :09:10 一次性写入
#   - auction    :09:14-09:26 抓取;09:29 + 15:15 一次性落盘
#
# 普通 kind(8 种统一时间窗,v6.15 用户最新指令):
#   snapshot / snapshot_index / minute / limitperformance / zt / break / hot / anomaly
#   写盘窗口:09:29-11:31 + 12:59-15:01
#   落盘窗口:09:30-11:46 + 13:00-15:16(每 15 分钟一次)
#
# 完整阶段时间线:
#   09:00-09:10  pre_open(空)
#   09:10-09:14  watchlist 写 + 落盘
#   09:14-09:26  auction_writer(auction 抓取)
#   09:26-09:29  idle(写入空窗)
#   09:29-11:31  morning_writer(8 个普通 kind 写盘)
#   11:31-11:46  morning_savedata 继续(写入已停,落盘收尾)
#   11:46-12:59  lunch(写停)/ savedata_paused(落盘也停)
#   12:59-13:00  idle(写入空窗)
#   13:00-15:01  afternoon_writer(8 个普通 kind 写盘)
#   15:01-15:16  afternoon_savedata 继续(写入已停,落盘收尾)
#   15:16-16:00  closed(全停)
#
# 一次性触发点:
#   09:00   service_cleanredis_online --once
#   09:29   service_savedata_auction --once(auction 数据 09:14-09:26,09:29 落)
#   15:15   service_savedata_auction --once(auction 兜底补落,v6.15 用户最新规定)
#   03:00   service_cleanredis_online --once(凌晨清残留,2026-09-16 用户拍板:生产真值 03:00,非 16:00)
#
# get_phase 返回两个 phase:writer_phase + savedata_phase

WRITER_PHASES = ["pre_open", "watchlist", "auction_writer", "idle_pre_morning",
                 "morning_writer", "lunch", "idle_pre_afternoon",
                 "afternoon_writer", "writer_stopped", "closed"]
SAVEDATA_PHASES = ["pre_open", "watchlist", "auction_savedata",
                   "idle_pre_morning",
                   "morning_savedata", "savedata_paused",
                   "idle_pre_afternoon",
                   "afternoon_savedata", "post_savedata", "closed"]


def _get_writer_phase(now: datetime) -> str:
    """写入 service 当前阶段(返回阶段名)— v6.15 用户最新统一窗口"""
    hh, mm = now.hour, now.minute
    t = (hh, mm)

    if (9, 0) <= t < (9, 10):
        return "pre_open"
    if (9, 10) <= t < (9, 14):
        return "watchlist"             # writeredis_watchlist 一次性写 Redis
    if (9, 14) <= t < (9, 26):
        return "auction_writer"        # writeredis_auction 抓取(9:14-9:26)
    if (9, 26) <= t < (9, 29):
        return "idle_pre_morning"      # 写入空窗(9:26-9:29 等普通 kind 启动)
    if (9, 29) <= t < (11, 31):
        return "morning_writer"        # 8 个普通写入 daemon
    if (11, 31) <= t < (12, 59):
        return "lunch"                 # 写入午休(11:31-12:59,88 分钟)
    if (12, 59) <= t < (13, 0):
        return "idle_pre_afternoon"    # 写入空窗(12:59-13:00 等下午启动)
    if (13, 0) <= t < (15, 1):
        return "afternoon_writer"      # 8 个普通写入 daemon
    # 15:01 后写入全停
    return "writer_stopped"


def _get_savedata_phase(now: datetime) -> str:
    """落盘 service 当前阶段(返回阶段名)— v6.15 用户最新统一窗口"""
    hh, mm = now.hour, now.minute
    t = (hh, mm)

    if (9, 0) <= t < (9, 10):
        return "pre_open"
    if (9, 10) <= t < (9, 14):
        return "watchlist"             # savedata_watchlist 跟着写入一起跑
    if (9, 14) <= t < (9, 26):
        return "auction_savedata"      # 等 09:29 触发 savedata_auction --once
    if (9, 26) <= t < (9, 30):
        return "idle_pre_morning"      # 落盘空窗(9:26-9:30 等普通落盘启动)
    if (9, 30) <= t < (11, 46):
        return "morning_savedata"      # 8 个普通落盘 daemon + watchlist 落盘
    if (11, 46) <= t < (12, 59):
        return "savedata_paused"       # 落盘午休(11:46-12:59)
    if (12, 59) <= t < (13, 0):
        return "idle_pre_afternoon"    # 落盘空窗(12:59-13:00)
    if (13, 0) <= t < (15, 16):
        return "afternoon_savedata"    # 8 个普通落盘 daemon + watchlist 落盘
    if (15, 16) <= t < (16, 0):
        return "post_savedata"         # 落盘也停
    return "closed"


def get_phase(now: datetime | None = None) -> tuple[str, str]:
    """返回 (writer_phase, savedata_phase)— v6.15 用户最新统一窗口

    时间线:
      - 09:00-09:10  pre_open / pre_open(空)
      - 09:10-09:14  watchlist(写+落盘都跑)
      - 09:14-09:26  auction_writer / auction_savedata(auction 抓取;等 09:29 --once)
      - 09:26-09:29  idle_pre_morning / idle_pre_morning(空窗)
      - 09:29-11:31  morning_writer(8 普通) / morning_savedata(8 普通 + watchlist)
      - 11:31-11:46  lunch / morning_savedata(写入停,落盘收尾 15 分钟)
      - 11:46-12:59  lunch / savedata_paused(都停)
      - 12:59-13:00  idle_pre_afternoon / idle_pre_afternoon
      - 13:00-15:01  afternoon_writer / afternoon_savedata
      - 15:01-15:16  writer_stopped / afternoon_savedata(写入停,落盘收尾)
      - 15:16-16:00  writer_stopped / post_savedata(都停)
    """
    now = now or datetime.now()
    weekday = now.weekday()
    if weekday >= 5:
        return ("weekend", "weekend")

    return (_get_writer_phase(now), _get_savedata_phase(now))


# ============================================================
# 子进程 spawn 列表(按类别,2026-09-11 v3)
# ============================================================
# 原子化:每个 service 只写一个 kind
# v6.15 (2026-09-16):删除 orderbook(用户原话"不再抓取");普通 kind 8 个(无 orderbook)
WRITER_DAEMONS_MORNING_AFTERNOON: list[dict] = [
    # --- 7 个常规写入 daemon(09:29-11:31 + 13:00-15:01),v6.15 删除 orderbook ---
    {"name": "service_writeredis_snapshot", "args": []},
    {"name": "service_writeredis_minute", "args": []},
    {"name": "service_writeredis_zt", "args": []},
    {"name": "service_writeredis_break", "args": []},
    {"name": "service_writeredis_anomaly", "args": []},
    {"name": "service_writeredis_hot", "args": []},
    # 涨停表现详情(盘中 30s/轮,09:29 写入阶段开始)
    {"name": "service_writeredis_limitperformance", "args": []},
    # v6.10 (2026-09-16):8 只指数,10s/轮(v6.15 改)
    {"name": "service_writeredis_snapshot_index", "args": []},
]

SAVEDATA_DAEMONS_MORNING_AFTERNOON: list[dict] = [
    # --- 8 个普通落盘 daemon(09:30-11:46 + 13:00-15:16),每 15 分钟一次,v6.15 删除 orderbook ---
    {"name": "service_savedata_snapshot", "args": []},
    {"name": "service_savedata_minute", "args": []},
    {"name": "service_savedata_zt", "args": []},
    {"name": "service_savedata_break", "args": []},
    {"name": "service_savedata_anomaly", "args": []},
    {"name": "service_savedata_hot", "args": []},
    # 涨停表现详情落盘
    {"name": "service_savedata_limitperformance", "args": []},
    # v6.10 (2026-09-16):指数落盘
    {"name": "service_savedata_snapshot_index", "args": []},
]

# writer/savedata daemon；watchlist 在 phase 之间保留，到 closed/weekend 或 scheduler 退出时清理
# v6.15:不含 orderbook(已停止抓取)
ALL_TRADE_DAEMONS: list[dict] = (
    WRITER_DAEMONS_MORNING_AFTERNOON
    + SAVEDATA_DAEMONS_MORNING_AFTERNOON
)


# ============================================================
# PHASE_SERVICES:写入阶段 × 写入 service / 落盘阶段 × 落盘 service
# ============================================================
# 写入 service 在 writer_phase 切换时 spawn/kill
# 落盘 service 在 savedata_phase 切换时 spawn/kill
# 两套独立调度
WRITER_PHASE_SERVICES: dict[str, list[dict]] = {
    "pre_open": [],
    "watchlist": [
        # 09:10-09:14:盘前监控列表一次性写 Redis
        {"name": "service_writeredis_watchlist", "args": [], "wait": True},
    ],
    "auction_writer": [
        # 09:14-09:26:auction 抓取(用户原话"9点14分开始抓取,9点26分结束抓取")
        {"name": "service_writeredis_auction", "args": [], "wait": True},
        # v6.15:limitperformance 不再 09:14 启动,09:29 才进普通阶段
    ],
    "idle_pre_morning": [
        # 09:26-09:29:写入空窗(等普通阶段启动)
    ],
    "morning_writer": [
        # 09:29-11:31:8 个普通写入 daemon(已含 snapshot_index,v6.15 删 orderbook)
        *[{**svc, "wait": True} for svc in WRITER_DAEMONS_MORNING_AFTERNOON],
    ],
    "lunch": [],
    "idle_pre_afternoon": [
        # 12:59-13:00:写入空窗(等下午启动)
    ],
    "afternoon_writer": [
        # 13:00-15:01:8 个普通写入 daemon(同 morning_writer,已含 snapshot_index)
        *[{**svc, "wait": True} for svc in WRITER_DAEMONS_MORNING_AFTERNOON],
    ],
    "writer_stopped": [],
    "closed": [],
    "weekend": [],
}

SAVEDATA_PHASE_SERVICES: dict[str, list[dict]] = {
    "pre_open": [],
    "watchlist": [
        # 09:10-09:14:跟 writeredis_watchlist 同步启动落盘 daemon
        {"name": "service_savedata_watchlist", "args": [], "wait": True},
    ],
    "auction_savedata": [
        # 09:14-09:30:无 daemon(等 09:29 触发 savedata_auction --once)
    ],
    "idle_pre_morning": [
        # 09:26-09:30:落盘空窗(等普通阶段启动)
    ],
    "morning_savedata": [
        # 09:30-11:46:8 个普通落盘 daemon(已含 savedata_snapshot_index)+ watchlist 落盘
        *[{**svc, "wait": True} for svc in SAVEDATA_DAEMONS_MORNING_AFTERNOON],
        {"name": "service_savedata_watchlist", "args": [], "wait": True},
    ],
    "savedata_paused": [],
    "idle_pre_afternoon": [
        # 12:59-13:00:落盘空窗
    ],
    "afternoon_savedata": [
        # 13:00-15:16:8 个普通落盘 daemon + watchlist 落盘
        *[{**svc, "wait": True} for svc in SAVEDATA_DAEMONS_MORNING_AFTERNOON],
        {"name": "service_savedata_watchlist", "args": [], "wait": True},
    ],
    "post_savedata": [],
    "closed": [],
    "weekend": [],
}

PHASE_SERVICES: dict[str, list[dict]] = {
    # 已废弃(v3 改用 WRITER_PHASE_SERVICES + SAVEDATA_PHASE_SERVICES)
    # 保留空 dict 防止 AttributeError
}

# Backward compatibility alias(给旧代码引用)
TRADE_LOOP_DAEMONS = ALL_TRADE_DAEMONS

# ============================================================
# 一次性触发点(scheduler 主循环按时间点触发)
# ============================================================
# 格式:(小时, 分钟, service_name, args 列表)
ONCE_TRIGGERS: list[tuple[int, int, str, list[str]]] = [
    (9, 0,  "service_cleanredis_online",  []),
    (9, 29, "service_savedata_auction",   []),                                  # auction 数据 09:14-09:26,09:29 落
    (15, 15, "service_savedata_auction",  []),                                  # v6.15 兜底补落(用户原话"auction 落盘在 15 点 15 分补落一次")
    (3, 0,  "service_cleanredis_online",  []),                                  # 生产真值:凌晨 03:00 清残留(2026-09-16 用户拍板,默认 3 点不再回 16:00)
]

# 已触发的点(当天)— 避免重复触发
_triggered_today: set[tuple[int, int, str]] = set()
_last_trigger_date: str = ""  # 跨天后清空 set


def reset_trigger_state_if_new_day(now: datetime) -> None:
    """每天 0 点重置已触发状态"""
    global _triggered_today, _last_trigger_date
    today = now.strftime("%Y%m%d")
    if today != _last_trigger_date:
        _triggered_today = set()
        _last_trigger_date = today


def check_once_triggers(now: datetime, log: logging.Logger) -> None:
    """主循环里每分钟检查一次,看是否到点触发"""
    reset_trigger_state_if_new_day(now)
    hh, mm = now.hour, now.minute
    for (th, tm, name, args) in ONCE_TRIGGERS:
        if hh == th and mm == tm:
            key = (th, tm, name)
            if key in _triggered_today:
                continue
            _triggered_today.add(key)
            log.info(f"[once_trigger] {hh:02d}:{tm:02d} → {name} {args}")
            spawn_service(name, args + ["--once"], log=log)


# ============================================================
# 子进程 spawn 函数
# ============================================================
def spawn_service(name: str, args: list[str], *, log: logging.Logger, timeout_sec: int = 300) -> int:
    """同步启动一个 service 子进程,返回 rc

    设计:
    - 不 capture_output(子进程自己写 logs/<name>_YYYYMMDD.log,launchd 后台跑也能看)
    - 如果想看实时输出,去 tail logs/<name>_YYYYMMDD.log

    注意(2026-09-11):
    - `--once` 时:同步等子进程跑完,返回 rc
    - 非 --once 时:异步 Popen,**立即返回 0**,子进程自己 while True 跑
    """
    cmd = [
        sys.executable,
        "-u",  # unbuffered
        "-m",
        f"service.{name}",
        *args,
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SCRIPTS_DIR) + os.pathsep + env.get("PYTHONPATH", "")

    is_once = "--once" in args
    log.info(f"spawn: {name} args={args} mode={'once' if is_once else 'daemon'}")

    if is_once:
        # 同步模式:等子进程跑完返回 rc
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(SCRIPTS_DIR),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout_sec,
                check=False,
            )
            rc = proc.returncode
            if rc != 0:
                log.warning(f"{name} 退出 rc={rc}(看 logs/{name}_*.log)")
            else:
                log.info(f"{name} 完成 rc=0")
            return rc
        except subprocess.TimeoutExpired:
            log.error(f"{name} 超时 ({timeout_sec}s)")
            return 124
        except Exception as e:
            log.error(f"{name} 异常: {e}", exc_info=True)
            return 1
    else:
        # 异步模式:Popen 后立即返回 0,子进程自己 while True 跑
        # phase 变化时上层会先 kill 旧进程
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(SCRIPTS_DIR),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,  # 独立进程组,方便 kill
            )
            _register_daemon(name, proc)
            log.info(f"{name} daemon 启动 pid={proc.pid}")
            return 0
        except Exception as e:
            log.error(f"{name} 启动失败: {e}", exc_info=True)
            return 1


@dataclass
class _OwnedDaemon:
    """持有 Popen 身份和独立进程组，避免仅凭可复用的 PID 发信号。"""

    proc: subprocess.Popen
    pid: int
    pgid: int


_DAEMON_PIDS: dict[str, _OwnedDaemon] = {}
_DAEMON_STOP_TIMEOUT_SEC = 5.0
_DAEMON_KILL_TIMEOUT_SEC = 2.0


def _register_daemon(name: str, proc: subprocess.Popen) -> None:
    """登记所有权。daemon 必须以 start_new_session=True 启动。"""
    _DAEMON_PIDS[name] = _OwnedDaemon(proc=proc, pid=proc.pid, pgid=proc.pid)


def _send_owned_group_signal(owned: _OwnedDaemon, sig: int) -> bool:
    """只在 Popen 仍确认其子进程存活且仍是原进程组 leader 时发信号。"""
    if owned.proc.pid != owned.pid or owned.proc.poll() is not None:
        return False
    try:
        if os.getpgid(owned.pid) != owned.pgid:
            return False
        os.killpg(owned.pgid, sig)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False


def kill_daemon(
    name: str,
    log: logging.Logger,
    *,
    timeout_sec: float = _DAEMON_STOP_TIMEOUT_SEC,
    kill_timeout_sec: float = _DAEMON_KILL_TIMEOUT_SEC,
) -> bool:
    """优雅停止已登记 daemon，超时后升级 SIGKILL，并确认直接子进程退出。"""
    owned = _DAEMON_PIDS.get(name)
    if owned is None:
        return True

    if owned.proc.poll() is None:
        _send_owned_group_signal(owned, signal.SIGTERM)
    try:
        owned.proc.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        log.warning(f"停止 {name} pid={owned.pid} 超时 ({timeout_sec:g}s)，升级 SIGKILL")
        if owned.proc.poll() is None:
            _send_owned_group_signal(owned, signal.SIGKILL)
        try:
            owned.proc.wait(timeout=kill_timeout_sec)
        except subprocess.TimeoutExpired:
            log.error(f"停止 {name} pid={owned.pid} 仍超时 ({kill_timeout_sec:g}s)，保留所有权记录")
            return False
    except Exception as e:
        log.error(f"等待 {name} pid={owned.pid} 退出失败: {e}", exc_info=True)
        return False

    if owned.proc.poll() is None:
        log.error(f"{name} pid={owned.pid} wait 返回但进程仍存活，保留所有权记录")
        return False
    try:
        os.killpg(owned.pgid, 0)
    except ProcessLookupError:
        pass
    except OSError as e:
        log.error(f"{name} pid={owned.pid} 无法确认进程组退出: {e}，保留所有权记录")
        return False
    else:
        # leader 已由 Popen wait 回收，不能再按数值 PGID 发信号，以免 PID/PGID 复用。
        log.error(f"{name} pid={owned.pid} 已退出但进程组 {owned.pgid} 仍存在；不对未验证的 PGID 发信号")
        return False
    if _DAEMON_PIDS.get(name) is owned:
        _DAEMON_PIDS.pop(name, None)
    log.info(f"daemon 已退出 {name} pid={owned.pid}")
    return True


def kill_all_trade_daemons(log: logging.Logger) -> bool:
    """杀掉所有 writeredis/savedata 类别下的 daemon(写入 + 落盘全 kill)

    2026-09-11 v3:不再区分 trade_loop 阶段,统一按"在跑 daemon 全部 kill"
    用于 closed / 周末 / 异常退出 时全清理
    """
    stopped = []
    failed = []
    for name in list(_DAEMON_PIDS.keys()):
        # watchlist 也由同一 scheduler 启动，必须在 shutdown 时收回。
        if name.startswith("service_writeredis_") or name.startswith("service_savedata_"):
            (stopped if kill_daemon(name, log) else failed).append(name)
    if failed:
        log.error(f"kill_all_trade_daemons: 已确认退出 {len(stopped)} 个 daemon，未确认 {failed}")
        return False
    if stopped:
        log.info(f"kill_all_trade_daemons: 已确认退出 {len(stopped)} 个 daemon")
    else:
        log.debug("kill_all_trade_daemons: 无 daemon 在跑")
    return True


def run_phase_services(
    phase: str,
    phase_services: dict[str, list[dict]],
    pid_scope: str,
    log: logging.Logger,
) -> int:
    """停止旧 daemon 并启动新 phase；旧进程未确认退出时不启动替代进程。"""
    services = phase_services.get(phase, [])
    new_names = {svc["name"] for svc in services}
    failed_stops = []

    for old_name in list(_DAEMON_PIDS.keys()):
        if old_name in new_names:
            continue
        if pid_scope == "writer" and old_name.startswith("service_writeredis_"):
            if old_name == "service_writeredis_watchlist" and phase not in {"closed", "weekend"}:
                continue
            if not kill_daemon(old_name, log):
                failed_stops.append(old_name)
        elif pid_scope == "savedata" and old_name.startswith("service_savedata_"):
            if not kill_daemon(old_name, log):
                failed_stops.append(old_name)

    # A same-name phase replacement must also wait for the previous Popen to exit.
    for svc in services:
        name = svc["name"]
        if name == "service_writeredis_watchlist" and name in _DAEMON_PIDS:
            continue  # startup补写已登记该进程，watchlist phase 不重复替换。
        if name in _DAEMON_PIDS and not kill_daemon(name, log):
            failed_stops.append(name)

    if failed_stops:
        log.error("[%s] phase=%s 停止旧 daemon 未确认退出，不启动新服务: %s", pid_scope, phase, failed_stops)
        return 1
    if not services:
        log.debug(f"[{pid_scope}] phase={phase} 无 service 需要执行")
        return 0

    log.info(f"[{pid_scope}] phase={phase} 启动 {len(services)} 个 service")
    max_rc = 0
    for svc in services:
        name = svc["name"]
        if name == "service_writeredis_watchlist" and name in _DAEMON_PIDS:
            continue  # 已由启动补写或上一阶段启动，保持同一所有权实例。
        rc = spawn_service(name, svc["args"], log=log)
        max_rc = max(max_rc, abs(rc))
        if rc != 0:
            log.warning(f"{svc['name']} 失败,但继续下一个")

    return max_rc


# ============================================================
# 主循环
# ============================================================
from core.logger import setup_logger  # noqa: E402


# 控制标志
_should_exit = False


def signal_handler(signum, frame):
    global _should_exit
    _should_exit = True


def _online_service_modules() -> set[str]:
    """启动清理只接受调度器明确启动的在线 writer/savedata 模块名。"""
    names = {f"service.{item['name']}" for item in WRITER_DAEMONS_MORNING_AFTERNOON}
    names.update(f"service.{item['name']}" for item in SAVEDATA_DAEMONS_MORNING_AFTERNOON)
    names.update({
        "service.service_writeredis_watchlist", "service.service_writeredis_auction",
        "service.service_savedata_watchlist", "service.service_savedata_auction",
    })
    return names


def _read_process_table() -> dict[int, tuple[int, str, str]] | None:
    """读取 PID、PGID、启动时刻和命令行；lsof 再确认 cwd。"""
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,pgid=,lstart=,command="],
            capture_output=True, text=True, check=False,
        )
    except OSError as e:
        logging.getLogger("scheduler_onlineData").error("读取进程表失败: %s", e)
        return None
    rows: dict[int, tuple[int, str, str]] = {}
    if result.returncode != 0:
        log = logging.getLogger("scheduler_onlineData")
        log.error("读取进程表失败，无法确认启动残留: %s", result.stderr.strip())
        return None
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 7)
        if len(fields) != 8:
            continue
        try:
            rows[int(fields[0])] = (int(fields[1]), " ".join(fields[2:7]), fields[7])
        except ValueError:
            continue
    return rows


def _process_cwd(pid: int) -> str | None:
    try:
        result = subprocess.run(
            ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
            capture_output=True, text=True, check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        if line.startswith("n"):
            return line[1:]
    return None


def _module_from_command(command: str) -> str | None:
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    for index, arg in enumerate(argv[:-1]):
        if arg == "-m":
            return argv[index + 1]
    return None


def kill_all_daemons(log: logging.Logger) -> bool:
    """清理本 checkout 的精确在线残留；检查失败或未确认进程组退出均 fail closed。"""
    project_cwd = str(SCRIPTS_DIR.resolve())
    allowed = _online_service_modules()
    process_table = _read_process_table()
    if process_table is None:
        log.error("启动清理无法读取进程表，fail closed")
        return False

    candidates = []
    for pid, (pgid, started_at, command) in process_table.items():
        if pid in {os.getpid(), os.getppid()} or pgid != pid:
            continue
        module = _module_from_command(command)
        if module not in allowed:
            continue
        cwd = _process_cwd(pid)
        if cwd is None:
            log.error("无法验证候选残留 %s pid=%s 的 cwd，fail closed", module, pid)
            return False
        if cwd != project_cwd:
            continue
        candidates.append((pid, pgid, started_at, command, module))

    cleaned = []
    for pid, pgid, started_at, command, module in candidates:
        # TERM 前重核 PID/PGID/启动时刻/命令/cwd，避免对复用 PID 发信号。
        current_table = _read_process_table()
        if current_table is None:
            log.error("启动清理复核进程表失败，fail closed")
            return False
        current = current_table.get(pid)
        identity = (pgid, started_at, command)
        if current is None or current != identity:
            cleaned.append(pid)  # 原进程退出或数值 PID 已复用，绝不信号当前进程。
            continue
        cwd = _process_cwd(pid)
        if cwd is None:
            log.error("无法复核候选残留 %s pid=%s 的 cwd，fail closed", module, pid)
            return False
        if cwd != project_cwd:
            log.error("候选残留 %s pid=%s 已不在本项目 cwd，fail closed", module, pid)
            return False

        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            cleaned.append(pid)
            continue
        except OSError as e:
            log.error("启动清理无法停止 %s pid=%s: %s", module, pid, e)
            continue

        deadline = time.monotonic() + _DAEMON_STOP_TIMEOUT_SEC
        exited = False
        last_members: list[int] = []
        while time.monotonic() < deadline:
            table_after_term = _read_process_table()
            if table_after_term is None:
                log.error("TERM 后无法读取进程表确认 %s 的 PGID=%s，fail closed", module, pgid)
                break
            # 不再向该数值 PGID 发信号；若它被复用，此处只会保守判定未确认。
            last_members = [member_pid for member_pid, (member_pgid, *_)
                            in table_after_term.items() if member_pgid == pgid]
            if not last_members:
                cleaned.append(pid)
                exited = True
                break
            time.sleep(0.1)
        if not exited:
            log.error("启动残留 %s pid=%s 的进程组仍未确认退出，剩余成员=%s", module, pid, last_members[:8])

    if cleaned:
        log.info("启动清理:已确认退出 %d 个本项目在线残留进程组", len(cleaned))
    return len(cleaned) == len(candidates)


def _reload_watchlist_if_in_trade_window(log: logging.Logger) -> None:
    """交易时间段内 [09:10, 15:30] 每次 scheduler 启动都补一次 watchlist

    背景:
      - 09:10-09:14 watchlist 阶段启 service_writeredis_watchlist 一次性写 Redis
      - 09:14 之后该 service 进入 idle sleep,等待 15:35 closed 阶段被杀
      - 如果盘中(09:14-15:30)scheduler 被 kill_all_daemons 重启,watchlist 进程被杀掉
      - 此时盘中阶段(morning_writer / afternoon_writer)不会主动启 watchlist
      - 需要 scheduler 启动时显式补一次,让 Redis 里的 watchlist 数据保持最新

    实现:
      - 当前时间在 [09:10, 15:30] 内 → subprocess.Popen 启 watchlist daemon
      - watchlist daemon 内部先写一次 Redis,再 idle sleep,15:35 阶段切换时被 kill
      - 使用统一 spawn_service 登记 Popen 和独立进程组；phase 切换或 scheduler finally 清理
    """
    now = datetime.now()
    hh, mm = now.hour, now.minute
    t = hh * 60 + mm

    # 交易窗口:09:10 - 15:30(确保 watchlist 阶段结束后到落盘全部结束前的窗口都覆盖)
    if not (9 * 60 + 10 <= t <= 15 * 60 + 30):
        log.info(f"[{now.strftime('%H:%M:%S')}] 非交易窗口,跳过 watchlist 补写")
        return

    # 周末跳过
    if now.weekday() >= 5:
        log.info("周末,跳过 watchlist 补写")
        return

    log.info(f"[{now.strftime('%H:%M:%S')}] 交易窗口内启动,补一次 watchlist service")

    rc = spawn_service("service_writeredis_watchlist", [], log=log)
    if rc == 0:
        log.info("watchlist service 已登记到 scheduler 所有权表")
    else:
        log.error("补写 watchlist 失败 rc=%s", rc)


def _run_scheduler_loop_body(*, log: logging.Logger, scan_interval_sec: float = 60.0, dry_run: bool = False) -> bool:
    """主循环:每分钟扫描一次阶段,阶段变化时触发 spawn

    2026-09-11 v3:writer_phase + savedata_phase 双阶段独立跟踪
    """
    global _should_exit
    last_writer_phase = None
    last_savedata_phase = None

    log.info(f"scheduler_onlineData 启动 dry_run={dry_run} scan_interval={scan_interval_sec}s")

    # 2026-09-11:启动时清理残留 daemon 进程；若退出未确认，不启动新服务。
    if not kill_all_daemons(log):
        log.error("启动残留清理未完成，fail closed：本轮不补 watchlist，也不启动 phase 服务")
        return False

    # 2026-09-11:交易时间段内每次重启都补一次 watchlist
    # (盘前 watchlist 阶段结束后,该 service 就 idle sleep 等待被杀;
    #  盘中重启时它已被 kill_all_daemons 杀掉,需要 scheduler 显式补一次)
    _reload_watchlist_if_in_trade_window(log)

    try:
        r = OnlineRedis()
        r.set_meta("scheduler_started_at", datetime.now().isoformat(timespec="milliseconds"))
    except Exception as e:
        log.error(f"Redis 初始化失败: {e}")
        r = None

    while not _should_exit:
        now = datetime.now()
        ts = now.strftime("%H:%M:%S")

        # 1. 一次性触发检查(每分钟)
        if not dry_run:
            check_once_triggers(now, log)

        # 2. 阶段判断 + 双阶段独立 spawn
        writer_phase, savedata_phase = get_phase(now)

        if writer_phase != last_writer_phase or savedata_phase != last_savedata_phase:
            log.info(f"[{ts}] 阶段变化: writer={last_writer_phase}→{writer_phase}, savedata={last_savedata_phase}→{savedata_phase}")

            if not dry_run:
                # 写入侧阶段变化
                if writer_phase != last_writer_phase:
                    if run_phase_services(writer_phase, WRITER_PHASE_SERVICES, "writer", log) != 0:
                        log.critical("writer phase 服务切换未完成，停止 scheduler")
                        return False
                    last_writer_phase = writer_phase
                # 落盘侧阶段变化
                if savedata_phase != last_savedata_phase:
                    if run_phase_services(savedata_phase, SAVEDATA_PHASE_SERVICES, "savedata", log) != 0:
                        log.critical("savedata phase 服务切换未完成，停止 scheduler")
                        return False
                    last_savedata_phase = savedata_phase
                if savedata_phase in {"closed", "weekend"}:
                    if not kill_daemon("service_writeredis_watchlist", log):
                        log.critical("收盘时 watchlist daemon 未确认退出，停止 scheduler")
                        return False

                if r:
                    r.set_meta("scheduler_writer_phase", writer_phase)
                    r.set_meta("scheduler_savedata_phase", savedata_phase)
                    r.set_meta("scheduler_last_ts", now.isoformat(timespec="milliseconds"))
            else:
                log.info(f"[dry_run] 不会实际跑 service")
        else:
            log.debug(f"[{ts}] writer={writer_phase} savedata={savedata_phase} 无变化")

        time.sleep(scan_interval_sec)

    log.info("收到退出信号,scheduler 退出")
    return True


def run_scheduler_loop(*, log: logging.Logger, scan_interval_sec: float = 60.0, dry_run: bool = False) -> bool:
    """运行原有时段循环；返回 False 表示 phase 或进程清理未确认完成。"""
    try:
        body_ok = _run_scheduler_loop_body(log=log, scan_interval_sec=scan_interval_sec, dry_run=dry_run)
    except BaseException:
        if not kill_all_trade_daemons(log):
            log.critical("scheduler 异常退出时仍有自建进程未确认退出")
        raise
    cleanup_ok = kill_all_trade_daemons(log)
    return bool(body_ok) and cleanup_ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Scheduler: onlineData (顶层调度器)")
    parser.add_argument("--daemon", action="store_true", help="后台模式(launchd 用)")
    parser.add_argument("--scan-interval", type=float, default=60.0, help="扫描间隔秒数")
    parser.add_argument("--dry-run", action="store_true", help="只打印不实际执行")
    args = parser.parse_args()

    log = setup_logger("scheduler_onlineData")
    log.info(f"启动 scheduler_onlineData args={vars(args)}")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if args.dry_run:
        # dry-run: 打印所有阶段对应的 spawn 计划
        log.info("=== DRY RUN:v3 双阶段(write + savedata)独立调度 ===")
        log.info("")
        log.info("--- 写入 service (WRITER_PHASE_SERVICES) ---")
        for phase in WRITER_PHASES:
            svcs = WRITER_PHASE_SERVICES.get(phase, [])
            log.info(f"  writer phase={phase}: {len(svcs)} services")
            for svc in svcs:
                log.info(f"    - python3 -m service.{svc['name']} {' '.join(svc['args'])}")
        log.info("")
        log.info("--- 落盘 service (SAVEDATA_PHASE_SERVICES) ---")
        for phase in SAVEDATA_PHASES:
            svcs = SAVEDATA_PHASE_SERVICES.get(phase, [])
            log.info(f"  savedata phase={phase}: {len(svcs)} services")
            for svc in svcs:
                log.info(f"    - python3 -m service.{svc['name']} {' '.join(svc['args'])}")
        log.info("")
        log.info("--- ONCE TRIGGERS ---")
        for (hh, mm, name, args2) in ONCE_TRIGGERS:
            log.info(f"  - {hh:02d}:{mm:02d} → python3 -m service.{name} {' '.join(args2)} --once")
        return 0

    try:
        if not run_scheduler_loop(log=log, scan_interval_sec=args.scan_interval, dry_run=False):
            return 1
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

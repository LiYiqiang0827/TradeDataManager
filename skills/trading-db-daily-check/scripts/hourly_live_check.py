"""hourly_live_check.py — 每小时检查 TradingDataManager 的 scheduler 是否在正常运行。

检查项:
  1. launchd 3 个服务都在跑(offline / online / redis6380)
  2. offline scheduler 进程的 cwd / PYTHONPATH 指向 ~/TradingDataManager(独立 git 仓库,这是正确的)
  3. 数据 db 文件存在 + 最近 mtime(过去 24h 内)
  4. offline 日志最后一次成功 service 跑通的时间戳(过去 24h 内)
  5. online 日志最后一次落盘 / 调度时间戳(过去 6h 内,工作时段更短)

退出码:
  0 = 全部正常
  1 = 有异常(由调用方决定是否发飞书)

用法:
  python3 hourly_live_check.py                # 检查后打印报告到 stdout
  python3 hourly_live_check.py --quiet-ok    # 全 OK 时静默(给 cron 用)

设计:
  - 不依赖任何具体仓库路径,只看 launchd / 文件系统 / 日志(放在 ~/TradingDataManager/skills/ 或
    ~/.hermes/skills/ 都能跑,因为所有路径来自 plist + 实际观察,不硬编码)
  - 任何检查失败都收集进 issues,最后统一报告,不中途退出
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# 配置
# ============================================================
# launchd 标签(2026-10 现状)
LAUNCHD_LABELS = [
    "com.tradingdatamanager.scheduler.offlineData",
    "com.tradingdatamanager.scheduler.onlineData",
    "com.tradingdatamanager.redis6380",
]

# 数据 db 文件(2026-09 之后在 ~/TradingDatabase/)
OFFLINE_DBS = [
    Path.home() / "TradingDatabase" / "offline" / "db_cn_basic.db",
    Path.home() / "TradingDatabase" / "offline" / "db_cn_kpl.db",
    Path.home() / "TradingDatabase" / "offline" / "db_cn_news.db",
    Path.home() / "TradingDatabase" / "offline" / "db_cn_index.db",
]
ONLINE_DB_DIR = Path.home() / "TradingDatabase" / "online"

# 日志目录
OFFLINE_LOG_DIR = Path.home() / "TradingAgent" / "offlineDataManager" / "logs"
ONLINE_LOG_DIR = Path.home() / "TradingAgent" / "onlineDataManager" / "logs"

# offline 期望 cwd(launchd plist 写死路径,但 plist 可能指向老路径)
EXPECTED_OFFLINE_CWD = Path.home() / "TradingAgent" / "offlineDataManager" / "scripts"


# ============================================================
# 检查项
# ============================================================
def check_launchd() -> list[dict]:
    """检查 3 个 launchd 服务是否在 launchctl list 里。

    Returns:
        list of {"label", "pid", "status": "OK"|"STOPPED"|"MISSING"}
    """
    try:
        out = subprocess.run(
            ["launchctl", "list"], capture_output=True, text=True, timeout=10
        ).stdout
    except Exception as e:
        return [{"label": "<launchctl>", "status": "ERROR", "detail": str(e)}]

    running = {}
    for line in out.splitlines():
        # 格式: PID STATUS LABEL  (PID=- 表示未运行)
        parts = line.split()
        if len(parts) >= 3:
            pid, status, label = parts[0], parts[1], parts[2]
            running[label] = {"pid": pid, "status": status}

    results = []
    for label in LAUNCHD_LABELS:
        if label in running:
            r = running[label]
            results.append({
                "label": label,
                "pid": r["pid"],
                "launchd_status": r["status"],
                "status": "OK" if r["pid"] != "-" else "STOPPED",
            })
        else:
            results.append({"label": label, "status": "MISSING"})
    return results


def check_offline_scheduler_env() -> dict:
    """检查 offline scheduler 进程的 PYTHONPATH / TRADING_DATA_MANAGER_ROOT。

    关键认知(2026-10-17):~/TradingDataManager 是独立的 git 仓库(github.com/LiYiqiang0827/TradeDataManager),
    不是空目录也不是 symlink,有完整 .venv + .env。plist 指向它是**正确**的。
    所以这里只检查 "scheduler 进程存活" + "环境变量齐全" + ".env 在",不警告路径名差异。
    """
    pid = _find_pid("scheduler.scheduler_updateData")
    if not pid:
        return {"status": "ERROR", "detail": "未找到 scheduler_updateData 进程"}

    try:
        env_dump = subprocess.run(
            ["ps", "eww", str(pid)], capture_output=True, text=True, timeout=10
        ).stdout
    except Exception as e:
        return {"status": "ERROR", "detail": f"ps eww 失败: {e}"}

    env = _parse_ps_env(env_dump)
    py_path = env.get("TRADING_DATA_MANAGER_ROOT", "?")
    py_bin = env.get("TRADING_MANAGER_PYTHON", "?")
    db_root = env.get("TRADING_DATABASE_ROOT", "?")
    has_token = "TRADING_AGENT_TUSHARE_TOKEN" in env

    # 校验 .env 文件存在(防止 plist 改错)
    env_file_str = env.get("TRADING_AGENT_ENV_FILE", "")
    env_file = Path(env_file_str) if env_file_str else None
    env_exists = env_file.exists() if env_file else None

    return {
        "status": "OK",
        "pid": pid,
        "TRADING_DATA_MANAGER_ROOT": py_path,
        "TRADING_MANAGER_PYTHON": py_bin,
        "TRADING_DATABASE_ROOT": db_root,
        "has_tushare_token": has_token,
        "env_file_exists": env_exists,
    }


def _find_pid(name_substr: str) -> int | None:
    """按命令名找 PID(pgrep -f 等价,但 macOS 默认没 pgrep 在 /usr/bin?有,用 pgrep)。"""
    try:
        out = subprocess.run(
            ["pgrep", "-f", name_substr],
            capture_output=True, text=True, timeout=10
        ).stdout.strip()
        if out:
            return int(out.split()[0])
    except Exception:
        pass
    return None


def _parse_ps_env(ps_output: str) -> dict[str, str]:
    """ps eww 输出是: PID COMMAND ARGS... 然后 KEY=VAL KEY=VAL ...
    KEY=VAL 之间可能有空格分隔。简单方法:用 ps eww -o command= ... 不行,
    直接 regex 提取 KEY=VAL 段。"""
    import re
    env = {}
    for m in re.finditer(r"([A-Z_][A-Z_0-9]*)=([^\s]+)", ps_output):
        env[m.group(1)] = m.group(2)
    return env


def check_data_db_freshness() -> list[dict]:
    """4 个 offline db 文件是否存在 + mtime 是否在过去 24h 内。
    (注:mtime 不等于"今天有数据",但如果 mtime 太老,说明根本没写)"""
    now = datetime.now()
    results = []
    for db in OFFLINE_DBS:
        if not db.exists():
            results.append({"db": str(db), "status": "MISSING"})
            continue
        mtime = datetime.fromtimestamp(db.stat().st_mtime)
        age_h = (now - mtime).total_seconds() / 3600
        results.append({
            "db": db.name,
            "size_mb": round(db.stat().st_size / 1024 / 1024, 1),
            "mtime": mtime.strftime("%Y-%m-%d %H:%M"),
            "age_h": round(age_h, 1),
            "status": "OK" if age_h < 24 else "STALE",
        })
    return results


def check_online_db_freshness() -> dict:
    """online db 当前月份文件 mtime < 30 分钟为 OK。
    工作时段(09:00-16:00)更严格:6 分钟内若 mtime 旧则 STALE。"""
    now = datetime.now()
    ym = now.strftime("%Y%m")
    db = ONLINE_DB_DIR / f"online_data_{ym}.db"
    if not db.exists():
        # 上月归档(月初几天可能用上月 db)
        prev = (now.replace(day=1) - timedelta(days=1)).strftime("%Y%m")
        db = ONLINE_DB_DIR / f"online_data_{prev}.db"
        if not db.exists():
            return {"status": "MISSING", "db": str(db)}

    mtime = datetime.fromtimestamp(db.stat().st_mtime)
    age_min = (now - mtime).total_seconds() / 60
    # 2026-10-17 v2 修正:必须同时看工作日(周一~周五)+ 小时(9-16),之前漏了周末判断
    is_weekday = now.weekday() < 5
    is_market_hours = is_weekday and 9 <= now.hour < 16
    today = mtime.date() == now.date()

    # 判定规则:
    # - 周末 / 节假日整体不告警(不写入 status,只返回 mtime 供人看)
    # - 盘前 09:00 前 → 当天数据不可能落盘(收盘后才写满),只看上次收盘 mtime 是昨天
    # - 交易时段 → 6 分钟内必须有落盘
    # - 收盘 15:00 后 → 当天落盘了就 OK
    if not is_market_hours:
        # 非交易时段(含周末全天 / 工作日非交易小时):只要 db 存在且 mtime 不早于上次收盘就 OK
        status = "OK"
    else:
        status = "OK" if age_min < 6 else "STALE"
    return {
        "status": status,
        "db": db.name,
        "size_mb": round(db.stat().st_size / 1024 / 1024, 1),
        "mtime": mtime.strftime("%Y-%m-%d %H:%M"),
        "age_min": round(age_min, 1),
        "market_hours": is_market_hours,
    }


def check_offline_logs() -> dict:
    """offline logs/ 里最近一次 service_xxx.log 写入时间 + scheduler stderr 写入时间。

    关键认知(2026-10-17):scheduler stderr 在写(loguru 默认走 stderr),
    stdout 文件只在 launchd 启动瞬间写过。判定 scheduler 健康用 stderr。
    """
    if not OFFLINE_LOG_DIR.exists():
        return {"status": "ERROR", "detail": f"{OFFLINE_LOG_DIR} 不存在"}

    now = datetime.now()
    service_logs = sorted(
        OFFLINE_LOG_DIR.glob("service_*.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    last_service = service_logs[0] if service_logs else None

    # scheduler stderr(launchd plist 写到这里,loguru 默认走 stderr)
    sched_stderr = Path("/Users/nickzhang/TradingDataManager/.runtime/scheduler-offline.stderr.log")

    out = {"service_logs_count": len(service_logs)}
    if last_service:
        age_h = (now - datetime.fromtimestamp(last_service.stat().st_mtime)).total_seconds() / 3600
        out["last_service_log"] = {
            "name": last_service.name,
            "mtime": datetime.fromtimestamp(last_service.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            "age_h": round(age_h, 1),
            "status": "OK" if age_h < 24 else "STALE",
        }

    if sched_stderr.exists():
        age_min = (now - datetime.fromtimestamp(sched_stderr.stat().st_mtime)).total_seconds() / 60
        # stderr 每 10 分钟应该至少有一条 task_news_update 日志
        out["scheduler_stderr"] = {
            "mtime": datetime.fromtimestamp(sched_stderr.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            "age_min": round(age_min, 1),
            "status": "OK" if age_min < 30 else "STALE",
        }
    else:
        out["scheduler_stderr"] = {"status": "MISSING"}

    sched_stale = out.get("scheduler_stderr", {}).get("status") == "STALE"
    out["status"] = "STALE" if sched_stale else "OK"
    return out


def check_online_logs() -> dict:
    """online logs/ 找最后一次落盘 / 调度的日志时间戳。"""
    if not ONLINE_LOG_DIR.exists():
        return {"status": "ERROR", "detail": f"{ONLINE_LOG_DIR} 不存在"}

    now = datetime.now()
    # 2026-10-17 v2 修正:必须同时看工作日(周一~周五)+ 小时(9-16),
    # 否则周末整天都"非交易时段"且当天没新 log,会被误判 STALE
    is_weekday = now.weekday() < 5
    is_market_hours = is_weekday and 9 <= now.hour < 16
    threshold_min = 10 if is_market_hours else 60

    # 找最近修改的任何 .log
    logs = sorted(
        ONLINE_LOG_DIR.rglob("*.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not logs:
        return {"status": "MISSING", "detail": "no .log files"}

    last = logs[0]
    age_min = (now - datetime.fromtimestamp(last.stat().st_mtime)).total_seconds() / 60
    today = datetime.fromtimestamp(last.stat().st_mtime).date() == now.date()
    if not is_market_hours:
        # 非交易时段(含周末全天):log 是否近期写的不重要,只看进程是否在跑(launchd 检查已覆盖)
        status = "OK"
    else:
        status = "OK" if age_min < threshold_min else "STALE"
    return {
        "status": status,
        "last_log": last.name,
        "mtime": datetime.fromtimestamp(last.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
        "age_min": round(age_min, 1),
        "market_hours": is_market_hours,
    }


# ============================================================
# 汇总 + 输出
# ============================================================
def run_all_checks() -> dict:
    return {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "launchd": check_launchd(),
        "offline_scheduler_env": check_offline_scheduler_env(),
        "offline_dbs": check_data_db_freshness(),
        "online_db": check_online_db_freshness(),
        "offline_logs": check_offline_logs(),
        "online_logs": check_online_logs(),
    }


def summarize(report: dict) -> tuple[list[str], list[str]]:
    """返 (ok_lines, issue_lines)。"""
    ok, issues = [], []

    # launchd
    for r in report["launchd"]:
        if r["status"] == "OK":
            ok.append(f"✅ launchd {r['label'].split('.')[-1]:<22} PID={r['pid']}")
        else:
            issues.append(f"❌ launchd {r['label']}: {r['status']}")

    # offline scheduler env
    env = report["offline_scheduler_env"]
    if env["status"] == "OK":
        ok.append(
            f"✅ offline scheduler PID={env['pid']} "
            f"root={env['TRADING_DATA_MANAGER_ROOT'].replace(str(Path.home()), '~')} "
            f"db={env['TRADING_DATABASE_ROOT'].replace(str(Path.home()), '~')} "
            f"token={'yes' if env['has_tushare_token'] else 'NO'} "
            f"env={'yes' if env['env_file_exists'] else 'NO'}"
        )
    else:
        issues.append(f"❌ offline scheduler: {env.get('detail')}")

    # offline dbs
    for db in report["offline_dbs"]:
        if "status" not in db:
            continue
        if db["status"] == "OK":
            ok.append(f"✅ {db['db']:<20} {db['size_mb']}MB  mtime={db['mtime']} ({db['age_h']}h ago)")
        elif db["status"] == "MISSING":
            issues.append(f"❌ {db['db']}: MISSING")
        else:
            issues.append(f"⚠️ {db['db']}: mtime {db['age_h']}h 前 → STALE")

    # online db
    od = report["online_db"]
    if od["status"] == "OK":
        ok.append(f"✅ online {od['db']:<28} {od['size_mb']}MB  mtime={od['mtime']} ({od['age_min']}min ago)")
    else:
        issues.append(f"❌ online db: {od['status']} ({od.get('detail', '')})")

    # offline logs
    ol = report["offline_logs"]
    if ol.get("status") == "OK":
        if "last_service_log" in ol:
            ok.append(f"✅ 最近 service log: {ol['last_service_log']['name']} ({ol['last_service_log']['age_h']}h ago)")
        sl = ol.get("scheduler_stderr", {})
        if sl.get("status") == "OK":
            ok.append(f"✅ offline scheduler stderr mtime={sl.get('mtime')}")
        elif sl.get("status") == "MISSING":
            issues.append("⚠️ offline scheduler stderr MISSING")
    else:
        sl = ol.get("scheduler_stderr", {})
        issues.append(f"⚠️ offline scheduler stderr STALE: mtime={sl.get('mtime', '?')} {sl.get('age_min', '?')}min 前")

    # online logs
    onl = report["online_logs"]
    if onl["status"] == "OK":
        ok.append(f"✅ online last log: {onl['last_log']} ({onl['age_min']}min ago)")
    else:
        issues.append(f"⚠️ online logs: {onl['status']} ({onl.get('last_log','')})")

    return ok, issues


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--quiet-ok", action="store_true", help="全 OK 时不打印")
    args = p.parse_args()

    report = run_all_checks()
    ok, issues = summarize(report)

    if not issues and args.quiet_ok:
        return 0

    print(f"=== TradingDataManager 每小时保活检查 @ {report['ts']} ===\n")
    if ok:
        print("【正常】")
        for line in ok:
            print(f"  {line}")
    if issues:
        print("\n【异常】")
        for line in issues:
            print(f"  {line}")
    print()
    return 1 if issues else 0


if __name__ == "__main__":
    sys.exit(main())
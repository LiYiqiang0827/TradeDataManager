"""
service/service_writeredis_zt.py
=================================

【2026-09-11 重构】原子化:1 个 service 只写 1 个 kind

涨停监控写入 service:
- fetch_limitup_pool()
- 30 秒/轮
- 写 Redis(2026-09-14 v6 MyATM 风格):
  - online:zt:archive:{unix_ts} HSET(field=ts_code, value=JSON 全字段含 lu_time)
    - 每个 snapshot 各自 EXPIRE 300s
  - online:zt:timeline ZSet(member=archive_key, score=unix_ts)
    - 整体 EXPIRE 12h + 写入时 prune 10min 前的 member(prune_linked_keys=True 同时 DEL archive_key)
  - online:zt:stream STREAM(字段展开,落盘用,不动)

启动:
    python3 -m service.service_writeredis_zt --interval 300
    python3 -m service.service_writeredis_zt --once
"""


from __future__ import annotations


# 2026-09-12 v3:coreClient 架构调整
import sys as _sys
from pathlib import Path as _Path
_HERE_SVC = _Path(__file__).resolve().parent
_CORE_SVC = _HERE_SVC.parent.parent.parent / 'coreClient'
if str(_CORE_SVC.parent) not in _sys.path:
    _sys.path.insert(0, str(_CORE_SVC.parent))

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from coreClient.ths_client import fetch_limitup_pool, HithinkCLIError            # noqa: E402
from core.redis_online import OnlineRedis                                   # noqa: E402
from core.logger import setup_logger                                         # noqa: E402


def is_trading_window(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    return (9, 30) <= (now.hour, now.minute) <= (15, 0)


def normalize_zt_item(it: dict, now_unix: float) -> dict:
    """涨停条目标准化 + 打 zt_timestamp(数据时间,setdefault 不覆盖 client 已打的)

    2026-09-14 v3 精简:删 data_timestamp / data_timestamp_iso / snap_ts_unix(老字段冗余)
        落盘端只读 zt_timestamp + created_at(SQLite DEFAULT)
        ths_client 源头已经用 _stamp_data_timestamp(field="zt_timestamp") 打过 zt_timestamp
    """
    it.setdefault("zt_timestamp", now_unix)
    thscode = it.get("thscode") or it.get("ts_code") or ""
    if not thscode and it.get("ticker"):
        thscode = it["ticker"]
    it["ts_code"] = thscode
    return it


def run_zt_loop(redis_client: OnlineRedis, *, interval_sec: float = 60.0,    # 2026-09-16 v6.15:60s/轮(用户最新统一规定:zt/break 频率)
                log: logging.Logger = None, stop_check=None) -> None:
    """主循环:5 分钟/轮拉涨停池,SET + STREAM 双写"""
    log = log or logging.getLogger("zt_loop")
    log.info(f"service_writeredis_zt 启动, interval={interval_sec}s")

    round_idx = 0
    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        if not is_trading_window():
            time.sleep(interval_sec)
            continue

        try:
            now_dt = datetime.now()
            now_unix = now_dt.timestamp()

            pool = fetch_limitup_pool(size=200)
            if pool:
                pool = [normalize_zt_item(it, now_unix) for it in pool]
                redis_client.put_zt_pool(pool, data_timestamp=now_unix)

            round_idx += 1
            redis_client.set_meta("zt_last_round", round_idx)
            redis_client.set_meta("zt_last_count", len(pool or []))
            redis_client.set_meta("zt_last_ts", now_dt.isoformat(timespec="milliseconds"))
            log.info(f"round {round_idx}: zt={len(pool or [])}")
        except HithinkCLIError as e:
            log.warning(f"CLI 错误: {e}")
            redis_client.set_meta("zt_last_error", str(e))
        except Exception as e:
            log.error(f"zt 异常: {e}", exc_info=True)
            redis_client.set_meta("zt_last_error", str(e))

        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: writeredis_zt (涨停池)")
    parser.add_argument("--interval", type=float, default=30.0)    # 2026-09-11:300s → 30s
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_zt")
    log.info(f"启动 service_writeredis_zt interval={args.interval}s")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        try:
            now_unix = datetime.now().timestamp()
            pool = fetch_limitup_pool(size=200)
            if pool:
                pool = [normalize_zt_item(it, now_unix) for it in pool]
                r.put_zt_pool(pool, data_timestamp=now_unix)
            log.info(f"once: zt={len(pool or [])}")
        except Exception as e:
            log.error(f"once 失败: {e}")
            return 1
        return 0

    try:
        run_zt_loop(r, interval_sec=args.interval, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

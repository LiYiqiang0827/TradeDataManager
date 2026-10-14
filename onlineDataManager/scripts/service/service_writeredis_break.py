"""
service/service_writeredis_break.py
====================================

【2026-09-11 新建 / 2026-09-14 v6 改 MyATM 风格】

炸板池写入 service:
- fetch_limitbreak_pool()
- 60s/轮(2026-09-11 从 300s 改)
- 写 Redis MyATM 风格:
    * online:break:archive:{unix_ts} HSET(每个 snapshot 300s 各自过期,用户拍板)
    * online:break:timeline ZSet(12h 兜底 + 滑窗 10 分钟)
    * online:break:stream STREAM(落盘用,1d)
- 不再写 online:break:pool SET(v6 删除)

启动:
    python3 -m service.service_writeredis_break --interval 300
    python3 -m service.service_writeredis_break --once
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

from coreClient.ths_client import fetch_limitbreak_pool, HithinkCLIError        # noqa: E402
from core.redis_online import OnlineRedis                                  # noqa: E402
from core.logger import setup_logger                                        # noqa: E402


def is_trading_window(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    return (9, 30) <= (now.hour, now.minute) <= (15, 0)


def normalize_break_item(it: dict, now_unix: float) -> dict:
    """炸板条目标准化 + 打 break_timestamp(数据时间,setdefault 不覆盖 client 已打的)

    2026-09-14 v3 精简:删 data_timestamp / data_timestamp_iso / snap_ts_unix(老字段冗余)
        落盘端只读 break_timestamp + created_at(SQLite DEFAULT)
        ths_client 源头已经用 _stamp_data_timestamp(field="break_timestamp") 打过 break_timestamp
    """
    it.setdefault("break_timestamp", now_unix)
    thscode = it.get("thscode") or it.get("ts_code") or ""
    if not thscode and it.get("ticker"):
        thscode = it["ticker"]
    it["ts_code"] = thscode
    return it


def run_break_loop(redis_client: OnlineRedis, *, interval_sec: float = 60.0,    # 2026-09-16 v6.15:60s/轮(用户最新统一规定:zt/break 频率)
                   log: logging.Logger = None, stop_check=None) -> None:
    log = log or logging.getLogger("break_loop")
    log.info(f"service_writeredis_break 启动, interval={interval_sec}s")

    round_idx = 0
    while True:
        if stop_check and stop_check():
            break

        if not is_trading_window():
            time.sleep(interval_sec)
            continue

        try:
            now_dt = datetime.now()
            now_unix = now_dt.timestamp()

            pool = fetch_limitbreak_pool(size=200)
            if pool:
                pool = [normalize_break_item(it, now_unix) for it in pool]
                redis_client.put_break_pool(pool, data_timestamp=now_unix)

            round_idx += 1
            redis_client.set_meta("break_last_round", round_idx)
            redis_client.set_meta("break_last_count", len(pool or []))
            redis_client.set_meta("break_last_ts", now_dt.isoformat(timespec="milliseconds"))
            log.info(f"round {round_idx}: break={len(pool or [])}")
        except HithinkCLIError as e:
            log.warning(f"CLI 错误: {e}")
            redis_client.set_meta("break_last_error", str(e))
        except Exception as e:
            log.error(f"break 异常: {e}", exc_info=True)
            redis_client.set_meta("break_last_error", str(e))

        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: writeredis_break (炸板池)")
    parser.add_argument("--interval", type=float, default=60.0)    # 2026-09-11:300s → 60s
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_break")
    log.info(f"启动 service_writeredis_break interval={args.interval}s")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        try:
            now_unix = datetime.now().timestamp()
            pool = fetch_limitbreak_pool(size=200)
            if pool:
                pool = [normalize_break_item(it, now_unix) for it in pool]
                r.put_break_pool(pool, data_timestamp=now_unix)
            log.info(f"once: break={len(pool or [])}")
        except Exception as e:
            log.error(f"once 失败: {e}")
            return 1
        return 0

    try:
        run_break_loop(r, interval_sec=args.interval, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

"""
service/service_writeredis_hot.py
==================================

【2026-09-11 新建 / 2026-09-14 v6 改 MyATM 风格】

热股榜写入 service:
- fetch_hot_stock(period='hour')
- 120s/轮(2 分钟,v6.15 用户最新统一规定:hot/anomaly 频率)
- 写 Redis MyATM 风格:
    * online:hot:archive:{unix_ts} HSET(每个 snapshot 1200s 各自过期,用户拍板 20 分钟)
    * online:hot:timeline ZSet(12h 兜底 + 滑窗 10 分钟)
    * online:hot:stream STREAM(落盘用,1d)
- 不再写 online:hot:rank LIST(v6 删除)

启动:
    python3 -m service.service_writeredis_hot --interval 300
    python3 -m service.service_writeredis_hot --once
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

from coreClient.ths_client import fetch_hot_stock, HithinkCLIError              # noqa: E402
from core.redis_online import OnlineRedis                                   # noqa: E402
from core.logger import setup_logger                                         # noqa: E402


def is_trading_window(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    return (9, 30) <= (now.hour, now.minute) <= (15, 0)


def run_hot_loop(redis_client: OnlineRedis, *, interval_sec: float = 120.0,   # 2026-09-16 v6.15:120s/轮(用户最新统一规定:hot/anomaly 频率)
                 log: logging.Logger = None, stop_check=None) -> None:
    log = log or logging.getLogger("hot_loop")
    log.info(f"service_writeredis_hot 启动, interval={interval_sec}s")

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

            hot = fetch_hot_stock(period="hour")
            if hot:
                redis_client.put_hot_rank(hot, data_timestamp=now_unix)

            round_idx += 1
            redis_client.set_meta("hot_last_round", round_idx)
            redis_client.set_meta("hot_last_count", len(hot or []))
            redis_client.set_meta("hot_last_ts", now_dt.isoformat(timespec="milliseconds"))
            log.info(f"round {round_idx}: hot={len(hot or [])}")
        except HithinkCLIError as e:
            log.warning(f"CLI 错误: {e}")
            redis_client.set_meta("hot_last_error", str(e))
        except Exception as e:
            log.error(f"hot 异常: {e}", exc_info=True)
            redis_client.set_meta("hot_last_error", str(e))

        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: writeredis_hot (热股榜)")
    parser.add_argument("--interval", type=float, default=120.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_hot")
    log.info(f"启动 service_writeredis_hot interval={args.interval}s")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        try:
            now_unix = datetime.now().timestamp()
            hot = fetch_hot_stock(period="hour")
            if hot:
                r.put_hot_rank(hot, data_timestamp=now_unix)
            log.info(f"once: hot={len(hot or [])}")
        except Exception as e:
            log.error(f"once 失败: {e}")
            return 1
        return 0

    try:
        run_hot_loop(r, interval_sec=args.interval, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

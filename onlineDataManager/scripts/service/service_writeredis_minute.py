"""
service/service_writeredis_minute.py
==========================

分时图监控 service(1 分钟 K 线)。
- watchlist = 在线 watchlist(由 service_watchlist 维护)
- 周期 = 60 秒/轮(分时图 1 分钟一根)
- 写入 = Redis(由 service_savedata 落盘)

2026-09-13 v4:
  - 改用 coreClient/tdx_client.get_minute_kline(ts_code) 实时接口
    (pytdx 实时分时不需要传 date,2026-09-13 用户确认)
  - trade_date 不再需要 — service 永远只写"今天"
  - 删除 ts_code_to_pytdx / normalize_minute(client 自动注入 time_idx/datetime/data_timestamp)
  - idx 0 = 09:31(开盘首笔),由 client 自动算(不是 09:30)
  - Redis ZSET member 每根 K 线展开 1 行:{time_idx, datetime, price, vol, data_timestamp}

启动:
    python3 -m service.service_writeredis_minute --interval 60
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

from coreClient.tdx_client import TdxClient                  # noqa: E402
from core.redis_online import OnlineRedis                 # noqa: E402


from core.logger import setup_logger  # noqa: E402


def run_minute_loop(
    redis_client: OnlineRedis,
    *,
    interval_sec: float = 30.0,    # 2026-09-16 v6.15:30s/轮(用户最新统一规定:普通 kind 频率)
    log: logging.Logger = None,
    stop_check=None,
) -> None:
    """主循环

    2026-09-13 v4:trade_date 不再需要参数 — 实时分时图只写今天
    """
    log = log or logging.getLogger("minute_loop")

    client = TdxClient()
    log.info(f"分时图服务启动, interval={interval_sec}s")

    round_idx = 0
    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        try:
            watchlist = redis_client.get_watchlist()
            if not watchlist:
                log.debug("watchlist 为空, 跳过本轮")
                time.sleep(interval_sec)
                continue

            now_iso = datetime.now().isoformat(timespec="milliseconds")
            written = 0
            for ts_code in watchlist:
                try:
                    # 2026-09-13 v4:实时分时接口,不传 date
                    rows = client.get_minute_kline(ts_code)
                    if not rows:
                        continue
                    # rows 已带完整字段(time_idx / datetime / price / vol / data_timestamp)
                    norm = {
                        "ts_code": ts_code,
                        "bar_count": len(rows),
                        "bars": rows,
                    }
                    norm.setdefault("snap_ts", now_iso)
                    redis_client.put_minute(ts_code=ts_code, data=norm)
                    written += 1
                except Exception as e:
                    log.error(f"分时 {ts_code} 失败: {e}")

            round_idx += 1
            log.info(f"round {round_idx}: watchlist={len(watchlist)} written={written}")
            redis_client.set_meta("minute_last_round", round_idx)
            redis_client.set_meta("minute_last_written", written)

        except Exception as e:
            log.error(f"minute 异常: {e}", exc_info=True)
            redis_client.set_meta("minute_last_error", str(e))
            try:
                client.disconnect()
                client = TdxClient()
            except Exception:
                pass

        time.sleep(interval_sec)

    try:
        client.disconnect()
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: minute (1分钟K分时)")
    parser.add_argument("--interval", type=float, default=60.0, help="拉取间隔秒数(默认 60s,2026-09-11 调整)")
    parser.add_argument("--once", action="store_true", help="只跑 1 轮就退出")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_minute")
    log.info(f"启动 service_writeredis_minute interval={args.interval}s")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        # 单轮模式:直接拉一次 + 写 Redis + 退出
        # ★ 2026-09-14 fix:--once 之前用 stop_check=False 死循环,改成一次性拉取
        log.info("单轮模式:拉 1 轮就退出")
        client = TdxClient()
        try:
            watchlist = r.get_watchlist()
            if not watchlist:
                log.warning("watchlist 为空,跳过")
                return 0
            now_iso = datetime.now().isoformat(timespec="milliseconds")
            written = 0
            for ts_code in watchlist:
                try:
                    rows = client.get_minute_kline(ts_code)
                    if not rows:
                        continue
                    norm = {
                        "ts_code": ts_code,
                        "bar_count": len(rows),
                        "bars": rows,
                    }
                    norm.setdefault("snap_ts", now_iso)
                    r.put_minute(ts_code=ts_code, data=norm)
                    written += 1
                except Exception as e:
                    log.error(f"分时 {ts_code} 失败: {e}")
            log.info(f"once: watchlist={len(watchlist)} written={written}")
            r.set_meta("minute_last_written", written)
            r.set_meta("minute_last_round", 1)
            return 0
        finally:
            try:
                client.disconnect()
            except Exception:
                pass

    try:
        run_minute_loop(r, interval_sec=args.interval, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

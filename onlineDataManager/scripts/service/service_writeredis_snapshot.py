"""
service/service_writeredis_snapshot.py
======================================

连续竞价快照监控 service(09:30-15:00,原 service_writeredis_realtime 改名)。
- 调用同花顺 fetch_snapshots()(不传 stage = 默认实时连续竞价)
- watchlist = 在线 watchlist(由 service_watchlist 维护)
- 周期 = 6 秒/轮(2026-09-11:10s → 6s,v2 batch 优化后 CPU 余量充足)
- 写入 Redis(kind=snapshot):
    1) STREAM online:snapshot:stream(200000 maxlen,落盘)
    2) 单股 ZSET online:snapshot:window:{ts_code}(滑窗 30min,12h 兜底)
    3) v6.3(2026-09-14)一轮末尾 commit_snapshots_batch 聚合写:
        - online:snapshot:archive:{ts} HSET 全市场快照
          EXPIRE 动态:11:00-11:30 用 7200s(跨午休),其它 1800s
        - online:snapshot:timeline ZSet(整体 12h 兜底)
          ZREMRANGEBYSCORE 滑窗 30min(11:00-11:30 扩展到 2h)

启动:
    python3 -m service.service_writeredis_snapshot --interval 6
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

from coreClient.ths_client import fetch_snapshots, HithinkCLIError          # noqa: E402
from core.redis_online import OnlineRedis                            # noqa: E402


from core.logger import setup_logger  # noqa: E402


def is_trading_window(now: datetime | None = None) -> bool:
    """是否在连续竞价窗口 09:30-15:00(工作日)"""
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    hh, mm = now.hour, now.minute
    return (9, 30) <= (hh, mm) <= (15, 0)


def run_realtime_loop(
    redis_client: OnlineRedis,
    *,
    interval_sec: float = 6.0,    # 2026-09-11:10s → 6s
    log: logging.Logger = None,
    stop_check=None,
    ) -> None:
    log = log or logging.getLogger("realtime_loop")
    log.info(f"连续竞价服务启动, interval={interval_sec}s")

    round_idx = 0
    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        if not is_trading_window():
            log.debug("不在连续竞价窗口(09:30-15:00 工作日),等待")
            time.sleep(interval_sec)
            continue

        try:
            watchlist = redis_client.get_watchlist()
            if not watchlist:
                log.debug("watchlist 为空, 跳过本轮")
                time.sleep(interval_sec)
                continue

            quotes = fetch_snapshots(watchlist)
            now_dt = datetime.now()
            # v6.14 清理:删 snap_ts / snap_ts_unix 死字段(同款 v6.13 auction);
            # snapshot_timestamp 同花顺 envelope.data.timestamp 必给,毫秒 int(秒级精度,末3位000)

            # ★ 2026-09-11 v2 优化:批量写 Redis,实测 2-2.5x 加速(101 只 12ms → 5ms)
            items = []
            for q in quotes:
                ts_code = q.get("thscode") or q.get("ts_code")
                if not ts_code:
                    continue
                items.append((ts_code, q))
            written = redis_client.put_snapshots_batch(items)
            # ★ v6.3(2026-09-14):一轮末尾聚合写全市场快照索引(timeline + archive)
            archive_key = redis_client.commit_snapshots_batch(items, now_dt=now_dt)

            round_idx += 1
            log.info(f"round {round_idx}: watchlist={len(watchlist)} quotes={len(quotes)} written={written} archive={archive_key}")
            redis_client.set_meta("snapshot_last_round", round_idx)
            redis_client.set_meta("snapshot_last_written", written)

        except HithinkCLIError as e:
            log.warning(f"CLI 错误: {e}")
            redis_client.set_meta("snapshot_last_error", str(e))
        except Exception as e:
            log.error(f"snapshot 异常: {e}", exc_info=True)
            redis_client.set_meta("snapshot_last_error", str(e))

        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: snapshot (连续竞价快照)")
    parser.add_argument("--interval", type=float, default=6.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_snapshot")
    log.info(f"启动 service_writeredis_snapshot interval={args.interval}s")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        log.info("单轮模式:强制跑 1 轮")
        try:
            watchlist = r.get_watchlist()
            if watchlist:
                quotes = fetch_snapshots(watchlist)
                now_dt = datetime.now()
                # v6.14 清理:删 snap_ts / snap_ts_unix 死字段(同款 v6.13 auction)

                # ★ 2026-09-11 v2 优化:批量写 Redis(与主循环一致)
                items = []
                for q in quotes:
                    ts_code = q.get("thscode") or q.get("ts_code")
                    if not ts_code:
                        continue
                    items.append((ts_code, q))
                written = r.put_snapshots_batch(items)
                # v6.3 一轮末尾聚合写全市场快照索引(timeline + archive)
                archive_key = r.commit_snapshots_batch(items, now_dt=now_dt)
                log.info(f"once: watchlist={len(watchlist)} quotes={len(quotes)} written={written} archive={archive_key}")
            else:
                log.warning("watchlist 为空,跳过")
        except Exception as e:
            log.error(f"once 失败: {e}")
            return 1
        return 0

    try:
        run_realtime_loop(r, interval_sec=args.interval, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

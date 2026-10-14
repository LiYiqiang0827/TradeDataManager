"""
service/service_writeredis_auction.py
==========================

集合竞价快照监控 service(09:15-09:25)。
- 调用同花顺 fetch_auction_snapshots(stage='live')
- watchlist = 在线 watchlist(由 service_watchlist 维护)
- 周期 = 30 秒/轮(竞价阶段短,频率高)
- 写入 Redis(kind=auction)

2026-09-14 v6.2 重构:
- 保持 put_auction 单股粒度(STREAM 落盘 + window ZSET 1d)
- 一轮末尾调 commit_auction_snapshot 聚合写全市场快照
  → online:auction:archive:{unix_ts}  HSET(各 ts_code 1 field)
  → online:auction:timeline          ZSet(score=unix_ts, member=archive_key)
- timeline 与 archive 都 EXPIRE 43200s(12h),**不滑窗**(用户拍板)
  → 区别于 zt/break/hot 的 10min 滑窗 — auction 是 09:15-09:25 短时竞价
  12h 内所有快照都保留,12h 后整体 GC

启动:
    python3 -m service.service_writeredis_auction --interval 30
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

from coreClient.ths_client import fetch_auction_snapshots, HithinkCLIError    # noqa: E402
from core.redis_online import OnlineRedis                              # noqa: E402


from core.logger import setup_logger  # noqa: E402


def is_auction_window(now: datetime | None = None) -> bool:
    """是否在竞价窗口 09:15-09:25(开盘前 5 分钟)"""
    now = now or datetime.now()
    # 仅周一-周五
    if now.weekday() >= 5:
        return False
    hh, mm = now.hour, now.minute
    return (9, 15) <= (hh, mm) <= (9, 25)


def run_auction_loop(
    redis_client: OnlineRedis,
    *,
    interval_sec: float = 6.0,    # 2026-09-11:30s → 6s
    stage: str = "live",
    log: logging.Logger = None,
    stop_check=None,
) -> None:
    log = log or logging.getLogger("auction_loop")
    log.info(f"集合竞价服务启动, interval={interval_sec}s stage={stage}")

    round_idx = 0
    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        if not is_auction_window():
            log.debug("不在竞价窗口(09:15-09:25 工作日),等待")
            time.sleep(interval_sec)
            continue

        try:
            watchlist = redis_client.get_watchlist()
            if not watchlist:
                log.debug("watchlist 为空, 跳过本轮")
                time.sleep(interval_sec)
                continue

            # 同花顺要求 thscode 格式(000001.SZ),watchlist 已经是这个格式
            quotes = fetch_auction_snapshots(watchlist, stage=stage)
            # v6.13 简化:不再记录抓取时刻(snap_ts / snap_ts_unix);auction 用同花顺 auction_timestamp
            written = 0
            snapshot_data: dict[str, dict] = {}  # 2026-09-14 v6.2 全市场快照聚合
            for q in quotes:
                ts_code = q.get("thscode") or q.get("ts_code")
                if not ts_code:
                    continue
                # v6.13 简化:auction 同花顺必返回 auction_timestamp,不再 setdefault 打抓取时刻
                redis_client.put_auction(ts_code=ts_code, data=q)
                snapshot_data[ts_code] = q  # 留作全市场快照聚合
                written += 1

            # 2026-09-14 v6.2:全市场快照聚合写 archive + timeline(不滑窗,12h 兜底)
            archive_key = redis_client.commit_auction_snapshot(snapshot_data)

            round_idx += 1
            log.info(f"round {round_idx}: watchlist={len(watchlist)} quotes={len(quotes)} written={written} archive={archive_key}")
            redis_client.set_meta("auction_last_round", round_idx)
            redis_client.set_meta("auction_last_written", written)
            if archive_key:
                redis_client.set_meta("auction_last_archive", archive_key)

        except HithinkCLIError as e:
            log.warning(f"CLI 错误: {e}")
            redis_client.set_meta("auction_last_error", str(e))
        except Exception as e:
            log.error(f"auction 异常: {e}", exc_info=True)
            redis_client.set_meta("auction_last_error", str(e))

        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: auction (集合竞价)")
    parser.add_argument("--interval", type=float, default=6.0)
    parser.add_argument("--stage", default="live", choices=["live", "predicted", "post"])
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_auction")
    log.info(f"启动 service_writeredis_auction interval={args.interval}s stage={args.stage}")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        # 强制跑 1 轮(忽略 is_auction_window)
        log.info("单轮模式:强制跑 1 轮")
        try:
            watchlist = r.get_watchlist()
            if watchlist:
                quotes = fetch_auction_snapshots(watchlist, stage=args.stage)
                # v6.13 简化:不再记录抓取时刻(snap_ts / snap_ts_unix);auction 用同花顺 auction_timestamp
                written = 0
                snapshot_data: dict[str, dict] = {}  # 2026-09-14 v6.2 全市场快照聚合
                for q in quotes:
                    ts_code = q.get("thscode") or q.get("ts_code")
                    if not ts_code:
                        continue
                    # v6.13 简化:auction 同花顺必返回 auction_timestamp,不再 setdefault 打抓取时刻
                    r.put_auction(ts_code=ts_code, data=q)
                    snapshot_data[ts_code] = q
                    written += 1
                # 2026-09-14 v6.2:全市场快照聚合
                archive_key = r.commit_auction_snapshot(snapshot_data)
                log.info(f"once 模式: watchlist={len(watchlist)} quotes={len(quotes)} written={written} archive={archive_key}")
            else:
                log.warning("watchlist 为空,跳过")
        except Exception as e:
            log.error(f"once 模式失败: {e}")
            return 1
        return 0

    try:
        run_auction_loop(r, interval_sec=args.interval, stage=args.stage, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

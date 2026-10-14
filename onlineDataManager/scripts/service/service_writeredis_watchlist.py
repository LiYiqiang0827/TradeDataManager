"""
service/service_writeredis_watchlist.py
=============================

实时监控观察列表 service(v6.7 2026-09-15:走 STREAM + timeline + archive 三件套;v6.15 2026-09-16:阶段名更新):

【职责】:只写 Redis,**不落盘**(落盘由 service_savedata_watchlist 负责,独立 daemon 循环)

【生命周期】:scheduler 只在 `watchlist`(09:10-09:14,v6.15)阶段启动一次
- 第一轮:生成 + 写 Redis STREAM + timeline + archive 三件套
- 之后 sleep 等被 kill
- scheduler 在 15:16 切到 post_savedata 时 SIGTERM 杀掉(原 15:35,v6.15 上调)

【v6.7 改动】:
  - 旧版:写 SET `online:watchlist` + HASH `online:watchlist:sources`(只 1 次 SET,12h 保留)
  - 新版:走 STREAM + timeline + archive(对齐 auction/snapshot)
    - `online:watchlist:stream`    STREAM  每只 ts_code 一条 (12h TTL)
    - `online:watchlist:timeline`  ZSET    score=unix_ts, member=archive_key (12h TTL)
    - `online:watchlist:archive:{ts}` HASH 全市场一次写完 (12h TTL)
  - read 端(get_watchlist / get_watchlist_sources):签名不动,实现改读最新 archive
  - savedata 端:从"覆盖生成 watchlist 重写 SQLite" → "读 STREAM 增量落 SQLite"
    走 persist_kind(kind="watchlist") 路由(2026-09-15 新增分支)

数据源:~/TradingAgent/offlineDataManager/data/db_cn_kpl.db
      (T+0,比 MyATM ~/database/cn_hotstock.db T+1 更及时)

启动:
    python3 -m service.service_writeredis_watchlist
    python3 -m service.service_writeredis_watchlist --once   # 单次,debug 用
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
ROOT = SERVICE_DIR.parents[2]
sys.path.insert(0, str(SCRIPTS_DIR))

from core.logger import setup_logger                                            # noqa: E402
from core.redis_online import OnlineRedis                                       # noqa: E402
from core.watchlist_fetch import generate_watchlist                             # noqa: E402

from coreClient._paths import offline_data_dir
DB_KPL = offline_data_dir() / "db_cn_kpl.db"


def write_watchlist_to_redis(redis_client: OnlineRedis, log: logging.Logger) -> int:
    """生成一次 watchlist,写 Redis STREAM + timeline + archive 三件套(v6.7)。

    返回写入 STREAM 的条目数(ts_code 数量)。

    2026-09-13 v4:generate_watchlist 签名变更
        - 旧:generate_watchlist(yesterday=None, prev_range=None, min_level=2, use_fallback=True)
        - 新:generate_watchlist(prev_window_days=10, min_level=2, use_fallback=True)

    2026-09-15 v6.7:从 SET/HASH 双 key 改为 STREAM + timeline + archive 三件套
        - 删 SET + HASH(老的 online:watchlist / online:watchlist:sources)
        - 新增 stream + timeline + archive:{ts},全 12h TTL
        - 落盘路径改走 persist_kind(kind="watchlist")(service_savedata_watchlist 改造)
    """
    today = redis_client.get_meta("trade_date") or datetime.now().strftime("%Y%m%d")
    wl = generate_watchlist(
        prev_window_days=10,
        min_level=2,
        use_fallback=True,
    )

    # v6.7:写 STREAM + timeline + archive 三件套(替代旧 SET/HASH)
    # v6.14:watchlist_timestamp 改 int 毫秒(对齐其它 kind);落盘端负责转 ISO 字符串
    watchlist_ts = wl.entries[0].watchlist_timestamp if wl.entries else int(time.time() * 1000)
    archive_key = redis_client.put_watchlist(
        ts_codes=wl.ts_codes,
        sources_map=wl.sources_map(),
        watchlist_timestamp=watchlist_ts,
    )

    # 写 meta(给调度器/调试用)
    redis_client.set_meta("watchlist_last_count", wl.count)
    redis_client.set_meta("watchlist_latest_date", wl.latest_date)
    redis_client.set_meta("watchlist_latest_source", wl.latest_source)
    redis_client.set_meta("watchlist_prev_window", f"{wl.prev_window[0]}-{wl.prev_window[1]}")
    redis_client.set_meta("watchlist_latest_lb_count", wl.latest_lb_count)
    redis_client.set_meta("watchlist_latest_count", wl.latest_count)
    redis_client.set_meta("watchlist_prev_lb_count", wl.prev_lb_count)
    redis_client.set_meta("watchlist_last_ts", datetime.now().isoformat(timespec="milliseconds"))
    redis_client.set_meta("watchlist_trade_date", today)
    if archive_key:
        redis_client.set_meta("watchlist_last_archive", archive_key)

    log.info(
        f"watchlist Redis 写入完成(v6.7): today={today} latest_date={wl.latest_date} "
        f"src={wl.latest_source} latest={wl.latest_count} "
        f"latest_lb={wl.latest_lb_count} prev_lb={wl.prev_lb_count} total={wl.count} "
        f"archive={archive_key}"
    )
    return wl.count


def run_watchlist_loop(redis_client: OnlineRedis, log: logging.Logger, stop_check=None) -> None:
    """watchlist daemon 主循环(2026-09-11 v3 重构:只写一次 Redis,然后 sleep)

    设计:
      - scheduler 只在 pre_auction(09:00-09:15)启动一次
      - 第一轮:生成 + 写 Redis
      - 之后 sleep forever(盘中不变)
      - scheduler 在 15:16 切到 post_savedata 时 SIGTERM 杀掉(v6.15 上调)

    【重要】落盘由 service_savedata_watchlist 独立循环负责,每 15 分钟覆盖一次(v6.15 由 30→15)
    """
    log.info("watchlist daemon 启动(单次写入 Redis 模式)")

    # === 第 1 轮:生成 + 写 Redis ===
    try:
        n = write_watchlist_to_redis(redis_client, log)
        log.info(f"watchlist Redis 已写入 {n} 条,进入 idle 等待落盘 daemon 处理")
    except Exception as e:
        log.error(f"watchlist 写入 Redis 失败: {e}", exc_info=True)
        redis_client.set_meta("watchlist_last_error", str(e))

    # === 第 2 步:sleep 等被 kill(盘中不变) ===
    log.info("watchlist 已完成,进入 idle 等待(scheduler 在 15:35 会 SIGTERM)")
    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break
        time.sleep(60)  # 1 分钟醒一次检查 stop_check


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: watchlist (只写 Redis,不落盘)")
    parser.add_argument("--db-path", default=str(DB_KPL))
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--once", action="store_true",
                        help="单轮写一次 Redis 然后退出(测试用)")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_watchlist")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        log.info("单轮模式(--once)")
        try:
            today = r.get_meta("trade_date") or datetime.now().strftime("%Y%m%d")
            wl = generate_watchlist(
                prev_window_days=10,
                min_level=2,
                use_fallback=True,
            )
            n = write_watchlist_to_redis(r, log)
            print(f"\n=== Watchlist 前 10 只 (按 priority DESC) ===")
            for entry in wl.entries[:10]:
                print(
                    f"  priority={entry.priority} {entry.ts_code} source={entry.source} "
                    f"reason='{entry.reason}' "
                    f"watchlist_ts={entry.watchlist_timestamp}"
                )
            print(f"\n=== Source 分布 ===")
            sources = wl.sources_map()
            for src in set(sources.values()):
                count = sum(1 for v in sources.values() if v == src)
                print(f"  {src}: {count} 只")
            print(f"\n=== 全 watchlist size: {n} ===")
            print(f"  latest_date={wl.latest_date} src={wl.latest_source}")
            print(f"  prev_window={wl.prev_window[0]}-{wl.prev_window[1]}")
            print(f"  latest_lb={wl.latest_lb_count}, latest_total={wl.latest_count}, "
                  f"prev_lb={wl.prev_lb_count}")
        except Exception as e:
            log.error(f"once 失败: {e}", exc_info=True)
            return 1
        return 0

    try:
        run_watchlist_loop(r, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

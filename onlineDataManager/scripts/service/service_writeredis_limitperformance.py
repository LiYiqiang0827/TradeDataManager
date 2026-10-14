"""
service/service_writeredis_limitperformance.py
==============================================

【2026-09-14 v5 新建 / 2026-09-14 v6 改 MyATM 风格】

涨停表现详情写入 service:
- KPLClient.fetch_realtime_limit_performance()(apphwhq 实时盯盘 host,盘中可用)
- 30 秒/轮
- 写 Redis MyATM 风格:
    * online:limitperformance:archive:{unix_ts} HSET(每个 snapshot 300s 各自过期)
    * online:limitperformance:timeline ZSet(12h 兜底 + 滑窗 10 分钟)
    * online:limitperformance:stream STREAM(落盘用,1d)
  每条 entry 字段(源头展开,见 redis_online.put_limitperformance):
    ts_code / name / board_type / board_count / lu_time(int unix) /
    theme / limit_reason / is_break / amplitude / turnover_rate /
    limit_order / lu_limit_order / net_change / main_in / main_out /
    amount / free_float / close_price / pct_chg / board_period /
    theme_id / sector_id / trade_date /
    limitperformance_timestamp(本批拉取瞬间 wall clock unix float)

落盘:
  service_savedata_limitperformance.py 15 min/轮从 STREAM 读 → SQLite
  表:limit_performance_<YYYYMMDD>
  PK:UNIQUE(ts_code, limitperformance_timestamp)

阶段挂载(scheduler_onlineData.py):
  auction_writer / morning_writer / afternoon_writer
  不挂 writer_stopped(15:10+) / post_savedata 阶段
  savedata 挂 4 个阶段:auction_savedata / morning_savedata / afternoon_savedata / writer_stopped

启动:
    python3 -m service.service_writeredis_limitperformance --interval 30
    python3 -m service.service_writeredis_limitperformance --once
"""


from __future__ import annotations


# 2026-09-14 v5:coreClient 架构调整,确保 from coreClient.kpl_client import 可用
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

from coreClient.kpl_client import KPLClient                                 # noqa: E402
from core.redis_online import OnlineRedis                                   # noqa: E402
from core.logger import setup_logger                                         # noqa: E402


def is_trading_window(now: datetime | None = None) -> bool:
    """盘中实时窗口(集合竞价尾 + 上午 + 下午)— 跟 auction/snapshot 一致

    09:14 集合竞价尾开始就有第一只涨停(09:25 开盘后陆续涨停)
    但 09:14 涨停可能为 0,所以窗口从 09:14 起到 15:00 收盘
    """
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    # 09:14 - 15:00
    t = (now.hour, now.minute)
    if t >= (9, 14) and t <= (15, 0):
        return True
    return False


def normalize_lp_item(it: dict) -> dict:
    """涨停详情条目标准化

    kpl_client.fetch_realtime_limit_performance() 返回的 dict 已经是同花顺原始结构
    (含 ts_code/name/board_type/board_count/lu_time(原始 unix int)/theme/limit_reason/...),
    我们只需:
      1. ts_code 兜底(防止某种 corner case)
      2. 保留 lu_time 为原始 unix int(用户要求原封不动)
      3. put_limitperformance 内部会打 limit_performance_timestamp
    """
    # 防御性:把 thscode 之类旧名映射到 ts_code
    if "thscode" in it and "ts_code" not in it:
        it["ts_code"] = it.pop("thscode")
    it.pop("ticker", None)
    return it


def run_limitperformance_loop(
    redis_client: OnlineRedis,
    *,
    interval_sec: float = 30.0,
    log: logging.Logger = None,
    stop_check=None,
) -> None:
    """主循环:30 秒/轮拉涨停表现详情,STREAM 单写"""
    log = log or logging.getLogger("limitperformance_loop")
    log.info(f"service_writeredis_limitperformance 启动, interval={interval_sec}s")

    round_idx = 0
    kpl = KPLClient()

    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        if not is_trading_window():
            # 非盘中:短暂 sleep(下一轮再判),避免空转
            time.sleep(interval_sec)
            continue

        try:
            now_dt = datetime.now()
            now_unix = now_dt.timestamp()

            df = kpl.fetch_realtime_limit_performance()
            if df is not None and len(df) > 0:
                lp_list = [normalize_lp_item(it) for it in df.to_dict(orient="records")]
                written = redis_client.put_limitperformance(
                    lp_list, data_timestamp=now_unix,
                )
            else:
                written = 0

            round_idx += 1
            redis_client.set_meta("limitperformance_last_round", round_idx)
            redis_client.set_meta("limitperformance_last_count", written)
            redis_client.set_meta(
                "limitperformance_last_ts",
                now_dt.isoformat(timespec="milliseconds"),
            )
            log.info(
                f"round {round_idx}: limitperformance={written} ts={now_dt.isoformat(timespec='seconds')}",
            )
        except Exception as e:
            log.error(f"limitperformance 异常: {e}", exc_info=True)
            redis_client.set_meta("limitperformance_last_error", str(e))

        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: writeredis_limitperformance (涨停表现详情)")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_limitperformance")
    log.info(f"启动 service_writeredis_limitperformance interval={args.interval}s")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        try:
            now_unix = datetime.now().timestamp()
            kpl = KPLClient()
            df = kpl.fetch_realtime_limit_performance()
            if df is not None and len(df) > 0:
                lp_list = [normalize_lp_item(it) for it in df.to_dict(orient="records")]
                written = r.put_limitperformance(lp_list, data_timestamp=now_unix)
            else:
                written = 0
            log.info(f"once: limitperformance={written}")
        except Exception as e:
            log.error(f"once 失败: {e}", exc_info=True)
            return 1
        return 0

    try:
        run_limitperformance_loop(r, interval_sec=args.interval, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())
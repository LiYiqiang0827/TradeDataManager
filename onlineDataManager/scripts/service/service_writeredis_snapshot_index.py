"""
service/service_writeredis_snapshot_index.py
============================================

指数行情快照监控 service(v6.10 2026-09-16 新增)。
- 8 只固定指数:000001.SH / 399001.SZ / 399006.SZ / 000688.SH / 000016.SH / 000300.SH / 000905.SH / 000852.SH
- 周期 = 30 秒/轮(用户拍板,2026-09-16)
- 启动时间:上午 09:29-11:31,下午 12:59-15:01(用户原话)
- 调用 ths_client.fetch_index_snapshot(ths_codes=INDEX_LIST)
- 写入 Redis(kind=snapshot_index):
    1) STREAM online:snapshot_index:stream(3000 maxlen,落盘)
    2) 单只 ZSET online:snapshot_index:window:{ts_code}(12h 兜底)
    3) 一轮末尾 commit_snapshots_index_batch 聚合写:
        - online:snapshot_index:archive:{ts} HSET 全指数快照
          EXPIRE 固定 21600s(6h,用户原话)
        - online:snapshot_index:timeline ZSet(12h 兜底,不做滑窗)

启动:
    python3 -m service.service_writeredis_snapshot_index --interval 30

⚠️ 注意:
    北证 50(疑似 ticker 899xxx.BJ)ths CLI 暂不支持,等 ths CLI 更新后再追加
    用户最新原话 "a 只用8个吧" — 2026-09-16
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
from datetime import datetime, time as dtime
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from coreClient.ths_client import fetch_index_snapshot, HithinkCLIError    # noqa: E402
from core.redis_online import OnlineRedis                                   # noqa: E402


from core.logger import setup_logger  # noqa: E402


# v6.10 固定 8 只指数(用户原话"a 只用8个吧")
INDEX_LIST = [
    "000001.SH",   # 上证指数
    "399001.SZ",   # 深证成指
    "399006.SZ",   # 创业板指
    "000688.SH",   # 科创 50
    "000016.SH",   # 上证 50
    "000300.SH",   # 沪深 300
    "000905.SH",   # 中证 500
    "000852.SH",   # 中证 1000
    # 北证 50 暂不纳入 — ths CLI 不支持(2026-09-16 实测 9 种代码组合全失败)
]


# 启动时间窗(用户原话,2026-09-16)
MORNING_START  = dtime(9, 29)
MORNING_END    = dtime(11, 31)
AFTERNOON_START = dtime(12, 59)
AFTERNOON_END   = dtime(15, 1)


def is_trading_window(now: datetime | None = None) -> bool:
    """是否在指数快照时间窗内(用户原话 09:29-11:31 + 12:59-15:01)

    写作日内连续区间,中午默认 sleep — 落盘逻辑(savedata service)在 11:45-12:45 内部 sleep
    """
    now = now or datetime.now()
    if now.weekday() >= 5:    # 周末不开
        return False
    t = now.time()
    return (MORNING_START <= t <= MORNING_END) or (AFTERNOON_START <= t <= AFTERNOON_END)


def run_index_loop(
    redis_client: OnlineRedis,
    *,
    interval_sec: float = 10.0,    # 2026-09-16 v6.15:10s/轮(用户最新统一规定:普通 kind 频率)
    log: logging.Logger = None,
    stop_check=None,
    ) -> None:
    log = log or logging.getLogger("index_loop")
    log.info(f"指数快照服务启动, interval={interval_sec}s, indices={len(INDEX_LIST)}")

    round_idx = 0
    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        if not is_trading_window():
            log.debug("不在指数快照时间窗,等待")
            time.sleep(interval_sec)
            continue

        try:
            quotes = fetch_index_snapshot(INDEX_LIST)
            # v6.14 清理:删 snapshot_unix_ms 死字段(变量名错 + 落盘不用);
            # snapshot_timestamp 同花顺 envelope.data.timestamp 必给,毫秒 int(秒级精度,末3位000)
            now_dt = datetime.now()
            now_iso = now_dt.isoformat(timespec="milliseconds")

            # ★ 与 snapshot 模板对齐:批量写 Redis
            items = []
            for q in quotes:
                ts_code = q.get("thscode") or q.get("ts_code")
                if not ts_code:
                    continue
                q.setdefault("snapshot_timestamp", now_iso)     # 兜底,实际同花顺必给
                items.append((ts_code, q))
            written = redis_client.put_snapshots_index_batch(items)
            # ★ 一轮末尾聚合写 snapshot_index:archive + timeline(无 lunch 动态,固定 6h)
            archive_key = redis_client.commit_snapshots_index_batch(items, now_dt=now_dt)

            round_idx += 1
            log.info(f"round {round_idx}: indices={len(INDEX_LIST)} quotes={len(quotes)} written={written} archive={archive_key}")
            redis_client.set_meta("snapshot_index_last_round", round_idx)
            redis_client.set_meta("snapshot_index_last_written", written)

        except HithinkCLIError as e:
            log.warning(f"CLI 错误: {e}")
            redis_client.set_meta("snapshot_index_last_error", str(e))
        except Exception as e:
            log.error(f"snapshot_index 异常: {e}", exc_info=True)
            redis_client.set_meta("snapshot_index_last_error", str(e))

        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: snapshot_index (指数行情快照)")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_snapshot_index")
    log.info(f"启动 service_writeredis_snapshot_index interval={args.interval}s")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        log.info("单轮模式:强制跑 1 轮")
        try:
            quotes = fetch_index_snapshot(INDEX_LIST)
            # v6.14 清理:删 snapshot_unix_ms 死字段;now_unix 不再需要
            now_dt = datetime.now()
            now_iso = now_dt.isoformat(timespec="milliseconds")

            items = []
            for q in quotes:
                ts_code = q.get("thscode") or q.get("ts_code")
                if not ts_code:
                    continue
                q.setdefault("snapshot_timestamp", now_iso)     # 兜底,实际同花顺必给
                items.append((ts_code, q))
            written = r.put_snapshots_index_batch(items)
            archive_key = r.commit_snapshots_index_batch(items, now_dt=now_dt)
            log.info(f"once: indices={len(INDEX_LIST)} quotes={len(quotes)} written={written} archive={archive_key}")
        except Exception as e:
            log.error(f"once 失败: {e}")
            return 1
        return 0

    try:
        run_index_loop(r, interval_sec=args.interval, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())
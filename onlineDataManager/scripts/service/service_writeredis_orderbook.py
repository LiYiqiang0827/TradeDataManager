"""
service/service_writeredis_orderbook.py
============================

5 档盘口监控 service。
- 监控列表 = 在线 watchlist(从 Redis 读,由 service_watchlist 维护)
- 周期 = 10 秒/轮(用户指定)
- 批量 = 80 只/批(pytdx 限制)
- 写入 = Redis HASH + LIST history(由 service_savedata 落盘)

启动:
    python3 -m service.service_writeredis_orderbook --interval 10
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
import json
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


def ts_code_to_pytdx(ts_code: str) -> tuple[int, str]:
    """'000988.SZ' → (0, '000988')"""
    code, suffix = ts_code.split(".")
    market = 1 if suffix == "SH" else 0
    return (market, code)


def normalize_quote(quote: dict) -> dict | None:
    """将 pytdx 原始 quote 转换为标准化 dict"""
    if not quote:
        return None
    market = quote.get("market", 0)
    code = quote.get("code", "")
    ts_code = f"{code}.{'SH' if market == 1 else 'SZ'}"

    def _f(v):
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    def _i(v):
        try:
            return int(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    return {
        "ts_code": ts_code,
        "last_price": _f(quote.get("price")),
        "prev_price": _f(quote.get("last_close")),
        "open_price": _f(quote.get("open")),
        "high_price": _f(quote.get("high")),
        "low_price": _f(quote.get("low")),
        "volume": _i(quote.get("vol")),
        "amount": _f(quote.get("amount")),
        "cur_vol": _i(quote.get("cur_vol")),
        # 5档
        "bid1": _f(quote.get("bid1")),
        "ask1": _f(quote.get("ask1")),
        "bid_vol1": _i(quote.get("bid_vol1")),
        "ask_vol1": _i(quote.get("ask_vol1")),
        "bid2": _f(quote.get("bid2")),
        "ask2": _f(quote.get("ask2")),
        "bid_vol2": _i(quote.get("bid_vol2")),
        "ask_vol2": _i(quote.get("ask_vol2")),
        "bid3": _f(quote.get("bid3")),
        "ask3": _f(quote.get("ask3")),
        "bid_vol3": _i(quote.get("bid_vol3")),
        "ask_vol3": _i(quote.get("ask_vol3")),
        "bid4": _f(quote.get("bid4")),
        "ask4": _f(quote.get("ask4")),
        "bid_vol4": _i(quote.get("bid_vol4")),
        "ask_vol4": _i(quote.get("ask_vol4")),
        "bid5": _f(quote.get("bid5")),
        "ask5": _f(quote.get("ask5")),
        "bid_vol5": _i(quote.get("bid_vol5")),
        "ask_vol5": _i(quote.get("ask_vol5")),
        # 内/外盘
        "s_vol": _i(quote.get("s_vol")),
        "b_vol": _i(quote.get("b_vol")),
    }


def run_orderbook_loop(
    redis_client: OnlineRedis,
    *,
    interval_sec: float = 10.0,
    batch_size: int = 80,
    log: logging.Logger = None,
    stop_check=None,
) -> None:
    """主循环

    Args:
        interval_sec: 拉取间隔
        batch_size: pytdx 批量大小(<=80)
        stop_check: callable,返回 True 时停止(用于测试)
    """
    log = log or logging.getLogger("orderbook_loop")
    client = TdxClient()
    log.info(f"5档盘口服务启动, interval={interval_sec}s batch={batch_size}")

    round_idx = 0
    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        try:
            # 1. 拉 watchlist
            watchlist = redis_client.get_watchlist()
            if not watchlist:
                log.debug("watchlist 为空, 跳过本轮")
                time.sleep(interval_sec)
                continue

            # 2. 批量拉盘口
            codes_p = [ts_code_to_pytdx(c) for c in watchlist]
            # 自己分批(80/批),不用 client.get_orderbook_batched(简化)
            quotes = []
            for i in range(0, len(codes_p), batch_size):
                batch = codes_p[i:i + batch_size]
                quotes.extend(client.get_orderbook(batch))

            now_dt = datetime.now()
            now_iso = now_dt.isoformat(timespec="milliseconds")
            now_unix = now_dt.timestamp()

            # 3. 写入 Redis
            written = 0
            for quote in quotes:
                norm = normalize_quote(quote)
                if not norm:
                    continue
                norm.setdefault("snap_ts", now_iso)
                norm.setdefault("snap_ts_unix", now_unix)
                redis_client.put_orderbook(ts_code=norm["ts_code"], data=norm)
                written += 1

            round_idx += 1
            log.info(
                f"round {round_idx}: watchlist={len(watchlist)} "
                f"got={len(quotes)} written={written}"
            )
            redis_client.set_meta("orderbook_last_round", round_idx)
            redis_client.set_meta("orderbook_last_written", written)

        except Exception as e:
            log.error(f"orderbook 异常: {e}", exc_info=True)
            redis_client.set_meta("orderbook_last_error", str(e))
            # 重连
            try:
                client.disconnect()
                client = TdxClient()
            except Exception:
                pass

        time.sleep(interval_sec)

    # 清理
    try:
        client.disconnect()
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: orderbook (5档盘口)")
    parser.add_argument("--interval", type=float, default=10.0,
                        help="拉取间隔秒数(默认 10s)")
    parser.add_argument("--batch", type=int, default=80,
                        help="pytdx 批量大小(<=80, 默认 80)")
    parser.add_argument("--once", action="store_true",
                        help="只跑 1 轮就退出(用于调试)")
    args = parser.parse_args()

    log = setup_logger("service_writeredis_orderbook")
    log.info(f"启动 service_writeredis_orderbook interval={args.interval}s batch={args.batch}")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    if args.once:
        # 单轮模式:直接内联 1 轮,不跑 while
        log.info("单轮模式:跑 1 轮")
        try:
            watchlist = r.get_watchlist()
            if not watchlist:
                log.warning("watchlist 为空,跳过")
                return 0
            codes_p = [ts_code_to_pytdx(c) for c in watchlist]
            quotes = []
            client = TdxClient()
            for i in range(0, len(codes_p), args.batch):
                batch = codes_p[i:i + args.batch]
                quotes.extend(client.get_orderbook(batch))

            now_dt = datetime.now()
            now_iso = now_dt.isoformat(timespec="milliseconds")
            now_unix = now_dt.timestamp()
            written = 0
            for quote in quotes:
                norm = normalize_quote(quote)
                if not norm:
                    continue
                norm.setdefault("snap_ts", now_iso)
                norm.setdefault("snap_ts_unix", now_unix)
                r.put_orderbook(ts_code=norm["ts_code"], data=norm)
                written += 1
            log.info(f"once: watchlist={len(watchlist)} got={len(quotes)} written={written}")
            try:
                client.disconnect()
            except Exception:
                pass
        except Exception as e:
            log.error(f"once 失败: {e}", exc_info=True)
            return 1
        return 0

    try:
        run_orderbook_loop(r, interval_sec=args.interval, batch_size=args.batch, log=log)
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt, 正常退出")
        return 0


if __name__ == "__main__":
    sys.exit(main())

"""
service/service_writeredis_minute.py
==========================

分时图监控 service(1 分钟 K 线)。
- watchlist = 在线 watchlist(由 service_watchlist 维护)
- 周期 = 60 秒/轮(分时图 1 分钟一根)
- 写入 = Redis(由 service_savedata 落盘)

分钟源:
  - 使用 coreClient/tdx_client.get_minute_kline(ts_code) 的原生1分钟K线
    (get_security_bars KLINE_TYPE_1MIN；price=未复权close，vol=股)
  - 不再写入旧 get_minute_time_data decoder 的分时 price/vol
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
from typing import Any

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from coreClient.tdx_client import TdxClient                  # noqa: E402
from coreClient.tdx_config import TDX_HISTORY_BAR_IP_POOL, TDX_IP_POOL  # noqa: E402
from core.redis_online import OnlineRedis                 # noqa: E402


from core.logger import setup_logger  # noqa: E402


MINUTE_SOURCE_PROBE_CODES = ("000001.SZ", "600000.SH")


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _waiting_for_first_completed_bar(now: datetime | None = None) -> bool:
    """Keep the normal opening wait distinct until the 09:31 bar is complete."""
    local_now = (now or datetime.now().astimezone()).astimezone()
    return local_now.strftime("%H:%M") < "09:32"


def _meta_int(redis_client: OnlineRedis, key: str) -> int:
    try:
        return int(redis_client.get_meta(key) or 0)
    except (TypeError, ValueError):
        return 0


def _record_preflight(redis_client: OnlineRedis, result: dict[str, Any]) -> None:
    redis_client.set_meta("minute_preflight_at", result["checked_at"])
    redis_client.set_meta("minute_preflight_status", result["status"])
    redis_client.set_meta("minute_preflight_native_history_counts", result["native_history_counts"])
    redis_client.set_meta("minute_preflight_getter_counts", result["getter_counts"])
    redis_client.set_meta("minute_preflight_primary_pool_size", result["primary_pool_size"])
    redis_client.set_meta("minute_preflight_fallback_pool_size", result["fallback_pool_size"])
    redis_client.set_meta("minute_preflight_endpoint", result["endpoint"] or "")
    redis_client.set_meta("minute_health_status", result["status"])
    redis_client.set_meta("minute_current_error", result["error"])
    # Keep the legacy key current too; an empty error must clear an old failure.
    redis_client.set_meta("minute_last_error", result["error"])


def _new_tdx_client(redis_client: OnlineRedis) -> TdxClient:
    if not TDX_IP_POOL:
        error = "TDX_IP_POOL is empty; minute source cannot be initialized"
        status = "degraded_config_primary_pool_empty"
    else:
        try:
            return TdxClient()
        except Exception as exc:
            error = f"TDX connect: {type(exc).__name__}: {exc}"
            status = "degraded_connect_error"
    checked_at = _now_iso()
    redis_client.set_meta("minute_preflight_at", checked_at)
    redis_client.set_meta("minute_preflight_status", status)
    redis_client.set_meta("minute_health_status", status)
    redis_client.set_meta("minute_current_error", error)
    redis_client.set_meta("minute_last_error", error)
    raise RuntimeError(error)


def probe_minute_source(client: TdxClient, *, now: datetime | None = None) -> dict[str, Any]:
    """Exercise configured native raw/getter paths without writing Redis data.

    Raw history is checked separately from today's live getter because a service
    can start before the current session has produced any bars. Two liquid
    reference symbols prevent one suspended or unsupported symbol from being
    mistaken for a source outage.
    """
    checked_at = _now_iso()
    native_history_counts: dict[str, int] = {}
    getter_counts: dict[str, int] = {}
    errors: dict[str, str] = {}
    for ts_code in MINUTE_SOURCE_PROBE_CODES:
        try:
            native_history_counts[ts_code] = len(client.download_one_minute(ts_code, max_pages=1))
        except Exception as exc:
            native_history_counts[ts_code] = 0
            errors[f"raw:{ts_code}"] = f"{type(exc).__name__}: {exc}"
        try:
            getter_counts[ts_code] = len(client.get_minute_kline(ts_code))
        except Exception as exc:
            getter_counts[ts_code] = 0
            errors[f"getter:{ts_code}"] = f"{type(exc).__name__}: {exc}"

    primary_configured = bool(TDX_IP_POOL)
    fallback_configured = bool(TDX_HISTORY_BAR_IP_POOL)
    raw_available = any(count > 0 for count in native_history_counts.values())
    getter_available = any(count > 0 for count in getter_counts.values())
    if not primary_configured:
        status = "degraded_config_primary_pool_empty"
        error = "TDX_IP_POOL is empty; native minute source is not configured"
    elif not raw_available:
        status = "degraded_native_raw_unavailable"
        error = "native raw probe returned no bars for either reference symbol"
    elif errors and not getter_available:
        status = "degraded_getter_errors"
        error = "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
    elif errors:
        status = "preflight_partial"
        error = "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
    elif not getter_available and _waiting_for_first_completed_bar(now):
        status = "waiting_first_completed_bar"
        error = "native history is available; current getter is empty before the first completed morning bar"
    elif not getter_available:
        status = "degraded_current_getter_empty"
        error = "native raw probe works but current-day getter returned no bars; cause unknown"
    elif not fallback_configured:
        status = "degraded_config_fallback_pool_empty"
        error = "native raw/getter returned bars but TDX_HISTORY_BAR_IP_POOL is empty"
    else:
        status = "preflight_ok"
        error = "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))

    return {
        "checked_at": checked_at,
        "status": status,
        "native_history_counts": native_history_counts,
        "getter_counts": getter_counts,
        "primary_pool_size": len(TDX_IP_POOL),
        "fallback_pool_size": len(TDX_HISTORY_BAR_IP_POOL),
        "endpoint": f"{client.ip}:{client.port}",
        "error": error,
    }


def process_minute_round(
    redis_client: OnlineRedis,
    client: TdxClient,
    *,
    log: logging.Logger | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Fetch and record one round, keeping empty data distinct from success."""
    log = log or logging.getLogger("minute_loop")
    attempted_at = _now_iso()
    try:
        watchlist = list(redis_client.get_watchlist() or [])
    except Exception as exc:
        error = f"watchlist: {type(exc).__name__}: {exc}"
        streak = _meta_int(redis_client, "minute_zero_write_streak") + 1
        round_idx = _meta_int(redis_client, "minute_last_round") + 1
        redis_client.set_meta("minute_last_round", round_idx)
        redis_client.set_meta("minute_last_attempt_at", attempted_at)
        redis_client.set_meta("minute_last_watchlist", -1)
        redis_client.set_meta("minute_last_written", 0)
        redis_client.set_meta("minute_last_empty", 0)
        redis_client.set_meta("minute_last_failed", -1)
        redis_client.set_meta("minute_zero_write_streak", streak)
        redis_client.set_meta("minute_health_status", "degraded_watchlist_error")
        redis_client.set_meta("minute_current_error", error)
        redis_client.set_meta("minute_last_error", error)
        redis_client.set_meta("minute_last_status_detail", "watchlist fetch failed")
        return {"status": "degraded_watchlist_error", "watchlist": -1, "written": 0, "error": error}

    if not watchlist:
        round_idx = _meta_int(redis_client, "minute_last_round") + 1
        redis_client.set_meta("minute_last_round", round_idx)
        redis_client.set_meta("minute_last_attempt_at", attempted_at)
        redis_client.set_meta("minute_last_watchlist", 0)
        redis_client.set_meta("minute_last_written", 0)
        redis_client.set_meta("minute_last_empty", 0)
        redis_client.set_meta("minute_last_failed", 0)
        redis_client.set_meta("minute_zero_write_streak", 0)
        redis_client.set_meta("minute_health_status", "no_watchlist")
        redis_client.set_meta("minute_current_error", "")
        redis_client.set_meta("minute_last_error", "")
        redis_client.set_meta("minute_last_status_detail", "watchlist empty; source not assessed")
        log.warning("minute round skipped: watchlist empty; source health unknown")
        return {"status": "no_watchlist", "watchlist": 0, "written": 0, "error": ""}

    now = (now or datetime.now().astimezone()).astimezone()
    now_iso = now.isoformat(timespec="milliseconds")
    written = 0
    empty_count = 0
    failures: list[str] = []
    for ts_code in watchlist:
        try:
            rows = client.get_minute_kline(ts_code)
            if not rows:
                empty_count += 1
                continue
            norm = {"ts_code": ts_code, "bar_count": len(rows), "bars": rows}
            norm.setdefault("snap_ts", now_iso)
            redis_client.put_minute(ts_code=ts_code, data=norm)
            written += 1
        except Exception as exc:
            failures.append(f"{ts_code}:{type(exc).__name__}:{exc}")
            log.error("分时 %s 失败: %s", ts_code, exc)

    previous_streak = _meta_int(redis_client, "minute_zero_write_streak")
    native_counts = redis_client.get_meta("minute_preflight_native_history_counts") or {}
    native_history_available = isinstance(native_counts, dict) and any(
        isinstance(count, (int, float)) and count > 0 for count in native_counts.values()
    )
    waiting = (
        written == 0 and not failures and native_history_available
        and _waiting_for_first_completed_bar(now)
    )
    streak = previous_streak + 1 if written == 0 and not waiting else 0 if written else previous_streak
    if waiting:
        status = "waiting_first_completed_bar"
        error = "native history is available; no completed current-session bar is expected yet"
        detail = f"all {len(watchlist)} symbols empty before first completed morning bar; errors=0"
    elif written == 0 and failures:
        status = "degraded_source_errors"
        error = "; ".join(failures[:8])
        detail = f"all {len(watchlist)} symbols unwritten; empty={empty_count}; errors={len(failures)}"
    elif written == 0:
        status = "degraded_empty_source_unknown"
        error = "all watchlist getters returned no rows; source/timing cause unknown"
        detail = f"all {len(watchlist)} symbols empty; errors=0"
    elif failures:
        status = "partial"
        error = "; ".join(failures[:8])
        detail = f"written={written}; empty={empty_count}; errors={len(failures)}"
    elif empty_count:
        status = "partial"
        error = ""
        detail = f"written={written}; empty={empty_count}; errors=0"
    else:
        status = "ok"
        error = ""
        detail = f"written={written}; empty=0; errors=0"

    round_idx = _meta_int(redis_client, "minute_last_round") + 1
    redis_client.set_meta("minute_last_round", round_idx)
    redis_client.set_meta("minute_last_attempt_at", attempted_at)
    redis_client.set_meta("minute_last_watchlist", len(watchlist))
    redis_client.set_meta("minute_last_written", written)
    redis_client.set_meta("minute_last_empty", empty_count)
    redis_client.set_meta("minute_last_failed", len(failures))
    redis_client.set_meta("minute_zero_write_streak", streak)
    redis_client.set_meta("minute_health_status", status)
    redis_client.set_meta("minute_current_error", error)
    redis_client.set_meta("minute_last_error", error)
    redis_client.set_meta("minute_last_status_detail", detail)
    if written:
        # A successful timestamp changes only when at least one payload was stored.
        redis_client.set_meta("minute_last_success_at", attempted_at)
    if written:
        redis_client.emit_update("minute", written, codes=watchlist, source_at=now.timestamp())
    log.info(
        "minute round %s: status=%s watchlist=%s written=%s empty=%s errors=%s zero_streak=%s",
        round_idx, status, len(watchlist), written, empty_count, len(failures), streak,
    )
    return {
        "status": status,
        "watchlist": len(watchlist),
        "written": written,
        "empty": empty_count,
        "failures": len(failures),
        "zero_write_streak": streak,
        "error": error,
    }


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

    client = _new_tdx_client(redis_client)
    log.info(f"分时图服务启动, interval={interval_sec}s")
    preflight = probe_minute_source(client)
    _record_preflight(redis_client, preflight)
    if preflight["status"] == "preflight_ok":
        log.info("minute native-source preflight passed: %s", preflight)
    else:
        log.error("minute native-source preflight degraded: %s", preflight)

    while True:
        if stop_check and stop_check():
            log.info("stop_check 返回 True, 退出循环")
            break

        try:
            process_minute_round(redis_client, client, log=log)
        except Exception as exc:
            error = f"round: {type(exc).__name__}: {exc}"
            streak = _meta_int(redis_client, "minute_zero_write_streak") + 1
            redis_client.set_meta("minute_last_round", _meta_int(redis_client, "minute_last_round") + 1)
            redis_client.set_meta("minute_last_attempt_at", _now_iso())
            redis_client.set_meta("minute_last_watchlist", -1)
            redis_client.set_meta("minute_last_written", 0)
            redis_client.set_meta("minute_last_empty", 0)
            redis_client.set_meta("minute_last_failed", -1)
            redis_client.set_meta("minute_zero_write_streak", streak)
            redis_client.set_meta("minute_health_status", "degraded_round_error")
            redis_client.set_meta("minute_current_error", error)
            redis_client.set_meta("minute_last_error", error)
            redis_client.set_meta("minute_last_status_detail", "round processing failed")
            log.error("minute round failed: %s", exc, exc_info=True)
            try:
                client.disconnect()
                client = TdxClient()
            except Exception as reconnect_exc:
                reconnect_error = f"reconnect: {type(reconnect_exc).__name__}: {reconnect_exc}"
                redis_client.set_meta("minute_health_status", "degraded_reconnect_error")
                redis_client.set_meta("minute_current_error", reconnect_error)
                redis_client.set_meta("minute_last_error", reconnect_error)

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
        try:
            client = _new_tdx_client(r)
        except RuntimeError as exc:
            log.error("minute startup preflight failed: %s", exc)
            return 1
        try:
            preflight = probe_minute_source(client)
            _record_preflight(r, preflight)
            if preflight["status"] == "preflight_ok":
                log.info("minute native-source preflight passed: %s", preflight)
            else:
                log.error("minute native-source preflight degraded: %s", preflight)
            result = process_minute_round(r, client, log=log)
            log.info("once result: %s", result)
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

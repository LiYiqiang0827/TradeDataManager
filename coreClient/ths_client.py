"""
~/TradingAgent/coreClient/ths_client.py
同花顺(THS / hithink-finance CLI)API 客户端封装

把 CLI 调用封装成 Python 函数,提供:
- 集合竞价快照 fetch_auction_snapshots()(service_writeredis_auction 用)
- 连续竞价快照 fetch_snapshots()(service_writeredis_realtime 用)
- 当日涨停池 fetch_limitup_pool()(service_writeredis_zt 用,也是 watchlist 补充来源)
- 当日跌停池 fetch_limitdown_pool()
- 当日炸板池 fetch_limitbreak_pool()
- 异动清单 fetch_anomaly_list()
- 指数快照 fetch_index_snapshot()
- 飙升榜 fetch_skyrocket()
- 热股榜 fetch_hot_stock()

特点:
- 调用 hithink-finance CLI(已通过 npm/pip 安装到 PATH)
- 单次调用 + 错误处理 + 重试 + 退避
- 批量调用(单次最多 100 thscode token,自动分批)— 见 ths_config.THS_CHUNK_SIZE_DEFAULT
- 所有函数返 list[dict],空表示当前不可用

CLI 调用失败抛 HithinkCLIError,调用方 try/except 处理。

2026-09-12 v3 + coreClient 架构调整:
- 从 onlineDataManager/scripts/core/ths_client.py 移到 ~/TradingAgent/coreClient/ths_client.py
- 配置项(CLI_CMD / chunk_size / retry 等)抽到 ths_config.py
"""
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

# 让 from coreClient.ths_config import ... 工作
_HERE = Path(__file__).resolve().parent
_PARENT = _HERE.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from coreClient.ths_config import (
    THS_CLI_CMD,
    THS_CLI_DEFAULT_TIMEOUT,
    THS_CLI_RETRY_MAX,
    THS_CLI_RETRY_BASE_SLEEP,
    THS_CHUNK_SIZE_DEFAULT,
    THS_POOL_SIZE_DEFAULT,
    THS_DEFAULT_TS_SUFFIXES,
)


class HithinkCLIError(Exception):
    pass


def _stamp_snapshot_timestamp(items: list[dict], *, field: str = "snapshot_timestamp") -> list[dict]:
    """2026-09-12 精简:只补缺失的 snapshot_timestamp(数据时间戳)

    新规则(用户 2026-09-12 决定):
        - 数据时间戳命名:以数据名_timestamp 命名(snapshot_timestamp / zt_timestamp / ...)
        - 同花顺 API 本身有时间戳 → 保留,不打
        - 没有 → ths_client 兜底补 <field>(unix 秒 float)
        - 存盘时由 persist_client 把 unix 秒 → ISO 字符串(SQLite TEXT 列)
    """
    if not items:
        return items
    ts = time.time()
    for item in items:
        if not isinstance(item, dict):
            continue
        item.setdefault(field, ts)
    return items

# 旧名兼容(部分代码可能还引用 _stamp_data_timestamp)
_stamp_data_timestamp = _stamp_snapshot_timestamp


def _run_cli(args: list, *, retries: int = THS_CLI_RETRY_MAX, timeout: int = THS_CLI_DEFAULT_TIMEOUT) -> dict:
    """执行 hithink-finance CLI,返回 parsed JSON envelope"""
    cmd = [THS_CLI_CMD] + args + ["--format", "json"]
    last_err = None
    for attempt in range(retries):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if r.returncode != 0:
                raise HithinkCLIError(f"CLI 失败(code={r.returncode}): {r.stderr[:500]}")
            return json.loads(r.stdout)
        except (subprocess.TimeoutExpired, HithinkCLIError, json.JSONDecodeError) as e:
            last_err = e
            wait = THS_CLI_RETRY_BASE_SLEEP * (2 ** attempt)
            time.sleep(wait)
    raise HithinkCLIError(f"CLI 重试{retries}次仍失败:{last_err}")


def _chunked(items: list, size: int) -> list:
    return [items[i:i + size] for i in range(0, len(items), size)]


def fetch_auction_snapshots(ts_codes: list[str], *, stage: str = "live", chunk_size: int = THS_CHUNK_SIZE_DEFAULT) -> list[dict]:
    """拉集合竞价快照。自动分批(每批最多 100 thscode)。

    Args:
        ts_codes: A 股 thscode 列表,如 ['000426.SZ', '600519.SH']
        stage: 'live' 或 'final'
        chunk_size: 每批 token 数,默认 THS_CHUNK_SIZE_DEFAULT(80)留点 buffer

    Returns:
        list of dict(源头已精简:thscode→ts_code 重命名 + 删 ticker,统一用 ts_code 标识)

    注意:
        同花顺 API 返回的外层字段 `auction_phase / data_status / timestamp`
        是这一批的共同状态,我们把它注入到每个 item 里,便于后续落盘。

    2026-09-12 v3 精简:
        - 源头:thscode → ts_code 重命名 + 删 ticker
        - 数据时间戳:auction_timestamp(同花顺 envelope 给,毫秒;fallback ths_client)
    """
    if not ts_codes:
        return []

    all_items = []
    for chunk in _chunked(ts_codes, chunk_size):
        codes_str = ",".join(chunk)
        envelope = _run_cli(["market", "auction-snapshot", "--thscodes", codes_str, "--stage", stage])
        data = envelope.get("data") or {}
        items = data.get("item") or []
        outer_phase = data.get("auction_phase")
        outer_status = data.get("data_status")
        outer_ts = data.get("timestamp")
        for item in items:
            # ★ v3 源头精简:thscode → ts_code 重命名 + 删 ticker
            if "thscode" in item and "ts_code" not in item:
                item["ts_code"] = item.pop("thscode")
            item.pop("ticker", None)
            # 注入外层公共字段
            if outer_phase and "auction_phase" not in item:
                item["auction_phase"] = outer_phase
            if outer_status and "data_status" not in item:
                item["data_status"] = outer_status
            # ★ v3 数据时间戳:auction_timestamp(同花顺 envelope 给,毫秒;fallback ths_client)
            if outer_ts and "auction_timestamp" not in item:
                item["auction_timestamp"] = outer_ts
        all_items.extend(items)
        # 小间隔避免被限流
        time.sleep(0.2)

    return _stamp_snapshot_timestamp(all_items, field="auction_timestamp")


def fetch_snapshots(ts_codes: list[str], *, chunk_size: int = THS_CHUNK_SIZE_DEFAULT) -> list[dict]:
    """拉普通行情快照。自动分批。

    Returns:
        list of dict (源头已精简:无 thscode/ticker 字段,统一用 ts_code 标识)

    2026-09-12 v3 源头精简:删 thscode(同 ts_code)/ ticker(可从 ts_code 推导)
    """
    if not ts_codes:
        return []

    all_items = []
    for chunk in _chunked(ts_codes, chunk_size):
        codes_str = ",".join(chunk)
        envelope = _run_cli(["market", "snapshot", "--thscodes", codes_str])
        data = envelope.get("data") or {}
        items = data.get("item") or []
        outer_ts = data.get("timestamp")
        for item in items:
            # ★ v3 源头精简 + 重命名:thscode → ts_code(同值,只是统一名字)
            # 同花顺原字段叫 thscode,我们 v3 统一叫 ts_code(已有 code 后缀的语义)
            if "thscode" in item:
                item["ts_code"] = item.pop("thscode")
            item.pop("ticker", None)
            if outer_ts and "snapshot_timestamp" not in item:
                item["snapshot_timestamp"] = outer_ts
        all_items.extend(items)
        time.sleep(0.2)

    return _stamp_data_timestamp(all_items)


# ============ 异动 / 涨停池 / 飙升 / 热股 ============

def fetch_anomaly_list() -> list[dict]:
    """全天异动清单(带 AI 解读)

    Returns:
        list of dict,每个 item 含 ts_code / stock_name / analysis_content / keyword_list / tag_name

    2026-09-12 v3 源头精简:
        - thscode → ts_code 重命名
        - ticker 删
        - 数据时间戳字段 anomaly_timestamp(由同花顺 envelope / ths_client 兜底打)
    """
    envelope = _run_cli(["special", "anomaly-list"])
    data = envelope.get("data") or {}
    items = data.get("item") or []
    outer_ts = data.get("timestamp")  # 同花顺 envelope 毫秒
    for item in items:
        if "thscode" in item and "ts_code" not in item:
            item["ts_code"] = item.pop("thscode")
        item.pop("ticker", None)
        # ★ v3 数据时间戳兜底
        if outer_ts and "anomaly_timestamp" not in item:
            item["anomaly_timestamp"] = outer_ts
    return _stamp_snapshot_timestamp(items, field="anomaly_timestamp")


def fetch_limitup_pool(*, size: int = THS_POOL_SIZE_DEFAULT, sort_field: str = "last_price", sort_dir: str = "desc") -> list[dict]:
    """当日涨停池

    Args:
        size: 每页 size (1-THS_POOL_SIZE_DEFAULT),默认 THS_POOL_SIZE_DEFAULT = 全量
        sort_field: 排序字段(last_price / continue_day_cnt / seal_money / limit_up_time)
        sort_dir: asc / desc

    2026-09-12 v3 源头精简:
        - thscode → ts_code 重命名
        - ticker 删除
        - 用 _stamp_data_timestamp(field="zt_timestamp") 打数据时间戳
    """
    envelope = _run_cli([
        "special", "limit-up-pool",
        "--size", str(size),
        "--sort-field", sort_field,
        "--sort-dir", sort_dir,
    ])
    data = envelope.get("data") or {}
    items = data.get("item") or []
    for item in items:
        if "thscode" in item:
            item["ts_code"] = item.pop("thscode")
        item.pop("ticker", None)
    return _stamp_data_timestamp(items, field="zt_timestamp")


def fetch_limitdown_pool(*, size: int = THS_POOL_SIZE_DEFAULT) -> list[dict]:
    """当日跌停池

    字段:thscode / last_price / price_change_ratio_pct / first_limit_time / last_limit_time / turnover_ratio_pct
    """
    envelope = _run_cli([
        "special", "limit-down-pool",
        "--size", str(size),
    ])
    data = envelope.get("data") or {}
    return _stamp_data_timestamp(data.get("item") or [])


def fetch_limitbreak_pool(*, size: int = THS_POOL_SIZE_DEFAULT) -> list[dict]:
    """当日炸板池(涨停后开板没回封)

    字段:thscode / last_price / price_change_ratio_pct / first_limit_time / last_limit_time /
         open_times(开板次数)/ turnover_ratio_pct / turnover

    2026-09-12 v3 精简:
        - 源头:thscode → ts_code 重命名 + 删 ticker
        - 数据时间戳:break_timestamp(同花顺 envelope 给,毫秒;fallback ths_client)
    """
    envelope = _run_cli([
        "special", "limit-break-pool",
        "--size", str(size),
    ])
    data = envelope.get("data") or {}
    items = data.get("item") or []
    outer_ts = data.get("timestamp")
    # ★ v3 源头精简:thscode → ts_code 重命名 + 删 ticker
    for item in items:
        if "thscode" in item and "ts_code" not in item:
            item["ts_code"] = item.pop("thscode")
        item.pop("ticker", None)
        # ★ 数据时间戳(同花顺 envelope 给)
        if outer_ts and "break_timestamp" not in item:
            item["break_timestamp"] = outer_ts
    return _stamp_snapshot_timestamp(items, field="break_timestamp")


def fetch_index_snapshot(ths_codes: list[str]) -> list[dict]:
    """指数实时快照

    Args:
        ths_codes: 指数 thscode 列表(最多 100),如
            ['000001.SH', '399001.SZ', '399006.SZ', '000688.SH']
    """
    if not ths_codes:
        return []
    codes_str = ",".join(ths_codes)
    envelope = _run_cli(["index", "snapshot", "--thscodes", codes_str])
    data = envelope.get("data") or {}
    items = data.get("item") or []
    outer_ts = data.get("timestamp")
    for item in items:
        if outer_ts and "snapshot_timestamp" not in item:
            item["snapshot_timestamp"] = outer_ts
    return _stamp_data_timestamp(items)


def fetch_skyrocket(*, period: str = "hour") -> list[dict]:
    """飙升榜(按 heat 排序)

    Args:
        period: 'day' 或 'hour'
    """
    envelope = _run_cli(["special", "skyrocket", "--period", period])
    data = envelope.get("data") or {}
    return _stamp_data_timestamp(data.get("item") or [])


def fetch_hot_stock(*, period: str = "hour") -> list[dict]:
    """热股榜(按 heat 排序)

    Args:
        period: 'day' 或 'hour'

    2026-09-12 v3 精简:
        - 源头:thscode → ts_code 重命名 + 删 ticker
        - 数据时间戳:hot_timestamp(同花顺 envelope 给,毫秒;fallback ths_client)
    """
    envelope = _run_cli(["special", "hot-stock", "--period", period])
    data = envelope.get("data") or {}
    items = data.get("item") or []
    outer_ts = data.get("timestamp")
    # ★ v3 源头精简:thscode → ts_code 重命名 + 删 ticker
    for item in items:
        if "thscode" in item and "ts_code" not in item:
            item["ts_code"] = item.pop("thscode")
        item.pop("ticker", None)
        # ★ 数据时间戳(同花顺 envelope 给)
        if outer_ts and "hot_timestamp" not in item:
            item["hot_timestamp"] = outer_ts
    return _stamp_snapshot_timestamp(items, field="hot_timestamp")


if __name__ == "__main__":
    # 自检
    print("=== auction-snapshot ===")
    items = fetch_auction_snapshots(["000426.SZ", "600519.SH"], stage="live")
    for it in items:
        print(f"  {it.get('thscode')} phase={it.get('auction_phase')} price={it.get('auction_price')}")

    print("\n=== snapshot ===")
    items = fetch_snapshots(["000426.SZ", "600519.SH"])
    for it in items:
        print(f"  {it.get('thscode')} last={it.get('last_price')}")

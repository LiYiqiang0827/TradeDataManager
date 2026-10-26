"""
data_provider.py — 统一数据接入层(2026-09-17 新增)

设计目标:
  - AI agent(尤其 policyStudy 研究代码)只需要懂这一套接口
  - source="database"(默认):本地 db,缺失自动 fallback 到 online
  - source="online":强制走实时 api
  - 统一提供本地数据库、Parquet和在线数据接口

用法:
  from coreClient.data_provider import get_minute, get_day, get_kpl_limit_performance

  df = get_minute("000006.SZ", trade_date="2026-08-28")
  # db 有 → 直接读;db 没有 → 自动 tdx 拉

  df = get_day("000006.SZ", trade_date="2026-08-28", qfq=True)
  # db 有 → 读前复权日 K;db 没有 → 自动 tushare 拉

接口清单:
  分钟 / 分笔:  get_oneMin / get_fifteenMin(全市场完整OHLC) / get_minute(旧分时价) / get_minute_index / get_ticks
  日线:        get_day / get_week / get_month (qfq 默认 True)
  基础:        get_basic / get_adj_factor / get_stk_limit / get_daily_basic / get_moneyflow
  指数:        get_index_basic / get_index_daily / get_index_weekly / get_index_monthly
  日历:        get_tradecal
  KPL:         get_kpl_list / get_kpl_concept_cons / get_kpl_limit_performance
  题材知识库:  get_theme_profile / get_theme_members / get_theme_daily / get_market_theme_review
                get_market_theme_review_series / get_theme_cycle_data / get_theme_analyses
                get_theme_taxonomy / get_stock_theme_history
  新闻:        get_news
"""
from __future__ import annotations

import sys
import json
import hashlib
from collections import Counter
from pathlib import Path
from datetime import datetime
from typing import Optional, Union

import pandas as pd
import logging
logger = logging.getLogger(__name__)


# ============================================================================
# 路径注入(2026-09-17)
# offline_db_client 内部要 import config.settings(需要 offlineDataManager/scripts/ 在 sys.path)
# coreClient 是 offlineDataManager 的同级目录,这里从 data_provider.py 反推路径
# ============================================================================
from coreClient._paths import manager_root

_MANAGER_ROOT = manager_root(__file__, 1)
_OFFLINE_SCRIPTS = _MANAGER_ROOT / "offlineDataManager" / "scripts"
if str(_OFFLINE_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_OFFLINE_SCRIPTS))


# ============================================================================
# client 单例(延迟初始化)
# ============================================================================
_tushare_client = None
_tdx_client = None
_kpl_client = None


def _get_tushare():
    global _tushare_client
    if _tushare_client is None:
        from coreClient.tushare_client import TushareClient
        _tushare_client = TushareClient()
    return _tushare_client


def _get_tdx():
    global _tdx_client
    if _tdx_client is None:
        from coreClient.tdx_client import TdxClient
        _tdx_client = TdxClient()
    return _tdx_client


def _get_kpl():
    global _kpl_client
    if _kpl_client is None:
        from coreClient.kpl_client import KPLClient
        _kpl_client = KPLClient()
    return _kpl_client


def _to_yyyymmdd(d: Union[str, datetime, None]) -> Optional[str]:
    """'2026-08-28' / datetime → '20260828',None 透传"""
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.strftime("%Y%m%d")
    s = str(d)
    if len(s) == 10 and s[4] == "-":
        return s.replace("-", "")
    if len(s) == 8 and s.isdigit():
        return s
    return s


def _to_dash_date(d: Union[str, datetime, None]) -> Optional[str]:
    """统一转 YYYY-MM-DD(用于 trade_date / start_date / end_date)"""
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    s = str(d)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if len(s) == 10 and s[4] == "-":
        return s
    return s


def _expand_trading_dates(start_date: str, end_date: str) -> List[str]:
    """把 [start, end] 日期段过交易日历过滤,返回 YYYY-MM-DD 列表

    借助 db_client.get_tradecal(market='SSE'),不联网,纯本地
    """
    from offlineDataManager.scripts.core.offline_db_client import get_tradecal

    sd_compact = _to_yyyymmdd(start_date)
    ed_compact = _to_yyyymmdd(end_date)
    cal_df = get_tradecal(start_date=sd_compact, end_date=ed_compact, market="SSE", is_open=True)
    if cal_df is None or len(cal_df) == 0:
        return []
    return [_to_dash_date(d) for d in cal_df["cal_date"].astype(str).tolist()]


# ============================================================================
# Online 多日 / 多股 拆日循环(2026-09-17)
# online 限制(用户原话):
#   - get_minute/get_ticks 必须传 ts_code(单只),不能全 None
#   - get_minute_index 不传 ts_code 默认 INDEX_CODES_HERE(5 指数)
#   - 多 ts 或 多 trade_date 二选一,不能同时多个
#   - 总调用次数 ≤ 30
# ============================================================================
_ONLINE_CALL_LIMIT = 30


def _online_call_count(ts_codes: list, dates: list) -> int:
    """算 online 实际调用次数(笛卡尔积)|"""
    return len(ts_codes) * len(dates)


def _validate_online_minute(
    ts_code: Optional[str],
    ts_codes: Optional[list],
    trade_date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    *,
    default_ts_codes: Optional[list] = None,
    allow_ts_code_none: bool = False,
    require_date: bool = True,
) -> tuple:
    """online 路径参数校验

    Args:
        ts_code: 单只(可省略)
        ts_codes: 多只(可省略,优先级高于 ts_code)
        default_ts_codes: 当 ts_code/ts_codes 都没传时,默认使用(如 INDEX_CODES_HERE)
        allow_ts_code_none: True 允许 ts_code/ts_codes 都为空(如 minute_index)
        require_date: True(默认)要求至少传一个日期参数
    """
    # 1. 选 ts_codes
    if ts_codes:
        ts_list = list(ts_codes)
    elif ts_code:
        ts_list = [ts_code]
    elif default_ts_codes:
        ts_list = list(default_ts_codes)
    else:
        if allow_ts_code_none:
            ts_list = []
        else:
            raise ValueError("online 模式:必须传 ts_code 或 ts_codes(单只/多只)")

    # 2. 选 dates
    if trade_date and (start_date or end_date):
        raise ValueError("online 模式:trade_date 跟 start_date/end_date 互斥")

    if trade_date:
        dates_list = [_to_dash_date(trade_date)]
    elif start_date or end_date:
        sd = start_date or end_date
        ed = end_date or start_date
        dates_list = _expand_trading_dates(sd, ed)
    else:
        dates_list = []

    # 2.5 必须传日期(get_minute/get_ticks 必须;get_minute_index 不强求因为有默认 ts 列表)
    if require_date and not dates_list:
        raise ValueError("online 模式:必须传 trade_date 或 (start_date + end_date)")

    # 3. 二选一约束(用户原话)
    # 例外:ts_list 是 default_ts_codes(不是 caller 主动传的),只校验日期不超过 30
    ts_from_caller = (ts_codes is not None) or (ts_code is not None)
    if len(ts_list) > 1 and len(dates_list) > 1 and ts_from_caller:
        raise ValueError(
            "online 模式:多 ts_code 跟多 trade_date 不能同时(笛卡尔积爆炸);"
            "请二选一:多 ts 单日,或多日单 ts"
        )

    # 4. 调用次数限制
    # 实际调用次数 = max(len(ts_list), len(dates_list)) * min(...) 不直观
    # 简化:分别校验两个维度,只要有一个超过限制就报错
    # (实际 tdx = ts × dates,KPL = dates,分钟指数 = 5 × dates)
    if len(dates_list) > _ONLINE_CALL_LIMIT:
        raise ValueError(
            f"online 多日请求 {len(dates_list)} 超过限制 {_ONLINE_CALL_LIMIT}"
        )
    if len(ts_list) > _ONLINE_CALL_LIMIT:
        raise ValueError(
            f"online 多 ts 请求 {len(ts_list)} 超过限制 {_ONLINE_CALL_LIMIT}"
        )
    n_calls = _online_call_count(ts_list, dates_list)
    if n_calls > _ONLINE_CALL_LIMIT:
        raise ValueError(
            f"online 调用次数 {n_calls} 超过限制 {_ONLINE_CALL_LIMIT};"
            f"ts_codes={len(ts_list)} × dates={len(dates_list)}。"
            f"请缩小范围(单只+多日 / 多只+单日)"
        )

    return ts_list, dates_list


def _validate_online_ts_code(
    ts_code: Optional[str],
    ts_codes: Optional[list],
    method_name: str,
) -> None:
    """online 路径校验 ts_code/ts_codes 必传(tushare 不支持全市场)

    已在 _online_loop_ts_codes 内部处理,但有些接口没走 helper,需要显式调
    """
    if not ts_code and not ts_codes:
        raise ValueError(
            f"online 模式 {method_name} 必须传 ts_code 或 ts_codes "
            f"(tushare 不支持全市场查询)"
        )


def _validate_online_date(
    trade_date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    method_name: str,
) -> None:
    """online 路径校验日期:必传 + trade_date/start_date 互斥"""
    if not trade_date and not (start_date and end_date):
        raise ValueError(
            f"online 模式 {method_name} 必须传 trade_date 或 (start_date + end_date)"
        )
    if trade_date and (start_date or end_date):
        raise ValueError(
            f"online 模式 {method_name} trade_date 跟 start_date/end_date 互斥"
        )


def _online_loop_ts_codes(
    method_name: str,
    ts_code: Optional[str],
    ts_codes: Optional[list],
    base_kwargs: dict,
    *,
    require_ts_code: bool = True,
    require_date: bool = False,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """online 路径上处理 ts_codes 列表

    tushare 各 method 对 ts_code 多值支持不一致:
      - daily: 支持逗号分隔的多个 ts_code
      - weekly / monthly / adj_factor / stk_limit / moneyflow / kpl_concept_cons: 不支持,需循环

    Args:
        method_name: tushare method 名(传给 _get_tushare().<method_name>)
        ts_code: 单值 str(透传)
        ts_codes: 列表,逐个循环
        base_kwargs: 其他 kwargs(dict)
        require_ts_code: True=ts_code/ts_codes 必须传一个(默认);False=允许 None(全市场)
        require_date: True=必须传日期(单日或多日);False=允许 None
        trade_date: 透传日期参数给 _validate_online_date
        start_date: 同上
        end_date: 同上

    Returns:
        拼接后的 pd.DataFrame(失败/空则跳过)
    """
    # 守卫:这些 tushare method 不支持"全市场"(必须传 ts_code)
    if require_ts_code and not ts_code and not ts_codes:
        raise ValueError(
            f"online 模式 {method_name} 必须传 ts_code 或 ts_codes "
            f"(tushare 不支持全市场查询)"
        )

    # 守卫:必传日期(可选开启)
    if require_date:
        _validate_online_date(trade_date, start_date, end_date, method_name)

    client = _get_tushare()

    if not ts_codes:
        # 单 ts_code(透传,可能 None)
        return getattr(client, method_name)(**base_kwargs)

    # 循环每个 ts_code
    frames = []
    for tc in ts_codes:
        kw = {**base_kwargs, "ts_code": tc}
        try:
            df = getattr(client, method_name)(**kw)
        except Exception:
            df = None
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _online_loop_tdx(
    ts_list: list,
    dates_list: list,
    *,
    kind: str,  # 'minute' / 'ticks' / 'minute_index'
) -> pd.DataFrame:
    """online 拆日循环调用 tdx 拉数据,按 kind 决定列结构 + 拼接

    限制(已由 _validate_online_minute 保证):
      - ts_list 或 dates_list 至少一个为空
      - len(ts_list) * len(dates_list) <= _ONLINE_CALL_LIMIT

    Returns:
        pd.DataFrame,按 (ts_code, trade_date, time_idx/seqId) 升序,带 created_at=None 列
    """
    client = _get_tdx()

    # 选 method + 字段映射
    method_map = {
        "minute":       ("get_history_minute", ["ts_code", "trade_date", "datetime", "time_idx", "price", "vol"]),
        "minute_index": ("get_history_minute", ["ts_code", "trade_date", "datetime", "time_idx", "price", "vol"]),
        "ticks":        ("get_history_ticks", ["ts_code", "trade_date", "datetime", "time", "seqId", "price", "vol", "buyorsell"]),
    }
    if kind not in method_map:
        raise ValueError(f"_online_loop_tdx 未知 kind: {kind}")
    method_name, cols = method_map[kind]
    method = getattr(client, method_name)

    if not ts_list or not dates_list:
        return pd.DataFrame(columns=cols + ["created_at"])

    frames = []
    # 二选一循环(根据 _validate_online_minute 的约束)
    def _safe_to_df(rows, cols):
        """rows 是 list[dict] 或 None → DataFrame(cols)
        如果 rows 不是 list(防御性),返回空 schema DataFrame
        """
        if not rows or not isinstance(rows, list):
            return pd.DataFrame(columns=cols)
        df = pd.DataFrame(rows)
        # 如果 df 没有 cols(空 list),用 cols 作 columns
        if df.empty and not df.columns.tolist():
            df = pd.DataFrame(columns=cols)
        # 只保留期望的列
        for c in cols:
            if c not in df.columns:
                df[c] = None
        return df[cols]

    if len(ts_list) > 1:
        # 多 ts 单日(每个 ts 一次调用)
        for ts in ts_list:
            rows = method(ts, int(_to_yyyymmdd(dates_list[0])))
            df = _safe_to_df(rows, cols)
            df["created_at"] = None
            frames.append(df)
    elif len(dates_list) > 1:
        # 单 ts 多日(每天一次调用)
        for d in dates_list:
            rows = method(ts_list[0], int(_to_yyyymmdd(d)))
            df = _safe_to_df(rows, cols)
            df["created_at"] = None
            frames.append(df)
    else:
        # 单只单日(基础情况,跟旧实现兼容)
        ts = ts_list[0]
        d = dates_list[0]
        rows = method(ts, int(_to_yyyymmdd(d)))
        df = _safe_to_df(rows, cols)
        df["created_at"] = None
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=cols + ["created_at"])
    return pd.concat(frames, ignore_index=True)


# ============================================================================
# source 路由核心逻辑
# ============================================================================
def _route(source: str, db_func, online_func, *args, **kwargs) -> pd.DataFrame:
    """source="database_only" 只读 db;空表不联网，异常原样抛出。
    source="database" 走 db;空就 fallback online
    source="online" 直接走 online

    注意:source="database" 模式 caller 可能不传日期参数(让 db 返回全表),
    这种情况下 db 模式返回 0 行也属于正常业务(可能是 db 真的没数据),
    不应该 fallback 到 online 并要求日期。
    """
    if source == "database_only":
        df = db_func(*args, **kwargs)
        return df if df is not None else pd.DataFrame()
    if source == "online":
        return online_func(*args, **kwargs)
    elif source == "database":
        df = db_func(*args, **kwargs)
        if df is not None and not df.empty:
            return df
        # db 没有,fallback 到 online
        # 如果 caller 没传日期参数(db 模式"全表"语义),fallback 也没意义
        # 因为 online 必须传日期,这种情况给清晰报错
        try:
            return online_func(*args, **kwargs)
        except ValueError as e:
            if "必须传 trade_date" in str(e) or "date" in str(e).lower():
                # caller 没传日期参数,db 真的没数据 — 报清晰错误
                raise ValueError(
                    f"db 模式无数据,且 online 模式需要日期参数;请传 trade_date 或 (start_date + end_date) 重新调用。原始错误: {e}"
                )
            raise
    else:
        raise ValueError(f"未知 source: {source!r},只支持 'database_only' / 'database' / 'online'")


# ============================================================================
# 分钟 / 分笔 / 指数分钟(都走 tdx)
# ============================================================================
def get_fifteenMin(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[list] = None,
    data_root: Optional[Union[str, Path]] = None,
    date_ranges: Optional[list] = None,
) -> pd.DataFrame:
    """读取TDX原生全市场15分钟OHLC DuckDB数据。

    主库为 ``offlineDataManager/data/db_fifteenMinute.db``。该接口不再读取
    原淘宝CSV转换库，也不在线回退；下载和增量更新由
    ``service_fifteenMinute.py``负责。
    ``date_ranges=[(start, end), ...]`` 可在一次查询中读取多个窗口，
    与 trade_date/start_date/end_date 互斥，不读取窗口之间的空档。
    """
    from offlineDataManager.scripts.core.offline_db_client import get_fifteen_min

    return get_fifteen_min(
        ts_code=ts_code,
        ts_codes=ts_codes,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        data_root=data_root,
        date_ranges=date_ranges,
    )


def get_oneMin(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[list] = None,
    data_root: Optional[Union[str, Path]] = None,
    date_ranges: Optional[list] = None,
) -> pd.DataFrame:
    """读取全市场完整1分钟OHLC Parquet数据。

    这是独立于 ``get_minute`` / ``policy_minute.db`` 的接口，不读取旧SQLite，
    也不进行在线回退。支持全市场单日、单股/多股单日和股票日期范围查询。

    返回字段默认包括::

        ts_code, name, trade_date, datetime, time_idx,
        open, high, low, close, vol, amount, adj_factor
    ``date_ranges=[(start, end), ...]`` 可在一次查询中读取多个窗口，
    与 trade_date/start_date/end_date 互斥，不读取窗口之间的空档。
    """
    from offlineDataManager.scripts.core.one_min_store import get_one_min

    frame = get_one_min(
        ts_code=ts_code,
        ts_codes=ts_codes,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        root=data_root,
        date_ranges=date_ranges,
    )
    frame.attrs["quality_query"] = "get_oneMin_quality(trade_date=..., ts_code=...)"
    if trade_date is not None:
        quality = get_oneMin_quality(trade_date=trade_date, ts_codes=ts_codes,
                                     ts_code=ts_code, data_root=data_root)
        frame.attrs["quality_summary"] = _one_min_quality_summary(
            trade_date, quality, data_root=data_root,
            code_scoped=bool(ts_code or ts_codes), ts_code=ts_code, ts_codes=ts_codes,
        )
    return frame


def _day_level_quality_audit(manifest):
    historical = manifest.get("historical_quality_audit") or {}
    if historical.get("status") != "audited":
        return None
    return {
        "coverage_status": "audited_numeric_domains",
        "quality_scope": "whole-day numeric-domain audit; not source-price validation",
        "informational_decoder_correction_rows": historical.get("informational_decoder_correction_rows"),
        "zero_reported_volume_positive_amount_rows_after_correction": historical.get("zero_reported_volume_positive_amount_rows_after_correction"),
        "source_price_validation_status": manifest.get("source_price_validation_status", "not_audited"),
    }


def _merge_one_min_quality_rows(frame: pd.DataFrame) -> pd.DataFrame:
    keys = ["trade_date", "ts_code", "time_idx"]
    if frame.empty:
        return frame
    duplicate_mask = frame.duplicated(keys, keep=False)
    if not duplicate_mask.any():
        return frame.sort_values(["ts_code", "time_idx"], kind="stable").reset_index(drop=True)
    unique_rows = frame.loc[~duplicate_mask]
    duplicate_rows = frame.loc[duplicate_mask]
    merged = []
    for _, group in duplicate_rows.groupby(keys, sort=False, dropna=False):
        first = group.iloc[0].to_dict()
        flags = sorted({
            flag
            for encoded in group["quality_flags"]
            for flag in json.loads(encoded)
        })
        sources = sorted(set(group["source"].dropna().astype(str))) if "source" in group else []
        payloads = []
        for payload in group.get("source_payload_json", pd.Series(dtype=str)).dropna().astype(str):
            if payload not in payloads:
                payloads.append(payload)
        first["quality_flags"] = json.dumps(flags, separators=(",", ":"))
        if sources:
            first["source"] = "+".join(sources)
        if len(payloads) > 1:
            first["source_payload_json"] = json.dumps({"records": payloads}, sort_keys=True, separators=(",", ":"))
            first["source_payload_sha256"] = hashlib.sha256(first["source_payload_json"].encode("utf-8")).hexdigest()
        merged.append(first)
    return pd.concat([unique_rows, pd.DataFrame(merged)], ignore_index=True).sort_values(
        ["ts_code", "time_idx"], kind="stable"
    ).reset_index(drop=True)


def _one_min_quality_summary(trade_date, quality, *, data_root=None, code_scoped=False, ts_code=None, ts_codes=None):
    """Summarize only audited quality coverage; an absent sidecar is unknown."""
    import json

    day = _to_yyyymmdd(trade_date)
    if data_root is None:
        from config.settings import ONE_MIN_PARQUET_ROOT
        base = Path(ONE_MIN_PARQUET_ROOT)
    else:
        base = Path(data_root)
    manifest_path = base / "_manifest" / f"trade_date={day}.json"
    sidecar_path = base / "_quality" / f"trade_date={day}" / "part-000.parquet"
    manifest = {}
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = {}

    historical = manifest.get("historical_quality_audit") or {}
    source_audit = manifest.get("quality_audit") or {}
    source_audit_exceptions = None
    if source_audit.get("exception_counts"):
        relative = source_audit.get("relative_to_manifest_dir") or {}
        acceptance_path = (
            (manifest_path.parent / relative["acceptance_manifest"]).resolve()
            if relative.get("acceptance_manifest") else None
        )
        evidence = {
            "status": source_audit.get("status"),
            "query_surface_status": "available",
            "scope": source_audit.get("scope"),
            "exception_counts": source_audit.get("exception_counts"),
            "evidence_path": str(acceptance_path) if acceptance_path else None,
            "evidence_sha256": (source_audit.get("sha256") or {}).get("acceptance_manifest"),
            "evidence_integrity_status": "unavailable",
            "details": {},
        }
        selected_codes = {str(code).upper() for code in (ts_codes or ([ts_code] if ts_code else []))}
        if selected_codes and acceptance_path and acceptance_path.is_file():
            try:
                acceptance_bytes = acceptance_path.read_bytes()
                actual_sha = hashlib.sha256(acceptance_bytes).hexdigest()
                expected_sha = evidence["evidence_sha256"]
                evidence["evidence_integrity_status"] = (
                    "verified" if expected_sha and actual_sha == expected_sha else "mismatch"
                )
                acceptance = json.loads(acceptance_bytes.decode("utf-8"))
                if evidence["evidence_integrity_status"] != "verified":
                    evidence["details_status"] = "evidence_hash_mismatch"
                else:
                    ratio_exceptions = acceptance.get("daily_ratio_exceptions") or {}
                    ohlc_exceptions = acceptance.get("daily_ohlc_field_exceptions_vs_database") or {}
                    crosschecks = acceptance.get("category9_exception_crosschecks") or []
                    amount_checks = acceptance.get("tdx_daily_amount_float32_precision_checks") or []
                    aggregate = acceptance.get("aggregate_quality_exceptions") or {}
                    evidence["details"] = {
                        "daily_ratio_exceptions": {
                            key: [row for row in rows if str(row.get("ts_code", "")).upper() in selected_codes]
                            for key, rows in ratio_exceptions.items()
                        },
                        "daily_ohlc_field_exceptions_vs_database": {
                            key: [row for row in rows if str(row.get("ts_code", "")).upper() in selected_codes]
                            for key, rows in ohlc_exceptions.items()
                        },
                        "category9_exception_crosschecks": [
                            row for row in crosschecks if str(row.get("ts_code", "")).upper() in selected_codes
                        ],
                        "tdx_daily_amount_float32_precision_checks": [
                            row for row in amount_checks if str(row.get("ts_code", "")).upper() in selected_codes
                        ],
                        "aggregate_quality_exceptions": {
                            key: [row for row in rows if str(row.get("ts_code", "")).upper() in selected_codes]
                            if isinstance(rows, list) else rows
                            for key, rows in aggregate.items()
                        },
                    }
            except (OSError, ValueError):
                evidence["details_status"] = "evidence_unreadable"
        elif selected_codes:
            evidence["details_status"] = "evidence_unavailable"
        else:
            evidence["details_status"] = "code_filter_required_for_row_details"
        source_audit_exceptions = evidence
    day_level_audit = None
    if historical.get("status") == "audited":
        day_level_audit = {
            "coverage_status": "audited_numeric_domains",
            "quality_scope": "whole-day numeric-domain audit; not source-price validation",
            "informational_decoder_correction_rows": historical.get("informational_decoder_correction_rows"),
            "zero_reported_volume_positive_amount_rows_after_correction": historical.get("zero_reported_volume_positive_amount_rows_after_correction"),
            "source_price_validation_status": manifest.get("source_price_validation_status", "not_audited"),
        }
    is_correction = lambda flag: flag.startswith("pytdx_zero_") and flag.endswith("_corrected")
    if not quality.empty:
        flag_sets = [json.loads(encoded) for encoded in quality["quality_flags"]]
        counts = Counter(flag for flags in flag_sets for flag in flags)
        corrections = {key: value for key, value in sorted(counts.items()) if is_correction(key)}
        warnings = {key: value for key, value in sorted(counts.items()) if not is_correction(key)}
        sources = set()
        if "source" in quality.columns:
            sources = {
                part
                for value in quality["source"].dropna().astype(str)
                for part in value.split("+")
            }
        if "historical_stored_values" in sources:
            coverage_status = "sidecar_plus_derived_stored_values" if len(sources) > 1 else "derived_historical_stored_values"
            stock_level_status = "derived_warning_rows_for_query"
        else:
            coverage_status = "sidecar_present"
            stock_level_status = "sidecar_flags_for_query"
        return {
            "trade_date": day,
            "coverage_status": coverage_status,
            "quality_scope": "quality flags matching this query; derived warnings are from stored values, not source payloads",
            "informational_decoder_correction_rows": sum(any(is_correction(flag) for flag in flags) for flags in flag_sets),
            "soft_quality_warning_rows": sum(any(not is_correction(flag) for flag in flags) for flags in flag_sets),
            "informational_decoder_corrections": corrections,
            "soft_quality_warnings": warnings,
            "quality_records_in_scope": len(quality),
            "quality_bars_in_scope": quality[["trade_date", "ts_code", "time_idx"]].drop_duplicates().shape[0],
            "stock_level_status": stock_level_status,
            "day_level_audit": day_level_audit,
            "source_audit_exceptions": source_audit_exceptions,
            "source_price_validation_status": manifest.get("source_price_validation_status", "not_audited"),
            "query": "get_oneMin_quality(trade_date=..., ts_code=...)",
        }

    if sidecar_path.is_file():
        return {
            "trade_date": day,
            "coverage_status": "sidecar_present_no_matching_flags",
            "quality_scope": "sidecar flags matching this query",
            "informational_decoder_correction_rows": 0,
            "soft_quality_warning_rows": 0,
            "informational_decoder_corrections": {},
            "soft_quality_warnings": {},
            "sidecar_rows_in_scope": 0,
            "stock_level_status": "no_matching_sidecar_flags",
            "day_level_audit": day_level_audit,
            "source_audit_exceptions": source_audit_exceptions,
            "source_price_validation_status": manifest.get("source_price_validation_status", "not_audited"),
            "query": "get_oneMin_quality(trade_date=..., ts_code=...)",
        }
    if historical.get("status") == "audited" and not code_scoped:
        return {
            "trade_date": day,
            "coverage_status": "audited_numeric_domains",
            "quality_scope": "whole-day numeric-domain audit; not source-price validation",
            "informational_decoder_correction_rows": historical.get("informational_decoder_correction_rows"),
            "soft_quality_warning_rows": historical.get("zero_reported_volume_positive_amount_rows_after_correction"),
            "informational_decoder_corrections": {"pytdx_zero_amount_corrected": historical.get("informational_decoder_correction_rows")},
            "soft_quality_warnings": {"zero_reported_volume_positive_amount": historical.get("zero_reported_volume_positive_amount_rows_after_correction")},
            "sidecar_rows_in_scope": None,
            "stock_level_status": "not_applicable_whole_day_query",
            "day_level_audit": day_level_audit,
            "source_audit_exceptions": source_audit_exceptions,
            "source_price_validation_status": manifest.get("source_price_validation_status", "not_audited"),
            "query": "get_oneMin_quality(trade_date=..., ts_code=...)",
        }
    if historical.get("status") == "audited":
        return {
            "trade_date": day,
            "coverage_status": "audited_numeric_domains_scope_unavailable",
            "quality_scope": "whole-day numeric-domain audit exists; per-code counts are unavailable",
            "informational_decoder_correction_rows": None,
            "soft_quality_warning_rows": None,
            "informational_decoder_corrections": None,
            "soft_quality_warnings": None,
            "sidecar_rows_in_scope": None,
            "stock_level_status": "unknown_no_per_code_sidecar",
            "day_level_audit": day_level_audit,
            "source_audit_exceptions": source_audit_exceptions,
            "source_price_validation_status": manifest.get("source_price_validation_status", "not_audited"),
            "query": "get_oneMin_quality(trade_date=..., ts_code=...)",
        }
    return {
        "trade_date": day,
        "coverage_status": "unknown_not_audited",
        "quality_scope": "no sidecar or applicable whole-day numeric audit",
        "informational_decoder_correction_rows": None,
        "soft_quality_warning_rows": None,
        "informational_decoder_corrections": None,
        "soft_quality_warnings": None,
        "sidecar_rows_in_scope": None,
        "stock_level_status": "unknown_not_audited",
        "day_level_audit": None,
        "source_audit_exceptions": source_audit_exceptions,
        "source_price_validation_status": manifest.get("source_price_validation_status", "not_audited"),
        "query": "get_oneMin_quality(trade_date=..., ts_code=...)",
    }


def get_oneMin_quality(
    trade_date: str,
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    data_root: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    """查询TDX原生1分钟Parquet的独立来源质量sidecar。"""
    from offlineDataManager.scripts.core.tdx_one_min_store import get_one_min_quality

    frame = get_one_min_quality(trade_date, ts_code=ts_code, root=data_root)
    if ts_codes:
        selected = {str(code).upper() for code in ts_codes}
        frame = frame[frame["ts_code"].astype(str).str.upper().isin(selected)].reset_index(drop=True)
    if data_root is None:
        from config.settings import ONE_MIN_PARQUET_ROOT
        base = Path(ONE_MIN_PARQUET_ROOT)
    else:
        base = Path(data_root)
    day = _to_yyyymmdd(trade_date)
    if ts_code and not ts_codes:
        frame = frame[frame["ts_code"].astype(str).str.upper() == str(ts_code).upper()].reset_index(drop=True)
    partition_glob = str(base / f"year={day[:4]}" / f"month={day[4:6]}" /
                          f"trade_date={day}" / "part-*.parquet")
    # Older partitions lack per-bar sidecars. Expose the known zero-volume /
    # positive-amount condition from stored values, clearly labeled as derived.
    partition_files = list((base / f"year={day[:4]}" / f"month={day[4:6]}" /
                            f"trade_date={day}").glob("part-*.parquet"))
    if partition_files:
        import duckdb
        predicates = []
        parameters = [partition_glob]
        selected_codes = [str(code).upper() for code in ts_codes] if ts_codes else None
        if selected_codes:
            predicates.append("upper(ts_code) IN (" + ",".join("?" for _ in selected_codes) + ")")
            parameters.extend(selected_codes)
        elif ts_code:
            predicates.append("upper(ts_code)=?")
            parameters.append(str(ts_code).upper())
        where_sql = " WHERE " + " AND ".join(predicates) if predicates else ""
        with duckdb.connect(":memory:") as conn:
            derived = conn.execute(
                "SELECT trade_date, ts_code, time_idx, datetime, vol, amount "
                "FROM read_parquet(?)" + where_sql +
                (" AND " if where_sql else " WHERE ") +
                "vol=0 AND amount>0 ORDER BY ts_code,time_idx",
                parameters,
            ).fetchdf()
    else:
        derived = pd.DataFrame(columns=["trade_date", "ts_code", "time_idx", "datetime", "vol", "amount"])
    if not derived.empty:
        existing_warning_keys = set()
        if not frame.empty:
            for row in frame.itertuples(index=False):
                try:
                    flags = json.loads(row.quality_flags)
                except (TypeError, ValueError):
                    flags = []
                if "amount_nonzero_with_zero_volume" in flags:
                    existing_warning_keys.add((str(row.trade_date), str(row.ts_code), int(row.time_idx)))
        selected_codes = {str(item).upper() for item in ts_codes} if ts_codes else None
        rows = []
        for row in derived.itertuples(index=False):
            code = str(row.ts_code).upper()
            if ts_code and code != str(ts_code).upper():
                continue
            if selected_codes is not None and code not in selected_codes:
                continue
            row_day = str(row.trade_date)
            if len(row_day) == 8 and row_day.isdigit():
                row_day = f"{row_day[:4]}-{row_day[4:6]}-{row_day[6:]}"
            key = (row_day, code, int(row.time_idx))
            if key in existing_warning_keys:
                continue
            payload = json.dumps({
                "vol": int(row.vol), "amount": float(row.amount),
                "provenance": "derived_from_current_canonical_stored_values_not_original_source_payload",
            }, sort_keys=True, separators=(",", ":"))
            rows.append({
                "trade_date": row_day, "ts_code": code, "time_idx": int(row.time_idx),
                "datetime": row.datetime, "quality_flags": '["amount_nonzero_with_zero_volume"]',
                "source": "historical_stored_values", "source_payload_json": payload,
                "source_payload_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            })
        if rows:
            derived_frame = pd.DataFrame(rows)
            frame = derived_frame if frame.empty else pd.concat([frame, derived_frame], ignore_index=True)
    return _merge_one_min_quality_rows(frame)


def get_minute(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """个股分钟 K(240 根/天)

    Args:
        ts_code: 单只 e.g. '000006.SZ'(与 ts_codes 互斥,ts_codes 优先)
        ts_codes: 多只 e.g. ['000006.SZ','000636.SZ']
        trade_date: 单日 '2026-08-28' / '20260828'
        start_date: 日期段起始(YYYY-MM-DD / YYYYMMDD)
        end_date: 日期段结束
        source: 'database' / 'online'

    数据库位置:policy_minute.db / tbl_minute + tbl_minute_ctrl
    online:TdxClient.get_history_minute(ts_code, date_int) 单只单日;
           多 ts/多日 → 拆成单只单日循环,≤ 30 次
    """
    from offlineDataManager.scripts.core.offline_db_client import PolicyDBClient

    def _db():
        c = PolicyDBClient()
        return c.get_minute(
            ts_codes=ts_codes if ts_codes else ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )

    def _online():
        ts_list, dates_list = _validate_online_minute(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date, start_date=start_date, end_date=end_date,
        )
        return _online_loop_tdx(ts_list, dates_list, kind="minute")

    return _route(source, _db, _online)


def get_minute_index(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """大盘指数分时兼容接口，优先原生指数分钟库，历史缺口读旧库。

    新代码优先使用 get_index_oneMin（完整OHLC和成交额）。本兼容接口的
    price 为分钟收盘点位，vol 为万元成交额，不是个股成交股数。

    Args:
        ts_code: 单指数 e.g. '000001.SH'(可省略)
        ts_codes: 多指数列表(可省略)
        trade_date: 单日
        start_date: 日期段起始
        end_date: 日期段结束
        source: 'database' / 'online'

    数据库位置:db_cn_index_minute.db；较早历史回退到 policy_minute.db。
    online:指数专用 get_index_bars 解码；不写库。
        不传 ts_code → 默认网页的8个指数
        多 ts/多日 → 拆成单只单日循环,≤ 30 次
    """
    from offlineDataManager.scripts.core.offline_db_client import PolicyDBClient
    from coreClient.index_minute import INDEX_CODES as _NATIVE_INDEX

    def _compat(frame):
        if frame is None or frame.empty:
            return pd.DataFrame()
        frame=frame.copy()
        frame["price"]=frame["close"]
        frame["vol"]=frame["amount"]/10000.0
        frame["volume_unit"]="turnover_10000_yuan"
        frame["trade_date"]=pd.to_datetime(frame["trade_date"],format="%Y%m%d").dt.strftime("%Y-%m-%d")
        minutes=pd.to_datetime(frame["datetime"]).dt.hour*60+pd.to_datetime(frame["datetime"]).dt.minute
        frame["time_idx"]=minutes.where(minutes<=690,minutes-90)-571
        return frame

    def _db():
        c = PolicyDBClient()
        legacy=c.get_minute_index(
            ts_codes=ts_codes if ts_codes else ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        requested=ts_codes if ts_codes else ([ts_code] if ts_code else list(_NATIVE_INDEX))
        if isinstance(requested,str): requested=[requested]
        frames=[legacy] if legacy is not None and not legacy.empty else []
        for code in requested:
            if code in _NATIVE_INDEX:
                frame=_compat(get_index_oneMin(code,trade_date,start_date,end_date,source="database_only"))
                if not frame.empty: frames.append(frame)
        if not frames: return pd.DataFrame()
        frame=pd.concat(frames,ignore_index=True)
        frame["volume_unit"]="turnover_10000_yuan"
        return frame.drop_duplicates(["ts_code","datetime"],keep="last").sort_values(["ts_code","datetime"]).reset_index(drop=True)

    def _online():
        ts_list, dates_list = _validate_online_minute(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date, start_date=start_date, end_date=end_date,
            default_ts_codes=_NATIVE_INDEX,
        )
        frames=[_compat(get_index_oneMin(code,trade_date=date,source="online")) for code in ts_list for date in dates_list]
        return pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()

    return _route(source, _db, _online)


def _get_native_index_bars(period, ts_code, trade_date, start_date, end_date, source):
    from coreClient.index_minute import read_index_bars, IndexMinuteDownloader
    def _db():
        return read_index_bars(ts_code, period, trade_date, start_date, end_date)
    def _online():
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date and date range are mutually exclusive")
        downloader = IndexMinuteDownloader()
        try:
            rows, _ = downloader.download(ts_code, period, trade_date or start_date, trade_date or end_date)
            return pd.DataFrame(rows)
        finally:
            downloader.close()
    return _route(source, _db, _online)


def get_index_oneMin(ts_code, trade_date=None, start_date=None, end_date=None, source="database_only"):
    """Native index OHLC/turnover; raw_volume is not a share-volume measure."""
    return _get_native_index_bars(1, ts_code, trade_date, start_date, end_date, source)


def get_index_fifteenMin(ts_code, trade_date=None, start_date=None, end_date=None, source="database_only"):
    """Read native 15-minute index bars without resampling one-minute prices."""
    return _get_native_index_bars(15, ts_code, trade_date, start_date, end_date, source)


def get_ticks(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """个股分笔成交(几千 ~ 几万行/天)

    Args:
        ts_code: 单只 e.g. '000006.SZ'
        ts_codes: 多只列表
        trade_date: 单日
        start_date: 日期段起始
        end_date: 日期段结束
        source: 'database' / 'online'

    数据库位置:policy_ticks.db / tbl_tick + tbl_tick_ctrl
    online:TdxClient.get_history_ticks(ts_code, date_int)
        多 ts/多日 → 拆成单只单日循环,≤ 30 次
    """
    from offlineDataManager.scripts.core.offline_db_client import PolicyDBClient

    def _db():
        c = PolicyDBClient()
        return c.get_ticks(
            ts_codes=ts_codes if ts_codes else ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )

    def _online():
        ts_list, dates_list = _validate_online_minute(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date, start_date=start_date, end_date=end_date,
        )
        return _online_loop_tdx(ts_list, dates_list, kind="ticks")

    return _route(source, _db, _online)


def get_ticks_legacy(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Read the preserved v1 tick archive without fallback or mutation.

    The v1 table's timestamps have minute precision and its ``seqId`` has not
    been certified as an exchange sequence. Direction values are returned as
    stored. This API is intentionally separate from :func:`get_ticks`, which
    addresses the active v2 store.
    """
    import sqlite3
    from urllib.parse import quote
    from coreClient._paths import database_root

    path = database_root() / "offline" / "legacy" / "policy_ticks_v1.db"
    if not path.is_file():
        raise FileNotFoundError(f"legacy v1 tick archive is unavailable: {path}")
    if (ts_code is not None) and (ts_codes is not None):
        raise ValueError("ts_code 和 ts_codes 互斥")
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise ValueError("trade_date 不能与 start_date/end_date 同时使用")
    if trade_date is None and (start_date is None or end_date is None):
        raise ValueError("必须提供 trade_date，或者同时提供 start_date 和 end_date")

    def normalize_date(value: str, label: str) -> str:
        text = str(value).strip().replace("-", "")
        try:
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"{label} 必须是有效 YYYYMMDD 或 YYYY-MM-DD 日期") from exc

    if trade_date is not None:
        normalized_trade_date = normalize_date(trade_date, "trade_date")
        normalized_start = normalized_end = None
    else:
        normalized_trade_date = None
        normalized_start = normalize_date(start_date, "start_date")
        normalized_end = normalize_date(end_date, "end_date")
        if normalized_start > normalized_end:
            raise ValueError("start_date 不能晚于 end_date")
    predicates = []
    parameters = []
    selected_codes = ts_codes if ts_codes is not None else ([ts_code] if ts_code else [])
    if selected_codes:
        predicates.append("ts_code IN (" + ",".join("?" for _ in selected_codes) + ")")
        parameters.extend(str(code).upper() for code in selected_codes)
    if trade_date is not None:
        predicates.append("trade_date=?")
        parameters.append(normalized_trade_date)
    else:
        predicates.append("trade_date BETWEEN ? AND ?")
        parameters.extend((normalized_start, normalized_end))
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    uri = "file:" + quote(str(path), safe="/") + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        frame = pd.read_sql_query(
            "SELECT ts_code, trade_date, seqId, datetime, time, price, vol, buyorsell, created_at "
            "FROM tbl_tick" + where + " ORDER BY trade_date, ts_code, seqId",
            conn,
            params=parameters,
        )
    frame.attrs.update({
        "source": "legacy_policy_ticks_v1",
        "database_path": str(path),
        "timestamp_precision": "minute",
        "seqId_semantics": "not_validated_as_exchange_sequence",
        "buyorsell_semantics": "raw_stored_codes",
        "historical_integrity_status": "internal_domains_and_control_audited; source_completeness_and_seq_semantics_unverified",
        "readonly": True,
    })
    return frame


# ============================================================================
# 日 K / 周 K / 月 K(走 tushare + db)
# ============================================================================
def get_day(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    trade_date: Optional[str] = None,
    qfq: bool = True,
    source: str = "database",
) -> pd.DataFrame:
    """日 K(qfq=True 时前复权,默认 True)

    Args:同 offline_db_client.get_day
        source: 'database' / 'online'

    数据库位置:db_cn_basic.db / tbl_cn_day(存不复权,qfq 在读时算)
    online:TushareClient.daily_qfq_range (qfq=True)
         或 TushareClient.daily + adj_factor 自己 join (qfq=False)
    """
    from offlineDataManager.scripts.core.offline_db_client import get_day as _db_get_day

    def _db():
        return _db_get_day(
            ts_code=ts_code, ts_codes=ts_codes,
            start_date=start_date, end_date=end_date, trade_date=trade_date,
            qfq=qfq,
        )

    def _online():
        client = _get_tushare()
        if trade_date:
            td = _to_yyyymmdd(trade_date)
            if qfq:
                df = client.daily_qfq_range(start_date=td, end_date=td, qfq=True)
                if ts_code:
                    df = df[df["ts_code"] == ts_code]
                elif ts_codes:
                    df = df[df["ts_code"].isin(ts_codes)]
                return df.reset_index(drop=True)
            # 非 qfq:tushare.daily 支持 ts_code 多值(逗号分隔)
            # ts_code 优先级高于 ts_codes;ts_code + ts_codes 同时传时 join
            ts_arg = ts_code or (",".join(ts_codes) if ts_codes else None)
            if not ts_arg:
                raise ValueError(
                    "online 模式 get_day 必须传 ts_code 或 ts_codes(tushare.daily 不支持全市场)"
                )
            df = client.daily(ts_code=ts_arg, trade_date=td)
            return df.reset_index(drop=True) if df is not None else pd.DataFrame()
        if start_date and end_date:
            sd, ed = _to_yyyymmdd(start_date), _to_yyyymmdd(end_date)
            if qfq:
                df = client.daily_qfq_range(start_date=sd, end_date=ed, qfq=True)
                if ts_code:
                    df = df[df["ts_code"] == ts_code]
                elif ts_codes:
                    df = df[df["ts_code"].isin(ts_codes)]
                return df.reset_index(drop=True)
            # 非 qfq:tushare.daily 支持 ts_code 多值(逗号分隔)
            ts_arg = ts_code or (",".join(ts_codes) if ts_codes else None)
            if not ts_arg:
                raise ValueError(
                    "online 模式 get_day 必须传 ts_code 或 ts_codes(tushare.daily 不支持全市场)"
                )
            df = client.daily(ts_code=ts_arg, start_date=sd, end_date=ed)
            return df.reset_index(drop=True) if df is not None else pd.DataFrame()
        raise ValueError("online 模式必须传 trade_date 或 start_date+end_date")

    return _route(source, _db, _online)


def get_week(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    qfq: bool = True,
    source: str = "database",
) -> pd.DataFrame:
    """周 K；qfq=True 读前复权表，False 读原始不复权表。

    ⚠️ online 模式不支持(tushare.weekly 需高积分,本项目未启用)
       - source='online' → 直接返回空 DataFrame
       - source='database' → 走 db
    """
    from offlineDataManager.scripts.core.offline_db_client import get_week as _db_get_week

    def _db():
        return _db_get_week(
            ts_code=ts_code,
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date,
            qfq=qfq,
        )

    def _online():
        # online 模式不支持(用户明确要求)
        logger.warning("[data_provider] get_week online 模式不支持,返回空 DataFrame")
        return pd.DataFrame()

    return _route(source, _db, _online)


def get_month(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    qfq: bool = True,
    source: str = "database",
) -> pd.DataFrame:
    """月 K；qfq=True 读前复权表，False 读原始不复权表。

    ⚠️ online 模式不支持(tushare.monthly 需高积分,本项目未启用)
       - source='online' → 直接返回空 DataFrame
       - source='database' → 走 db
    """
    from offlineDataManager.scripts.core.offline_db_client import get_month as _db_get_month

    def _db():
        return _db_get_month(
            ts_code=ts_code,
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date,
            qfq=qfq,
        )

    def _online():
        # online 模式不支持(用户明确要求)
        logger.warning("[data_provider] get_month online 模式不支持,返回空 DataFrame")
        return pd.DataFrame()

    return _route(source, _db, _online)


# ============================================================================
# 基础数据(走 tushare + db)
# ============================================================================
def get_basic(
    exchange: Optional[str] = None,
    market: Optional[str] = None,
    list_status: str = "L",
    source: str = "database",
) -> pd.DataFrame:
    """股票基本信息(默认 list_status='L' 上市中)

    数据库位置:db_cn_basic.db / tbl_cn_basic
    online:TushareClient.stock_basic
    注:db get_basic 没有 ts_code 过滤参数(15 列固定),
       online tushare.stock_basic 支持 ts_code 过滤,返回结果可能不一致
    """
    from offlineDataManager.scripts.core.offline_db_client import get_basic as _db_get_basic

    def _db():
        return _db_get_basic(exchange=exchange, market=market, list_status=list_status)

    def _online():
        # db 端支持 exchange / market 过滤;tushare stock_basic 也支持这两个参数
        # 但 tushare market 是"主板/创业板/科创板/CDR",跟 db 的 market 含义可能不同,
        # 稳妥起见 exchange / market 都透传(由 tushare 自身处理兼容性)
        df = _get_tushare().stock_basic(
            exchange=exchange,
            market=market,
            list_status=list_status,
            fields="ts_code,symbol,name,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_ent_type,act_name,area",
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        return _align_cols(df, [
            "ts_code", "symbol", "name", "industry", "fullname", "enname",
            "cnspell", "market", "exchange", "curr_type", "list_status",
            "list_date", "delist_date", "is_hs",
            "act_ent_type", "act_name", "area", "snap_ts",
        ])

    return _route(source, _db, _online)


def _align_cols(df: pd.DataFrame, target_cols: list) -> pd.DataFrame:
    """把 df 列对齐到 target_cols,缺的列填 None(用于 db vs online schema 对齐)"""
    for c in target_cols:
        if c not in df.columns:
            df[c] = None
    return df[target_cols]


def get_adj_factor(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """复权因子

    online:tushare.adj_factor 不支持 ts_code 列表 → 自动循环每个 ts_code
    """
    from offlineDataManager.scripts.core.offline_db_client import get_adj_factor as _db_get_adj

    def _db():
        return _db_get_adj(ts_code=ts_code, ts_codes=ts_codes, start_date=start_date, end_date=end_date)

    def _online():
        df = _online_loop_ts_codes(
            "adj_factor",
            ts_code=ts_code, ts_codes=ts_codes,
            base_kwargs={
                "ts_code": ts_code,
                "start_date": _to_yyyymmdd(start_date),
                "end_date": _to_yyyymmdd(end_date),
            },
            require_date=True,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or (hasattr(df, 'empty') and df.empty):
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        return _align_cols(df, ["ts_code", "trade_date", "adj_factor", "snap_ts"])

    return _route(source, _db, _online)


def get_stk_limit(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """涨跌停价格(每日)

    online:tushare.stk_limit 不支持 ts_code 列表 → 自动循环
    """
    from offlineDataManager.scripts.core.offline_db_client import get_stk_limit as _db_get_stk_limit

    def _db():
        return _db_get_stk_limit(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        df = _online_loop_ts_codes(
            "stk_limit",
            ts_code=ts_code, ts_codes=ts_codes,
            base_kwargs={
                "ts_code": ts_code,
                "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                "start_date": _to_yyyymmdd(start_date) if start_date else None,
                "end_date": _to_yyyymmdd(end_date) if end_date else None,
            },
            require_date=True,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or (hasattr(df, 'empty') and df.empty):
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        return _align_cols(df, ["trade_date", "ts_code", "up_limit", "down_limit", "snap_ts"])

    return _route(source, _db, _online)


def get_daily_basic(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """每日指标(换手率/量比/PE/PB 等)"""
    from offlineDataManager.scripts.core.offline_db_client import get_daily_basic as _db_get_daily_basic

    def _db():
        return _db_get_daily_basic(
            ts_code=ts_code, trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        # online 必传日期(允许全市场:ts_code 可不传)
        _validate_online_date(trade_date, start_date, end_date, "get_daily_basic")
        df = _get_tushare().daily_basic(
            ts_code=ts_code,
            trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
            start_date=_to_yyyymmdd(start_date) if start_date else None,
            end_date=_to_yyyymmdd(end_date) if end_date else None,
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code", "close", "turnover_rate", "turnover_rate_f",
            "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio",
            "dv_ttm", "total_share", "float_share", "free_share", "total_mv",
            "circ_mv", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_moneyflow(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """资金流向(主力/中单/小单)

    online:tushare.moneyflow 不支持 ts_code 列表 → 自动循环
    """
    from offlineDataManager.scripts.core.offline_db_client import get_moneyflow as _db_get_moneyflow

    def _db():
        return _db_get_moneyflow(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        df = _online_loop_ts_codes(
            "moneyflow",
            ts_code=ts_code, ts_codes=ts_codes,
            base_kwargs={
                "ts_code": ts_code,
                "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                "start_date": _to_yyyymmdd(start_date) if start_date else None,
                "end_date": _to_yyyymmdd(end_date) if end_date else None,
            },
            require_date=True,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or (hasattr(df, 'empty') and df.empty):
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code",
            "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
            "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
            "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
            "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
            "net_mf_vol", "net_mf_amount", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


# ============================================================================
# 指数(走 tushare + db)
# ============================================================================
def get_index_basic(
    ts_code: Optional[str] = None,
    market: Optional[str] = None,
    publisher: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """指数基本信息(db 端支持 market / publisher 过滤,tushare 也支持,全部透传)"""
    from offlineDataManager.scripts.core.offline_db_client import get_index_basic as _db_get_index_basic

    def _db():
        return _db_get_index_basic(ts_code=ts_code, market=market, publisher=publisher)

    def _online():
        df = _get_tushare().index_basic(ts_code=ts_code, market=market, publisher=publisher)
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "ts_code", "name", "market", "publisher", "category",
            "base_date", "base_point", "list_date", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_index_daily(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """指数日 K"""
    from offlineDataManager.scripts.core.offline_db_client import get_index_daily as _db_get_index_daily

    def _db():
        return _db_get_index_daily(
            ts_code=ts_code, trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        # online 必须传 ts_code + 日期(单日 / 多日二选一,二选一互斥)
        if not ts_code:
            raise ValueError(
                "online 模式 get_index_daily 必须传 ts_code"
            )
        _validate_online_date(trade_date, start_date, end_date, "get_index_daily")
        df = _get_tushare().index_daily(
            ts_code=ts_code,
            trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
            start_date=_to_yyyymmdd(start_date) if start_date else None,
            end_date=_to_yyyymmdd(end_date) if end_date else None,
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code", "open", "high", "low", "close",
            "pre_close", "change", "pct_chg", "vol", "amount", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_index_weekly(ts_code=None, trade_date=None, start_date=None, end_date=None, source="database_only") -> pd.DataFrame:
    """Read persisted index weekly bars. Native daily units: vol=lots, amount=thousand yuan.

    trade_date is the last actual trading date in a stored period. No online fallback.
    """
    if source not in ("database_only", "database"):
        raise ValueError("Index weekly bars are offline preaggregated; use database_only")
    from coreClient.index_period import IndexPeriodStore
    return IndexPeriodStore().read("weekly", ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)


def get_index_monthly(ts_code=None, trade_date=None, start_date=None, end_date=None, source="database_only") -> pd.DataFrame:
    """Read persisted index monthly bars; same field and date semantics as get_index_weekly."""
    if source not in ("database_only", "database"):
        raise ValueError("Index monthly bars are offline preaggregated; use database_only")
    from coreClient.index_period import IndexPeriodStore
    return IndexPeriodStore().read("monthly", ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)


# ============================================================================
# 交易日历(走 tushare + db)
# ============================================================================
def get_tradecal(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    market: str = "SSE",
    source: str = "database",
) -> pd.DataFrame:
    """交易日历(返回 is_open=1 的交易日)

    注:db 和 tushare 都用 market 参数(SSE/SZSE/BSE),不是 exchange
    """
    from offlineDataManager.scripts.core.offline_db_client import get_tradecal as _db_get_tradecal

    def _db():
        return _db_get_tradecal(start_date=start_date, end_date=end_date, market=market)

    def _online():
        # online 必传日期(tushare.trade_cal 需 start_date/end_date)
        if not start_date or not end_date:
            raise ValueError(
                "online 模式 get_tradecal 必须传 start_date 和 end_date"
            )
        df = _get_tushare().trade_cal(
            start_date=_to_yyyymmdd(start_date) if start_date else None,
            end_date=_to_yyyymmdd(end_date) if end_date else None,
            exchange=market,
            is_open="1",
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        return _align_cols(df, ["cal_date", "exchange", "is_open", "pretrade_date", "snap_ts"])

    return _route(source, _db, _online)


# ============================================================================
# KPL 开盘啦(走 tushare + db)
# ============================================================================
def get_kpl_list(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tags: Optional[Union[str, list]] = "涨停",
    source: str = "database",
) -> pd.DataFrame:
    """KPL 涨停/炸板/跌停榜单

    注:db 参数是 tags (复数/str 或 list),online tushare kpl_list 是单数 tag
       - tags=str: 透传给 tushare tag=tags
       - tags=list: 循环每个 tag,合并结果(去重)

    数据库位置:db_cn_kpl.db / tbl_cn_kpl_list
    online:TushareClient.kpl_list
    """
    from offlineDataManager.scripts.core.offline_db_client import get_kpl_list as _db_get_kpl_list

    def _db():
        return _db_get_kpl_list(
            trade_date=trade_date, start_date=start_date, end_date=end_date, tags=tags,
        )

    def _online():
        # online 必传日期(单日 / 多日二选一,互斥)
        _validate_online_date(trade_date, start_date, end_date, "get_kpl_list")

        # tags list → 循环单 tag 合并
        if isinstance(tags, list):
            frames = []
            for t in tags:
                try:
                    df = _get_tushare().kpl_list(
                        trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
                        start_date=_to_yyyymmdd(start_date) if start_date else None,
                        end_date=_to_yyyymmdd(end_date) if end_date else None,
                        tag=t,  # tushare 单数
                    )
                except Exception:
                    df = None
                if df is not None and not df.empty:
                    frames.append(df)
            if not frames:
                return pd.DataFrame()
            df = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
        else:
            df = _get_tushare().kpl_list(
                trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
                start_date=_to_yyyymmdd(start_date) if start_date else None,
                end_date=_to_yyyymmdd(end_date) if end_date else None,
                tag=tags,  # tushare 单数
            )
            if df is None:
                return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "ts_code", "name", "trade_date", "lu_time", "ld_time",
            "open_time", "last_time", "lu_desc", "tag", "theme",
            "net_change", "bid_amount", "status", "bid_change",
            "bid_turnover", "lu_bid_vol", "pct_chg", "bid_pct_chg",
            "rt_pct_chg", "limit_order", "amount", "turnover_rate",
            "free_float", "lu_limit_order", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_kpl_concept_cons(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    source: str = "database",
) -> pd.DataFrame:
    """KPL 题材成分股

    online:tushare.kpl_concept_cons 不支持 ts_code 列表 → 自动循环
    db 端支持 ts_code/ts_codes 过滤(新增透传)
    """
    from offlineDataManager.scripts.core.offline_db_client import get_kpl_concept_cons as _db_get_kpl_concept_cons

    def _db():
        # db 端签名只有 ts_codes(没有 ts_code),所以把 ts_code 包成 list
        db_ts_codes = ts_codes[:] if ts_codes else None
        if ts_code and not db_ts_codes:
            db_ts_codes = [ts_code]
        return _db_get_kpl_concept_cons(
            trade_date=trade_date, start_date=start_date, end_date=end_date,
            ts_codes=db_ts_codes,
        )

    def _online():
        # online 必传日期
        _validate_online_date(trade_date, start_date, end_date, "get_kpl_concept_cons")

        # kpl_concept_cons 单次最多返回 3000 行；一个交易日的完整题材成员
        # 通常超过一万行。必须分页，否则按接口默认值得到的前 3000 行会
        # 系统性漏掉后续题材，进而低估题材成员数和封板率分母。
        def _fetch_all() -> pd.DataFrame:
            page_size = 3000
            frames = []
            base_params = {
                "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                "start_date": _to_yyyymmdd(start_date) if start_date else None,
                "end_date": _to_yyyymmdd(end_date) if end_date else None,
            }
            for page in range(50):
                df = _get_tushare().kpl_concept_cons(
                    **base_params,
                    limit=page_size,
                    offset=page * page_size,
                )
                if df is None or df.empty:
                    break
                frames.append(df)
                if len(df) < page_size:
                    break
            if not frames:
                return pd.DataFrame()
            return pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)

        # 没有 ts_code 限制时返回完整分页结果
        if not ts_code and not ts_codes:
            return _fetch_all()
        # 有 ts_code 限制 → tushare.kpl_concept_cons 不支持 ts_code 参数,
        # 必须先拉所有再 client-side 过滤(浪费一点,trade_date 通常 1-2 天数据量小)
        df = _fetch_all()
        if df is None or df.empty:
            return pd.DataFrame()
        if ts_code:
            df = df[df["ts_code"] == ts_code]
        elif ts_codes:
            df = df[df["ts_code"].isin(ts_codes)]
        return df.reset_index(drop=True)

    return _route(source, _db, _online)


def get_kpl_limit_performance(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sort_by: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """KPL 涨停表现(连板数/封单金额/封板时间等)

    Args:
        trade_date: 单日 '2026-09-11'(与 start_date/end_date 互斥)
        start_date: 日期段起始
        end_date: 日期段结束
        sort_by: 排序字段(单日模式传)
        source: 'database' / 'online'

    数据库位置:db_cn_kpl.db / tbl_cn_kpl_limit_performance
    online:KPLClient.get_daily_limit_performance(tushare 没这个接口)

    Online 限制(跟 get_minute/ticks/minute_index 一致):
      - **不支持 ts_code**(KPL HTTP 接口就是按日查的)
      - 多日 → 拆成单日循环,**每循环一次 = 1 次 KPL 调用**
      - ≤ 30 次(总交易日数限制)
      - 周末/节假日自动过滤(用交易日历)
      - trade_date 跟 start_date/end_date 互斥
    """
    from offlineDataManager.scripts.core.offline_db_client import get_kpl_limit_performance as _db_get_kpl_lp

    def _db():
        return _db_get_kpl_lp(
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )

    def _online():
        # 复用统一的 _validate_online_minute 校验
        # KPL 不需要 ts_code 校验,传 ts_code=None / ts_codes=None / default_ts_codes=None
        # require_date=True 强制传日期
        # KPL 多日次数 ≤ 30 跟 tdx 一致(_validate_online_minute 内置校验)
        _, dates_list = _validate_online_minute(
            ts_code=None, ts_codes=None,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
            allow_ts_code_none=True,  # KPL 不需要 ts_code
            require_date=True,         # 强制日期
        )

        # 拆日循环
        # 关键:如果日期是今日,KPL 历史接口(apphis)盘中拉会返 0 行,
        #       必须走实时接口(apphwhq)拿盘中实时涨停快照
        kpl = _get_kpl()
        today = datetime.now().strftime("%Y-%m-%d")
        frames = []
        for d in dates_list:
            if d == today:
                # 今日 → 实时接口(盘中可用)
                try:
                    df_day = kpl.fetch_realtime_limit_performance(sort_by=sort_by)
                    # 实时接口强制 trade_date=today,但 _parse 内已设,无需覆盖
                except Exception:
                    df_day = pd.DataFrame()
            else:
                # 历史日期 → 历史接口
                try:
                    df_day = kpl.get_daily_limit_performance(_to_yyyymmdd(d), sort_by=sort_by)
                except Exception:
                    df_day = pd.DataFrame()
            if df_day is not None and not df_day.empty:
                frames.append(df_day)
        if not frames:
            return pd.DataFrame()
        # align cols(不同日期可能 schema 微差)
        all_cols = sorted({c for df in frames for c in df.columns})
        aligned = [df.reindex(columns=all_cols) for df in frames]
        return pd.concat(aligned, ignore_index=True)

    return _route(source, _db, _online)


# ============================================================================
# 新闻(走 tushare + db)
# ============================================================================
def get_news(
    src: Optional[Union[str, list]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
    *,
    start_datetime: Optional[str] = None,
    end_datetime: Optional[str] = None,
    limit: Optional[int] = 5000,
    offset: int = 0,
) -> pd.DataFrame:
    """新闻(多源聚合,默认全部 9 个活跃源)

    src 支持单值(str)或列表(list):
      - src=str: 透传给 tushare news
      - src=list: 循环每个 src,合并结果(去重)
    """
    from offlineDataManager.scripts.core.offline_db_client import get_news as _db_get_news

    def _db():
        return _db_get_news(src=src, start_date=start_date, end_date=end_date,
                            start_datetime=start_datetime, end_datetime=end_datetime,
                            limit=limit, offset=offset)

    def _online():
        if start_datetime is not None or end_datetime is not None or limit != 5000 or offset:
            raise ValueError("精确新闻时间窗口/分页仅支持本地读取，请使用 source='database_only'")
        # online 必传日期(tushare.news 不支持无日期)
        _validate_online_date(None, start_date, end_date, "get_news")
        # 注:get_news 只有 start_date/end_date 没有 trade_date,
        # 这里传 trade_date=None 让 _validate_online_date 检查"必传 start+end"

        if isinstance(src, list):
            frames = []
            for s in src:
                try:
                    df = _get_tushare().news(
                        src=s,
                        start_date=_to_yyyymmdd(start_date) if start_date else None,
                        end_date=_to_yyyymmdd(end_date) if end_date else None,
                    )
                except Exception:
                    df = None
                if df is not None and not df.empty:
                    frames.append(df)
            if not frames:
                df = pd.DataFrame()
            else:
                df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["datetime", "src", "title"]).reset_index(drop=True)
        else:
            df = _get_tushare().news(
                src=src,
                start_date=_to_yyyymmdd(start_date) if start_date else None,
                end_date=_to_yyyymmdd(end_date) if end_date else None,
            )
            if df is None:
                return pd.DataFrame()
        df = df.reset_index(drop=True)
        # tushare online 返回的字段比 db 少(datetime/title/content),
        # 缺的列填 None 保持 schema 一致
        target = [
            "datetime", "src", "title", "content",
            "channels", "score", "md5", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)



# ============================================================================
# 扩展数据接口(2026-09-18 新增:对齐 offline_db_client 全部 get_xxx)
# ============================================================================

def get_suspend(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    suspend_type: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """停复牌信息

    数据库位置:db_cn_basic.db / tbl_cn_suspend
    online:tushare.suspend_d(支持 trade_date 单日查全市场)
    """
    from offlineDataManager.scripts.core.offline_db_client import get_suspend as _db_get_suspend

    def _db():
        return _db_get_suspend(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
            suspend_type=suspend_type,
        )

    def _online():
        # suspend_d 支持 trade_date 单日查全市场,ts_code 可选
        _validate_online_date(trade_date, start_date, end_date, "get_suspend")
        if ts_codes:
            df = _online_loop_ts_codes(
                "suspend_d",
                ts_code=ts_code, ts_codes=ts_codes,
                base_kwargs={
                    "ts_code": ts_code,
                    "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                    "start_date": _to_yyyymmdd(start_date) if start_date else None,
                    "end_date": _to_yyyymmdd(end_date) if end_date else None,
                    "suspend_type": suspend_type,
                },
                require_date=True,
                trade_date=trade_date, start_date=start_date, end_date=end_date,
            )
        else:
            df = _get_tushare().suspend_d(
                trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
                start_date=_to_yyyymmdd(start_date) if start_date else None,
                end_date=_to_yyyymmdd(end_date) if end_date else None,
                suspend_type=suspend_type,
            )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = ["ts_code", "trade_date", "suspend_timing", "suspend_type", "snap_ts"]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_top_list(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """龙虎榜每日榜单

    数据库位置:db_cn_kpl.db / tbl_cn_top_list
    online:tushare.top_list
    """
    from offlineDataManager.scripts.core.offline_db_client import get_top_list as _db_get_top_list

    def _db():
        return _db_get_top_list(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        # top_list 支持 trade_date 单日查全市场,ts_code 可选
        _validate_online_date(trade_date, start_date, end_date, "get_top_list")
        if ts_codes:
            df = _online_loop_ts_codes(
                "top_list",
                ts_code=ts_code, ts_codes=ts_codes,
                base_kwargs={
                    "ts_code": ts_code,
                    "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                    "start_date": _to_yyyymmdd(start_date) if start_date else None,
                    "end_date": _to_yyyymmdd(end_date) if end_date else None,
                },
                require_date=True,
                trade_date=trade_date, start_date=start_date, end_date=end_date,
            )
        else:
            df = _get_tushare().top_list(
                trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
                start_date=_to_yyyymmdd(start_date) if start_date else None,
                end_date=_to_yyyymmdd(end_date) if end_date else None,
            )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code", "name", "close", "pct_change", "turnover_rate",
            "amount", "l_sell", "l_buy", "l_amount", "net_amount",
            "net_rate", "amount_rate", "float_values", "reason",
            "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_top_inst(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    exalter: Optional[str] = None,
    side: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """龙虎榜机构/营业部明细

    数据库位置:db_cn_kpl.db / tbl_cn_top_inst
    online:tushare.top_inst
    """
    from offlineDataManager.scripts.core.offline_db_client import get_top_inst as _db_get_top_inst

    def _db():
        return _db_get_top_inst(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
            exalter=exalter, side=side,
        )

    def _online():
        # top_inst 支持 trade_date 单日查全市场,ts_code 可选
        _validate_online_date(trade_date, start_date, end_date, "get_top_inst")
        if ts_codes:
            df = _online_loop_ts_codes(
                "top_inst",
                ts_code=ts_code, ts_codes=ts_codes,
                base_kwargs={
                    "ts_code": ts_code,
                    "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                    "start_date": _to_yyyymmdd(start_date) if start_date else None,
                    "end_date": _to_yyyymmdd(end_date) if end_date else None,
                    "exalter": exalter,
                    "side": side,
                },
                require_date=True,
                trade_date=trade_date, start_date=start_date, end_date=end_date,
            )
        else:
            df = _get_tushare().top_inst(
                trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
                start_date=_to_yyyymmdd(start_date) if start_date else None,
                end_date=_to_yyyymmdd(end_date) if end_date else None,
                exalter=exalter,
                side=side,
            )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code", "exalter", "side", "buy", "buy_rate",
            "sell", "sell_rate", "net_buy", "reason", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_block_trade(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """大宗交易

    数据库位置:db_cn_kpl.db / tbl_cn_block_trade
    online:tushare.block_trade
    """
    from offlineDataManager.scripts.core.offline_db_client import get_block_trade as _db_get_block_trade

    def _db():
        return _db_get_block_trade(
            ts_code=ts_code, trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        if not ts_code:
            raise ValueError("online 模式 get_block_trade 必须传 ts_code")
        _validate_online_date(trade_date, start_date, end_date, "get_block_trade")
        df = _get_tushare().block_trade(
            ts_code=ts_code,
            trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
            start_date=_to_yyyymmdd(start_date) if start_date else None,
            end_date=_to_yyyymmdd(end_date) if end_date else None,
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = ["ts_code", "trade_date", "price", "vol", "amount", "buyer", "seller", "snap_ts"]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_ggt_daily(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """港股通每日成交(沪深港通南向)

    数据库位置:db_cn_kpl.db / tbl_cn_ggt_daily
    online:tushare.ggt_daily
    """
    from offlineDataManager.scripts.core.offline_db_client import get_ggt_daily as _db_get_ggt_daily

    def _db():
        return _db_get_ggt_daily(
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        _validate_online_date(trade_date, start_date, end_date, "get_ggt_daily")
        df = _get_tushare().ggt_daily(
            trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
            start_date=_to_yyyymmdd(start_date) if start_date else None,
            end_date=_to_yyyymmdd(end_date) if end_date else None,
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "buy_amount", "buy_volume", "sell_amount", "sell_volume",
            "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_hsgt_top10(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    market_type: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """沪深港通 top10(北向资金)

    数据库位置:db_cn_kpl.db / tbl_cn_hsgt_top10
    online:tushare.hsgt_top10
    """
    from offlineDataManager.scripts.core.offline_db_client import get_hsgt_top10 as _db_get_hsgt_top10

    def _db():
        return _db_get_hsgt_top10(
            ts_code=ts_code, trade_date=trade_date,
            start_date=start_date, end_date=end_date,
            market_type=market_type,
        )

    def _online():
        if not ts_code:
            raise ValueError("online 模式 get_hsgt_top10 必须传 ts_code")
        _validate_online_date(trade_date, start_date, end_date, "get_hsgt_top10")
        df = _get_tushare().hsgt_top10(
            ts_code=ts_code,
            trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
            start_date=_to_yyyymmdd(start_date) if start_date else None,
            end_date=_to_yyyymmdd(end_date) if end_date else None,
            market_type=market_type,
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code", "name", "close", "change", "rank",
            "market_type", "amount", "net_amount", "buy", "sell",
            "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_limit_list(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """涨停/跌停清单(每日)

    数据库位置:db_cn_kpl.db / tbl_cn_limit_list
    online:tushare.limit_list_d

    Args:
        limit: 'U' = 涨停 / 'D' = 跌停 / 'Z' = 炸板(选填)
    """
    from offlineDataManager.scripts.core.offline_db_client import get_limit_list as _db_get_limit_list

    def _db():
        return _db_get_limit_list(
            ts_code=ts_code, trade_date=trade_date,
            start_date=start_date, end_date=end_date,
            limit=limit,
        )

    def _online():
        # limit_list_d 支持 trade_date 单日查全市场,ts_code 可选
        _validate_online_date(trade_date, start_date, end_date, "get_limit_list")
        if ts_code:
            df = _online_loop_ts_codes(
                "limit_list_d",
                ts_code=ts_code, ts_codes=None,
                base_kwargs={
                    "ts_code": ts_code,
                    "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                    "start_date": _to_yyyymmdd(start_date) if start_date else None,
                    "end_date": _to_yyyymmdd(end_date) if end_date else None,
                    "limit": limit,
                },
                require_date=True,
                trade_date=trade_date, start_date=start_date, end_date=end_date,
            )
        else:
            df = _get_tushare().limit_list_d(
                trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
                start_date=_to_yyyymmdd(start_date) if start_date else None,
                end_date=_to_yyyymmdd(end_date) if end_date else None,
                limit=limit,
            )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code", "name", "industry", "close", "pct_chg",
            "amount", "limit_amount", "board_count", "limit", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_margin(
    exchange_id: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """融资融券汇总(交易所维度)

    数据库位置:db_cn_basic.db / tbl_cn_margin
    online:tushare.margin
    """
    from offlineDataManager.scripts.core.offline_db_client import get_margin as _db_get_margin

    def _db():
        return _db_get_margin(
            exchange_id=exchange_id,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        # margin 支持 trade_date 单日查全交易所,ts_code 无用
        _validate_online_date(trade_date, start_date, end_date, "get_margin")
        df = _get_tushare().margin(
            exchange_id=exchange_id,
            trade_date=_to_yyyymmdd(trade_date) if trade_date else None,
            start_date=_to_yyyymmdd(start_date) if start_date else None,
            end_date=_to_yyyymmdd(end_date) if end_date else None,
        )
        if df is None:
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "exchange_id", "rzye", "rzmre", "rzche", "rqye",
            "rqmcl", "rzrqye", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_margin_detail(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """融资融券明细(个股维度)

    数据库位置:db_cn_basic.db / tbl_cn_margin_detail
    online:tushare.margin_detail
    """
    from offlineDataManager.scripts.core.offline_db_client import get_margin_detail as _db_get_margin_detail

    def _db():
        return _db_get_margin_detail(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
        )

    def _online():
        _validate_online_ts_code(ts_code, ts_codes, "get_margin_detail")
        _validate_online_date(trade_date, start_date, end_date, "get_margin_detail")
        df = _online_loop_ts_codes(
            "margin_detail",
            ts_code=ts_code, ts_codes=ts_codes,
            base_kwargs={
                "ts_code": ts_code,
                "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                "start_date": _to_yyyymmdd(start_date) if start_date else None,
                "end_date": _to_yyyymmdd(end_date) if end_date else None,
            },
            require_date=True,
            trade_date=trade_date, start_date=start_date, end_date=end_date,
        )
        if df is None or (hasattr(df, 'empty') and df.empty):
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "trade_date", "ts_code", "name", "rzye", "rqye", "rzmre", "rqyl",
            "rzche", "rqchl", "rqmcl", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_cyq_perf(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_winner_rate: Optional[float] = None,
    max_winner_rate: Optional[float] = None,
    source: str = "database",
) -> pd.DataFrame:
    """筹码胜率(每日)

    数据库位置:db_cn_basic.db / tbl_cn_cyq_perf
    online:tushare.cyq_perf
    """
    from offlineDataManager.scripts.core.offline_db_client import get_cyq_perf as _db_get_cyq_perf

    def _db():
        return _db_get_cyq_perf(
            ts_code=ts_code, ts_codes=ts_codes,
            trade_date=trade_date,
            start_date=start_date, end_date=end_date,
            min_winner_rate=min_winner_rate,
            max_winner_rate=max_winner_rate,
        )

    def _online():
        _validate_online_ts_code(ts_code, ts_codes, "get_cyq_perf")
        _validate_online_date(trade_date, start_date, end_date, "get_cyq_perf")
        df = _online_loop_ts_codes(
            "cyq_perf",
            ts_code=ts_code, ts_codes=ts_codes,
            base_kwargs={
                "ts_code": ts_code,
                "trade_date": _to_yyyymmdd(trade_date) if trade_date else None,
                "start_date": _to_yyyymmdd(start_date) if start_date else None,
                "end_date": _to_yyyymmdd(end_date) if end_date else None,
            },
            require_date=True,
            trade_date=trade_date, start_date=start_date, end_date=end_date,
        )
        if df is None or (hasattr(df, 'empty') and df.empty):
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "ts_code", "trade_date", "winner_rate", "cost_50", "cost_85",
            "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_major_news(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    trade_date: Optional[str] = None,
    src: Optional[Union[str, list]] = None,
    limit: int = 5000,
    offset: int = 0,
    source: str = "database",
) -> pd.DataFrame:
    """头条新闻(重要新闻,跟普通 get_news 区分)

    数据库位置:db_cn_news.db / tbl_major_news
    online:tushare.major_news
    """
    from offlineDataManager.scripts.core.offline_db_client import get_major_news as _db_get_major_news

    def _db():
        return _db_get_major_news(
            start_date=start_date, end_date=end_date, trade_date=trade_date,
            src=src, limit=limit, offset=offset,
        )

    def _online():
        _validate_online_date(trade_date, start_date, end_date, "get_major_news")
        # src 是 tushare 的可选项,这里也支持 list(但 tushare.major_news 实际上按 source 过滤)
        if isinstance(src, list):
            frames = []
            for s in src:
                try:
                    df = _get_tushare().major_news(
                        start_date=_to_yyyymmdd(start_date) if start_date else None,
                        end_date=_to_yyyymmdd(end_date) if end_date else None,
                        src=s,
                        limit=limit,
                        offset=offset,
                    )
                except Exception:
                    df = None
                if df is not None and not df.empty:
                    frames.append(df)
            if not frames:
                df = pd.DataFrame()
            else:
                df = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
        else:
            df = _get_tushare().major_news(
                start_date=_to_yyyymmdd(start_date) if start_date else None,
                end_date=_to_yyyymmdd(end_date) if end_date else None,
                src=src,
                limit=limit,
                offset=offset,
            )
            if df is None:
                return pd.DataFrame()
        df = df.reset_index(drop=True)
        target = [
            "datetime", "src", "title", "content",
            "channels", "score", "md5", "snap_ts",
        ]
        return _align_cols(df, target)

    return _route(source, _db, _online)


def get_cctv_news(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    trade_date: Optional[str] = None,
    content: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
    """CCTV 新闻(联播/朝闻天下等)

    数据库位置:db_cn_news.db / tbl_cctv_news
    online:当前无 tushare/ths 在线源,source='online' 返回空 DataFrame
    """
    from offlineDataManager.scripts.core.offline_db_client import get_cctv_news as _db_get_cctv_news

    def _db():
        return _db_get_cctv_news(
            start_date=start_date, end_date=end_date,
            trade_date=trade_date, content=content,
        )

    def _online():
        # 当前无 online 源,直接返回空 + WARN
        logger.warning("[data_provider] get_cctv_news online 模式无数据源,返回空 DataFrame")
        return pd.DataFrame()

    return _route(source, _db, _online)


def get_news_events(
    start_date: str,
    end_date: str,
    *,
    include_excluded: bool = False,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """读取去重后的 Major News 事件，默认排除纯A股行情结果稿。"""
    from offlineDataManager.scripts.core.news_event_store import NewsEventStore

    return NewsEventStore().query_events(
        start_date=start_date,
        end_date=end_date,
        include_excluded=include_excluded,
        limit=limit,
    )


def get_news_event_daily_stats(start_date: str, end_date: str) -> pd.DataFrame:
    """读取 Major News 清洗层的每日原始量、事件量、有效量和压缩率。"""
    from offlineDataManager.scripts.core.news_event_store import NewsEventStore

    return NewsEventStore().query_daily_stats(start_date, end_date)


def get_news_insights(
    start_date: str,
    end_date: str,
    *,
    min_importance: int = 0,
    include_dropped: bool = False,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """读取 LLM 提炼后的有效 Major News 及其类别、摘要、关键事实和相关题材。"""
    from offlineDataManager.scripts.core.news_event_store import NewsEventStore

    return NewsEventStore().query_insights(
        start_date=start_date,
        end_date=end_date,
        min_importance=min_importance,
        include_dropped=include_dropped,
        limit=limit,
    )


def get_news_llm_daily_stats(start_date: str, end_date: str) -> pd.DataFrame:
    """读取每日确定性清洗事件数、LLM已处理数和LLM有效新闻数。"""
    from offlineDataManager.scripts.core.news_event_store import NewsEventStore

    return NewsEventStore().query_llm_daily_stats(start_date, end_date)


def get_major_news_analysis(
    start_date: str,
    end_date: str,
    *,
    theme_id: Optional[str] = None,
    theme_label: Optional[str] = None,
    entity: Optional[str] = None,
    keyword: Optional[str] = None,
    market_impact: Optional[str] = None,
    min_importance: int = 0,
) -> pd.DataFrame:
    """查询正式归档的 Major News 评分、评级和深度提炼结果。"""
    from offlineDataManager.scripts.core.major_news_graph_store import MajorNewsGraphStore

    return MajorNewsGraphStore().query_analysis(
        start_date, end_date, theme_id=theme_id,
        theme_label=theme_label, entity=entity, keyword=keyword,
        market_impact=market_impact, min_importance=min_importance,
    )


def get_major_news_theme_edges(
    start_date: str,
    end_date: str,
    *,
    theme_id: Optional[str] = None,
) -> pd.DataFrame:
    """查询新闻到题材的候选关系，保留未匹配标签和审计状态。"""
    from offlineDataManager.scripts.core.major_news_graph_store import MajorNewsGraphStore

    return MajorNewsGraphStore().query_theme_edges(start_date, end_date, theme_id=theme_id)


def get_major_news_daily_summary(start_date: str, end_date: str) -> pd.DataFrame:
    """读取 Major News 每日归档量、评级分布与输入版本。"""
    from offlineDataManager.scripts.core.major_news_graph_store import MajorNewsGraphStore

    return MajorNewsGraphStore().query_daily_summary(start_date, end_date)


def get_major_news_event_graph(semantic_event_id: str, *, as_of: Optional[str] = None) -> dict:
    """从新闻语义事件回溯规则事件、原始报道、题材和实体关系。"""
    from offlineDataManager.scripts.core.major_news_graph_store import MajorNewsGraphStore

    return MajorNewsGraphStore().query_event_graph(semantic_event_id, as_of=as_of)


# ============================================================================
# 题材时序知识库（DuckDB 派生层；事实源仍为 KPL）
# ============================================================================
def _theme_store():
    from core.theme_graph_store import ThemeGraphStore
    return ThemeGraphStore()


def get_theme_profile(
    theme_id: Optional[str] = None,
    name: Optional[str] = None,
    as_of: Optional[str] = None,
) -> dict:
    """返回题材身份、截至 ``as_of`` 的最近热度与历史周期。

    ``name`` 会经题材别名表解析为稳定 ``theme_id``；历史查询不会读取
    ``as_of`` 之后的每日热度记录。
    """
    return _theme_store().query_theme_profile(theme_id=theme_id, name=name, as_of=as_of)


def get_theme_members(
    theme_id: str,
    as_of: Optional[str] = None,
    historical: bool = False,
) -> pd.DataFrame:
    """查询题材截至指定时点的实际主归因涨停股；historical 返回逐次事件。"""
    return _theme_store().query_theme_members(theme_id=theme_id, as_of=as_of, historical=historical)


def get_theme_daily(
    theme_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """查询程序化题材日度宽度、高度、热度与生命周期候选状态。"""
    return _theme_store().query_theme_daily(theme_id=theme_id, start_date=start_date, end_date=end_date)


def get_market_theme_review(
    trade_date: Optional[str] = None,
    top_n: int = 10,
    leader_count: int = 3,
) -> dict:
    """返回每日题材情绪、热门题材排名、主线结构和各题材龙一至龙三。

    ``trade_date`` 为空时读取题材库最新交易日。结果严格按截至该日的可见
    数据计算，并排除 ST板块、ST摘帽、次新股及其一级分类。
    """
    return _theme_store().query_market_theme_review(
        trade_date=trade_date,
        top_n=top_n,
        leader_count=leader_count,
    )


def get_market_theme_review_series(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    top_n: int = 10,
    leader_count: int = 3,
) -> list[dict]:
    """批量返回历史可见的全市场题材情绪、结构、排名和龙头序列。"""
    return _theme_store().query_market_theme_review_series(
        start_date=start_date,
        end_date=end_date,
        top_n=top_n,
        leader_count=leader_count,
    )


def get_theme_cycle_data(
    theme_id: Optional[str] = None,
    name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    as_of: Optional[str] = None,
) -> dict:
    """返回单题材周期图使用的日度、周期、个股事件、龙头和催化数据。"""
    return _theme_store().query_theme_cycle_data(
        theme_id=theme_id,
        name=name,
        start_date=start_date,
        end_date=end_date,
        as_of=as_of,
    )


def get_stock_theme_history(ts_code: str) -> pd.DataFrame:
    """返回个股历史涨停事件与当日主/辅题材归因。"""
    return _theme_store().query_stock_theme_history(ts_code=ts_code)


def get_theme_analyses(
    theme_id: Optional[str] = None,
    name: Optional[str] = None,
    episode_id: Optional[str] = None,
    latest: bool = True,
) -> pd.DataFrame:
    """返回已沉淀的题材周期归因、证据、审计状态和模型版本。

    后续研究应先调用此接口复用已有成果；只有新周期、数据修订或已有
    ``unresolved`` 明确指出证据缺口时才重新检索新闻。默认每个周期、
    每类分析只返回最新版本。
    """
    return _theme_store().query_theme_analyses(
        theme_id=theme_id, name=name, episode_id=episode_id, latest=latest,
    )


def get_theme_taxonomy(level1_name: Optional[str] = None) -> pd.DataFrame:
    """返回一级/二级题材映射；可按一级题材名称筛选。"""
    return _theme_store().query_theme_taxonomy(level1_name=level1_name)

"""从题材日度事实生成可回测的市场题材复盘摘要。"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from typing import Any

import pandas as pd


METHOD_VERSION = "theme-market-review-v2"
EXCLUDED_LEVEL1 = {"ST与次新"}
EXCLUDED_THEMES = {"ST板块", "ST摘帽", "次新股"}
TOP3_WEIGHTS = (0.5, 0.3, 0.2)


def _scalar(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _number(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _time_strength(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.5
    parts = text.split(":")
    try:
        hour, minute = int(parts[0]), int(parts[1])
        second = int(parts[2]) if len(parts) > 2 else 0
    except (ValueError, IndexError):
        return 0.5
    absolute = hour * 60 + minute + second / 60.0
    return max(0.0, min(1.0, (15 * 60 - absolute) / (15 * 60 - (9 * 60 + 30))))


def _percentile(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().sum() <= 1:
        return pd.Series([1.0 if pd.notna(value) else 0.5 for value in numeric], index=values.index)
    ranked = numeric.rank(method="average", pct=True)
    return ranked.fillna(0.5)


def _sentiment_level(score: float) -> str:
    if score >= 80:
        return "高热"
    if score >= 60:
        return "偏高"
    if score >= 40:
        return "中性"
    if score >= 20:
        return "偏低"
    return "低迷"


def _leader_rows(
    part: pd.DataFrame,
    stock_date_indices: dict[str, list[int]],
    date_index: dict[str, int],
    count: int,
) -> list[dict[str, Any]]:
    if part.empty:
        return []
    frame = part.drop_duplicates("ts_code").copy()
    frame["board_height"] = pd.to_numeric(frame["board_height"], errors="coerce").fillna(1).clip(lower=1)
    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
    order = pd.to_numeric(frame["limit_order"], errors="coerce")
    order = order.where(order > 0, pd.to_numeric(frame["lu_limit_order"], errors="coerce"))
    order = order.where(order > 0, pd.to_numeric(frame["bid_amount"], errors="coerce"))
    free_float = pd.to_numeric(frame["free_float"], errors="coerce")
    frame["order_float"] = (order / free_float.where(free_float > 0)).replace([float("inf")], pd.NA)
    current_idx = date_index[str(frame.iloc[0]["trade_date"])]

    def recent_days(code: str) -> int:
        indices = stock_date_indices.get(str(code), [])
        return bisect_right(indices, current_idx) - bisect_left(indices, current_idx - 19)

    frame["limit_days_20"] = frame["ts_code"].map(recent_days)
    max_height = max(1.0, _number(frame["board_height"].max(), 1.0))
    frame["recurrence_component"] = (frame["limit_days_20"] / 5.0).clip(upper=1.0)
    frame["early_component"] = frame["lu_time"].map(_time_strength)
    frame["order_component"] = _percentile(frame["order_float"])
    frame["amount_component"] = _percentile(frame["amount"])
    frame["same_height_score"] = (
        40 * frame["recurrence_component"]
        + 20 * frame["early_component"]
        + 20 * frame["order_component"]
        + 20 * frame["amount_component"]
    )
    # 每一板高度占一个互不重叠的分数区间，保证高一板必然排在低一板之前；
    # 近20日人气、封板速度、封单强度和成交额只在同板高内决定顺序。
    frame["leader_score"] = 100 * (
        (frame["board_height"] - 1) + frame["same_height_score"] / 100
    ) / max_height

    earliest = frame["lu_time"].replace("", pd.NA).dropna().min()
    max_amount = frame["amount"].max()
    max_order = frame["order_float"].max()
    max_recent = frame["limit_days_20"].max()

    def roles(row: pd.Series) -> list[str]:
        result: list[str] = []
        if _number(row["board_height"]) == max_height:
            result.append("高度核心")
        if earliest is not None and not pd.isna(earliest) and row["lu_time"] == earliest:
            result.append("先锋")
        if pd.notna(max_amount) and row["amount"] == max_amount:
            result.append("容量核心")
        if pd.notna(max_order) and row["order_float"] == max_order:
            result.append("封单核心")
        if max_recent >= 2 and row["limit_days_20"] == max_recent:
            result.append("人气核心")
        return result

    frame = frame.sort_values(
        ["leader_score", "board_height", "limit_days_20", "lu_time", "amount", "ts_code"],
        ascending=[False, False, False, True, False, True],
        na_position="last",
    )
    result = []
    for rank, (_, row) in enumerate(frame.head(count).iterrows(), start=1):
        result.append({
            "rank": rank,
            "title": f"龙{['一', '二', '三'][rank - 1]}",
            "ts_code": str(row["ts_code"]),
            "name": str(row.get("name") or row["ts_code"]),
            "leader_score": round(_number(row["leader_score"]), 1),
            "board_height": int(_number(row["board_height"], 1)),
            "limit_days_20": int(_number(row["limit_days_20"])),
            "limit_time": str(row.get("lu_time") or "") or None,
            "order_float": round(_number(row["order_float"]), 6) if pd.notna(row["order_float"]) else None,
            "amount": _scalar(row["amount"]),
            "roles": roles(row),
        })
    return result


def _classify_structure(
    current: pd.DataFrame,
    previous: pd.DataFrame,
    sentiment_score: float,
) -> dict[str, Any]:
    top = current.iloc[0]
    second_heat = _number(current.iloc[1]["heat_score"]) if len(current) > 1 else 0.0
    total_width = max(1.0, _number(current["limit_up_count"].sum(), 1.0))
    top_share = _number(top["limit_up_count"]) / total_width
    top3_share = _number(current.head(3)["limit_up_count"].sum()) / total_width
    shares = pd.to_numeric(current["limit_up_count"], errors="coerce").fillna(0) / total_width
    hhi = float((shares * shares).sum())
    seal = pd.to_numeric(current["seal_rate"], errors="coerce").fillna(0)
    family_source = current.copy()
    family_source["_family_key"] = family_source.apply(
        lambda row: (
            str(row.get("level1_name"))
            if str(row.get("level1_name") or "") not in {"", "待归类", "nan"}
            else f"待归类::{row.get('theme_id')}"
        ),
        axis=1,
    )
    family_rows = []
    for family_key, part in family_source.groupby("_family_key", dropna=False, sort=False):
        level1_name = str(part.iloc[0].get("level1_name") or "待归类")
        width = _number(part["limit_up_count"].sum())
        breaks = _number(part["break_count"].sum())
        weighted_persistence = (
            float((part["persistence_5"].fillna(0) * part["limit_up_count"].fillna(0)).sum()) / width
            if width else 0.0
        )
        family_rows.append({
            "level1_name": level1_name,
            "family_key": str(family_key),
            "heat_score": _number(part["heat_score"].max()),
            "limit_up_count": width,
            "break_count": breaks,
            "max_board_height": _number(part["max_board_height"].max()),
            "seal_rate": width / (width + breaks) if width + breaks else 0.0,
            "persistence_5": weighted_persistence,
            "branches": "、".join(part.head(3)["canonical_name"].astype(str).tolist()),
        })
    families = pd.DataFrame(family_rows).sort_values(
        ["heat_score", "limit_up_count", "level1_name"], ascending=[False, False, True]
    ).reset_index(drop=True)
    family_broad = families[
        (families["heat_score"] >= 60)
        & (families["limit_up_count"] >= 8)
        & (families["max_board_height"] >= 3)
        & (
            (families["persistence_5"] >= 0.6)
            | ((families["limit_up_count"] >= 15) & (families["max_board_height"] >= 4))
        )
        & (families["seal_rate"] >= 0.70)
    ].copy()
    strong = current[
        (current["heat_score"] >= 55)
        & ((current["limit_up_count"] >= 3) | (current["max_board_height"].fillna(0) >= 3))
        & (seal >= 0.60)
    ]

    previous_top = previous.iloc[0] if not previous.empty else None
    previous_rank = {str(row.theme_id): index + 1 for index, row in enumerate(previous.itertuples(index=False))}
    top_previous_rank = previous_rank.get(str(top["theme_id"]))
    changed_top = previous_top is not None and str(previous_top["theme_id"]) != str(top["theme_id"])
    same_level1 = bool(
        previous_top is not None
        and str(top.get("level1_name") or "") not in {"", "待归类"}
        and str(top.get("level1_name")) == str(previous_top.get("level1_name"))
    )
    old_current = current[current["theme_id"] == previous_top["theme_id"]] if previous_top is not None else pd.DataFrame()
    old_weakened = bool(
        previous_top is not None
        and (
            old_current.empty
            or int(old_current.index[0]) >= 3
            or _number(old_current.iloc[0]["heat_score"]) <= _number(previous_top["heat_score"]) * 0.72
        )
    )
    top_candidate = (
        _number(top["heat_score"]) >= 60
        and _number(top["limit_up_count"]) >= 5
        and _number(top["seal_rate"]) >= 0.65
    )
    is_replacement = bool(
        top_candidate
        and _number(top["limit_up_count"]) >= 10
        and _number(top["max_board_height"]) >= 3
        and _number(top["persistence_5"]) >= 0.6
        and changed_top
        and not same_level1
        and (top_previous_rank is None or top_previous_rank > 3)
        and old_weakened
    )
    is_multi = bool(
        len(family_broad) >= 2
        and _number(family_broad.iloc[0]["heat_score"]) - _number(family_broad.iloc[1]["heat_score"]) <= 15
    )
    is_single = bool(
        top_candidate
        and (
            (
                _number(top["persistence_5"]) >= 0.6
                and _number(top["max_board_height"]) >= 3
                and ((_number(top["heat_score"]) - second_heat >= 12) or top_share >= 0.25)
            )
            or (
                _number(top["limit_up_count"]) >= 15
                and _number(top["heat_score"]) - second_heat >= 12
                and _number(top["seal_rate"]) >= 0.75
            )
        )
    )

    top3_heat = current.head(3)["heat_score"].fillna(0).tolist()
    top3_heat += [0.0] * (3 - len(top3_heat))
    top3_composite = sum(weight * value for weight, value in zip(TOP3_WEIGHTS, top3_heat))
    if is_replacement:
        code, label, confidence = "mainline_replacement", "主线替换", "高"
        summary = f"{previous_top['canonical_name']}明显降温，{top['canonical_name']}以更大宽度升至首位，处于新旧主线切换。"
    elif is_multi:
        code, label, confidence = "multiple_mainlines", "多主线", "中高"
        names = "、".join(str(value) for value in family_broad.head(2)["level1_name"].tolist())
        summary = f"{names}同时具备宽度与封板质量，热度接近，市场呈多主线共存。"
    elif is_single:
        code, label, confidence = "clear_single_mainline", "主线明确", "中高"
        summary = f"{top['canonical_name']}在热度、宽度和持续性上显著领先，单一主线较明确。"
    elif top3_composite < 55:
        code, label, confidence = "cold_no_mainline", "热点低迷、无主线", "中高"
        summary = "前三题材绝对热度较低，当前没有形成具备宽度和持续性的有效主线。"
    else:
        code, label, confidence = "scattered_no_mainline", "热点分散、无明确主线", "中"
        summary = "局部题材仍有活跃度，但领先差、持续性或板块宽度不足，尚不能确认单一或多主线。"

    evidence = {
        "top_theme": str(top["canonical_name"]),
        "top_heat": round(_number(top["heat_score"]), 2),
        "top_width": int(_number(top["limit_up_count"])),
        "top_share": round(top_share, 4),
        "top3_share": round(top3_share, 4),
        "heat_gap_top1_top2": round(_number(top["heat_score"]) - second_heat, 2),
        "strong_theme_count": int(len(strong)),
        "broad_strong_theme_count": int(len(family_broad)),
        "broad_level1_themes": family_broad.head(5)["level1_name"].astype(str).tolist(),
        "theme_width_hhi": round(hhi, 4),
        "previous_top_theme": str(previous_top["canonical_name"]) if previous_top is not None else None,
        "same_level1_as_previous_top": same_level1 if previous_top is not None else None,
        "previous_top_weakened": old_weakened if previous_top is not None else None,
    }
    return {"code": code, "label": label, "confidence": confidence, "summary": summary, "evidence": evidence}


def build_market_theme_reviews(
    daily: pd.DataFrame,
    events: pd.DataFrame,
    *,
    top_n: int = 10,
    leader_count: int = 3,
) -> dict[str, dict[str, Any]]:
    """Build all as-of daily reviews without reading any future date."""
    if daily.empty:
        return {}
    frame = daily.copy()
    frame = frame[
        (pd.to_numeric(frame["limit_up_count"], errors="coerce").fillna(0) > 0)
        & ~frame["canonical_name"].isin(EXCLUDED_THEMES)
        & ~frame.get("level1_name", pd.Series("", index=frame.index)).isin(EXCLUDED_LEVEL1)
    ].copy()
    if frame.empty:
        return {}
    for column in ["heat_score", "limit_up_count", "break_count", "max_board_height", "seal_rate", "persistence_5"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    dates = sorted(str(value) for value in frame["trade_date"].unique())
    date_index = {date: index for index, date in enumerate(dates)}
    ranked_by_date: dict[str, pd.DataFrame] = {}
    composites: dict[str, float] = {}
    for date in dates:
        part = frame[frame["trade_date"].astype(str) == date].sort_values(
            ["heat_score", "limit_up_count", "canonical_name", "theme_id"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)
        part["rank"] = range(1, len(part) + 1)
        ranked_by_date[date] = part
        heat = part["heat_score"].fillna(0).tolist()[:3]
        heat += [0.0] * (3 - len(heat))
        composites[date] = sum(weight * value for weight, value in zip(TOP3_WEIGHTS, heat))

    event_frame = events.copy() if events is not None else pd.DataFrame()
    if not event_frame.empty:
        event_frame = event_frame[
            (event_frame["tag"] == "涨停")
            & (event_frame["attribution_role"] == "primary")
            & event_frame["trade_date"].astype(str).isin(dates)
        ].copy()
    stock_date_indices: dict[str, list[int]] = {}
    if not event_frame.empty:
        for code, part in event_frame.groupby("ts_code"):
            stock_date_indices[str(code)] = sorted({date_index[str(value)] for value in part["trade_date"]})

    reviews: dict[str, dict[str, Any]] = {}
    for position, date in enumerate(dates):
        current = ranked_by_date[date]
        previous = ranked_by_date[dates[position - 1]] if position else pd.DataFrame()
        history_dates = dates[max(0, position - 119): position + 1]
        history_values = [composites[item] for item in history_dates]
        composite = composites[date]
        sentiment = 100.0 * sum(value <= composite for value in history_values) / len(history_values)
        previous_ranks = {
            str(row.theme_id): int(row.rank) for row in previous.itertuples(index=False)
        }
        hot_themes = []
        date_events = event_frame[event_frame["trade_date"].astype(str) == date] if not event_frame.empty else pd.DataFrame()
        for row in current.head(top_n).to_dict("records"):
            theme_id = str(row["theme_id"])
            leaders = _leader_rows(
                date_events[date_events["theme_id"].astype(str) == theme_id] if not date_events.empty else pd.DataFrame(),
                stock_date_indices,
                date_index,
                leader_count,
            )
            previous_rank = previous_ranks.get(theme_id)
            hot_themes.append({
                "rank": int(row["rank"]),
                "theme_id": theme_id,
                "theme": str(row["canonical_name"]),
                "level1_name": str(row.get("level1_name") or "待归类"),
                "heat_score": round(_number(row["heat_score"]), 2),
                "limit_up_count": int(_number(row["limit_up_count"])),
                "break_count": int(_number(row["break_count"])),
                "max_board_height": int(_number(row["max_board_height"])) if pd.notna(row["max_board_height"]) else None,
                "seal_rate": round(_number(row["seal_rate"]), 4) if pd.notna(row["seal_rate"]) else None,
                "persistence_5": round(_number(row["persistence_5"]), 4),
                "lifecycle_state": str(row["lifecycle_state"]),
                "previous_rank": previous_rank,
                "rank_change": None if previous_rank is None else previous_rank - int(row["rank"]),
                "leaders": leaders,
            })
        structure = _classify_structure(current, previous, sentiment)
        reviews[date] = {
            "trade_date": date,
            "method_version": METHOD_VERSION,
            "theme_sentiment_score": round(sentiment, 1),
            "theme_sentiment_level": _sentiment_level(sentiment),
            "top3_heat_composite": round(composite, 2),
            "top3_weights": list(TOP3_WEIGHTS),
            "history_window_sessions": len(history_values),
            "structure": structure,
            "hot_themes": hot_themes,
            "excluded_scope": ["ST板块", "ST摘帽", "次新股", "一级分类ST与次新"],
            "point_in_time": True,
        }
    return reviews

"""为一次题材行情构建有界、可复核的新闻与行情证据包。"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, research_root
ROOT = manager_root(__file__, 1)
RESEARCH_ROOT = research_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

import duckdb
from config.settings import DB_PATH_NEWS, THEME_GRAPH_DB_PATH


BASE = Path(__file__).resolve().parent
SEARCH_TERMS_PATH = BASE / "taxonomy" / "theme_search_terms_v1.json"
GENERIC_LEVEL1 = {
    "大消费", "AI应用", "新能源", "半导体", "机械设备", "金融", "农业", "军工",
    "医药医疗", "政策与区域", "化工与新材料", "有色与资源", "其他事件",
}
MARKET_RECAP_TITLE_RE = re.compile(
    r"(?:异动|涨停|封板|触及涨停|直线涨停|拉升|走强|活跃|大涨|冲高|领涨|涨超|"
    r"股价|连板|牛股|飙|涨势|涨幅|盘中|收盘|跌停|跳水)"
)
INDEPENDENT_FACT_RE = re.compile(
    r"(?:发布|印发|公告|签署|中标|获批|立项|发射|首飞|投产|停产|收购|并购|"
    r"上调|下调|涨价|降价|突破|量产|开工|订单|政策|会议决定|正式启动|数据(?:显示|公布))"
)


def _date(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d")


def _iso_day(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _datetime_value(value: str) -> datetime:
    return datetime.fromisoformat(str(value).strip())


def _text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _normal_title(title: str, content: str) -> str:
    value = title or content[:100]
    value = re.sub(r"[\s\W_]+", "", value).lower()
    return value[:120]


def _market_recap_features(title: str, content: str) -> tuple[bool, bool, float]:
    """识别以行情结果为主体的异动稿，并给检索排序降权。

    异动稿中可能夹带真实事件，因此只降权、不删除；后续模型必须把独立事件
    与“股票涨了”这一结果拆开判断。
    """
    is_recap = bool(MARKET_RECAP_TITLE_RE.search(title))
    has_independent_fact = bool(INDEPENDENT_FACT_RE.search(content))
    penalty = 0.0
    if is_recap:
        penalty = 7.0 if has_independent_fact else 13.0
    return is_recap, has_independent_fact, penalty


def _load_overrides() -> dict[str, list[str]]:
    if not SEARCH_TERMS_PATH.exists():
        return {}
    return json.loads(SEARCH_TERMS_PATH.read_text(encoding="utf-8"))


def _episode_context(database: Path, episode_id: str) -> dict[str, Any]:
    conn = duckdb.connect(str(database), read_only=True)
    try:
        episode = conn.execute(
            """SELECT e.*,t.canonical_name,t.theme_level,
                      COALESCE(x.level1_name,'待归类') AS level1_name,
                      COALESCE(x.secondary_level1_json,'[]') AS secondary_level1_json
               FROM fact_theme_episode e JOIN dim_theme t USING(theme_id)
               LEFT JOIN dim_theme_taxonomy x USING(theme_id)
               WHERE e.episode_id=?""", [episode_id]).fetchdf()
        if episode.empty:
            raise ValueError(f"不存在的 episode_id: {episode_id}")
        row = episode.iloc[0].to_dict()
        start, end = row["start_date"], row["last_active_date"]
        daily = conn.execute(
            """SELECT * FROM fact_theme_daily WHERE theme_id=? AND trade_date BETWEEN ? AND ?
               ORDER BY trade_date""", [row["theme_id"], start, end]).fetchdf()
        stocks = conn.execute(
            """SELECT e.* FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
               WHERE r.theme_id=? AND r.attribution_role='primary'
                 AND e.trade_date BETWEEN ? AND ?
               ORDER BY e.trade_date,COALESCE(e.lu_time,'99:99:99'),e.board_height DESC,e.ts_code""",
            [row["theme_id"], start, end]).fetchdf()
        aliases = [item[0] for item in conn.execute(
            "SELECT DISTINCT alias FROM theme_alias WHERE theme_id=? ORDER BY alias", [row["theme_id"]]).fetchall()]
        related = [item[0] for item in conn.execute(
            """SELECT t2.canonical_name FROM rel_limit_theme p
               JOIN rel_limit_theme a ON p.event_id=a.event_id
               JOIN dim_theme t2 ON a.theme_id=t2.theme_id
               JOIN fact_limit_event e ON p.event_id=e.event_id
               WHERE p.theme_id=? AND p.attribution_role='primary'
                 AND a.attribution_role='auxiliary' AND e.trade_date BETWEEN ? AND ?
               GROUP BY t2.canonical_name ORDER BY COUNT(*) DESC,t2.canonical_name LIMIT 8""",
            [row["theme_id"], start, end]).fetchall()]
    finally:
        conn.close()

    market_evidence = []
    for index, item in enumerate(daily.to_dict("records"), 1):
        market_evidence.append({
            "evidence_id": f"M{index}", "trade_date": item["trade_date"],
            "limit_up_count": int(item["limit_up_count"]), "break_count": int(item["break_count"]),
            "max_board_height": int(item["max_board_height"]) if item["max_board_height"] is not None else None,
            "first_board_count": int(item["first_board_count"]),
            "multi_board_count": int(item["multi_board_count"]),
            "seal_rate": round(float(item["seal_rate"]), 4) if item["seal_rate"] is not None else None,
            "heat_score": round(float(item["heat_score"]), 4),
            "lifecycle_state": item["lifecycle_state"],
        })
    stock_evidence = []
    for index, item in enumerate(stocks.to_dict("records"), 1):
        stock_evidence.append({
            "evidence_id": f"S{index}", "trade_date": item["trade_date"], "ts_code": item["ts_code"],
            "name": _text(item["name"]), "tag": item["tag"], "status_raw": _text(item["status_raw"]),
            "board_height": int(item["board_height"]) if item["board_height"] is not None else None,
            "lu_time": _text(item["lu_time"]), "lu_desc": _text(item["lu_desc"]),
            "theme_raw": _text(item["theme_raw"]),
        })
    row["secondary_level1"] = json.loads(row.pop("secondary_level1_json"))
    return {"episode": row, "daily": market_evidence, "stocks": stock_evidence,
            "aliases": aliases, "related": related}


def _search_terms(context: dict[str, Any]) -> list[str]:
    episode = context["episode"]
    canonical = _text(episode["canonical_name"])
    terms = [canonical]
    stripped = re.sub(r"(?:概念|板块|产业链|类)$", "", canonical).strip()
    if len(stripped) >= 2 and stripped != canonical:
        terms.append(stripped)
    terms.extend(_load_overrides().get(canonical, []))
    terms.extend(_text(item) for item in context["aliases"])
    # 不把同一股票的辅助题材直接当作新闻检索词；它们常是公司多概念标签，
    # 会把与本轮主线无关的新闻带入候选池。相关分支由人工检索词表维护。
    if episode["level1_name"] not in GENERIC_LEVEL1:
        terms.append(_text(episode["level1_name"]))

    # 启动初期最早封板或最高板公司的公告有时是公司事件型催化；只加入少量名称。
    early_days = sorted({item["trade_date"] for item in context["stocks"]})[:3]
    early = [item for item in context["stocks"] if item["trade_date"] in early_days]
    early.sort(key=lambda item: (item["trade_date"], item.get("lu_time") or "99:99:99",
                                 -(item.get("board_height") or 0)))
    terms.extend(item["name"] for item in early[:4] if len(item["name"]) >= 2)
    return list(dict.fromkeys(term for term in terms if 2 <= len(term) <= 20))[:18]


def _query_news(news_db: Path, start: str, end: str, terms: list[str], focus_dates: list[str],
                per_term: int = 18) -> list[dict]:
    conn = sqlite3.connect(str(news_db))
    conn.row_factory = sqlite3.Row
    rows: dict[tuple, dict] = {}
    try:
        for table in ("tbl_news", "tbl_major_news"):
            for term in terms:
                params = [f"{start} 00:00:00", f"{end} 23:59:59", f"%{term}%", f"%{term}%", per_term]
                base = (f"SELECT datetime,src,title,content,channels,md5,snap_ts,'{table}' AS source_table "
                        f"FROM {table} WHERE datetime BETWEEN ? AND ? AND (title LIKE ? OR content LIKE ?)")
                for order in ("ASC", "DESC"):
                    for raw in conn.execute(base + f" ORDER BY datetime {order} LIMIT ?", params).fetchall():
                        item = dict(raw)
                        key = (item.get("md5") or "", item["datetime"], item.get("src") or "",
                               item.get("title") or "")
                        rows[key] = item
            # 长周期按关键行情日补取，避免候选全部集中在启动日前后。
            term_sql = " OR ".join(["title LIKE ? OR content LIKE ?"] * len(terms))
            term_params = [value for term in terms for value in (f"%{term}%", f"%{term}%")]
            for focus in focus_dates:
                day = _date(focus)
                phase_start, phase_end = _iso_day(day - timedelta(days=2)), _iso_day(day + timedelta(days=2))
                base = (f"SELECT datetime,src,title,content,channels,md5,snap_ts,'{table}' AS source_table "
                        f"FROM {table} WHERE datetime BETWEEN ? AND ? AND ({term_sql})")
                params = [f"{phase_start} 00:00:00", f"{phase_end} 23:59:59", *term_params, 80]
                for order in ("ASC", "DESC"):
                    for raw in conn.execute(base + f" ORDER BY datetime {order} LIMIT ?", params).fetchall():
                        item = dict(raw)
                        key = (item.get("md5") or "", item["datetime"], item.get("src") or "",
                               item.get("title") or "")
                        rows[key] = item
        for term in terms:
            params = [start.replace("-", ""), end.replace("-", ""), f"%{term}%", f"%{term}%", per_term]
            sql = ("SELECT datetime,COALESCE(src,'cctv') AS src,title,content,NULL AS channels,md5,snap_ts,"
                   "'tbl_cctv_news' AS source_table FROM tbl_cctv_news "
                   "WHERE datetime BETWEEN ? AND ? AND (title LIKE ? OR content LIKE ?) "
                   "ORDER BY datetime ASC LIMIT ?")
            for raw in conn.execute(sql, params).fetchall():
                item = dict(raw)
                item["datetime"] = f"{item['datetime'][:4]}-{item['datetime'][4:6]}-{item['datetime'][6:]} 20:00:00"
                key = (item.get("md5") or "", item["datetime"], item.get("src") or "", item.get("title") or "")
                rows[key] = item
    finally:
        conn.close()
    return list(rows.values())


def _rank_candidates(rows: list[dict], terms: list[str], theme: str, episode_start: str,
                     focus_dates: list[str],
                     max_candidates: int) -> tuple[list[dict], list[dict]]:
    start_dt = _date(episode_start)
    focus = [_date(value) for value in focus_dates] or [start_dt]
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        title, content = _text(row.get("title")), _text(row.get("content"))
        grouped[_normal_title(title, content)].append(row)

    ranked = []
    for group_key, group in grouped.items():
        # 同标题转载只保留文本最完整的一条，来源列表仍写入证据。
        row = max(group, key=lambda item: len(_text(item.get("content"))))
        title, content = _text(row.get("title")), _text(row.get("content"))
        matched = [term for term in terms if term in title or term in content]
        is_market_recap, has_independent_fact, recap_penalty = _market_recap_features(title, content)
        published = _datetime_value(row["datetime"])
        nearest = min(focus, key=lambda value: abs((published.date() - value.date()).days))
        delta = abs((published.date() - nearest.date()).days)
        score = 0.0
        score += 10 if theme and theme in title else 0
        score += min(6, 3 * sum(term in title for term in matched))
        score += min(4, sum(term in content for term in matched))
        score += 7 if delta <= 1 else 5 if delta <= 3 else 3 if delta <= 7 else 1
        score += 12 if delta == 0 else 4 if delta == 1 else 0
        score += 1 if row["source_table"] in {"tbl_major_news", "tbl_cctv_news"} else 0
        score -= recap_penalty
        ranked.append({"score": score, "published": published, "row": row, "matched": matched,
                       "duplicate_sources": sorted({_text(item.get("src")) for item in group if _text(item.get("src"))}),
                       "group_key": group_key, "focus_date": nearest.strftime("%Y%m%d"),
                       "is_market_recap": is_market_recap,
                       "has_independent_fact": has_independent_fact,
                       "market_recap_penalty": recap_penalty})
    ranked.sort(key=lambda item: (-item["score"], item["published"], item["group_key"]))
    # 每个关键行情日轮流取候选，再用全局得分补满，防止长周期中段/高潮段被启动日淹没。
    by_focus: dict[str, list[dict]] = defaultdict(list)
    for item in ranked:
        by_focus[item["focus_date"]].append(item)
    selected, seen = [], set()
    selection_order = ([focus_dates[0]] + ([focus_dates[1]] if len(focus_dates) > 1 else []) + focus_dates)
    while len(selected) < max_candidates and any(by_focus.values()):
        for date in selection_order:
            bucket = by_focus.get(date, [])
            if bucket:
                item = bucket.pop(0)
                if item["group_key"] not in seen:
                    selected.append(item)
                    seen.add(item["group_key"])
                    if len(selected) >= max_candidates:
                        break
    if len(selected) < max_candidates:
        for item in ranked:
            if item["group_key"] not in seen:
                selected.append(item)
                seen.add(item["group_key"])
                if len(selected) >= max_candidates:
                    break
    ranked = sorted(selected, key=lambda item: (-item["score"], item["published"], item["group_key"]))

    candidates, full = [], []
    for index, item in enumerate(ranked, 1):
        row = item["row"]
        evidence_id = f"N{index}"
        title, content = _text(row.get("title")), _text(row.get("content"))
        common = {
            "evidence_id": evidence_id, "published_at": row["datetime"],
            "observed_at": _text(row.get("snap_ts")) or "unknown",
            "availability_basis": "source_timestamp_unverified",
            "source": _text(row.get("src")) or row["source_table"], "source_table": row["source_table"],
            "title": title, "matched_terms": item["matched"],
            "nearest_focus_date": item["focus_date"],
            "duplicate_sources": item["duplicate_sources"], "candidate_score": item["score"],
            "evidence_kind_hint": "market_recap" if item["is_market_recap"] else "source_event",
            "has_independent_fact_marker": item["has_independent_fact"],
            "market_recap_penalty": item["market_recap_penalty"],
            "source_record_id": _text(row.get("md5")) or hashlib.sha256(
                f"{row['datetime']}|{row.get('src')}|{title}".encode("utf-8")).hexdigest(),
        }
        candidates.append({**common, "snippet": content[:500]})
        full.append({**common, "content": content[:4000]})
    return candidates, full


def build_episode_packet(episode_id: str, database: Path = THEME_GRAPH_DB_PATH,
                         news_db: Path = DB_PATH_NEWS, pre_days: int = 10,
                         post_days: int = 1, max_candidates: int = 40) -> tuple[dict, list[dict]]:
    context = _episode_context(Path(database), episode_id)
    episode = context["episode"]
    terms = _search_terms(context)
    market_ranked = sorted(context["daily"],
                           key=lambda item: (item["heat_score"], item["limit_up_count"],
                                             item.get("max_board_height") or 0), reverse=True)
    focus_dates = list(dict.fromkeys(
        [episode["start_date"], episode["peak_date"]] +
        [item["trade_date"] for item in market_ranked[:6]]
    ))[:8]
    start = _date(episode["start_date"]) - timedelta(days=pre_days)
    end = _date(episode["last_active_date"]) + timedelta(days=post_days)
    raw = _query_news(Path(news_db), _iso_day(start), _iso_day(end), terms, focus_dates)
    candidates, full = _rank_candidates(raw, terms, episode["canonical_name"], episode["start_date"],
                                        focus_dates, max_candidates)
    packet = {
        "entity_type": "theme_episode", "entity_id": episode_id,
        "episode_id": episode_id, "valid_at": f"{episode['last_active_date']} 23:59:59",
        "analysis_mode": "retrospective_episode_with_stage_time_guard",
        "theme": {key: episode[key] for key in ("theme_id", "canonical_name", "level1_name", "secondary_level1")},
        "episode": {key: episode[key] for key in (
            "episode_id", "start_date", "last_active_date", "end_date", "status", "peak_date",
            "peak_heat", "peak_breadth", "active_sessions", "segmentation_method", "computed_as_of")},
        "news_window": {"start": _iso_day(start), "end": _iso_day(end),
                        "publication_time_note": "source timestamp, historical availability not independently proven"},
        "search_terms": terms, "focus_dates": focus_dates, "market_evidence": context["daily"],
        "stock_evidence": context["stocks"], "candidate_evidence": candidates,
    }
    return packet, full


def main() -> int:
    parser = argparse.ArgumentParser(description="构建题材行情周期的催化证据包")
    parser.add_argument("--episode-id", required=True)
    parser.add_argument("--database", type=Path, default=THEME_GRAPH_DB_PATH)
    parser.add_argument("--news-db", type=Path, default=DB_PATH_NEWS)
    parser.add_argument("--pre-days", type=int, default=10)
    parser.add_argument("--post-days", type=int, default=1)
    parser.add_argument("--max-candidates", type=int, default=40)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    packet, full = build_episode_packet(args.episode_id, args.database, args.news_db,
                                        args.pre_days, args.post_days, args.max_candidates)
    target = args.output_dir or RESEARCH_ROOT / "outputs" / "theme_catalyst_analysis" / args.episode_id
    target.mkdir(parents=True, exist_ok=True)
    (target / "screening_packet.json").write_text(json.dumps(packet, ensure_ascii=False, indent=2), encoding="utf-8")
    (target / "news_full.json").write_text(json.dumps(full, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"episode_id": args.episode_id, "output_dir": str(target),
                      "candidate_news": len(full), "market_days": len(packet["market_evidence"]),
                      "stock_events": len(packet["stock_evidence"]), "search_terms": packet["search_terms"]},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

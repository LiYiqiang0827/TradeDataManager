"""开盘啦题材时序知识库。

本模块只把 ``db_cn_kpl.db / tbl_cn_kpl_list`` 投影成可按历史时点查询的
DuckDB 结构。原则是：原始榜单不改写、主归因与辅助标签分开、派生分数只使用
当日及此前数据、每次构建都有清单和版本记录。membership schema 仅为以后扩展
预留，第一版不读取 ``kpl_concept_cons``。
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable
import uuid

import duckdb
import pandas as pd

from config.settings import DB_PATH_KPL, THEME_GRAPH_DB_PATH
from core.theme_daily_review import _leader_rows, build_market_theme_reviews


GRAPH_SCHEMA_VERSION = "1.0.0"
SOURCE_LIST = "kpl_list"


SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS graph_meta (
    key VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS graph_build_run (
    run_id VARCHAR PRIMARY KEY,
    mode VARCHAR NOT NULL,
    start_date VARCHAR NOT NULL,
    end_date VARCHAR NOT NULL,
    as_of_date VARCHAR,
    status VARCHAR NOT NULL,
    changed_dates_json VARCHAR,
    stats_json VARCHAR,
    error VARCHAR,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS source_day_manifest (
    source_name VARCHAR NOT NULL,
    trade_date VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    content_hash VARCHAR NOT NULL,
    max_snap_ts VARCHAR,
    last_run_id VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY(source_name, trade_date)
);
CREATE TABLE IF NOT EXISTS source_checkpoint (
    source_name VARCHAR PRIMARY KEY,
    max_trade_date VARCHAR,
    last_complete_date VARCHAR,
    last_success_run_id VARCHAR,
    row_count BIGINT,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_theme (
    theme_id VARCHAR PRIMARY KEY,
    canonical_name VARCHAR NOT NULL,
    theme_level VARCHAR NOT NULL,
    parent_theme_id VARCHAR,
    status VARCHAR NOT NULL,
    first_seen_date VARCHAR NOT NULL,
    last_seen_date VARCHAR NOT NULL,
    source_priority VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS theme_alias (
    theme_id VARCHAR NOT NULL,
    alias VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    source_theme_code VARCHAR,
    valid_from VARCHAR NOT NULL,
    valid_to VARCHAR,
    first_seen_at TIMESTAMP NOT NULL,
    last_seen_at TIMESTAMP NOT NULL,
    confidence DOUBLE NOT NULL,
    PRIMARY KEY(theme_id, alias, source, valid_from)
);
CREATE TABLE IF NOT EXISTS dim_theme_taxonomy (
    theme_id VARCHAR PRIMARY KEY,
    level1_id VARCHAR NOT NULL,
    level1_name VARCHAR NOT NULL,
    level2_name VARCHAR NOT NULL,
    secondary_level1_json VARCHAR NOT NULL,
    classification_source VARCHAR NOT NULL,
    confidence DOUBLE NOT NULL,
    rationale VARCHAR,
    taxonomy_version VARCHAR NOT NULL,
    review_status VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_stock (
    ts_code VARCHAR PRIMARY KEY,
    name VARCHAR,
    first_seen_date VARCHAR NOT NULL,
    last_seen_date VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_limit_event (
    event_id VARCHAR PRIMARY KEY,
    trade_date VARCHAR NOT NULL,
    ts_code VARCHAR NOT NULL,
    name VARCHAR,
    tag VARCHAR NOT NULL,
    status_raw VARCHAR,
    continuous_board INTEGER,
    period_days INTEGER,
    period_boards INTEGER,
    board_height INTEGER,
    lu_time VARCHAR,
    ld_time VARCHAR,
    open_time VARCHAR,
    last_time VARCHAR,
    lu_desc VARCHAR,
    theme_raw VARCHAR,
    limit_order DOUBLE,
    lu_limit_order DOUBLE,
    bid_amount DOUBLE,
    amount DOUBLE,
    turnover_rate DOUBLE,
    free_float DOUBLE,
    snap_ts VARCHAR,
    source VARCHAR NOT NULL,
    row_hash VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_limit_date ON fact_limit_event(trade_date);
CREATE INDEX IF NOT EXISTS idx_limit_stock ON fact_limit_event(ts_code, trade_date);
CREATE TABLE IF NOT EXISTS rel_limit_theme (
    event_id VARCHAR NOT NULL,
    theme_id VARCHAR NOT NULL,
    attribution_role VARCHAR NOT NULL,
    source_label VARCHAR NOT NULL,
    weight DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    evidence_source VARCHAR NOT NULL,
    valid_at VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY(event_id, theme_id, attribution_role)
);
CREATE INDEX IF NOT EXISTS idx_limit_theme ON rel_limit_theme(theme_id, valid_at);
CREATE TABLE IF NOT EXISTS fact_membership_snapshot (
    trade_date VARCHAR NOT NULL,
    theme_id VARCHAR NOT NULL,
    source_theme_code VARCHAR NOT NULL,
    ts_code VARCHAR NOT NULL,
    stock_name VARCHAR,
    description VARCHAR,
    hot_num BIGINT,
    snap_ts VARCHAR,
    row_hash VARCHAR NOT NULL,
    PRIMARY KEY(trade_date, theme_id, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_member_date ON fact_membership_snapshot(trade_date);
CREATE INDEX IF NOT EXISTS idx_member_stock ON fact_membership_snapshot(ts_code, trade_date);
CREATE TABLE IF NOT EXISTS rel_theme_stock_membership (
    theme_id VARCHAR NOT NULL,
    ts_code VARCHAR NOT NULL,
    valid_from VARCHAR NOT NULL,
    valid_to VARCHAR,
    description VARCHAR,
    first_hot_num BIGINT,
    last_hot_num BIGINT,
    source VARCHAR NOT NULL,
    confidence DOUBLE NOT NULL,
    PRIMARY KEY(theme_id, ts_code, valid_from)
);
CREATE TABLE IF NOT EXISTS fact_theme_daily (
    trade_date VARCHAR NOT NULL,
    theme_id VARCHAR NOT NULL,
    limit_up_count INTEGER NOT NULL,
    break_count INTEGER NOT NULL,
    max_board_height INTEGER,
    first_board_count INTEGER NOT NULL,
    multi_board_count INTEGER NOT NULL,
    ladder_points DOUBLE NOT NULL,
    seal_rate DOUBLE,
    early_limit_share DOUBLE,
    persistence_5 DOUBLE NOT NULL,
    days_since_last_active INTEGER,
    heat_score DOUBLE NOT NULL,
    heat_percentile_120 DOUBLE NOT NULL,
    lifecycle_state VARCHAR NOT NULL,
    episode_id VARCHAR,
    run_id VARCHAR NOT NULL,
    PRIMARY KEY(trade_date, theme_id)
);
CREATE INDEX IF NOT EXISTS idx_theme_daily ON fact_theme_daily(theme_id, trade_date);
CREATE TABLE IF NOT EXISTS fact_theme_episode (
    episode_id VARCHAR PRIMARY KEY,
    theme_id VARCHAR NOT NULL,
    start_date VARCHAR NOT NULL,
    last_active_date VARCHAR NOT NULL,
    end_date VARCHAR,
    status VARCHAR NOT NULL,
    peak_date VARCHAR NOT NULL,
    peak_heat DOUBLE NOT NULL,
    peak_breadth INTEGER NOT NULL,
    active_sessions INTEGER NOT NULL,
    segmentation_method VARCHAR NOT NULL,
    computed_as_of VARCHAR NOT NULL,
    graph_version VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS dirty_entity_queue (
    entity_type VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    dirty_from VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    PRIMARY KEY(entity_type, entity_id, dirty_from, reason)
);
CREATE TABLE IF NOT EXISTS llm_analysis (
    analysis_id VARCHAR PRIMARY KEY,
    entity_type VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    analysis_type VARCHAR NOT NULL,
    valid_at VARCHAR,
    provider VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    prompt_version VARCHAR NOT NULL,
    input_hash VARCHAR NOT NULL,
    output_json VARCHAR,
    evidence_json VARCHAR,
    confidence DOUBLE,
    review_status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE(entity_type, entity_id, analysis_type, valid_at, provider, model, prompt_version, input_hash)
);
CREATE TABLE IF NOT EXISTS source_conflict (
    conflict_id VARCHAR PRIMARY KEY,
    entity_type VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    valid_at VARCHAR,
    field_name VARCHAR NOT NULL,
    primary_value VARCHAR,
    reference_value VARCHAR,
    primary_source VARCHAR NOT NULL,
    reference_source VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);
"""


def compact_date(value: str) -> str:
    text = str(value).strip().replace("-", "").replace("/", "")
    if not re.fullmatch(r"\d{8}", text):
        raise ValueError(f"日期必须为 YYYYMMDD 或 YYYY-MM-DD: {value}")
    datetime.strptime(text, "%Y%m%d")
    return text


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _clean(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _float(value: Any) -> float | None:
    try:
        if value in (None, "") or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def stable_id(prefix: str, value: str, length: int = 14) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:length].upper()
    return f"{prefix}-{digest}"


def concept_theme_id(source_code: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9]+", "-", source_code.strip()).strip("-").upper()
    return f"T-KPL-{safe}"


def name_theme_id(name: str) -> str:
    return stable_id("T-NAME", name)


def split_theme_labels(raw: Any) -> list[str]:
    text = _clean(raw)
    if not text:
        return []
    labels = [part.strip() for part in re.split(r"[、,，;+；]+", text) if part.strip()]
    return list(dict.fromkeys(labels))


def parse_board_status(raw: Any, tag: str) -> tuple[int | None, int | None, int | None, int | None]:
    """返回 continuous_board, period_days, period_boards, board_height。"""
    text = _clean(raw)
    if tag != "涨停":
        return None, None, None, None
    if text in {"首板", "1板", "1连板"}:
        return 1, 1, 1, 1
    match = re.search(r"(\d+)连板", text)
    if match:
        height = int(match.group(1))
        return height, height, height, height
    match = re.search(r"(\d+)天(\d+)板", text)
    if match:
        days, boards = int(match.group(1)), int(match.group(2))
        return None, days, boards, boards
    # 已封板但源状态为空时，只能确认当日是首个可见板，不能伪造连板数。
    return None, None, None, 1


def board_points(height: int | None) -> float:
    h = max(1, int(height or 1))
    points = {1: 1.0, 2: 2.5, 3: 5.0, 4: 8.0, 5: 12.0}
    return points.get(h, 12.0 + 4.0 * (h - 5))


def dataframe_hash(df: pd.DataFrame, columns: Iterable[str]) -> str:
    selected = [column for column in columns if column in df.columns]
    if df.empty:
        payload = b""
    else:
        normalized = df[selected].fillna("").astype(str).sort_values(selected).reset_index(drop=True)
        payload = normalized.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass
class BuildStats:
    source_dates_seen: int = 0
    changed_list_dates: int = 0
    limit_events: int = 0
    themes: int = 0
    stocks: int = 0
    daily_rows: int = 0
    episodes: int = 0


class ThemeGraphStore:
    def __init__(self, database: str | Path = THEME_GRAPH_DB_PATH, source_db: str | Path = DB_PATH_KPL):
        self.database = Path(database).expanduser()
        self.source_db = Path(source_db).expanduser()
        self.database.parent.mkdir(parents=True, exist_ok=True)

    def connect(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(str(self.database), read_only=read_only)
        if not read_only:
            conn.execute(SCHEMA_SQL)
            conn.execute(
                "INSERT OR REPLACE INTO graph_meta VALUES (?, ?, ?)",
                ["schema_version", GRAPH_SCHEMA_VERSION, utc_now()],
            )
        return conn

    def source_bounds(self) -> tuple[str | None, str | None]:
        with sqlite3.connect(self.source_db) as source:
            row = source.execute(
                "SELECT MIN(trade_date), MAX(trade_date) FROM tbl_cn_kpl_list"
            ).fetchone()
        return (row[0], row[1]) if row else (None, None)

    def incremental_range(self, lookback_days: int = 7) -> tuple[str, str]:
        source_min, source_max = self.source_bounds()
        if not source_min or not source_max:
            raise RuntimeError("开盘啦源数据库没有可构建日期")
        conn = self.connect()
        try:
            last = conn.execute(
                "SELECT max_trade_date FROM source_checkpoint WHERE source_name=?",
                [SOURCE_LIST],
            ).fetchone()[0]
        finally:
            conn.close()
        if not last:
            # 第一次自动运行只建源库最新年份；更早历史必须显式 backfill，
            # 避免调度器意外触发多年全量回填。
            return max(compact_date(source_min), compact_date(source_max)[:4] + "0101"), compact_date(source_max)
        start = (datetime.strptime(last, "%Y%m%d") - pd.Timedelta(days=lookback_days)).strftime("%Y%m%d")
        return max(compact_date(source_min), start), compact_date(source_max)

    def build(self, start_date: str, end_date: str, mode: str = "backfill", force: bool = False) -> dict[str, Any]:
        start_date, end_date = compact_date(start_date), compact_date(end_date)
        if start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")
        run_id = str(uuid.uuid4())
        started = utc_now()
        stats = BuildStats()
        changed: dict[str, list[str]] = {SOURCE_LIST: []}
        conn = self.connect()
        conn.execute(
            "INSERT INTO graph_build_run VALUES (?, ?, ?, ?, ?, 'running', NULL, NULL, NULL, ?, NULL)",
            [run_id, mode, start_date, end_date, end_date, started],
        )
        try:
            conn.execute("BEGIN TRANSACTION")
            with sqlite3.connect(self.source_db) as source:
                source.row_factory = sqlite3.Row
                dates = self._source_dates(source, start_date, end_date)
                stats.source_dates_seen = len(dates)
                for trade_date in dates:
                    list_df = pd.read_sql_query(
                        "SELECT * FROM tbl_cn_kpl_list WHERE trade_date = ?",
                        source,
                        params=[trade_date],
                    )
                    if self._source_changed(conn, SOURCE_LIST, trade_date, list_df, force):
                        self._replace_limit_day(conn, trade_date, list_df, run_id)
                        self._write_manifest(conn, SOURCE_LIST, trade_date, list_df, run_id)
                        changed[SOURCE_LIST].append(trade_date)
                        stats.limit_events += len(list_df)

            if changed[SOURCE_LIST]:
                stats.daily_rows, stats.episodes = self._rebuild_daily_and_episodes(conn, run_id)
            stats.changed_list_dates = len(changed[SOURCE_LIST])
            stats.themes = conn.execute("SELECT COUNT(*) FROM dim_theme").fetchone()[0]
            stats.stocks = conn.execute("SELECT COUNT(*) FROM dim_stock").fetchone()[0]
            for source_name in (SOURCE_LIST,):
                row = conn.execute(
                    "SELECT MAX(trade_date), SUM(row_count) FROM source_day_manifest WHERE source_name=?",
                    [source_name],
                ).fetchone()
                if row and row[0]:
                    conn.execute(
                        "INSERT OR REPLACE INTO source_checkpoint VALUES (?, ?, ?, ?, ?, ?)",
                        [source_name, row[0], row[0], run_id, int(row[1] or 0), utc_now()],
                    )
            changed_json = json.dumps(changed, ensure_ascii=False)
            stats_json = json.dumps(asdict(stats), ensure_ascii=False)
            conn.execute(
                """UPDATE graph_build_run SET status='success', changed_dates_json=?, stats_json=?,
                   completed_at=? WHERE run_id=?""",
                [changed_json, stats_json, utc_now(), run_id],
            )
            conn.execute("COMMIT")
            return {"run_id": run_id, "mode": mode, "start_date": start_date,
                    "end_date": end_date, "changed_dates": changed, **asdict(stats)}
        except Exception as exc:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            conn.execute(
                "UPDATE graph_build_run SET status='failed', error=?, completed_at=? WHERE run_id=?",
                [str(exc), utc_now(), run_id],
            )
            raise
        finally:
            conn.close()

    @staticmethod
    def _source_dates(source: sqlite3.Connection, start_date: str, end_date: str) -> list[str]:
        rows = source.execute(
            """SELECT DISTINCT trade_date FROM tbl_cn_kpl_list
               WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date""",
            [start_date, end_date],
        ).fetchall()
        return [compact_date(row[0]) for row in rows]

    @staticmethod
    def _manifest_columns(source_name: str) -> list[str]:
        if source_name == SOURCE_LIST:
            return ["ts_code", "name", "trade_date", "lu_time", "lu_desc", "tag", "theme",
                    "status", "limit_order", "bid_amount", "amount", "turnover_rate", "free_float"]
        raise ValueError(f"不支持的题材图谱源: {source_name}")

    def _source_changed(self, conn, source_name: str, trade_date: str, df: pd.DataFrame, force: bool) -> bool:
        if force:
            return True
        digest = dataframe_hash(df, self._manifest_columns(source_name))
        row = conn.execute(
            "SELECT row_count, content_hash FROM source_day_manifest WHERE source_name=? AND trade_date=?",
            [source_name, trade_date],
        ).fetchone()
        return not row or int(row[0]) != len(df) or row[1] != digest

    def _write_manifest(self, conn, source_name: str, trade_date: str, df: pd.DataFrame, run_id: str) -> None:
        digest = dataframe_hash(df, self._manifest_columns(source_name))
        max_snap = ""
        if "snap_ts" in df.columns and not df.empty:
            max_snap = _clean(df["snap_ts"].dropna().max())
        conn.execute(
            "INSERT OR REPLACE INTO source_day_manifest VALUES (?, ?, ?, ?, ?, ?, ?)",
            [source_name, trade_date, len(df), digest, max_snap, run_id, utc_now()],
        )

    def _upsert_theme(self, conn, theme_id: str, name: str, date: str, level: str,
                      source_priority: str, source_code: str | None = None) -> None:
        now = utc_now()
        conn.execute(
            """INSERT INTO dim_theme VALUES (?, ?, ?, NULL, 'active', ?, ?, ?, ?, ?)
               ON CONFLICT(theme_id) DO UPDATE SET
                 canonical_name=CASE WHEN excluded.source_priority='kpl_concept' THEN excluded.canonical_name
                                     ELSE dim_theme.canonical_name END,
                 first_seen_date=LEAST(dim_theme.first_seen_date, excluded.first_seen_date),
                 last_seen_date=GREATEST(dim_theme.last_seen_date, excluded.last_seen_date),
                 updated_at=excluded.updated_at""",
            [theme_id, name, level, date, date, source_priority, now, now],
        )
        # 每日快照保留在 fact 表；alias 的同名记录压成有效区间。
        found = conn.execute(
            """SELECT valid_from FROM theme_alias WHERE theme_id=? AND alias=? AND source=?
               ORDER BY valid_from DESC LIMIT 1""",
            [theme_id, name, source_priority],
        ).fetchone()
        if found:
            conn.execute(
                "UPDATE theme_alias SET valid_to=NULL, last_seen_at=? WHERE theme_id=? AND alias=? AND source=? AND valid_from=?",
                [now, theme_id, name, source_priority, found[0]],
            )
        else:
            conn.execute(
                "INSERT INTO theme_alias VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)",
                [theme_id, name, source_priority, source_code, date, now, now,
                 1.0 if source_priority == "kpl_concept" else 0.85],
            )

    @staticmethod
    def _upsert_stock(conn, ts_code: str, name: str, date: str) -> None:
        now = utc_now()
        conn.execute(
            """INSERT INTO dim_stock VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(ts_code) DO UPDATE SET name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE dim_stock.name END,
                 first_seen_date=LEAST(dim_stock.first_seen_date, excluded.first_seen_date),
                 last_seen_date=GREATEST(dim_stock.last_seen_date, excluded.last_seen_date), updated_at=excluded.updated_at""",
            [ts_code, name, date, date, now, now],
        )

    @staticmethod
    def _bulk_upsert_stocks(conn, frame: pd.DataFrame, date: str) -> None:
        if frame.empty:
            return
        now = utc_now()
        incoming = (frame[["ts_code", "name"]].fillna("").astype(str)
                    .query("ts_code != ''").drop_duplicates("ts_code", keep="last").copy())
        incoming["first_seen_date"] = date
        incoming["last_seen_date"] = date
        incoming["created_at"] = now
        incoming["updated_at"] = now
        conn.register("incoming_stocks", incoming)
        conn.execute(
            """INSERT INTO dim_stock SELECT * FROM incoming_stocks
               ON CONFLICT(ts_code) DO UPDATE SET
                 name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE dim_stock.name END,
                 first_seen_date=LEAST(dim_stock.first_seen_date, excluded.first_seen_date),
                 last_seen_date=GREATEST(dim_stock.last_seen_date, excluded.last_seen_date),
                 updated_at=excluded.updated_at"""
        )
        conn.unregister("incoming_stocks")

    @staticmethod
    def _bulk_upsert_concept_themes(conn, frame: pd.DataFrame, date: str) -> None:
        if frame.empty:
            return
        now = utc_now()
        incoming = frame[["theme_id", "canonical_name", "source_theme_code"]].drop_duplicates(
            ["theme_id", "canonical_name"], keep="last").copy()
        incoming["theme_level"] = "theme"
        incoming["parent_theme_id"] = None
        incoming["status"] = "active"
        incoming["first_seen_date"] = date
        incoming["last_seen_date"] = date
        incoming["source_priority"] = "kpl_concept"
        incoming["created_at"] = now
        incoming["updated_at"] = now
        theme_cols = ["theme_id", "canonical_name", "theme_level", "parent_theme_id", "status",
                      "first_seen_date", "last_seen_date", "source_priority", "created_at", "updated_at"]
        conn.register("incoming_themes", incoming[theme_cols])
        conn.execute(
            """INSERT INTO dim_theme SELECT * FROM incoming_themes
               ON CONFLICT(theme_id) DO UPDATE SET canonical_name=excluded.canonical_name,
                 first_seen_date=LEAST(dim_theme.first_seen_date, excluded.first_seen_date),
                 last_seen_date=GREATEST(dim_theme.last_seen_date, excluded.last_seen_date),
                 source_priority='kpl_concept', updated_at=excluded.updated_at"""
        )
        conn.unregister("incoming_themes")
        alias = incoming[["theme_id", "canonical_name", "source_theme_code"]].rename(
            columns={"canonical_name": "alias"}).copy()
        alias["source"] = "kpl_concept"
        alias["valid_from"] = date
        alias["valid_to"] = None
        alias["first_seen_at"] = now
        alias["last_seen_at"] = now
        alias["confidence"] = 1.0
        alias = alias[["theme_id", "alias", "source", "source_theme_code", "valid_from", "valid_to",
                       "first_seen_at", "last_seen_at", "confidence"]]
        conn.register("incoming_aliases", alias)
        # 同一 code/name 只保留首个 valid_from；名称变化会自然产生新 alias。
        conn.execute(
            """INSERT INTO theme_alias
               SELECT a.* FROM incoming_aliases a
               WHERE NOT EXISTS (SELECT 1 FROM theme_alias t WHERE t.theme_id=a.theme_id
                 AND t.alias=a.alias AND t.source=a.source)"""
        )
        conn.execute(
            """UPDATE theme_alias SET last_seen_at=? WHERE (theme_id, alias, source) IN
               (SELECT theme_id, alias, source FROM incoming_aliases)""", [now]
        )
        conn.unregister("incoming_aliases")
        # 如果这个名称此前只从每日榜单出现过，曾临时分配 T-NAME ID；现在 KPL
        # 成分接口给出正式代码，立即把旧关系并入正式实体。
        candidates = [(name_theme_id(row.canonical_name), row.theme_id)
                      for row in incoming[["canonical_name", "theme_id"]].itertuples(index=False)]
        for old_id, new_id in candidates:
            if old_id == new_id or not conn.execute(
                    "SELECT 1 FROM dim_theme WHERE theme_id=?", [old_id]).fetchone():
                continue
            conn.execute(
                """INSERT INTO rel_limit_theme
                   SELECT event_id, ?, attribution_role, source_label, weight, confidence,
                          evidence_source, valid_at, created_at FROM rel_limit_theme old
                   WHERE theme_id=? AND NOT EXISTS (
                     SELECT 1 FROM rel_limit_theme current WHERE current.event_id=old.event_id
                     AND current.theme_id=? AND current.attribution_role=old.attribution_role)""",
                [new_id, old_id, new_id],
            )
            conn.execute("DELETE FROM rel_limit_theme WHERE theme_id=?", [old_id])
            conn.execute("UPDATE theme_alias SET theme_id=? WHERE theme_id=?", [new_id, old_id])
            conn.execute("DELETE FROM dim_theme WHERE theme_id=?", [old_id])

    def _name_map(self, conn) -> dict[str, str]:
        rows = conn.execute(
            """SELECT alias, theme_id FROM theme_alias
               QUALIFY ROW_NUMBER() OVER(PARTITION BY alias ORDER BY confidence DESC, last_seen_at DESC)=1"""
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def _replace_membership_day(self, conn, trade_date: str, df: pd.DataFrame, run_id: str) -> None:
        conn.execute("DELETE FROM fact_membership_snapshot WHERE trade_date=?", [trade_date])
        if df.empty:
            return
        source_frame = df.copy()
        source_frame["source_theme_code"] = source_frame["ts_code"].fillna("").astype(str).str.strip()
        source_frame["canonical_name"] = source_frame["name"].fillna("").astype(str).str.strip()
        source_frame = source_frame[(source_frame["source_theme_code"] != "") &
                                    (source_frame["canonical_name"] != "")].copy()
        source_frame["theme_id"] = source_frame["source_theme_code"].map(concept_theme_id)
        self._bulk_upsert_concept_themes(
            conn, source_frame[["theme_id", "canonical_name", "source_theme_code"]], trade_date)
        # 成分接口覆盖大量未涨停股票。完整快照只作为关系证据保存，不把所有
        # 成分股提升为图谱股票节点；dim_stock 由涨停/炸板事件创建。
        records = []
        for row in source_frame.to_dict("records"):
            code, name = _clean(row.get("source_theme_code")), _clean(row.get("canonical_name"))
            stock, stock_name = _clean(row.get("con_code")), _clean(row.get("con_name"))
            if not code or not name or not stock:
                continue
            theme_id = _clean(row.get("theme_id"))
            values = [trade_date, theme_id, code, stock, stock_name, _clean(row.get("desc")),
                      int(_float(row.get("hot_num")) or 0), _clean(row.get("snap_ts"))]
            row_hash = hashlib.sha256("|".join(map(str, values)).encode("utf-8")).hexdigest()
            records.append(values + [row_hash])
        if records:
            frame = pd.DataFrame(records, columns=["trade_date", "theme_id", "source_theme_code", "ts_code",
                                                       "stock_name", "description", "hot_num", "snap_ts", "row_hash"])
            conn.register("incoming_membership", frame)
            conn.execute("INSERT INTO fact_membership_snapshot SELECT * FROM incoming_membership")
            conn.unregister("incoming_membership")
        self._queue_dirty(conn, "membership_date", trade_date, trade_date, "source_changed", run_id)

    def _replace_limit_day(self, conn, trade_date: str, df: pd.DataFrame, run_id: str) -> None:
        conn.execute(
            "DELETE FROM rel_limit_theme WHERE event_id IN (SELECT event_id FROM fact_limit_event WHERE trade_date=?)",
            [trade_date],
        )
        conn.execute("DELETE FROM fact_limit_event WHERE trade_date=?", [trade_date])
        if df.empty:
            return
        name_map = self._name_map(conn)
        events, relations = [], []
        now = utc_now()
        dirty_themes: set[str] = set()
        for row in df.to_dict("records"):
            stock, name, tag = _clean(row.get("ts_code")), _clean(row.get("name")), _clean(row.get("tag"))
            if not stock or not tag:
                continue
            if tag == "涨停":
                self._upsert_stock(conn, stock, name, trade_date)
            event_id = stable_id("LE", f"{trade_date}|{stock}|{tag}", 20)
            cont, days, boards, height = parse_board_status(row.get("status"), tag)
            values = [event_id, trade_date, stock, name, tag, _clean(row.get("status")), cont, days, boards,
                      height, _clean(row.get("lu_time")), _clean(row.get("ld_time")),
                      _clean(row.get("open_time")), _clean(row.get("last_time")),
                      _clean(row.get("lu_desc")), _clean(row.get("theme")), _float(row.get("limit_order")),
                      _float(row.get("lu_limit_order")), _float(row.get("bid_amount")), _float(row.get("amount")),
                      _float(row.get("turnover_rate")), _float(row.get("free_float")),
                      _clean(row.get("snap_ts")), SOURCE_LIST]
            row_hash = hashlib.sha256("|".join("" if v is None else str(v) for v in values).encode("utf-8")).hexdigest()
            events.append(values + [row_hash])

            primary = _clean(row.get("lu_desc")) if tag == "涨停" else ""
            labels = split_theme_labels(row.get("theme"))
            if primary:
                theme_id = name_map.get(primary) or name_theme_id(primary)
                if primary not in name_map:
                    self._upsert_theme(conn, theme_id, primary, trade_date, "event_branch", "kpl_daily")
                    name_map[primary] = theme_id
                relations.append([event_id, theme_id, "primary", primary, 1.0, 1.0,
                                  "kpl_list.lu_desc", trade_date, now])
                dirty_themes.add(theme_id)
            for label in labels:
                theme_id = name_map.get(label) or name_theme_id(label)
                if label not in name_map:
                    self._upsert_theme(conn, theme_id, label, trade_date, "theme", "kpl_daily")
                    name_map[label] = theme_id
                if primary and theme_id == (name_map.get(primary)):
                    continue
                relations.append([event_id, theme_id, "auxiliary", label, 0.35, 0.75,
                                  "kpl_list.theme", trade_date, now])
                dirty_themes.add(theme_id)
        if events:
            cols = ["event_id", "trade_date", "ts_code", "name", "tag", "status_raw", "continuous_board",
                    "period_days", "period_boards", "board_height", "lu_time", "ld_time", "open_time",
                    "last_time", "lu_desc", "theme_raw", "limit_order", "lu_limit_order", "bid_amount",
                    "amount", "turnover_rate", "free_float", "snap_ts", "source", "row_hash"]
            frame = pd.DataFrame(events, columns=cols)
            conn.register("incoming_events", frame)
            conn.execute("INSERT INTO fact_limit_event SELECT * FROM incoming_events")
            conn.unregister("incoming_events")
        if relations:
            frame = pd.DataFrame(relations, columns=["event_id", "theme_id", "attribution_role", "source_label",
                                                       "weight", "confidence", "evidence_source", "valid_at", "created_at"])
            frame = frame.drop_duplicates(["event_id", "theme_id", "attribution_role"])
            conn.register("incoming_relations", frame)
            conn.execute("INSERT INTO rel_limit_theme SELECT * FROM incoming_relations")
            conn.unregister("incoming_relations")
        for theme_id in dirty_themes:
            self._queue_dirty(conn, "theme", theme_id, trade_date, "limit_event_changed", run_id)

    @staticmethod
    def _queue_dirty(conn, entity_type: str, entity_id: str, dirty_from: str, reason: str, run_id: str) -> None:
        conn.execute(
            """INSERT OR REPLACE INTO dirty_entity_queue VALUES (?, ?, ?, ?, ?, 'pending', ?, NULL)""",
            [entity_type, entity_id, dirty_from, reason, run_id, utc_now()],
        )

    @staticmethod
    def _rebuild_membership_intervals(conn) -> None:
        conn.execute("DELETE FROM rel_theme_stock_membership")
        conn.execute(
            """
            INSERT INTO rel_theme_stock_membership
            WITH snapshot_dates AS (
                SELECT trade_date, DENSE_RANK() OVER(ORDER BY trade_date) AS date_idx
                FROM (SELECT DISTINCT trade_date FROM fact_membership_snapshot)
            ), ordered AS (
                SELECT s.*, d.date_idx,
                       LAG(d.date_idx) OVER(PARTITION BY theme_id, ts_code ORDER BY s.trade_date) AS prev_idx
                FROM fact_membership_snapshot s JOIN snapshot_dates d USING(trade_date)
            ), marked AS (
                SELECT *, CASE WHEN prev_idx IS NULL OR date_idx<>prev_idx+1 THEN 1 ELSE 0 END AS is_new
                FROM ordered
            ), grouped AS (
                SELECT *, SUM(is_new) OVER(PARTITION BY theme_id, ts_code ORDER BY trade_date) AS grp
                FROM marked
            ), bounds AS (SELECT MAX(date_idx) AS max_idx FROM snapshot_dates)
            SELECT theme_id, ts_code, MIN(trade_date),
                   CASE WHEN MAX(date_idx)=(SELECT max_idx FROM bounds) THEN NULL ELSE MAX(trade_date) END,
                   arg_max(description, trade_date), arg_min(hot_num, trade_date), arg_max(hot_num, trade_date),
                   'kpl_concept_cons', 1.0
            FROM grouped GROUP BY theme_id, ts_code, grp
            """
        )

    def _rebuild_daily_and_episodes(self, conn, run_id: str) -> tuple[int, int]:
        conn.execute(
            """UPDATE dim_theme SET first_seen_date=x.first_seen,
                      last_seen_date=x.last_seen, updated_at=?
               FROM (SELECT r.theme_id,MIN(r.valid_at) AS first_seen,MAX(r.valid_at) AS last_seen
                     FROM rel_limit_theme r GROUP BY r.theme_id) x
               WHERE dim_theme.theme_id=x.theme_id""",
            [utc_now()],
        )
        frame = conn.execute(
            """SELECT e.trade_date, r.theme_id, e.tag, e.board_height, e.lu_time, r.attribution_role
               FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
               WHERE (e.tag='涨停' AND r.attribution_role='primary')
                  OR (e.tag='炸板' AND r.attribution_role='auxiliary')
               ORDER BY e.trade_date, r.theme_id"""
        ).df()
        conn.execute("DELETE FROM fact_theme_daily")
        conn.execute("DELETE FROM fact_theme_episode")
        if frame.empty:
            return 0, 0
        market_dates = sorted(frame["trade_date"].unique().tolist())
        date_index = {date: idx for idx, date in enumerate(market_dates)}
        active_history: dict[str, list[int]] = {}
        score_history: dict[str, list[float]] = {}
        peak_history: dict[str, float] = {}
        episode_no: dict[str, int] = {}
        current_episode: dict[str, str] = {}
        daily_records: list[list[Any]] = []

        grouped = {(date, theme): part for (date, theme), part in frame.groupby(["trade_date", "theme_id"], sort=True)}
        for (trade_date, theme_id), part in grouped.items():
            idx = date_index[trade_date]
            limits = part[part["tag"] == "涨停"]
            breaks = part[part["tag"] == "炸板"]
            limit_count, break_count = len(limits), len(breaks)
            heights = [int(x) for x in limits["board_height"].dropna().tolist()]
            max_height = max(heights) if heights else None
            first_count = sum(1 for h in heights if h <= 1)
            multi_count = sum(1 for h in heights if h >= 2)
            ladder = sum(board_points(h) for h in heights)
            seal_rate = limit_count / (limit_count + break_count) if limit_count + break_count else None
            early = 0
            if limit_count:
                early = sum(1 for value in limits["lu_time"].tolist() if _clean(value) and _clean(value) <= "10:00:00") / limit_count
            previous = active_history.setdefault(theme_id, [])
            recent_active = sum(1 for old_idx in previous if idx - 4 <= old_idx < idx)
            persistence = (recent_active + 1) / 5.0
            since_last = idx - previous[-1] if previous else None
            score = min(25.0, limit_count * 2.5) + min(35.0, ladder) + min(15.0, (max_height or 0) * 2.5)
            score += 10.0 * (seal_rate or 0.0) + 10.0 * persistence + 5.0 * early
            score = round(score, 4)
            old_scores = score_history.setdefault(theme_id, [])
            trailing = (old_scores[-119:] + [score])
            percentile = sum(1 for item in trailing if item <= score) / len(trailing)

            starts_new = not previous or idx - previous[-1] > 2
            if starts_new:
                episode_no[theme_id] = episode_no.get(theme_id, 0) + 1
                current_episode[theme_id] = f"EP-{theme_id}-{trade_date}-{episode_no[theme_id]:02d}"
                state = "启动" if limit_count >= 3 or (max_height or 0) >= 2 else "试探"
                peak_history[theme_id] = score
            else:
                old_peak = peak_history.get(theme_id, score)
                prior = old_scores[-1] if old_scores else score
                if break_count >= 2 and (seal_rate or 0) < 0.75:
                    state = "分歧"
                elif score <= old_peak * 0.55 and limit_count <= 2:
                    state = "退潮"
                elif percentile >= 0.9 and limit_count >= 6 and (seal_rate or 0) >= 0.8:
                    state = "高潮"
                elif (max_height or 0) >= 3 and score >= prior * 1.08:
                    state = "加速"
                elif score >= prior * 1.08:
                    state = "发酵"
                elif score < prior * 0.8:
                    state = "分歧"
                else:
                    state = "延续"
                peak_history[theme_id] = max(old_peak, score)
            episode_id = current_episode[theme_id]
            daily_records.append([trade_date, theme_id, limit_count, break_count, max_height, first_count,
                                  multi_count, ladder, seal_rate, early, persistence, since_last, score,
                                  round(percentile, 4), state, episode_id, run_id])
            previous.append(idx)
            old_scores.append(score)

        daily = pd.DataFrame(daily_records, columns=["trade_date", "theme_id", "limit_up_count", "break_count",
            "max_board_height", "first_board_count", "multi_board_count", "ladder_points", "seal_rate",
            "early_limit_share", "persistence_5", "days_since_last_active", "heat_score",
            "heat_percentile_120", "lifecycle_state", "episode_id", "run_id"])
        conn.register("daily_metrics", daily)
        conn.execute("INSERT INTO fact_theme_daily SELECT * FROM daily_metrics")
        conn.unregister("daily_metrics")
        max_date = daily["trade_date"].max()
        episodes = []
        for (episode_id, theme_id), part in daily.groupby(["episode_id", "theme_id"], sort=True):
            peak = part.sort_values(["heat_score", "trade_date"], ascending=[False, True]).iloc[0]
            last_date = part["trade_date"].max()
            is_active = date_index[max_date] - date_index[last_date] <= 2
            episodes.append([episode_id, theme_id, part["trade_date"].min(), last_date,
                             None if is_active else last_date, "active" if is_active else "closed",
                             peak["trade_date"], float(peak["heat_score"]), int(part["limit_up_count"].max()),
                             len(part), "active-gap<=1-market-session", max_date, GRAPH_SCHEMA_VERSION])
        ep = pd.DataFrame(episodes, columns=["episode_id", "theme_id", "start_date", "last_active_date", "end_date",
                                                   "status", "peak_date", "peak_heat", "peak_breadth", "active_sessions",
                                                   "segmentation_method", "computed_as_of", "graph_version"])
        conn.register("episode_metrics", ep)
        conn.execute("INSERT INTO fact_theme_episode SELECT * FROM episode_metrics")
        conn.unregister("episode_metrics")
        conn.execute("UPDATE dirty_entity_queue SET status='processed', processed_at=? WHERE status='pending'", [utc_now()])
        return len(daily), len(ep)

    def query_theme_daily(self, theme_id: str | None = None, start_date: str | None = None,
                          end_date: str | None = None) -> pd.DataFrame:
        clauses, params = [], []
        if theme_id:
            clauses.append("d.theme_id=?")
            params.append(theme_id)
        if start_date:
            clauses.append("d.trade_date>=?")
            params.append(compact_date(start_date))
        if end_date:
            clauses.append("d.trade_date<=?")
            params.append(compact_date(end_date))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        conn = self.connect(read_only=True)
        try:
            return conn.execute(
                """SELECT d.*,t.canonical_name,COALESCE(x.level1_name,'待归类') AS level1_name
                   FROM fact_theme_daily d JOIN dim_theme t USING(theme_id)
                   LEFT JOIN dim_theme_taxonomy x USING(theme_id)"""
                + where + " ORDER BY d.trade_date, d.heat_score DESC", params).df()
        finally:
            conn.close()

    def query_market_theme_review_series(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        top_n: int = 10,
        leader_count: int = 3,
    ) -> list[dict[str, Any]]:
        """批量返回历史可见的每日题材情绪、结构、排名和龙头梯队。"""
        conn = self.connect(read_only=True)
        try:
            cutoff = compact_date(end_date) if end_date else conn.execute(
                "SELECT MAX(trade_date) FROM fact_theme_daily"
            ).fetchone()[0]
            if not cutoff:
                return []
            daily = conn.execute(
                """SELECT d.*,t.canonical_name,
                          COALESCE(x.level1_name,'待归类') AS level1_name
                   FROM fact_theme_daily d
                   JOIN dim_theme t USING(theme_id)
                   LEFT JOIN dim_theme_taxonomy x USING(theme_id)
                   WHERE d.trade_date<=? AND EXISTS (
                     SELECT 1 FROM rel_limit_theme r
                     WHERE r.theme_id=d.theme_id AND r.attribution_role='primary')
                   ORDER BY d.trade_date,d.heat_score DESC,t.canonical_name,d.theme_id""",
                [cutoff],
            ).df()
            events = conn.execute(
                """SELECT e.trade_date,e.ts_code,e.name,e.tag,e.status_raw,e.board_height,
                          e.lu_time,e.limit_order,e.lu_limit_order,e.bid_amount,e.amount,e.free_float,
                          r.theme_id,r.attribution_role
                   FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
                   WHERE e.trade_date<=? AND e.tag='涨停' AND r.attribution_role='primary'
                   ORDER BY e.trade_date,e.ts_code,r.theme_id""",
                [cutoff],
            ).df()
            breadth = conn.execute(
                """SELECT e.trade_date,
                          COUNT(DISTINCT CASE WHEN e.tag='涨停' THEN e.event_id END) AS market_limit_up_count,
                          COUNT(DISTINCT CASE WHEN e.tag='炸板' THEN e.event_id END) AS market_break_count,
                          MAX(CASE WHEN e.tag='涨停' THEN e.board_height END) AS market_max_board_height
                   FROM fact_limit_event e
                   JOIN rel_limit_theme r USING(event_id)
                   JOIN dim_theme t USING(theme_id)
                   LEFT JOIN dim_theme_taxonomy x USING(theme_id)
                   WHERE e.trade_date<=?
                     AND ((e.tag='涨停' AND r.attribution_role='primary') OR e.tag='炸板')
                     AND t.canonical_name NOT IN ('ST板块','ST摘帽','次新股')
                     AND COALESCE(x.level1_name,'待归类')<>'ST与次新'
                   GROUP BY e.trade_date ORDER BY e.trade_date""",
                [cutoff],
            ).df()
        finally:
            conn.close()
        reviews = build_market_theme_reviews(
            daily,
            events,
            top_n=max(1, int(top_n)),
            leader_count=max(1, min(3, int(leader_count))),
        )
        breadth_by_date = {
            str(row.trade_date): row for row in breadth.itertuples(index=False)
        }
        lower = compact_date(start_date) if start_date else None
        result: list[dict[str, Any]] = []
        for trade_date in sorted(reviews):
            if lower and trade_date < lower:
                continue
            item = reviews[trade_date]
            market = breadth_by_date.get(trade_date)
            if market is not None:
                limit_count = int(market.market_limit_up_count or 0)
                break_count = int(market.market_break_count or 0)
                item["market_breadth"] = {
                    "limit_up_count": limit_count,
                    "break_count": break_count,
                    "seal_rate": round(limit_count / (limit_count + break_count), 4)
                    if limit_count + break_count else None,
                    "max_board_height": int(market.market_max_board_height or 0),
                }
            item["available"] = True
            item["source"] = "db_theme_graph.duckdb / KPL primary limit-up attribution"
            result.append(item)
        return result

    def query_market_theme_review(self, trade_date: str | None = None, top_n: int = 10,
                                  leader_count: int = 3) -> dict[str, Any]:
        """返回指定交易日的题材情绪、热门题材、市场结构与龙头梯队。"""
        if trade_date:
            result = self.query_market_theme_review_series(
                start_date=trade_date,
                end_date=trade_date,
                top_n=top_n,
                leader_count=leader_count,
            )
        else:
            result = self.query_market_theme_review_series(
                top_n=top_n,
                leader_count=leader_count,
            )
        return result[-1] if result else {}

    def query_theme_cycle_data(
        self,
        theme_id: str | None = None,
        name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        as_of: str | None = None,
    ) -> dict[str, Any]:
        """返回单题材周期图所需的日度、周期、个股事件和催化分析。

        ``as_of`` 存在时按历史可见模式截断行情，并过滤当时尚未生成的模型
        分析；否则为完整研究模式，可显示事后确认的周期边界和归因。
        """
        conn = self.connect(read_only=True)
        try:
            if not theme_id:
                if not name:
                    raise ValueError("theme_id 和 name 至少传一个")
                row = conn.execute(
                    """SELECT theme_id FROM theme_alias WHERE alias=?
                       ORDER BY confidence DESC,last_seen_at DESC LIMIT 1""",
                    [name],
                ).fetchone()
                if not row:
                    return {}
                theme_id = str(row[0])
            theme = conn.execute(
                """SELECT t.theme_id,t.canonical_name,t.first_seen_date,t.last_seen_date,
                          COALESCE(x.level1_name,'待归类') AS level1_name
                   FROM dim_theme t LEFT JOIN dim_theme_taxonomy x USING(theme_id)
                   WHERE t.theme_id=?""",
                [theme_id],
            ).df()
            if theme.empty:
                return {}
            latest = conn.execute(
                "SELECT MAX(trade_date) FROM source_day_manifest WHERE source_name=?",
                [SOURCE_LIST],
            ).fetchone()[0]
            if not latest:
                return {}
            cutoff = min(
                value for value in [
                    compact_date(end_date) if end_date else str(latest),
                    compact_date(as_of) if as_of else str(latest),
                    str(latest),
                ] if value
            )
            lower = compact_date(start_date) if start_date else conn.execute(
                "SELECT MIN(trade_date) FROM source_day_manifest WHERE source_name=? AND trade_date<=?",
                [SOURCE_LIST, cutoff],
            ).fetchone()[0]
            if not lower or lower > cutoff:
                raise ValueError("开始日期不能晚于截止日期")
            market_dates_all = [
                str(row[0]) for row in conn.execute(
                    """SELECT trade_date FROM source_day_manifest
                       WHERE source_name=? AND trade_date<=? ORDER BY trade_date""",
                    [SOURCE_LIST, cutoff],
                ).fetchall()
            ]
            market_dates = [value for value in market_dates_all if value >= lower]
            daily_history = conn.execute(
                """SELECT * FROM fact_theme_daily
                   WHERE theme_id=? AND trade_date<=? ORDER BY trade_date""",
                [theme_id, cutoff],
            ).df()
            ranked = conn.execute(
                """WITH eligible AS (
                     SELECT d.trade_date,d.theme_id,d.heat_score,d.limit_up_count,t.canonical_name,
                            ROW_NUMBER() OVER (
                              PARTITION BY d.trade_date
                              ORDER BY d.heat_score DESC,d.limit_up_count DESC,t.canonical_name,d.theme_id
                            ) AS market_rank,
                            COUNT(*) OVER (PARTITION BY d.trade_date) AS market_theme_count,
                            SUM(d.limit_up_count) OVER (PARTITION BY d.trade_date) AS market_limit_up_count
                     FROM fact_theme_daily d JOIN dim_theme t USING(theme_id)
                     LEFT JOIN dim_theme_taxonomy x USING(theme_id)
                     WHERE d.trade_date<=? AND d.limit_up_count>0
                       AND t.canonical_name NOT IN ('ST板块','ST摘帽','次新股')
                       AND COALESCE(x.level1_name,'待归类')<>'ST与次新'
                       AND EXISTS (SELECT 1 FROM rel_limit_theme r
                                   WHERE r.theme_id=d.theme_id AND r.attribution_role='primary')
                   )
                   SELECT trade_date,market_rank,market_theme_count,market_limit_up_count,
                          limit_up_count/NULLIF(market_limit_up_count,0) AS limit_up_share
                   FROM eligible WHERE theme_id=? ORDER BY trade_date""",
                [cutoff, theme_id],
            ).df()
            calendar = pd.DataFrame({"trade_date": market_dates})
            selected_daily = daily_history[
                daily_history["trade_date"].astype(str).between(lower, cutoff)
            ].copy()
            daily = calendar.merge(selected_daily, on="trade_date", how="left").merge(
                ranked, on="trade_date", how="left"
            )
            daily["data_status"] = daily["theme_id"].map(
                lambda value: "active" if pd.notna(value) else "complete_zero_activity"
            )
            zero_columns = [
                "limit_up_count", "break_count", "max_board_height", "first_board_count",
                "multi_board_count", "ladder_points", "early_limit_share", "persistence_5",
                "heat_score", "heat_percentile_120", "limit_up_share",
            ]
            for column in zero_columns:
                daily[column] = pd.to_numeric(daily[column], errors="coerce").fillna(0)
            daily["lifecycle_state"] = daily["lifecycle_state"].fillna("沉寂")
            daily["theme_id"] = daily["theme_id"].fillna(theme_id)
            events = conn.execute(
                """SELECT * EXCLUDE(rn) FROM (
                     SELECT e.event_id,e.trade_date,e.ts_code,e.name,e.tag,e.status_raw,
                            e.board_height,e.lu_time,e.limit_order,e.lu_limit_order,e.bid_amount,
                            e.amount,e.free_float,r.attribution_role,r.source_label,
                            ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY r.attribution_role) AS rn
                     FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
                     WHERE r.theme_id=? AND e.trade_date BETWEEN ? AND ?
                       AND ((e.tag='涨停' AND r.attribution_role='primary') OR e.tag='炸板')
                   ) WHERE rn=1 ORDER BY trade_date,tag,board_height DESC NULLS LAST,ts_code""",
                [theme_id, lower, cutoff],
            ).df()
            all_limits = conn.execute(
                """SELECT trade_date,ts_code FROM fact_limit_event
                   WHERE tag='涨停' AND trade_date<=? ORDER BY trade_date,ts_code""",
                [cutoff],
            ).df()
            date_index = {date: index for index, date in enumerate(market_dates_all)}
            stock_date_indices: dict[str, list[int]] = {}
            for code, part in all_limits.groupby("ts_code"):
                stock_date_indices[str(code)] = sorted({
                    date_index[str(value)] for value in part["trade_date"]
                    if str(value) in date_index
                })
            leader_records: list[dict[str, Any]] = []
            if not events.empty:
                for trade_date, part in events[events["tag"] == "涨停"].groupby("trade_date"):
                    for leader in _leader_rows(
                        part.assign(attribution_role="primary"),
                        stock_date_indices,
                        date_index,
                        3,
                    ):
                        leader_records.append({"trade_date": str(trade_date), **leader})
            leaders = pd.DataFrame(leader_records)

            if as_of:
                episode_records = []
                for episode_id, part in daily_history.groupby("episode_id", dropna=True, sort=True):
                    part = part.sort_values("trade_date")
                    last_active = str(part.iloc[-1]["trade_date"])
                    if last_active < lower:
                        continue
                    peak = part.sort_values(
                        ["heat_score", "trade_date"], ascending=[False, True]
                    ).iloc[0]
                    gap = date_index.get(cutoff, len(market_dates_all) - 1) - date_index.get(last_active, 0)
                    episode_records.append({
                        "episode_id": episode_id,
                        "theme_id": theme_id,
                        "start_date": str(part.iloc[0]["trade_date"]),
                        "last_active_date": last_active,
                        "end_date": last_active if gap > 2 else None,
                        "status": "closed_as_of" if gap > 2 else "active_as_of",
                        "peak_date": str(peak["trade_date"]),
                        "peak_heat": float(peak["heat_score"]),
                        "peak_breadth": int(part["limit_up_count"].max()),
                        "active_sessions": int(len(part)),
                        "computed_as_of": cutoff,
                    })
                episodes = pd.DataFrame(episode_records)
            else:
                episodes = conn.execute(
                    """SELECT * FROM fact_theme_episode
                       WHERE theme_id=? AND start_date<=? AND last_active_date>=?
                       ORDER BY start_date""",
                    [theme_id, cutoff, lower],
                ).df()
        finally:
            conn.close()

        analyses = self.query_theme_analyses(theme_id=theme_id, latest=True)
        if not analyses.empty:
            analyses = analyses[analyses["entity_id"].isin(set(episodes.get("episode_id", [])))].copy()
            if as_of:
                analyses = analyses[
                    pd.to_datetime(analyses["created_at"], errors="coerce")
                    <= pd.Timestamp(cutoff) + pd.Timedelta(days=1)
                ].copy()
        market_reviews = self.query_market_theme_review_series(
            start_date=lower,
            end_date=cutoff,
            top_n=3,
            leader_count=1,
        )
        return {
            "profile": theme.iloc[0].to_dict(),
            "start_date": lower,
            "end_date": cutoff,
            "as_of": compact_date(as_of) if as_of else None,
            "mode": "as_of" if as_of else "retrospective",
            "daily": daily,
            "episodes": episodes,
            "events": events,
            "leaders": leaders,
            "analyses": analyses,
            "market_reviews": market_reviews,
        }

    def query_theme_members(self, theme_id: str, as_of: str | None = None,
                            historical: bool = False) -> pd.DataFrame:
        """返回题材实际涨停股，而不是 KPL 全量概念成分池。

        ``historical=False`` 按股票聚合截至时点的主归因涨停记录；``True``
        返回逐次涨停事件。两种模式都不会把未涨停的普通概念成分带入结果。
        """
        conn = self.connect(read_only=True)
        try:
            cutoff = compact_date(as_of) if as_of else conn.execute(
                "SELECT MAX(trade_date) FROM fact_limit_event").fetchone()[0]
            if not cutoff:
                return pd.DataFrame()
            if historical:
                return conn.execute(
                    """SELECT e.trade_date,e.ts_code,e.name,e.status_raw,e.board_height,e.lu_time,
                              r.source_label,r.confidence
                       FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
                       WHERE r.theme_id=? AND r.attribution_role='primary' AND e.tag='涨停'
                         AND e.trade_date<=? ORDER BY e.trade_date,e.board_height DESC,e.ts_code""",
                    [theme_id, cutoff]).df()
            return conn.execute(
                """SELECT r.theme_id,e.ts_code,arg_max(e.name,e.trade_date) AS name,
                          MIN(e.trade_date) AS first_limit_date,MAX(e.trade_date) AS last_limit_date,
                          COUNT(DISTINCT e.trade_date) AS limit_days,MAX(e.board_height) AS max_board_height,
                          arg_max(r.source_label,e.trade_date) AS latest_source_label
                   FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
                   WHERE r.theme_id=? AND r.attribution_role='primary' AND e.tag='涨停'
                     AND e.trade_date<=? GROUP BY r.theme_id,e.ts_code
                   ORDER BY limit_days DESC,max_board_height DESC NULLS LAST,last_limit_date DESC,e.ts_code""",
                [theme_id, cutoff]).df()
        finally:
            conn.close()

    def query_theme_profile(self, theme_id: str | None = None, name: str | None = None,
                            as_of: str | None = None) -> dict[str, Any]:
        conn = self.connect(read_only=True)
        try:
            if not theme_id:
                if not name:
                    raise ValueError("theme_id 和 name 至少传一个")
                row = conn.execute(
                    """SELECT theme_id FROM theme_alias WHERE alias=?
                       ORDER BY confidence DESC, last_seen_at DESC LIMIT 1""", [name]).fetchone()
                if not row:
                    return {}
                theme_id = row[0]
            theme = conn.execute("SELECT * FROM dim_theme WHERE theme_id=?", [theme_id]).df()
            if theme.empty:
                return {}
            cutoff = compact_date(as_of) if as_of else "99999999"
            latest = conn.execute(
                """SELECT * FROM fact_theme_daily WHERE theme_id=? AND trade_date<=?
                   ORDER BY trade_date DESC LIMIT 1""", [theme_id, cutoff]).df()
            if as_of:
                # 不能把完整 episode 的事后结束日泄露给历史时点查询；按截至日
                # 已可见的 daily 记录重新形成截断视图。
                episodes = conn.execute(
                    """SELECT episode_id,theme_id,MIN(trade_date) AS start_date,
                              MAX(trade_date) AS last_active_date,NULL::VARCHAR AS end_date,
                              'as_of_snapshot' AS status,arg_max(trade_date,heat_score) AS peak_date,
                              MAX(heat_score) AS peak_heat,MAX(limit_up_count) AS peak_breadth,
                              COUNT(*) AS active_sessions,'as-of-truncated' AS segmentation_method,
                              ? AS computed_as_of,? AS graph_version
                       FROM fact_theme_daily WHERE theme_id=? AND trade_date<=?
                       GROUP BY episode_id,theme_id ORDER BY start_date DESC""",
                    [cutoff, GRAPH_SCHEMA_VERSION, theme_id, cutoff]).df()
            else:
                episodes = conn.execute(
                    """SELECT * FROM fact_theme_episode WHERE theme_id=?
                       ORDER BY start_date DESC""", [theme_id]).df()
            result = theme.iloc[0].to_dict()
            result["latest_daily"] = latest.iloc[0].to_dict() if not latest.empty else None
            result["episodes"] = episodes.to_dict("records")
            return result
        finally:
            conn.close()

    def query_stock_theme_history(self, ts_code: str) -> pd.DataFrame:
        conn = self.connect(read_only=True)
        try:
            return conn.execute(
                """SELECT e.trade_date, e.ts_code, e.name, e.tag, e.status_raw, e.board_height,
                          r.theme_id, t.canonical_name, r.attribution_role, r.source_label
                   FROM fact_limit_event e JOIN rel_limit_theme r USING(event_id)
                   JOIN dim_theme t USING(theme_id) WHERE e.ts_code=?
                   ORDER BY e.trade_date, r.attribution_role""", [ts_code.upper()]).df()
        finally:
            conn.close()

    def query_theme_analyses(self, theme_id: str | None = None, name: str | None = None,
                             episode_id: str | None = None, latest: bool = True) -> pd.DataFrame:
        """读取已沉淀的周期催化分析、证据和模型版本。

        默认每个周期、每类分析只返回最新版本。``output_json`` 与
        ``evidence_json`` 原文保留，同时增加已解析的 ``analysis`` 和
        ``evidence`` 字典列，供后续研究直接复用而无需重新检索新闻。
        """
        conn = self.connect(read_only=True)
        try:
            if not theme_id and name:
                row = conn.execute(
                    """SELECT theme_id FROM theme_alias WHERE alias=?
                       ORDER BY confidence DESC,last_seen_at DESC LIMIT 1""", [name]).fetchone()
                if not row:
                    return pd.DataFrame()
                theme_id = row[0]
            clauses = ["a.entity_type='theme_episode'",
                       "a.analysis_type IN ('catalyst_screening','catalyst_extraction',"
                       "'catalyst_audit','episode_summary')"]
            params: list[Any] = []
            if theme_id:
                clauses.append("e.theme_id=?")
                params.append(theme_id)
            if episode_id:
                clauses.append("a.entity_id=?")
                params.append(episode_id)
            ranked_filter = " AND ".join(clauses)
            latest_filter = "WHERE rn=1" if latest else ""
            result = conn.execute(
                f"""WITH ranked AS (
                       SELECT a.*,e.theme_id,t.canonical_name,e.start_date,e.last_active_date,
                              e.peak_date,
                              ROW_NUMBER() OVER (
                                PARTITION BY a.entity_id,a.analysis_type
                                ORDER BY a.created_at DESC,a.analysis_id DESC
                              ) AS rn
                       FROM llm_analysis a
                       JOIN fact_theme_episode e ON e.episode_id=a.entity_id
                       JOIN dim_theme t ON t.theme_id=e.theme_id
                       WHERE {ranked_filter}
                     )
                     SELECT * EXCLUDE(rn) FROM ranked {latest_filter}
                     ORDER BY start_date DESC,analysis_type,created_at DESC""", params).df()
            if result.empty:
                return result
            result["analysis"] = result["output_json"].map(
                lambda value: json.loads(value) if value else {}
            )
            result["evidence"] = result["evidence_json"].map(
                lambda value: json.loads(value) if value else {}
            )
            return result
        finally:
            conn.close()

    def query_theme_taxonomy(self, level1_name: str | None = None) -> pd.DataFrame:
        conn = self.connect(read_only=True)
        try:
            params: list[Any] = []
            where = ""
            if level1_name:
                where = " WHERE x.level1_name=?"
                params.append(level1_name)
            return conn.execute(
                """SELECT x.*,t.first_seen_date,t.last_seen_date
                   FROM dim_theme_taxonomy x JOIN dim_theme t USING(theme_id)"""
                + where + " ORDER BY x.level1_name,x.level2_name", params).df()
        finally:
            conn.close()

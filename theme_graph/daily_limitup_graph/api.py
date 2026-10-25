"""Read-only query interface for the topic/limit-up graph database."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def connect(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def get_daily_themes(db_path: str | Path, trade_date: str) -> list[dict]:
    """Return the topic-day edge summary and its stock attribution edges."""
    with connect(db_path) as conn:
        rows = conn.execute(
            """SELECT t.topic_id, t.canonical_name AS topic_name,
                      d.trade_date, s.catalyst_reason, s.strength_assessment,
                      s.strength_score, s.ladder_json, s.limitup_breadth,
                      s.highest_consecutive_board, s.capital_flow_overview,
                      s.capital_flow_status, td.topic_date_id
               FROM topic_date_nodes td
               JOIN topic_nodes t ON t.topic_id = td.topic_id
               JOIN date_nodes d ON d.trade_date = td.trade_date
               JOIN topic_date_summary_edges s ON s.topic_date_id = td.topic_date_id
               WHERE d.trade_date = ?
               ORDER BY s.limitup_breadth DESC, t.canonical_name""",
            (trade_date,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["ladder"] = json.loads(item.pop("ladder_json"))
            stocks = conn.execute(
                """SELECT e.stock_code, n.stock_name, e.limitup_status,
                          e.seal_time, e.consecutive_board, e.limitup_reason,
                          e.stock_theme_bridge, e.realization_or_counterevidence,
                          e.confidence_raw, e.source_refs_json
                   FROM topic_date_stock_edges e
                   JOIN stock_nodes n ON n.stock_code = e.stock_code
                   WHERE e.topic_date_id = ? ORDER BY e.row_order""",
                (item["topic_date_id"],),
            ).fetchall()
            item["stocks"] = [dict(stock) for stock in stocks]
            for stock in item["stocks"]:
                stock["source_refs"] = json.loads(stock.pop("source_refs_json"))
            result.append(item)
        return result


def get_stock_history(db_path: str | Path, stock_code: str) -> list[dict]:
    """Return all dated topic attributions for a stock, oldest first."""
    with connect(db_path) as conn:
        rows = conn.execute(
            """SELECT d.trade_date, t.canonical_name AS topic_name,
                      e.limitup_status, e.seal_time, e.consecutive_board,
                      e.limitup_reason, e.stock_theme_bridge,
                      e.realization_or_counterevidence, e.confidence_raw,
                      e.source_refs_json
               FROM topic_date_stock_edges e
               JOIN topic_date_nodes td ON td.topic_date_id = e.topic_date_id
               JOIN topic_nodes t ON t.topic_id = td.topic_id
               JOIN date_nodes d ON d.trade_date = td.trade_date
               WHERE e.stock_code = ? ORDER BY d.trade_date, t.canonical_name""",
            (stock_code,),
        ).fetchall()
        result = [dict(row) for row in rows]
        for row in result:
            row["source_refs"] = json.loads(row.pop("source_refs_json"))
        return result


def get_topic_history(db_path: str | Path, topic_name: str) -> list[dict]:
    """Return a topic's daily summaries and attached stock edges."""
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT topic_id FROM topic_nodes WHERE canonical_name = ?", (topic_name,)
        ).fetchone()
    if row is None:
        return []
    with connect(db_path) as conn:
        dates = conn.execute(
            "SELECT trade_date FROM topic_date_nodes WHERE topic_id = ? ORDER BY trade_date",
            (row["topic_id"],),
        ).fetchall()
    return [item for date_row in dates for item in get_daily_themes(db_path, date_row["trade_date"])
            if item["topic_id"] == row["topic_id"]]

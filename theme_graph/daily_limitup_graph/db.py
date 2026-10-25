"""Database setup and operational integrity checks."""
from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path


SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def initialize_database(db_path: str | Path) -> None:
    path = Path(db_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(str(path))) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))


def integrity_report(db_path: str | Path) -> dict:
    path = Path(db_path).expanduser()
    with closing(sqlite3.connect(str(path))) as conn:
        conn.row_factory = sqlite3.Row
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_errors = [dict(row) for row in conn.execute("PRAGMA foreign_key_check")]
        counts = {}
        for table in (
            "topic_nodes", "date_nodes", "topic_date_nodes",
            "topic_date_summary_edges", "stock_nodes",
            "topic_date_stock_edges", "source_documents", "import_runs",
        ):
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        imported_dates = [row[0] for row in conn.execute(
            "SELECT trade_date FROM date_nodes ORDER BY trade_date"
        )]
    return {
        "database": str(path.resolve()),
        "integrity": integrity,
        "foreign_key_errors": foreign_key_errors,
        "counts": counts,
        "trade_dates": imported_dates,
    }

"""CLI for creating, importing, querying and checking the limit-up graph DB."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from coreClient._paths import database_root

from .api import get_daily_themes, get_stock_history, get_topic_history
from .db import initialize_database, integrity_report
from .import_report import import_report


DEFAULT_DB = database_root() / "theme_graph/topic_limitup_graph.sqlite3"


def _db_argument(value: str | None) -> Path:
    return Path(value or os.environ.get("TRADING_LIMITUP_GRAPH_DB", str(DEFAULT_DB))).expanduser()


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily theme and limit-up attribution graph database")
    parser.add_argument("--db", help="SQLite path (or set TRADING_LIMITUP_GRAPH_DB)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create database schema")
    imp = sub.add_parser("import-report", help="import a daily Markdown analysis report")
    imp.add_argument("report")
    imp.add_argument("--source-uri", help="original report URI/path for provenance")

    themes = sub.add_parser("daily-themes", help="show all topics and stock attributions for a date")
    themes.add_argument("trade_date", help="YYYYMMDD")
    stock = sub.add_parser("stock-history", help="show a stock's topic attribution history")
    stock.add_argument("stock_code", help="e.g. 002343.SZ")
    topic = sub.add_parser("topic-history", help="show one exact topic name over time")
    topic.add_argument("topic_name")
    sub.add_parser("integrity", help="run SQLite integrity and foreign-key checks")

    args = parser.parse_args()
    db_path = _db_argument(args.db)
    if args.command == "init":
        initialize_database(db_path)
        result = {"initialized": str(db_path.resolve())}
    elif args.command == "import-report":
        result = import_report(args.report, db_path, source_uri=args.source_uri)
    elif args.command == "daily-themes":
        result = get_daily_themes(db_path, args.trade_date)
    elif args.command == "stock-history":
        result = get_stock_history(db_path, args.stock_code)
    elif args.command == "topic-history":
        result = get_topic_history(db_path, args.topic_name)
    else:
        result = integrity_report(db_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.command == "integrity" and (result["integrity"] != "ok" or result["foreign_key_errors"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

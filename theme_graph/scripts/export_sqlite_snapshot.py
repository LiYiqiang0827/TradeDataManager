"""Export the complete SQLite graph snapshot for the idempotent Neo4j importer."""
from __future__ import annotations
import argparse, json, sqlite3
from pathlib import Path

TABLES = (
    "topic_nodes", "date_nodes", "topic_date_nodes", "topic_date_summary_edges",
    "stock_nodes", "topic_date_stock_edges", "source_documents", "import_runs",
)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args()
    if not a.db.is_file():
        raise FileNotFoundError(a.db)
    conn = sqlite3.connect(f"file:{a.db.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        result = {table: [dict(row) for row in conn.execute(f"SELECT * FROM {table}")] for table in TABLES}
    finally:
        conn.close()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = a.out.with_suffix(a.out.suffix + ".tmp")
    tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    tmp.replace(a.out)
    print(json.dumps({"path": str(a.out), "counts": {k: len(v) for k,v in result.items()}}, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

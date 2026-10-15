# Online SQLite history merge helper

This tool builds a **new isolated SQLite output** from two read-only sources. It is for review and recovery staging. It never edits either source, never operates on a live service database, and refuses to overwrite an existing output. It does not promote or rename the output into a canonical location.

## Table selection is explicit

Pass a reviewed per-run domain table allowlist with repeated `--include-table` options (or `include_tables` in the Python API). The tool never defaults to merging every table. Tables omitted from the allowlist remain exactly as copied from Mini; local-only omitted tables are not copied. `online_stream_cursor` and common cursor/checkpoint/state control table names are forbidden even if requested, so Mini remains authoritative for process-control state. `minute_source_quality` may be explicitly included as a domain-quality table.

Before a run, create the exact table list from a reviewed schema inventory. Include only domain daily-family tables whose merge/replacement behavior has been approved, plus `minute_source_quality` when its provenance is part of the repair. Do not include control/lease/cursor tables or assume that a table name alone establishes a complete source.

## Merge rules

The Mini database is copied first and wins for rows with the same business key. For each selected common table, the tool inspects `PRAGMA index_list`, `index_info`, and `table_info`. It accepts exactly one matching non-surrogate UNIQUE index as the business key. A single `INTEGER PRIMARY KEY`/rowid column is excluded from matching, payload comparison, and local-only inserts; SQLite assigns new IDs for inserted rows. This prevents identical rowids from hiding local-only events.

Rows with a matching business key are classified as same-payload duplicates or payload conflicts. Comparison ignores surrogate IDs and common ingestion timestamps (`snap_ts`, `created_at`, `updated_at`, `ingested_at`, `ingest_ts`). For a payload conflict, the Mini row remains in output, up to ten examples per table report the business key and differing columns with both values, and the output is not promotable. Local-only rows are inserted. If another SQLite constraint blocks those rows, the table insert rolls back and is quarantined.

Tables with no unique business key, multiple plausible unique keys, NULL key values, or schema drift are quarantined; no local rows from that table are added. The report identifies the table and reason. Local-only selected tables are copied with their table/index/trigger definitions and data. `--replace-table` is reserved for selected tables whose full local source has already been independently verified complete; it replaces that whole table in the output.

`ready_to_promote` is true only when there are no quarantined tables, no unresolved business-key payload conflicts, `PRAGMA integrity_check` returns exactly `ok`, and `PRAGMA foreign_key_check` reports no violations. This is a necessary database-health gate, not proof that source rows are semantically correct. Source hashes, table completeness, business-key decisions, and any data-quality exceptions belong in the surrounding deployment receipt.

## Python API

```python
from pathlib import Path
from merge_online_sqlite_history import merge_databases

report = merge_databases(
    Path("mini_source.db"),
    Path("local_repaired_source.db"),
    Path("new_review_output.db"),
    include_tables={"reviewed_daily_table", "minute_source_quality"},
    replace_tables=set(),
)
if not report["ready_to_promote"]:
    raise RuntimeError("review report before any promotion")
```

## CLI

```sh
/Users/nickzhang/TradingAgent/.venv/bin/python merge_online_sqlite_history.py \
  --mini /path/to/mini_source.db \
  --local /path/to/local_repaired_source.db \
  --output /path/to/new_staging_output.db \
  --report /path/to/new_staging_output.merge.json \
  --include-table reviewed_daily_table \
  --include-table minute_source_quality
```

Repeat `--include-table` for every table in the reviewed allowlist and `--replace-table` only for independently verified complete tables. Exit code `0` means no table quarantine, unresolved payload conflict, integrity error, or foreign-key violation; exit code `2` means review-only and must not be promoted. Other errors abort the run. Use new output/report paths and preserve source hashes in the surrounding merge receipt.

Validation uses isolated temporary fixture databases only. No production or archive database was opened or modified while implementing this helper.

# Trading data layout and code preservation — 2026-10-15

## Canonical paths

- Code and maintenance tools: `/Users/nickzhang/TradingDataManager`.
- Offline and online databases: `/Users/nickzhang/TradingDatabase/offline` and `/Users/nickzhang/TradingDatabase/online`.
- Redis persistence: `/Users/nickzhang/TradingDatabase/online/redis`; its executable/runtime remains outside Git.
- Research code and outputs: `/Users/nickzhang/TradingAgent/MrLi` and `/Users/nickzhang/TradingAgent/MrWu`, with `TRADING_RESEARCH_ROOT` selecting the research output root.

The main TradingAgent entries `coreClient`, `offlineDataManager`, `onlineDataManager`, and `scripts` are relative compatibility symlinks to the corresponding TDM directories. The old flat migration and TDX matrix script names are compatibility symlinks within TDM. The research paths for the data skills, `fetch_trend_dividends.py`, and `data_gen.py` also resolve to TDM canonical files. `data_gen.py` keeps its research watchlist default under `TRADING_RESEARCH_ROOT/policy/题材涨停研究/watchlist`, while code/service lookup uses the TDM root and database reads use the database root.

The old main code directories were copied to `~/.local/share/TradingDataManager/migrations/20261014/main_code_backup` before replacement. The archive root and its parent are mode `0700`; `/private/tmp/trading_restructure_20261014/main_code_backup` remains a compatibility symlink. The archive contains 1,146 regular files (66,792,864 bytes) and 2 symlinks. Every regular-file SHA-256 matched its source during the archive check, and database paths remained symlinks rather than being followed. This is a protected rollback archive, not a Git artifact. The initial ownership audit also matched all 24 dirty data-side files against their recorded hashes in both the main checkout and e43d worktree; e43d was left unchanged.

The original LaunchAgent plists and cutover receipts are also retained under `~/.local/share/TradingDataManager/migrations/20261014/`; the task-temp `cutover/` and receipt paths are compatibility symlinks. Private rollback material is not committed to Git.

The 26,488,832-byte research snapshot `db_theme_graph_2025.duckdb` was also moved by same-volume rename from the old MrLi outputs path to `TradingDatabase/offline/research_snapshots/fifteen_min_core_reactivation_2026/`. No process held it open. The source and target SHA-256 both equal `8b3886811aed0cba0b29ce144051decb16abb31fab310016ec6acdae17585c85`; its old research path is a compatibility symlink. Header inspection confirmed it is DuckDB. The similarly named 2.5 GB `db_fifteenMinute.db` was also identified as DuckDB from its file header, not its extension.

The two installed Codex skill paths, `~/.codex/skills/a-share-theme-database` and `~/.codex/skills/a-share-news-database`, now symlink to TDM's canonical skills. The previous installed folders are preserved under `~/.local/share/TradingDataManager/migrations/20261014/installed_skills_backup` (mode `0700`). Their supporting references and UI metadata matched the TDM versions; only the old `SKILL.md` files differed. Both canonical skills passed `skill-creator`'s `quick_validate.py`.

A non-following scan found no physical SQLite or DuckDB files remaining in the TradingAgent tree; the only database-suffixed research path is the snapshot symlink to TDB. It found 1,440 physical Parquet artifacts totaling 1,521,337,881 bytes under `MrLi/outputs`. These are frozen experiment inputs/results, not service database partitions, so they remain under the research root and are excluded from public-provider scans. Their paths, sizes, and SHA-256 digests are recorded in [the research Parquet manifest](research_parquet_manifest_20261015.json).

TDM now has an editable local Python package definition and the `trading-data` provider CLI. An isolated editable install under `/private/tmp` succeeded without changing the shared environment. The CLI lists 49 providers; a `database_only` day query matching the frozen acceptance case returned 3 rows from the migrated database. Database files, sidecars, and machine-local provider configuration are excluded from Git; sanitized configuration templates and `.env.example` contain no credentials. This round did not publish or distribute a wheel.

## Local services and validation

At final verification, the three local LaunchAgents were loaded:

- `com.tradingagent.redis`, PID 32888.
- `com.tradingagent.scheduler.offlineData`, PID 33462.
- `com.tradingagent.scheduler.onlineData`, PID 35463.

Their plists are under `~/Library/LaunchAgents/`; all were previously linted and point to TDM code and the new database paths. The Mac mini was not contacted or changed. No full-market refresh was started during cutover. Independent full-period audit concluded that `service_month` does not need to be rerun: its first two runs on 2026-10-14 completed, the later timeout occurred before paired-table commit, and all four period tables passed full key/latest-date comparison against daily data; all monthly qfq closes also matched the current daily/factor basis. The detailed receipts are in [data integrity verification](data_integrity_verification_20261014.md) and the path/inode receipt is in [data cutover](data_cutover_20261014.md).

The relocated TDX matrix checker now has a real `--help` parser and writes by default to `${TRADING_DATA_AUDIT_ROOT:-/tmp/trading_data_audit}`, outside the code tree; it accepts an explicit `--output`. Its normal run performs 24 external, read-only history-tick requests. During this work, before that parser was added, an attempted `--help` invocation accidentally ran the matrix: all 24 requests succeeded, with no database writes. Its 27 KB result was moved to `/private/tmp/trading_restructure_20261014/tdx_matrix_validation_v1.json`; the matrix was not rerun after the parser fix.

Syntax checks passed for the CLI and relocated scripts. Safe help checks passed for the migration, research-input, and TDX matrix tools. The scheduler lifecycle tests and database/provider fingerprints are recorded in their own receipts; no historical rows were cleaned or rewritten as part of this layout change.

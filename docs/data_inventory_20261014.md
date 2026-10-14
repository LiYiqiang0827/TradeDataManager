# TradingAgent data and manager inventory

Snapshot: 2026-10-14, read-only. Scope covered `/Users/nickzhang/TradingAgent` and `/Users/nickzhang/.codex/worktrees/e43d/TradingAgent`, plus a targeted read-only Mac mini process/file-handle check. No database contents, tokens, cookies, or `.env` values were read. No downloads, database writes, worker stops, or moves were performed.

## Repository and working-tree state

The primary checkout and the `e43d` worktree are separate directories with different inodes, but both are at `5e26a51f7f6e77c29d06d3b5d219cd0f1d6d3571`. Each had 28 tracked modified files and 137 non-ignored untracked files at the initial snapshot. A later recheck found the worktree had one additional untracked file, `docs/project_restructure_plan_20261014.md`, while the primary checkout remained at 137; the two trees share all 165 initial dirty paths. Among 808 same-path source/document files outside bulky data/output/runtime trees, SHA256 matched for all 808. There were no differing common hashes. The primary checkout contains the live ignored data and runtime directories; the worktree does not. Keep all dirty source and untracked files intact during the restructuring.

## Data volumes and formats

Measured allocated directory sizes with `du`:

| Path | Size |
|---|---:|
| `offlineDataManager/data` | 37,988,524,032 bytes (~35.4 GiB; `du -sh` shows 35G) |
| `offlineDataManager/data/oneMinute/parquet` | 6,969,610,240 bytes (~6.49 GiB) |
| `onlineDataManager/data` | 1,921,003,520 bytes (~1.79 GiB; `du -sh` shows 1.8G) |
| `.runtime/redis` | 183,738,368 bytes (~175 MiB) |
| `outputs` | 7,031,902,208 bytes (~6.55 GiB), research artifacts, classify separately |
| `policyStudy/policy/题材涨停研究/data` | 1,213,796,352 bytes (~1.13 GiB), includes derived caches |

The scan found 9 SQLite files (29,706,899,456 logical bytes), 4 `-wal` sidecars (901,703,232 bytes), 4 `-shm` sidecars (1,867,776 bytes), 6 DuckDB files (2,321,358,848 bytes), 1,873 Parquet files (8,486,533,985 bytes), two Redis RDB snapshots (145,963,342 bytes), and one Redis AOF (37,733,792 bytes). The recognized database/Parquet/sidecar/Redis files sum to about 41.6 GB logical; adding 702 research pickle caches gives 42.6 GB. Logical file lengths exceed allocated `du` sizes, so use allocated sizes for storage planning and investigate sparse/hard-linked files before estimating copy time.

Canonical-looking locations are:

- Offline SQLite files: `offlineDataManager/data/db_cn_basic.db`, `db_cn_news.db`, `db_cn_kpl.db`, `db_cn_index.db`, `db_fifteenMinute.db`, `policy_minute.db`, and `policy_ticks.db`.
- Offline DuckDB: `offlineDataManager/data/db_major_news_events.duckdb`, `db_news_events.duckdb`, `db_theme_graph.duckdb`, `fifteenMinute/fifteen_min.duckdb`, and `oneMinute/catalog.duckdb`.
- Offline Parquet: `offlineDataManager/data/oneMinute/parquet/schema_v2/...` (1,873 files).
- Online SQLite: `onlineDataManager/data/online_data_202609.db` and `online_data_202610.db`.
- Redis persistence: `.runtime/redis/dump.rdb`, `appendonlydir/appendonly.aof.*`.
- Research-derived DuckDB: `outputs/fifteen_min_core_reactivation_2026/db_theme_graph_2025.duckdb`; classify under the user's all-database rule, while retaining its experiment context.
- Derived research caches: 702 `.pkl` files, about 973 MB logical, under strategy data/cache locations. These are not canonical SQLite/DuckDB/Parquet stores and should stay with the research artifact unless the owner elects to relocate them.

There are four SQLite WAL/SHM pairs. `db_cn_basic.db-wal` is 901,703,232 bytes. The three other WAL files are currently zero bytes, but their SHM files and DB/WAL handles are live. Do not move a SQLite main DB separately from its sidecars while any connection is open. The DuckDB scan found six `.duckdb.lock`-style lock files in the broader tree, including a zero-byte lock beside `db_theme_graph.duckdb`; file existence alone does not prove a live writer. No local DuckDB file handle appeared in the lsof sample, so recheck at cutover.

Redis is a live append-only persistence store, not an inert cache snapshot. Preserve its RDB/AOF directory and start it against the new path only after a graceful stop and file-handle check.

## Active processes and automatic restart

Local lsof found three processes holding actual DB/Redis files:

- PID 53008: `python -m scheduler.scheduler_updateData`, PPID 1 (launchd).
- PID 19735: `python -m service.service_month`, child of 53008; holds `db_cn_basic`, `db_cn_kpl`, `db_cn_news`, and `db_cn_index` plus WAL/SHM.
- PID 19740: child of 19735; sanitized command inspection did not identify its module.
- PID 22767: `python -m scheduler.scheduler_onlineData`, PPID 1, with 17 children PID 70050–70066 running `service_writeredis_*` and `service_savedata_*` modules.
- PID 22730: `redis-server`, PPID 1, with Redis AOF open.

The three local launchd labels are loaded: `com.tradingagent.scheduler.offlineData`, `com.tradingagent.scheduler.onlineData`, and `com.tradingagent.redis`. All three plist files set `RunAtLoad=true` and `KeepAlive=true`, so killing a process without unloading its label can cause it to restart. Offline and online launch agents run `/Users/nickzhang/TradingAgent/scripts/run_scheduler.sh offline|online`, whose script pins `root=/Users/nickzhang/TradingAgent`, sources `$root/.env`, and sets `PYTHONPATH` to that root. The Redis agent invokes `$root/.runtime/bin/redis-server` with `--dir $root/.runtime/redis` and `--appendonly yes`.

## Mac mini scope

A targeted SSH inspection saw an independent Mac mini offline process holding the same four SQLite database/WAL/SHM basenames and 17 online service workers. The Mac mini is a separate host. This inventory did not find or establish a shared filesystem, and no remote process or file was changed. The requested restructuring is local-only; do not stop the Mac mini workers based only on matching path names.

## Migration map and code dependencies

Recommended code move set to the independent local Git repo `/Users/nickzhang/TradingDataManager`:

- `offlineDataManager/scripts/` (core downloaders, DB clients/maintenance, services, scheduler, tests, config) and its manager docs/logs.
- `onlineDataManager/scripts/` (Redis writers, SQLite persistence services, scheduler, tests) and its manager docs/logs.
- `scripts/run_scheduler.sh`, updating launchd paths and deriving the project root through `TRADING_DATA_MANAGER_ROOT` rather than hardcoding the old checkout.
- Preserve the environment file and its secrets outside Git; the current scheduler entry point reads `.env`.

Recommended database move set to `/Users/nickzhang/TradingDatabase`:

- `offlineDataManager/data/` → `offline/`, including all SQLite, DuckDB, Parquet and sidecars.
- `onlineDataManager/data/` → `online/`.
- `.runtime/redis/` persistence → `/Users/nickzhang/TradingDatabase/online/redis/`; keep the Redis executable/runtime code outside the database repository and update the Redis launchd config accordingly.
- Include the DuckDB under `outputs/` in the database inventory, and preserve its linked experiment inputs/output context.

Keep the shared `coreClient/` API available to the strategy code in TradingAgent. Its `data_provider.py`, SQLite/Redis clients and data adapters are dependencies of both research and service code. Define explicit `TRADING_DATABASE_ROOT` and `TRADING_DATA_MANAGER_ROOT` overrides, preserve `source="database_only"` semantics, and make old `offlineDataManager/data` / `onlineDataManager/data` imports or paths point to the new roots during transition. Do not let legacy `TRADE_AGENT_ROOT_PATH` silently create a new empty database tree.

Review separately: `.runtime/bin` contains the Redis runtime executable (~3 MB); `outputs/` contains ~6.55 GiB of strategy outputs and one DuckDB; strategy `.pkl` caches total ~973 MB logical. The only symlinks found in the primary checkout are four relative `outputs/second_wave_exits_* /inputs` links pointing to the shared v1 inputs directory. No data directory symlink exists in the `e43d` worktree.

## Safe local cutover sequence

Current active writers block rename. During a planned local maintenance window:

1. Record a final process and `lsof` snapshot, then `launchctl bootout` the two local offline/online scheduler labels so `KeepAlive` cannot relaunch them. Do not touch remote Mac mini processes or unrelated research Python tasks.
2. Wait for the known offline child/service chain and online children to exit. The online scheduler has SIGTERM handling and maintains worker process groups; the offline scheduler has no SIGTERM handler, so verify its child service has stopped. Use module, PID ancestry, cwd, and open-file evidence to target only the listed TradingAgent workers.
3. Confirm no SQLite handles remain. Checkpoint/close through the intended DB client after writers have stopped, then move the database and its WAL/SHM together on the same filesystem (or use a verified copy plus checksums if crossing volumes). Recheck no DuckDB handle/active writer before moving each DuckDB file.
4. Boot out the local Redis label, allow Redis to exit cleanly and flush AOF/RDB, then check no process holds the persistence files before moving them.
5. Update manager config, the compatibility paths, and all three launchd plists; only then bootstrap the new labels and confirm each starts against the new absolute paths. Verify old paths do not create empty DBs.

This is a proposed sequence only; no steps above have been executed.

Full machine-readable path/size manifest: `data_inventory_manifest_20261014.json` in this directory.

## 2026-10-14 implementation addendum

The independent code tree is now initialized at `/Users/nickzhang/TradingDataManager`; the initial repository contains scripts, shared clients, docs, tests, and `theme_graph`, but no database, Parquet, Redis persistence, or secret files. The three roots are explicit: manager code in TDM, strategy artifacts under `~/TradingAgent/MrLi`, and databases under `~/TradingDatabase`. The old strategy `TRADE_AGENT_ROOT_PATH` does not redirect database paths. Provider `get_*` signatures (49 public interfaces) were checked against the source checkout and match.

The `theme_graph` entity now lives in TDM and the old MrLi path is a compatibility symlink. Data-maintenance skills point at TDM code/docs and the new default database paths. LaunchAgent plists and the live database/Redis directories are still unchanged; no scheduler, worker, Redis, or remote Mac mini process was stopped or started.

The TDM scheduler launcher now derives its manager root from its own location unless `TRADING_DATA_MANAGER_ROOT` is explicitly set. It reads `TRADING_AGENT_ENV_FILE`, exports the database root (caller override, then environment-file setting, then `~/TradingDatabase`), uses `TRADING_MANAGER_PYTHON`, and sets `PYTHONPATH` to TDM. Isolated fake-Python smoke tests for both offline and online entry points passed; they did not start services. The launchd plists remain for the later reviewed cutover.

Path and database-only tests passed (11 tests) with an absent temporary database root; the test confirms a missing production path is not created on import. The broader historical provider test scripts expect several populated production databases and could not be isolated by the temporary partial database fixture; they were stopped without writing production data. Before cutover, run the 18-query `database_only` fingerprint suite against the frozen original tree and repeat it against the migrated TDM tree.

Independent validation found online minute rows assigned across the processing date boundary (2026-10-13 table includes 2026-10-12 rows; 2026-10-14 table includes 2026-10-13 rows), and recent auction-turnover values are NULL. Preserve these records unchanged during migration and surface a quality flag; do not silently repair or delete them. The persist-layer date guard is now in TDM and its 3 isolated tests pass. The guard affects future writes only; historical-row queries and treatment are documented in [online_minute_root_cause_20261014.md](online_minute_root_cause_20261014.md).

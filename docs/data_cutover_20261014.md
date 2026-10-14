# Trading data cutover receipt — 2026-10-14

## Preflight snapshot

Local host only. The Mac mini is out of scope and was not contacted or changed.

Loaded local launchd labels at preflight:

- `com.tradingagent.scheduler.offlineData`, PID 53008, `python -m scheduler.scheduler_updateData --daemon`, parent PID 1.
- `com.tradingagent.scheduler.onlineData`, PID 22767, `python -m scheduler.scheduler_onlineData --daemon`, parent PID 1, with 17 direct worker children PIDs 70050–70066 (`service_writeredis_*` and `service_savedata_*`).
- `com.tradingagent.redis`, PID 22730, parent PID 1.

At the final preflight handle check, offline PID 53008 held `db_cn_basic.db` and its WAL/SHM at `/Users/nickzhang/TradingAgent/offlineDataManager/data/`. No `service_month` process was present; the scheduler itself owned the open handle. Redis PID 22730 had cwd `/Users/nickzhang/TradingAgent/.runtime/redis` and an AOF file open. No online SQLite data handle was seen in this snapshot. Unrelated PIDs 30064 and 30075 were not identifiable as TradingAgent data services and were not targeted.

Source directories `offlineDataManager/data`, `onlineDataManager/data`, `.runtime/redis`, and target volume root share filesystem device `16777234`. `/Users/nickzhang/TradingDatabase` did not exist at preflight. The source data will be moved by same-volume rename; no full copy is planned.

The three launchd plist originals were copied byte-for-byte to `/private/tmp/trading_restructure_20261014/cutover/plists/` with mode 600 for rollback. Their preflight SHA-256 values and effective paths are in the session record; no secret values are included here. All three set `RunAtLoad=true` and `KeepAlive=true`.

Preflight plist SHA-256:

- Offline scheduler: `ad93f8c7011806d2573d1ab41668ebf276f06b90be06c58789f6a5d7dabbd954`.
- Online scheduler: `aad650131a5c0093b791595ae62a7634a0120d27cb4255cf55aa336c6276e691`.
- Redis: `c0aaf936ba5932742174bd6d5f0466c57be82cff3bff393ad67a5e2686c23442`.

No process has yet been stopped and no data path has yet been moved at this preflight stage.

## Freeze actions

Both local scheduler labels were booted out. `launchctl print` confirmed both labels were unloaded. Offline scheduler PID 53008 and online scheduler PID 22767 exited. Their 17 confirmed online workers (PIDs 70050–70066) were sent SIGTERM and all exited; no SIGKILL was used. A subsequent lsof scan of both source database directories found no open handles. Redis PID 22730 remains running for source fingerprints.

The offline scheduler log shows the 2026-10-14 full-update target was `20261014`. Its `service_month` subprocess started at 22:22:58 and timed out at 22:52:59 after 1800 seconds. It is no longer running and has no open DB handle; later independent stages of the full update completed at 22:58:08. Do not rerun the full update. After the frozen source fingerprint is captured and the target has passed its initial comparison, request approval to rerun only the single-instance `service_month` job for the same target date, then verify both monthly tables.

At the freeze checkpoint, no data directory had yet been renamed and no plist had been changed.

Validation agent completed source fingerprints while Redis remained running and all database writers were stopped. The 18 API query cases passed (18/18); the two online month databases and 308 Redis keys were fingerprinted read-only. The weekly/monthly semantic sample covered three codes and two full week/month periods; raw/origin and qfq OHLC/volume/amount matched daily API aggregation in all four period tables. Each weekly table contained 2,511,637 rows and each monthly table 598,960 rows across 5,832 codes (2015-01-05 through 2026-10-14). `tbl_basic_ctrl` had no `cn_week` or `cn_month` checkpoint entries.

Source fingerprint artifacts remain outside the code repository under `/private/tmp/trading_restructure_20261014/`:

- `api_fingerprint_source.json` SHA-256 `b9be1da54633ddf32e8f352476087556c4e708b9d5c1c7940cf59fa3a4e58c0f`.
- `online_fingerprint_source_202609.json` SHA-256 `c2da4094f410ec56bffd2ba3475f3b9eb62539f1e1c870f558b9ade9916b782d`.
- `online_fingerprint_source_202610.json` SHA-256 `d854182394b192a453bf3d56f5e2714f82f96605a387bea1fa3548d60005aa1b`.
- `redis_fingerprint_source.json` SHA-256 `984cd3b9a19e881d3516e905f2678dd9d8262194979908b775028b7ce29ea2da`.
- `period_semantic_source.json` SHA-256 `a81077051ea2135680595b224ba4af3b182ff24a2d3dd17bcc8e4c49fc025fef`.

## Path move and target acceptance

After frozen source fingerprints completed, all moves were performed on filesystem device `16777234` with same-volume `os.rename`; no full copy was made. The directory inodes were preserved: `offlineDataManager/data` inode `233754524` is now `/Users/nickzhang/TradingDatabase/offline`; `onlineDataManager/data` inode `233756979` is now `/Users/nickzhang/TradingDatabase/online`; `.runtime/redis` inode `233960264` is now `/Users/nickzhang/TradingDatabase/online/redis`. The old data paths are absolute compatibility symlinks to those targets. This directory-inode evidence is not an exhaustive per-file checksum manifest; acceptance also used the database and provider fingerprints below.

The frozen-source to target checks passed: provider API cases 18/18 exact; online SQLite schema and table counts exact across all 112 tables in 202609 and 91 tables in 202610, with selected recent-row hashes matching; weekly/monthly semantic samples and table counts matched. Redis had 308/308 keys, type counts and DUMP hashes exact. The first Redis comparison used a 2-second TTL tolerance and flagged five keys whose remaining TTL differed by about 2,011 ms after a 662-second sampling interval; rerunning with a 3-second tolerance passed all 308 keys with no added, missing, or changed values. This was TTL sampling drift only.

All three local LaunchAgent plists point at the TDM launcher and new database root; `plutil -lint` passed. Redis PID `32888` is running with its AOF in `/Users/nickzhang/TradingDatabase/online/redis`. The offline scheduler PID `33462` is running; its log confirms `--skip-startup-update` and that it entered the scheduling loop without a full refresh. The online scheduler lifecycle fix passed 11 isolated tests and was committed as `6473ae5`, `ae829d6`, and `9a85968`. Its first launch failed closed before starting any writer because macOS `ps` rejected the `sid` field; the label was unloaded during the fix, and the code now uses the locally verified `pid,pgid,lstart,command` fields. The online scheduler is now running as PID `35463` after a graceful after-hours restart to load commit `9a85968`. At the last process check, the only online scheduler/service module was PID `35463`; no service worker was running, and the only open file under the online database tree was Redis AOF. No `service_month` process is running; the 2026-10-14 attempt timed out after 1800 seconds. A same-date single-service recovery remains pending coordinator direction; do not run the full scheduler refresh.

Source and target fingerprint files are retained outside the code repository at `/private/tmp/trading_restructure_20261014/`; see the filenames and source hashes above plus `api_fingerprint_target.json`, `online_fingerprint_target_202609.json`, `online_fingerprint_target_202610.json`, `redis_fingerprint_target.json`, and `period_semantic_target.json`. The receipt records process actions without tokens, cookies, or environment secret values.

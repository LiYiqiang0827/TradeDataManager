# Online minute cross-day anomaly audit

This audit followed the date flow from the TDX client through Redis, savedata, SQLite, and scheduler. It did not modify production Redis or the online database. The reproducer and baseline candidate artifacts remain under `/private/tmp/trading_restructure_20261014/online_minute_staging/`; the approved guard and its formal test now live in the TDM code tree.

The parent agent’s independent read-only audit reported 27,600 rows in `minute_20261013`, including 20,880 rows whose `datetime` is 2026-10-12, and 28,800 rows in `minute_20261014`, including 15,120 rows whose `datetime` is 2026-10-13.

The strongest code-supported explanation is a cross-day Redis/save race. Minute ZSETs live for 24 hours. The minute writer refreshes one stock key at a time, while the scheduler starts the morning writer at 09:29 and the savedata stage at 09:30. A savedata daemon persists immediately at startup. `persist_minute` takes the current `trade_date` supplied by savedata, copies each Redis member’s `datetime` unchanged, and does not compare the two dates. If the first persist sees yesterday’s Redis member for a stock, it writes yesterday’s datetime into today’s table. The table’s `(ts_code, trade_date, time_idx)` uniqueness plus `INSERT OR IGNORE` then prevents a later current-day bar at that index from replacing it.

This mechanism reproduces the cross-day table shape in an isolated in-memory test. It is high-confidence as a possible corruption mechanism; the exact historical event is not uniquely confirmed because the relevant old table `created_at`/`data_timestamp` groups and first-write logs were not available in this sandbox.

There is also a separate source-date limitation: pytdx’s date-less minute parser returns price and volume but no date. `TdxClient.get_minute_kline` labels those rows with the local current date. That creates a source-verification blind spot, but no evidence here attributes the reported previous-day rows to a stale TDX server response.

The isolated baseline test wrote a 2026-10-13 Redis bar into partition `20261014`, proving the persist layer accepted the mismatch. The approved guard is applied in `onlineDataManager/scripts/core/persist_client.py`; it parses each bar timestamp, filters invalid or cross-date bars, and logs skipped counts. The formal test at `onlineDataManager/scripts/test/test_persist_minute_date_guard.py` has 3 passing cases: mixed old/new payloads, missing/invalid timestamps, and a stale bar followed by a current bar at the same unique-key index. The staged baseline patch and `repro_cross_day.py` remain in `/private/tmp/trading_restructure_20261014/online_minute_staging/` for reproducibility.

A bounded read-only Redis sample found `watchlist_count=0`, service metadata `minute_last_round=461` and `minute_last_written=97`, and no bars for four known stock keys. It did not provide a current payload correlated with the audited dates. The canonical online SQLite path resolved to `/Users/nickzhang/TradingDatabase/online/online_data_202610.db`, which was absent in this sandbox; no alternate database path was searched.

The guard prevents new invalid rows from being persisted; it does not rewrite existing rows. Preserve the historical contents and mark suspect rows as a data-quality issue. Exclude them from research until reviewed. Identify suspect rows before downstream use with a read-only query such as:

```sql
SELECT ts_code, trade_date, time_idx, datetime, data_timestamp, created_at
FROM minute_20261014
WHERE datetime IS NULL
   OR length(datetime) < 16
   OR substr(datetime, 1, 10) != substr(trade_date, 1, 4) || '-' ||
                               substr(trade_date, 5, 2) || '-' ||
                               substr(trade_date, 7, 2);
```

Run this for each relevant `minute_YYYYMMDD` table, group results by partition date, embedded datetime date, and `ts_code`, and quarantine/exclude matching rows from research while retaining a read-only snapshot. Rebuild only affected ticker/day partitions from a date-qualified historical source and validate coverage before replacement. No historical repair was executed.

Auction `turnover` remains unresolved. The current code carries an item field named `turnover` through Redis and maps the same name into SQLite, but without a redacted source item and its Redis/SQLite counterparts there is no basis to say whether the source omitted it, returned null, or used another field.

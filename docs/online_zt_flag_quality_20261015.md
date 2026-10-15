# Online zt flag quality warning (2026-10-15)

`query_db.fetch_zt_quality(trade_date)` reports date-scoped confidence for `is_st` and `is_new`. `fetch_zt()` keeps its existing `list[dict]` return shape and emits a warning; `to_dataframe("zt", ...)` attaches the same result in `DataFrame.attrs["quality"]` and `DataFrame.attrs["quality_warnings"]`.

## Observed scope

A Redis string-decoding defect converted string values such as `"False"` to integer `1` through Python truthiness. The following 15 dates were observed with both flags stored as integer `1` across every row in each daily table:

| Date | Rows |
|---|---:|
| 2026-09-16 | 4,450 |
| 2026-09-17 | 14,928 |
| 2026-09-18 | 15,095 |
| 2026-09-21 | 32,049 |
| 2026-09-22 | 31,113 |
| 2026-09-23 | 32,110 |
| 2026-09-24 | 32,142 |
| 2026-09-28 | 30,305 |
| 2026-09-29 | 32,029 |
| 2026-09-30 | 30,146 |
| 2026-10-08 | 16,017 |
| 2026-10-09 | 15,926 |
| 2026-10-12 | 16,067 |
| 2026-10-13 | 15,155 |
| 2026-10-14 | 16,056 |

Those 15 dates contain 333,588 rows. Historical source evidence is unavailable to reconstruct the original flags, so consumers must not use these values for historical ST filtering.

On 2026-10-15, the table had 11,591 rows. An audit observed source matches for 5,000 keys; 6,591 rows had no matching source coverage. This records observed coverage, not a claim that every source-matched value has been verified in the current database. Treat the whole day's flags as partial and unsuitable for full-day historical ST filtering until a dated repair receipt establishes the stored state.

Dates outside this recorded scope are `not_assessed`; absence from the list does not certify their flags.

## Current decoding behavior

The Redis-to-SQLite decoder now parses string booleans explicitly. Recognized true/false and 0/1 values retain their meaning; null, blank, unknown, non-finite, and out-of-domain values become SQL NULL, with a warning for unsupported values. This prevents the same string-truthiness defect on new writes. It does not repair historical rows.

The persistent data governance directory for the 2026-10-15 audit is `~/.local/share/TradingDataManager/data_governance/20261015/audits/`. The relevant table-level status should be kept in a dated repair/audit receipt there; this document is the code-facing warning contract. Do not infer a successful historical repair from this warning metadata alone.

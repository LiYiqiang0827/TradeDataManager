# Auction coverage follow-up (2026-10-15)

## Source and timestamp contract

The Hithink auction endpoint accepts `live` and `final` request stages. Its outer `timestamp` is the time the response is assembled; it is not the auction event time. The database keeps this as `auction_timestamp` with `auction_timestamp_source=api_response_assembly`, while `auction_request_stage` records the stage actually requested. A later `final` request must retain its actual response time and must not be backdated to 09:25. Historical replay must also respect the later collection time as its information cutoff.

Native fields such as `auction_price`, `auction_volume`, `auction_amount`, `auction_unmatched`, and `auction_turnover_pct` remain separate from generic quote fields. The published endpoint description does not establish the units for `auction_volume` or `auction_amount`; ingestion stores their raw numeric values and does not infer a conversion. `last_price` remains the current quote and is not substituted for `auction_price`.

## Observed gap and current limit

The read-only audit of the 2026-10-15 auction table found 1,000 rows across 73 codes, with source response timestamps only from approximately 09:15:03 to 09:16:33 and no observed final-stage records. The table has no preserved raw response payload from which the native auction fields can be reconstructed. This does not establish why collection stopped early or why only 1,000 rows were persisted; the previous saver invocation read at most one 1,000-message batch unless explicitly run in drain mode.

The latest user instruction deferred exporting or replaying the old Redis stream. No historical rows were reconstructed or written from that stream. Until a separately authorized, verifiable source artifact exists, the observed table remains incomplete for stage and native-field coverage; missing values are unknown, not zero. An afternoon `final` request, if separately approved and verified to return that day's fixed auction result, would be a new observation with its actual fetch time and cannot repair the earlier as-of record.

## Forward code path

TDM commits `f8b34dd` and `1a13966` add an explicit 09:26 `--stage final --once` request, drain the auction stream at both scheduled saves, and make draining the auction saver default even when launched manually. Isolated tests cover the final-stage CLI call and 2,401 stream messages across multiple batches. This code change is published but does not mean the scheduler has been deployed or that 2026-10-15 final-stage data was collected.

The published endpoint contract is documented at [Hithink finance CLI reference](https://fuyao.aicubes.cn/llms-full.txt), auction-snapshot section.

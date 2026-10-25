# Daily Topic and Limit-Up Graph

This module owns the build, import, maintenance, query API, and prompt workflow for daily topic reviews and individual limit-up attributions. It is separate from `offlineDataManager/scripts/core/theme_graph_store.py`, which builds the KPL-derived historical fact graph. This module stores analyst conclusions from reviewed reports and keeps them distinct from source market facts.

## Graph model

- `topic_nodes`: persistent topic entities. Imported chapter headings are retained verbatim and marked `needs_review`; the importer does not guess that differently worded topics are identical.
- `date_nodes`: one node per trading date.
- `topic_date_nodes`: one context node per `(topic, date)`. It is the anchor for all stock attribution edges for that topic-day.
- `topic_date_summary_edges`: the topic-to-date edge and its properties: catalyst reason, strength assessment, ladder distribution, limit-up breadth, and highest explicitly stated consecutive-board height. A numeric strength score is left null unless the report provides one.
- `stock_nodes`: one canonical security node per exchange-qualified code.
- `topic_date_stock_edges`: topic-date-to-stock relation with the report's limit-up reason, stock/theme bridge, realization caveats, confidence wording, seal time, and source links.
- `source_documents` and `import_runs`: source hashes and import history.

Each individual attribution is a market explanation captured from its source report, not a verified causal fact. Market rumors and investor speculation may be recorded because they affect decisions, but reports must label them unverified and preserve source/channel and publication time. A rumor first reported after a stock's seal or after the close cannot be written as a pre-seal catalyst without evidence of earlier circulation. Textual references to funds, turnover or volume may be stored as `qualitative_only`; they are never presented as net capital inflow. When no such references exist, flow remains null with `capital_flow_status=not_reported`. Breadth and ladder counts are calculated only from rows in the report's topic table.

Topic headings in the 15 and 16 October reports are compound and not consistently normalized. They are preserved as separate topic nodes until reviewed; this avoids silently merging distinct narratives. The database can later add reviewed canonical aliases without rewriting the raw source report.

## Database location

The default local path is:

`$TRADING_DATABASE_ROOT/theme_graph/topic_limitup_graph.sqlite3`

Override it with `--db /path/to/topic_limitup_graph.sqlite3` or `TRADING_LIMITUP_GRAPH_DB`.

## Build and import

```bash
cd ~/TradingDataManager
python3 -m theme_graph.daily_limitup_graph.cli init

python3 -m theme_graph.daily_limitup_graph.cli import-report \
  --source-uri 'macmini:~/TradingAlertSystem/offlineAlertSystem/alert_zt/output/20261015/20261015_涨停原因独立归因.md' \
  ~/Documents/ChatGPT/TradingDataBase/topic_limitup_graph/source_documents/20261015_涨停原因独立归因.md

python3 -m theme_graph.daily_limitup_graph.cli import-report \
  --source-uri 'macmini:~/TradingAlertSystem/offlineAlertSystem/alert_zt/output/20261016/20261016_涨停原因独立归因.md' \
  ~/Documents/ChatGPT/TradingDataBase/topic_limitup_graph/source_documents/20261016_涨停原因独立归因.md
```

An import is idempotent for the current date view. Reimporting a corrected report replaces that date's graph view, while source-document hashes and import-run records remain available for provenance.

## Query and maintenance

```bash
python3 -m theme_graph.daily_limitup_graph.cli daily-themes 20261016
python3 -m theme_graph.daily_limitup_graph.cli stock-history 000530.SZ
python3 -m theme_graph.daily_limitup_graph.cli topic-history '冰雪产业与冷热设备'
python3 -m theme_graph.daily_limitup_graph.cli integrity
```

Python callers can import `get_daily_themes`, `get_stock_history`, and `get_topic_history` from `api.py`.

## Prompt workflow

- `prompts/daily_limitup_attribution_v3.md` remains the stable prompt path; its current content is v3.2 and is mirrored as `prompts/daily_limitup_attribution_v3_2.md`. The Mac mini TradingDataManager copy is `theme_graph/prompts/limit_attribution_v3_2.md`.
- `prompts/graph_ingestion_v1.md` defines the structured JSON contract for future model-assisted extraction. Existing reports are imported with the deterministic Markdown parser; the LLM prompt is not used to fabricate or backfill missing values.

## Imported reports

The initial database build imports the 15 and 16 October 2026 attribution reports from the Mac mini TradingAlertSystem output directory. The 21 September 2026 trading-day reports were later imported from the sibling `每日涨停原因/` folder in TradingDataBase. Their source reports and evidence snapshots are retained there; the isolated Neo4j proof of concept on Mac mini is a separate database and was not changed by this import.

Portable setup and the current default data root: [../PORTABLE_SETUP.md](../PORTABLE_SETUP.md). `TRADING_DATABASE_ROOT` defaults to `~/TradingDatabase`; use an explicit data root on other machines.

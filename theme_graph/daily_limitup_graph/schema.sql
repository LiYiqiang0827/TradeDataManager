PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1');

CREATE TABLE IF NOT EXISTS source_documents (
    source_document_id TEXT PRIMARY KEY,
    trade_date TEXT NOT NULL,
    file_name TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    sha256 TEXT NOT NULL UNIQUE,
    imported_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topic_nodes (
    topic_id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL UNIQUE,
    source_heading TEXT NOT NULL,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    normalization_status TEXT NOT NULL DEFAULT 'needs_review',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS date_nodes (
    trade_date TEXT PRIMARY KEY,
    date_kind TEXT NOT NULL DEFAULT 'trading_day'
);

-- One context node per (topic, trading date), so stock attribution edges can
-- point to a particular topic-day even when the stock appears under multiple topics.
CREATE TABLE IF NOT EXISTS topic_date_nodes (
    topic_date_id TEXT PRIMARY KEY,
    topic_id TEXT NOT NULL REFERENCES topic_nodes(topic_id),
    trade_date TEXT NOT NULL REFERENCES date_nodes(trade_date),
    source_document_id TEXT NOT NULL REFERENCES source_documents(source_document_id),
    section_order INTEGER NOT NULL,
    UNIQUE(topic_id, trade_date),
    UNIQUE(topic_date_id, topic_id, trade_date)
);

-- This is the attributed edge from a topic node to a date node. Theme-wide
-- catalyst and market structure are properties of this edge, not stock edges.
CREATE TABLE IF NOT EXISTS topic_date_summary_edges (
    topic_date_id TEXT PRIMARY KEY,
    topic_id TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    catalyst_reason TEXT NOT NULL DEFAULT '',
    strength_assessment TEXT NOT NULL DEFAULT '',
    strength_score REAL,
    ladder_json TEXT NOT NULL DEFAULT '{}',
    limitup_breadth INTEGER NOT NULL DEFAULT 0,
    highest_consecutive_board INTEGER,
    capital_flow_overview TEXT,
    capital_flow_status TEXT NOT NULL DEFAULT 'not_reported',
    summary_source_text TEXT NOT NULL DEFAULT '',
    source_document_id TEXT NOT NULL REFERENCES source_documents(source_document_id),
    FOREIGN KEY(topic_date_id, topic_id, trade_date)
        REFERENCES topic_date_nodes(topic_date_id, topic_id, trade_date)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stock_nodes (
    stock_code TEXT PRIMARY KEY,
    raw_code TEXT NOT NULL,
    exchange TEXT NOT NULL,
    stock_name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Each row is a topic-date -> stock edge. The narrative is a market attribution
-- from the source report, not a verified causal fact.
CREATE TABLE IF NOT EXISTS topic_date_stock_edges (
    topic_date_id TEXT NOT NULL REFERENCES topic_date_nodes(topic_date_id) ON DELETE CASCADE,
    stock_code TEXT NOT NULL REFERENCES stock_nodes(stock_code),
    row_order INTEGER NOT NULL,
    limitup_status TEXT NOT NULL DEFAULT '',
    seal_time TEXT,
    consecutive_board INTEGER,
    limitup_reason TEXT NOT NULL,
    stock_theme_bridge TEXT NOT NULL DEFAULT '',
    realization_or_counterevidence TEXT NOT NULL DEFAULT '',
    confidence_raw TEXT NOT NULL DEFAULT '',
    source_refs_json TEXT NOT NULL DEFAULT '[]',
    source_document_id TEXT NOT NULL REFERENCES source_documents(source_document_id),
    PRIMARY KEY(topic_date_id, stock_code)
);

CREATE TABLE IF NOT EXISTS import_runs (
    import_run_id TEXT PRIMARY KEY,
    trade_date TEXT NOT NULL,
    source_document_id TEXT NOT NULL REFERENCES source_documents(source_document_id),
    status TEXT NOT NULL,
    topic_count INTEGER NOT NULL DEFAULT 0,
    stock_edge_count INTEGER NOT NULL DEFAULT 0,
    warnings_json TEXT NOT NULL DEFAULT '[]',
    started_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_topic_dates_trade_date
    ON topic_date_nodes(trade_date, topic_id);
CREATE INDEX IF NOT EXISTS idx_stock_edges_stock
    ON topic_date_stock_edges(stock_code, topic_date_id);
CREATE INDEX IF NOT EXISTS idx_stock_edges_date
    ON topic_date_nodes(trade_date, topic_date_id);

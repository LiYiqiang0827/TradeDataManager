CREATE CONSTRAINT topic_id_unique IF NOT EXISTS FOR (n:Topic) REQUIRE n.topic_id IS UNIQUE;
CREATE CONSTRAINT trade_date_unique IF NOT EXISTS FOR (n:TradeDate) REQUIRE n.trade_date IS UNIQUE;
CREATE CONSTRAINT topic_date_id_unique IF NOT EXISTS FOR (n:TopicDate) REQUIRE n.topic_date_id IS UNIQUE;
CREATE CONSTRAINT stock_code_unique IF NOT EXISTS FOR (n:Stock) REQUIRE n.stock_code IS UNIQUE;
CREATE CONSTRAINT source_document_id_unique IF NOT EXISTS FOR (n:SourceDocument) REQUIRE n.source_document_id IS UNIQUE;
CREATE CONSTRAINT import_run_id_unique IF NOT EXISTS FOR (n:ImportRun) REQUIRE n.import_run_id IS UNIQUE;
CREATE INDEX topic_date_trade_date IF NOT EXISTS FOR (n:TopicDate) ON (n.trade_date);
CREATE INDEX topic_name IF NOT EXISTS FOR (n:Topic) ON (n.canonical_name);

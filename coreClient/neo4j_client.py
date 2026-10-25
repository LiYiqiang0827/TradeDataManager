"""Read/write API for the TradingDataManager theme-attribution Neo4j graph."""
from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import re
from typing import Any, Mapping

_TOPIC_PROPERTIES = {"canonical_name", "source_heading", "aliases_json", "normalization_status"}
_SNAPSHOT_PROPERTIES = {
    "catalyst_reason", "strength_assessment", "strength_score", "ladder_json",
    "limitup_breadth", "highest_consecutive_board", "capital_flow_overview",
    "capital_flow_status", "summary_source_text", "source_document_id",
}
_ATTRIBUTION_PROPERTIES = {
    "row_order", "limitup_status", "seal_time", "consecutive_board", "limitup_reason",
    "stock_theme_bridge", "realization_or_counterevidence", "confidence_raw",
    "source_refs_json", "source_document_id",
}


def normalize_trade_date(value: str) -> str:
    """Return a validated YYYYMMDD trading-date key."""
    date = re.sub(r"[-/]", "", str(value).strip())
    if not re.fullmatch(r"\d{8}", date):
        raise ValueError(f"trade_date must be YYYYMMDD or YYYY-MM-DD: {value!r}")
    datetime.strptime(date, "%Y%m%d")
    return date


def _properties(values: Mapping[str, Any] | None, allowed: set[str]) -> dict[str, Any]:
    values = values or {}
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(f"Unsupported graph properties: {', '.join(sorted(unknown))}")
    # Neo4j properties cannot be arbitrary nested dictionaries; JSON fields stay encoded strings.
    return {key: value for key, value in values.items() if value is not None}


class ThemeGraphNeo4jStore:
    """Application API for Topic, TopicDate, Stock and their attribution relationships.

    Connection settings are read from TRADING_THEME_NEO4J_* environment variables.
    If TRADING_THEME_NEO4J_PASSWORD is absent, the trial password file is used.
    """

    def __init__(
        self,
        uri: str | None = None,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
        password_file: str | Path | None = None,
    ) -> None:
        try:
            from neo4j import GraphDatabase
        except ImportError as exc:
            raise RuntimeError(
                "Neo4j Python driver is required; install TradingDataManager with the [graph] extra."
            ) from exc

        self.uri = uri or os.getenv("TRADING_THEME_NEO4J_URI", "bolt://127.0.0.1:7687")
        self.username = username or os.getenv("TRADING_THEME_NEO4J_USERNAME", "neo4j")
        self.database = database or os.getenv("TRADING_THEME_NEO4J_DATABASE", "neo4j")
        self.password = password or os.getenv("TRADING_THEME_NEO4J_PASSWORD")
        if not self.password:
            secret_path = Path(password_file or os.getenv(
                "TRADING_THEME_NEO4J_PASSWORD_FILE",
                "~/Applications/Neo4jCommunity/.trial-password",
            )).expanduser()
            if not secret_path.is_file():
                raise RuntimeError(
                    "Set TRADING_THEME_NEO4J_PASSWORD or TRADING_THEME_NEO4J_PASSWORD_FILE "
                    f"(password file not found: {secret_path})"
                )
            self.password = secret_path.read_text(encoding="utf-8").strip()
        if not self.password:
            raise RuntimeError("Neo4j password is empty")
        self._driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

    def close(self) -> None:
        self._driver.close()

    def __enter__(self) -> "ThemeGraphNeo4jStore":
        try:
            self._driver.verify_connectivity()
        except Exception:
            self.close()
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _run(self, cypher: str, parameters: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        records, _, _ = self._driver.execute_query(
            cypher,
            parameters_=dict(parameters or {}),
            database_=self.database,
        )
        return [record.data() for record in records]

    def ensure_schema(self) -> None:
        """Idempotently create the graph's business-key constraints and date indexes."""
        schema = Path(__file__).with_name("neo4j_schema.cypher").read_text(encoding="utf-8")
        for statement in schema.split(";"):
            if statement.strip():
                self._run(statement)

    def list_topic_days(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> list[dict[str, Any]]:
        """List daily topic summaries and attribution counts for an optional date range."""
        start = normalize_trade_date(start_date) if start_date else None
        end = normalize_trade_date(end_date) if end_date else None
        return self._run(
            """
            MATCH (t:Topic)-[snapshot:HAS_DAILY_SNAPSHOT]->(d:TradeDate)
            WHERE ($start IS NULL OR d.trade_date >= $start)
              AND ($end IS NULL OR d.trade_date <= $end)
            OPTIONAL MATCH (td:TopicDate {topic_date_id: snapshot.topic_date_id})
                          -[:ATTRIBUTES_LIMITUP]->(s:Stock)
            WITH t, snapshot, d, td, count(s) AS attribution_count
            RETURN properties(t) AS topic,
                   properties(d) AS trade_date,
                   properties(snapshot) AS summary,
                   properties(td) AS topic_date,
                   attribution_count
            ORDER BY d.trade_date, t.canonical_name
            """,
            {"start": start, "end": end},
        )

    def get_topic_day(self, trade_date: str, topic_id: str) -> dict[str, Any] | None:
        """Read one topic-day summary together with its linked stock attributions."""
        rows = self._run(
            """
            MATCH (t:Topic)-[snapshot:HAS_DAILY_SNAPSHOT]->
                  (d:TradeDate {trade_date: $trade_date})
            WHERE t.topic_id = $topic_id OR t.canonical_name = $topic_id
            OPTIONAL MATCH (td:TopicDate {topic_date_id: snapshot.topic_date_id})
                          -[attribution:ATTRIBUTES_LIMITUP]->(s:Stock)
            WITH t, snapshot, d, td,
                 collect(CASE WHEN s IS NULL THEN null ELSE {
                     stock: properties(s), attribution: properties(attribution)
                 } END) AS rows
            RETURN properties(t) AS topic,
                   properties(d) AS trade_date,
                   properties(snapshot) AS summary,
                   properties(td) AS topic_date,
                   [item IN rows WHERE item IS NOT NULL] AS attributions
            """,
            {"trade_date": normalize_trade_date(trade_date), "topic_id": topic_id},
        )
        return rows[0] if rows else None

    def get_stock_attributions(
        self, trade_date: str, stock_code: str | None = None
    ) -> list[dict[str, Any]]:
        """Read stock-level limit-up reasons, optionally for one stock code."""
        return self._run(
            """
            MATCH (td:TopicDate {trade_date: $trade_date})
                  -[attribution:ATTRIBUTES_LIMITUP]->(s:Stock)
            MATCH (td)-[:ABOUT_TOPIC]->(t:Topic)
            WHERE $stock_code IS NULL OR s.stock_code = $stock_code
            RETURN properties(t) AS topic,
                   properties(td) AS topic_date,
                   properties(s) AS stock,
                   properties(attribution) AS attribution
            ORDER BY td.topic_date_id, attribution.row_order
            """,
            {"trade_date": normalize_trade_date(trade_date), "stock_code": stock_code},
        )

    def upsert_topic_day(
        self,
        trade_date: str,
        topic_id: str,
        topic_name: str,
        summary: Mapping[str, Any],
        topic_properties: Mapping[str, Any] | None = None,
        section_order: int | None = None,
    ) -> str:
        """Create/update a topic, its TopicDate node, and the daily summary edge.

        Context is stored on Topic-[:HAS_DAILY_SNAPSHOT]->TradeDate; stock-level
        attribution edges remain attached to the TopicDate node.
        """
        date = normalize_trade_date(trade_date)
        if not topic_id.strip() or not topic_name.strip():
            raise ValueError("topic_id and topic_name must be non-empty")
        topic_props = _properties(topic_properties, _TOPIC_PROPERTIES)
        summary_props = _properties(summary, _SNAPSHOT_PROPERTIES)
        suffix = topic_id.removeprefix("TOPIC-")
        topic_date_id = f"TD-{date}-{suffix}"
        rows = self._run(
            """
            MERGE (t:Topic {topic_id: $topic_id})
            SET t.canonical_name = $topic_name
            SET t += $topic_properties
            MERGE (d:TradeDate {trade_date: $trade_date})
            MERGE (td:TopicDate {topic_date_id: $topic_date_id})
            SET td.topic_id = $topic_id, td.trade_date = $trade_date
            FOREACH (_ IN CASE WHEN $section_order IS NULL THEN [] ELSE [1] END |
                SET td.section_order = $section_order
            )
            MERGE (td)-[:ABOUT_TOPIC]->(t)
            MERGE (td)-[:FOR_DATE]->(d)
            MERGE (t)-[snapshot:HAS_DAILY_SNAPSHOT {topic_date_id: $topic_date_id}]->(d)
            SET snapshot.topic_id = $topic_id,
                snapshot.trade_date = $trade_date,
                snapshot += $summary_properties
            RETURN td.topic_date_id AS topic_date_id
            """,
            {
                "topic_id": topic_id,
                "topic_name": topic_name,
                "topic_properties": topic_props,
                "trade_date": date,
                "topic_date_id": topic_date_id,
                "section_order": section_order,
                "summary_properties": summary_props,
            },
        )
        if not rows:
            raise RuntimeError("Neo4j did not create/update the topic-day record")
        return str(rows[0]["topic_date_id"])

    def upsert_stock_attribution(
        self,
        topic_date_id: str,
        stock_code: str,
        stock_name: str,
        attribution: Mapping[str, Any],
    ) -> None:
        """Create/update one TopicDate-to-Stock limit-up attribution edge."""
        if not topic_date_id.strip() or not stock_code.strip() or not stock_name.strip():
            raise ValueError("topic_date_id, stock_code and stock_name must be non-empty")
        attrs = _properties(attribution, _ATTRIBUTION_PROPERTIES)
        digits = re.sub(r"\D", "", stock_code.split(".", 1)[0])
        exchange = stock_code.rsplit(".", 1)[-1] if "." in stock_code else ""
        rows = self._run(
            """
            MATCH (td:TopicDate {topic_date_id: $topic_date_id})
            MERGE (s:Stock {stock_code: $stock_code})
            ON CREATE SET s.raw_code = $raw_code,
                          s.exchange = $exchange,
                          s.stock_name = $stock_name
            SET s.stock_name = $stock_name
            MERGE (td)-[edge:ATTRIBUTES_LIMITUP {stock_code: $stock_code}]->(s)
            SET edge += $attribution
            RETURN edge.stock_code AS stock_code
            """,
            {
                "topic_date_id": topic_date_id,
                "stock_code": stock_code,
                "stock_name": stock_name,
                "raw_code": digits,
                "exchange": exchange,
                "attribution": attrs,
            },
        )
        if not rows:
            raise KeyError(f"TopicDate not found: {topic_date_id}")

    def get_topic_history(self, topic_id_or_name: str) -> list[dict[str, Any]]:
        """List every available date for a topic, with its summary and stock reasons.

        The argument may be the stable topic_id or the exact canonical topic name.
        """
        return self._run(
            """
            MATCH (t:Topic)-[snapshot:HAS_DAILY_SNAPSHOT]->(d:TradeDate)
            WHERE t.topic_id = $query OR t.canonical_name = $query
            OPTIONAL MATCH (td:TopicDate {topic_date_id: snapshot.topic_date_id})
                          -[attribution:ATTRIBUTES_LIMITUP]->(s:Stock)
            WITH t, snapshot, d, td,
                 collect(CASE WHEN s IS NULL THEN null ELSE {
                     stock: properties(s), attribution: properties(attribution)
                 } END) AS rows
            RETURN properties(t) AS topic,
                   properties(d) AS trade_date,
                   properties(snapshot) AS summary,
                   properties(td) AS topic_date,
                   [item IN rows WHERE item IS NOT NULL] AS attributions
            ORDER BY d.trade_date
            """,
            {"query": topic_id_or_name},
        )

    def get_topic(self, topic_id: str) -> dict[str, Any] | None:
        rows = self._run(
            "MATCH (t:Topic {topic_id: $topic_id}) RETURN properties(t) AS topic",
            {"topic_id": topic_id},
        )
        return rows[0]["topic"] if rows else None

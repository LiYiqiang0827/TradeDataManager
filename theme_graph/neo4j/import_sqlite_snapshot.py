"""Idempotently import daily_limitup_graph SQLite table snapshots into Neo4j."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from theme_graph.neo4j import ThemeGraphNeo4jStore, normalize_trade_date


def load(path: Path) -> dict[str, list[dict[str, Any]]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "topic_nodes", "date_nodes", "topic_date_nodes", "topic_date_summary_edges",
        "stock_nodes", "topic_date_stock_edges", "source_documents", "import_runs",
    }
    missing = required - set(data)
    if missing:
        raise ValueError(f"snapshot misses tables: {sorted(missing)}")
    keys = {"topic_nodes": "topic_id", "date_nodes": "trade_date",
            "topic_date_nodes": "topic_date_id", "topic_date_summary_edges": "topic_date_id",
            "stock_nodes": "stock_code", "source_documents": "source_document_id",
            "import_runs": "import_run_id"}
    identities = {}
    for table, key in keys.items():
        values = [row[key] for row in data[table]]
        if len(values) != len(set(values)):
            raise ValueError(f"duplicate {table}.{key}")
        identities[table] = set(values)
    for day in identities["date_nodes"]:
        normalize_trade_date(day)
    if identities["topic_date_nodes"] != identities["topic_date_summary_edges"]:
        raise ValueError("every TopicDate requires exactly one summary")
    for td in data["topic_date_nodes"]:
        if (td["topic_id"] not in identities["topic_nodes"] or
                td["trade_date"] not in identities["date_nodes"] or
                td["source_document_id"] not in identities["source_documents"]):
            raise ValueError("orphan TopicDate in snapshot")
    edge_keys = set()
    for edge in data["topic_date_stock_edges"]:
        identity = (edge["topic_date_id"], edge["stock_code"])
        if (identity in edge_keys or edge["topic_date_id"] not in identities["topic_date_nodes"] or
                edge["stock_code"] not in identities["stock_nodes"]):
            raise ValueError("duplicate or orphan stock attribution")
        edge_keys.add(identity)
    return data


def merge_node(graph: ThemeGraphNeo4jStore, label: str, key: str, row: dict[str, Any]) -> None:
    # label/key are constants selected by this script; property values remain parameters.
    graph._run(
        f"MERGE (n:{label} {{{key}: $key_value}}) SET n += $properties",
        {"key_value": row[key], "properties": row},
    )


def migrate(path: Path, *, replace_dates: bool = False) -> dict[str, Any]:
    data = load(path)
    topics = {r["topic_id"]: r for r in data["topic_nodes"]}
    dates = {r["trade_date"]: r for r in data["date_nodes"]}
    topic_dates = {r["topic_date_id"]: r for r in data["topic_date_nodes"]}
    summaries = {r["topic_date_id"]: r for r in data["topic_date_summary_edges"]}
    stocks = {r["stock_code"]: r for r in data["stock_nodes"]}
    edges_by_td: dict[str, list[dict[str, Any]]] = {}
    for edge in data["topic_date_stock_edges"]:
        edges_by_td.setdefault(edge["topic_date_id"], []).append(edge)

    with ThemeGraphNeo4jStore() as graph:
        graph.ensure_schema()
        if replace_dates:
            # Explicit opt-in: only replace current views for dates in this snapshot.
            # Provenance nodes and all dates outside the input remain intact.
            graph._run("MATCH (td:TopicDate) WHERE td.trade_date IN $dates DETACH DELETE td",
                       {"dates": sorted(dates)})
            graph._run("MATCH ()-[r:HAS_DAILY_SNAPSHOT]->(d:TradeDate) "
                       "WHERE d.trade_date IN $dates DELETE r", {"dates": sorted(dates)})
        # Preserve all source/topic/date identities and provenance before edges.
        for row in data["source_documents"]:
            merge_node(graph, "SourceDocument", "source_document_id", row)
        for row in data["topic_nodes"]:
            merge_node(graph, "Topic", "topic_id", row)
        for row in data["date_nodes"]:
            merge_node(graph, "TradeDate", "trade_date", row)

        for td_id, td in sorted(topic_dates.items(), key=lambda item: (item[1]["trade_date"], item[1]["section_order"])):
            topic = topics[td["topic_id"]]
            summary = summaries[td_id]
            props = {
                key: summary[key]
                for key in (
                    "catalyst_reason", "strength_assessment", "strength_score", "ladder_json",
                    "limitup_breadth", "highest_consecutive_board", "capital_flow_overview",
                    "capital_flow_status", "summary_source_text", "source_document_id",
                )
                if key in summary
            }
            actual_td_id = graph.upsert_topic_day(
                td["trade_date"], td["topic_id"], topic["canonical_name"], props,
                {key: topic[key] for key in ("source_heading", "aliases_json", "normalization_status") if key in topic},
                td["section_order"],
            )
            if actual_td_id != td_id:
                raise ValueError(f"Neo4j generated unexpected topic_date_id {actual_td_id} != {td_id}")
            graph._run(
                "MATCH (td:TopicDate {topic_date_id: $td_id}) "
                "SET td.source_document_id = $source_id "
                "WITH td MATCH (sd:SourceDocument {source_document_id: $source_id}) "
                "MERGE (td)-[:FROM_SOURCE]->(sd)",
                {"td_id": td_id, "source_id": td["source_document_id"]},
            )
            for edge in sorted(edges_by_td.get(td_id, []), key=lambda r: r["row_order"]):
                stock = stocks[edge["stock_code"]]
                attrs = {
                    key: edge[key]
                    for key in (
                        "row_order", "limitup_status", "seal_time", "consecutive_board",
                        "limitup_reason", "stock_theme_bridge", "realization_or_counterevidence",
                        "confidence_raw", "source_refs_json", "source_document_id",
                    )
                    if key in edge
                }
                graph.upsert_stock_attribution(td_id, edge["stock_code"], stock["stock_name"], attrs)
                graph._run(
                    "MATCH (s:Stock {stock_code: $stock_code}) SET s += $properties",
                    {"stock_code": stock["stock_code"], "properties": stock},
                )

        for row in data["import_runs"]:
            merge_node(graph, "ImportRun", "import_run_id", row)

        expected = {
            "topics": len(data["topic_nodes"]),
            "dates": len(data["date_nodes"]),
            "topic_dates": len(data["topic_date_nodes"]),
            "stocks": len(data["stock_nodes"]),
            "source_documents": len(data["source_documents"]),
            "import_runs": len(data["import_runs"]),
            "snapshots": len(data["topic_date_summary_edges"]),
            "attributions": len(data["topic_date_stock_edges"]),
            "source_links": len(data["topic_date_nodes"]),
        }
        actual_rows = graph._run(
            "MATCH (t:Topic) WITH count(t) AS topics "
            "MATCH (d:TradeDate) WITH topics, count(d) AS dates "
            "MATCH (td:TopicDate) WITH topics, dates, count(td) AS topic_dates "
            "MATCH (s:Stock) WITH topics, dates, topic_dates, count(s) AS stocks "
            "MATCH (sd:SourceDocument) WITH topics, dates, topic_dates, stocks, count(sd) AS source_documents "
            "MATCH (ir:ImportRun) WITH topics, dates, topic_dates, stocks, source_documents, count(ir) AS import_runs "
            "MATCH ()-[snap:HAS_DAILY_SNAPSHOT]->() WITH topics, dates, topic_dates, stocks, source_documents, import_runs, count(snap) AS snapshots "
            "MATCH (:TopicDate)-[a:ATTRIBUTES_LIMITUP]->(:Stock) WITH topics, dates, topic_dates, stocks, source_documents, import_runs, snapshots, count(a) AS attributions "
            "MATCH (:TopicDate)-[src:FROM_SOURCE]->(:SourceDocument) "
            "RETURN topics, dates, topic_dates, stocks, source_documents, import_runs, snapshots, attributions, count(src) AS source_links"
        )
        actual = actual_rows[0] if actual_rows else {}
        counts = {key: int(actual.get(key, -1)) for key in expected}
        if counts != expected:
            raise RuntimeError(f"Neo4j graph counts differ from SQLite snapshot: expected={expected}, actual={counts}")

        by_date = {}
        for date in sorted(dates):
            rows = graph.list_topic_days(date, date)
            wanted_sections = sum(1 for td in topic_dates.values() if td["trade_date"] == date)
            wanted_edges = sum(len(edges_by_td.get(td_id, [])) for td_id, td in topic_dates.items() if td["trade_date"] == date)
            found_edges = sum(int(row["attribution_count"]) for row in rows)
            if len(rows) != wanted_sections or found_edges != wanted_edges:
                raise RuntimeError(f"query acceptance failed for {date}: sections={len(rows)}/{wanted_sections}, edges={found_edges}/{wanted_edges}")
            by_date[date] = {"topic_days": len(rows), "stock_attributions": found_edges}

    return {"status": "completed", "expected_counts": expected, "verified_dates": by_date}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--replace-dates", action="store_true",
                        help="replace only the current date views present in the complete SQLite snapshot")
    args = parser.parse_args()
    result = migrate(args.snapshot.expanduser(), replace_dates=args.replace_dates)
    print(json.dumps(result, ensure_ascii=False, indent=2))

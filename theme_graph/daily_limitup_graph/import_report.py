"""Import a daily limit-up attribution Markdown report into SQLite graph tables."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path

from .db import initialize_database


THEME_HEADING = re.compile(r"^##\s+[一二三四五六七八九十百]+、\s*(.+?)\s*$")
STOCK_CODE = re.compile(r"^(.*?)\s*[（(](\d{6})[）)]")
MARKDOWN_LINK = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
FLOW_TERMS = re.compile(r"资金|成交额|成交量|量比|放量|换手|封单")


def _split_cells(line: str) -> list[str]:
    # Markdown table cells may contain an escaped pipe (for example, a stock's
    # raw theme labels joined with ``\|``). Do not treat those as column
    # separators; unescape them after splitting so the stored text stays useful.
    cells = re.split(r"(?<!\\)\|", line.strip().strip("|"))
    return [cell.replace(r"\|", "|").strip() for cell in cells]


def _is_separator(line: str) -> bool:
    cells = _split_cells(line)
    return bool(cells) and all(re.fullmatch(r"\s*:?-{3,}:?\s*", c) for c in cells)


def _header_index(headers: list[str], *needles: str) -> int | None:
    for i, header in enumerate(headers):
        if all(needle in header for needle in needles):
            return i
    return None


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("<br>", " ")).strip()


def _section_records(lines: list[str], report_path: Path) -> tuple[str, list[dict]]:
    report_date = re.search(r"(20\d{6})", report_path.name)
    if not report_date:
        raise ValueError(f"cannot infer trade date from file name: {report_path.name}")
    trade_date = report_date.group(1)

    sections: list[tuple[str, list[str]]] = []
    current_name: str | None = None
    current_lines: list[str] = []
    for line in lines:
        match = THEME_HEADING.match(line)
        if match:
            if current_name is not None:
                sections.append((current_name, current_lines))
            current_name, current_lines = match.group(1), []
        elif current_name is not None and line.startswith("## "):
            sections.append((current_name, current_lines))
            current_name, current_lines = None, []
        elif current_name is not None:
            current_lines.append(line)
    if current_name is not None:
        sections.append((current_name, current_lines))

    parsed_sections = []
    for section_order, (topic_name, body) in enumerate(sections, start=1):
        header_pos = None
        for i, line in enumerate(body):
            if line.lstrip().startswith("|") and _split_cells(line) and _split_cells(line)[0].startswith("股票"):
                header_pos = i
                break
        if header_pos is None:
            continue

        headers = _split_cells(body[header_pos])
        if header_pos + 1 >= len(body) or not _is_separator(body[header_pos + 1]):
            raise ValueError(f"markdown table header has no separator under topic {topic_name}")

        rows: list[dict] = []
        for line in body[header_pos + 2:]:
            if not line.lstrip().startswith("|"):
                if rows:
                    break
                continue
            cells = _split_cells(line)
            if len(cells) != len(headers):
                continue
            code_match = STOCK_CODE.match(cells[0])
            if not code_match:
                continue
            name, raw_code = _clean(code_match.group(1)), code_match.group(2)
            # The post-reform Beijing exchange codes use the 920xxx range;
            # older Beijing listings commonly start with 4 or 8.
            exchange = (
                "BJ" if raw_code.startswith(("4", "8", "920"))
                else "SH" if raw_code.startswith(("6", "9"))
                else "SZ"
            )
            canonical_code = f"{raw_code}.{exchange}"

            state_idx = _header_index(headers, "状态")
            if state_idx is None:
                state_idx = _header_index(headers, "连板")
            reason_idx = _header_index(headers, "为什么涨停")
            if reason_idx is None:
                reason_idx = _header_index(headers, "涨停原因")
            bridge_idx = _header_index(headers, "桥梁")
            confidence_idx = _header_index(headers, "信心")
            if confidence_idx is None:
                confidence_idx = _header_index(headers, "判断强度")
            caveat_idx = _header_index(headers, "兑现")
            if caveat_idx is None:
                caveat_idx = _header_index(headers, "反证")
            refs_idx = _header_index(headers, "资料来源")

            state = _clean(cells[state_idx]) if state_idx is not None else ""
            time_match = re.search(r"(?<!\d)((?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d)?)", state)
            seal_time = time_match.group(1) if time_match else None
            board_match = re.search(r"(\d+)\s*连板", state)
            if board_match:
                consecutive_board = int(board_match.group(1))
            elif "首板" in state:
                consecutive_board = 1
            else:
                consecutive_board = None

            reason = _clean(cells[reason_idx]) if reason_idx is not None else ""
            bridge = _clean(cells[bridge_idx]) if bridge_idx is not None else ""
            caveat = _clean(cells[caveat_idx]) if caveat_idx is not None else ""
            raw_refs = cells[refs_idx] if refs_idx is not None else ""
            links = [{"title": title, "url": url} for title, url in MARKDOWN_LINK.findall(raw_refs)]
            if not links:
                links = [{"title": title, "url": url} for title, url in MARKDOWN_LINK.findall(reason)]

            rows.append({
                "stock_code": canonical_code,
                "raw_code": raw_code,
                "exchange": exchange,
                "stock_name": name,
                "limitup_status": state,
                "seal_time": seal_time,
                "consecutive_board": consecutive_board,
                "limitup_reason": reason,
                "stock_theme_bridge": bridge,
                "realization_or_counterevidence": caveat,
                "confidence_raw": _clean(cells[confidence_idx]) if confidence_idx is not None else "",
                "source_refs_json": json.dumps(links, ensure_ascii=False),
                "source_refs_raw": raw_refs,
            })

        if not rows:
            continue
        summary_lines = body[:header_pos]
        summary = "\n".join(line for line in summary_lines if line.strip()).strip()
        catalyst = ""
        for line in summary_lines:
            cleaned = _clean(line)
            if "题材逻辑" in cleaned or "当日共性消息" in cleaned:
                catalyst = re.sub(r"^\*\*[^*]+：\*\*\s*", "", cleaned)
                break
        flow_fragments = []
        flow_text = "。".join(
            [summary] + [stock["limitup_reason"] for stock in rows]
            + [stock["realization_or_counterevidence"] for stock in rows]
        )
        for fragment in re.split(r"(?<=[。；;])", flow_text):
            fragment = _clean(fragment)
            if FLOW_TERMS.search(fragment) and fragment not in flow_fragments:
                flow_fragments.append(fragment)
        source_heading = _clean(topic_name)
        # Stock counts are presentation metadata in report headings, not part
        # of a theme's stable identity across trading dates.
        canonical_name = re.sub(r"\s*[（(]\s*\d+\s*只\s*[）)]\s*$", "", source_heading).strip()
        parsed_sections.append({
            "trade_date": trade_date,
            "topic_name": canonical_name or source_heading,
            "source_heading": source_heading,
            "section_order": section_order,
            "summary_source_text": summary,
            "catalyst_reason": catalyst or summary,
            "capital_flow_overview": "；".join(flow_fragments)[:2000] or None,
            "capital_flow_status": "qualitative_only" if flow_fragments else "not_reported",
            "stocks": rows,
        })
    return trade_date, parsed_sections


def parse_report(report_path: str | Path) -> tuple[str, list[dict], str]:
    path = Path(report_path)
    content = path.read_text(encoding="utf-8")
    trade_date, sections = _section_records(content.splitlines(), path)
    if not sections:
        raise ValueError(f"no topic sections with stock tables found in {path}")
    return trade_date, sections, content


def _topic_id(name: str) -> str:
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:16].upper()
    return f"TOPIC-{digest}"


def _topic_date_id(topic_id: str, trade_date: str) -> str:
    return f"TD-{trade_date}-{topic_id.removeprefix('TOPIC-')}"


def import_report(report_path: str | Path, db_path: str | Path, source_uri: str | None = None) -> dict:
    path = Path(report_path)
    trade_date, sections, content = parse_report(path)
    source_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    source_id = f"SRC-{source_hash[:20].upper()}"
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    source_uri = source_uri or str(path.resolve())
    warnings: list[str] = []

    initialize_database(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """INSERT OR IGNORE INTO source_documents
               (source_document_id, trade_date, file_name, source_uri, sha256, imported_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (source_id, trade_date, path.name, source_uri, source_hash, now),
        )
        conn.execute("INSERT OR IGNORE INTO date_nodes(trade_date) VALUES (?)", (trade_date,))

        # A report is the current imported snapshot for this date. Re-imports
        # replace that date's graph view while retaining prior source documents.
        conn.execute("DELETE FROM topic_date_nodes WHERE trade_date = ?", (trade_date,))
        total_edges = 0
        for section in sections:
            name = section["topic_name"]
            source_heading = section.get("source_heading", name)
            topic_id = _topic_id(name)
            theme_date_id = _topic_date_id(topic_id, trade_date)
            conn.execute(
                """INSERT INTO topic_nodes
                   (topic_id, canonical_name, source_heading, aliases_json, normalization_status, created_at)
                   VALUES (?, ?, ?, '[]', 'needs_review', ?)
                   ON CONFLICT(topic_id) DO UPDATE SET source_heading = excluded.source_heading""",
                (topic_id, name, source_heading, now),
            )
            conn.execute(
                """INSERT INTO topic_date_nodes
                   (topic_date_id, topic_id, trade_date, source_document_id, section_order)
                   VALUES (?, ?, ?, ?, ?)""",
                (theme_date_id, topic_id, trade_date, source_id, section["section_order"]),
            )
            ladder = Counter(
                str(stock["consecutive_board"])
                for stock in section["stocks"]
                if stock["consecutive_board"] is not None
            )
            ladder["unclassified"] = sum(
                stock["consecutive_board"] is None for stock in section["stocks"]
            )
            heights = [stock["consecutive_board"] for stock in section["stocks"]
                       if stock["consecutive_board"] is not None]
            conn.execute(
                """INSERT INTO topic_date_summary_edges
                   (topic_date_id, topic_id, trade_date, catalyst_reason,
                    strength_assessment, strength_score, ladder_json, limitup_breadth,
                    highest_consecutive_board, capital_flow_overview, capital_flow_status,
                    summary_source_text, source_document_id)
                   VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)""",
                (theme_date_id, topic_id, trade_date, section["catalyst_reason"],
                 section["summary_source_text"], json.dumps(
                     {key: value for key, value in sorted(
                         ladder.items(), key=lambda item: (item[0] == "unclassified", int(item[0]) if item[0].isdigit() else 0)
                     )}, ensure_ascii=False),
                 len(section["stocks"]), max(heights) if heights else None,
                 section["capital_flow_overview"], section["capital_flow_status"],
                 section["summary_source_text"], source_id),
            )
            for row_order, stock in enumerate(section["stocks"], start=1):
                conn.execute(
                    """INSERT INTO stock_nodes(stock_code, raw_code, exchange, stock_name, created_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(stock_code) DO UPDATE SET stock_name = excluded.stock_name""",
                    (stock["stock_code"], stock["raw_code"], stock["exchange"], stock["stock_name"], now),
                )
                conn.execute(
                    """INSERT INTO topic_date_stock_edges
                       (topic_date_id, stock_code, row_order, limitup_status, seal_time,
                        consecutive_board, limitup_reason, stock_theme_bridge,
                        realization_or_counterevidence, confidence_raw, source_refs_json,
                        source_document_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (theme_date_id, stock["stock_code"], row_order, stock["limitup_status"],
                     stock["seal_time"], stock["consecutive_board"], stock["limitup_reason"],
                     stock["stock_theme_bridge"], stock["realization_or_counterevidence"],
                     stock["confidence_raw"], stock["source_refs_json"], source_id),
                )
                total_edges += 1

        run_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO import_runs
               (import_run_id, trade_date, source_document_id, status, topic_count,
                stock_edge_count, warnings_json, started_at, completed_at)
               VALUES (?, ?, ?, 'completed', ?, ?, ?, ?, ?)""",
            (run_id, trade_date, source_id, len(sections), total_edges,
             json.dumps(warnings, ensure_ascii=False), now, now),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "trade_date": trade_date,
        "source_document_id": source_id,
        "topic_count": len(sections),
        "stock_edge_count": total_edges,
        "unique_stocks": len({row["stock_code"] for section in sections for row in section["stocks"]}),
        "warnings": warnings,
    }

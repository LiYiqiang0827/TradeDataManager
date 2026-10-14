#!/usr/bin/env python3
"""单日财经新闻语义事件聚类试验。

读取 ``fact_news_event`` 中已完成规则去重且未被排除的事件，通过远端
OpenAI-compatible embeddings 服务生成向量，再结合标题字面相似度、时间和数字
保护规则合并为更高一级的语义事件簇。结果只写入 outputs，不修改正式数据库。
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
from typing import Any
from urllib.request import Request, urlopen

import duckdb
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors


from coreClient._paths import manager_root, offline_data_dir, research_root
ROOT = manager_root(__file__, 3)
OFFLINE_DB_DIR = offline_data_dir()
DEFAULT_DB = OFFLINE_DB_DIR / "db_major_news_events.duckdb"
DEFAULT_OUTPUT_ROOT = research_root() / "outputs" / "major_news_analysis"

PUNCT_RE = re.compile(r"[^0-9a-zA-Z\u4e00-\u9fff%]+")
NUMBER_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:\d+(?:\.\d+)?%|\d+(?:\.\d+)?(?:亿元|万元|元|美元|"
    r"万吨|万桶|万股|亿股|吨|桶|点|基点|倍|GW|MW|GWh|MWh))(?![A-Za-z0-9])",
    re.I,
)
LEAD_RE = re.compile(r"^(?:【[^】]{1,60}】)+")


def compact(value: Any) -> str:
    return PUNCT_RE.sub("", str(value or "").lower())


def grams(value: Any) -> set[str]:
    text = compact(value)
    if len(text) < 2:
        return {text} if text else set()
    return {text[index:index + 2] for index in range(len(text) - 1)}


def jaccard(left: set[str], right: set[str]) -> float:
    return len(left & right) / len(left | right) if left and right else 0.0


def numbers(value: Any) -> set[str]:
    return {item.lower() for item in NUMBER_RE.findall(str(value or ""))}


def subject(value: Any) -> str:
    text = LEAD_RE.sub("", str(value or "")).strip()
    prefix = re.split(r"[:：|丨]", text, maxsplit=1)[0].strip()
    if 2 <= len(prefix) <= 28:
        return compact(prefix)
    return ""


def embedding_text(row: dict[str, Any]) -> str:
    title = str(row.get("title") or "").strip()
    content = str(row.get("content") or "").strip()
    if not title:
        title = re.split(r"[。！？；;]", content, maxsplit=1)[0][:160]
    if title and content.startswith(title):
        content = content[len(title):].lstrip(" :：,，。")
    return f"标题：{title[:180]}\n正文：{content[:420]}"


def load_day_snapshot(conn: duckdb.DuckDBPyConnection, day: str) -> pd.DataFrame:
    """按新闻发布日期截断成员，避免后来报道改写历史日的代表文本和传播量。"""
    members = conn.execute("""
        SELECT e.event_id,e.event_date,m.item_id,m.published_at,m.src,m.title,m.content,
               m.excluded,m.representative_score
        FROM fact_news_event e JOIN rel_news_event_member m USING(event_id)
        WHERE e.event_date=? AND m.published_at>=? AND m.published_at<?
        ORDER BY e.event_id,m.published_at,m.item_id
    """, [day, day, (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")]).fetchdf()
    records = []
    for event_id, group in members.groupby("event_id", sort=False):
        group = group.sort_values(
            ["representative_score", "published_at", "item_id"],
            ascending=[False, True, True],
        )
        representative = group.iloc[0]
        if bool(representative["excluded"]):
            continue
        sources = sorted({str(value) for value in group["src"] if value})
        records.append({
            "event_id": event_id, "event_date": day,
            "first_published_at": group["published_at"].min(),
            "last_published_at": group["published_at"].max(),
            "representative_item_id": representative["item_id"],
            "representative_src": representative["src"],
            "title": representative["title"], "content": representative["content"],
            "item_count": len(group), "source_count": len(sources),
            "sources_json": json.dumps(sources, ensure_ascii=False),
            "representative_score": representative["representative_score"],
        })
    return pd.DataFrame(records)


def _post_local(url: str, payload: dict[str, Any], timeout: int) -> dict[str, Any]:
    request = Request(
        url.rstrip("/") + "/embeddings",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_ssh(host: str, remote_url: str, payload: dict[str, Any], timeout: int) -> dict[str, Any]:
    command = [
        "ssh", "-T", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=20",
        "-o", "ServerAliveCountMax=30", host,
        "curl", "-sS", "--fail-with-body", "--max-time", str(timeout),
        "-H", "Content-Type:application/json", "--data-binary", "@-",
        remote_url.rstrip("/") + "/embeddings",
    ]
    run = subprocess.run(
        command,
        input=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        capture_output=True,
        timeout=timeout + 30,
        check=True,
    )
    return json.loads(run.stdout.decode("utf-8"))


def embed(
    texts: list[str], *, model: str, batch_size: int, timeout: int,
    base_url: str, ssh_host: str | None,
) -> np.ndarray:
    chunks: list[np.ndarray] = []
    for offset in range(0, len(texts), batch_size):
        batch = texts[offset:offset + batch_size]
        payload = {"model": model, "input": batch, "encoding_format": "float"}
        if ssh_host:
            result = _post_ssh(ssh_host, base_url, payload, timeout)
        else:
            result = _post_local(base_url, payload, timeout)
        values = sorted(result["data"], key=lambda item: int(item["index"]))
        chunks.append(np.asarray([item["embedding"] for item in values], dtype=np.float32))
        print(f"embedded {min(offset + batch_size, len(texts))}/{len(texts)}", flush=True)
    matrix = np.vstack(chunks)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / np.maximum(norms, 1e-12)


def should_merge(
    similarity: float, lexical: float, same_subject: bool,
    left_numbers: set[str], right_numbers: set[str],
) -> tuple[bool, str]:
    number_conflict = bool(left_numbers and right_numbers and not (left_numbers & right_numbers))
    if number_conflict and similarity < 0.94 and lexical < 0.34:
        return False, "number_conflict"
    if similarity >= 0.94:
        return True, "semantic_very_high"
    if similarity >= 0.89 and lexical >= 0.12:
        return True, "semantic_high_lexical"
    if similarity >= 0.85 and lexical >= 0.24:
        return True, "semantic_lexical"
    if same_subject and similarity >= 0.86 and lexical >= 0.14:
        return True, "same_subject"
    return False, "below_threshold"


class UnionFind:
    def __init__(self, size: int):
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, value: int) -> int:
        while value != self.parent[value]:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        left, right = self.find(left), self.find(right)
        if left == right:
            return
        if self.rank[left] < self.rank[right]:
            left, right = right, left
        self.parent[right] = left
        if self.rank[left] == self.rank[right]:
            self.rank[left] += 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default="20260924")
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--model", default="bge-m3")
    parser.add_argument("--base-url", default="http://127.0.0.1:8003/v1")
    parser.add_argument("--ssh-host", default="llm-server")
    parser.add_argument("--batch-size", type=int, default=96)
    parser.add_argument("--neighbors", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--force-embed", action="store_true")
    args = parser.parse_args()

    day = datetime.strptime(re.sub(r"\D", "", args.trade_date)[:8], "%Y%m%d").date().isoformat()
    out = args.output_root / day.replace("-", "")
    out.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    timings: dict[str, float] = {}

    conn = duckdb.connect(str(args.database), read_only=True)
    rows = load_day_snapshot(conn, day)
    conn.close()
    timings["load_seconds"] = time.perf_counter() - started
    if rows.empty:
        raise SystemExit(f"{day} 没有可处理新闻")
    records = rows.sort_values(["first_published_at", "event_id"]).to_dict("records")
    texts = [embedding_text(row) for row in records]
    text_hash = hashlib.sha256("\n".join(
        f"{row['event_id']}\t{text}" for row, text in zip(records, texts)
    ).encode("utf-8")).hexdigest()
    vector_path = out / "embeddings.npy"
    meta_path = out / "embeddings_meta.json"

    embed_started = time.perf_counter()
    cached = False
    if not args.force_embed and vector_path.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("text_hash") == text_hash and meta.get("model") == args.model:
            matrix = np.load(vector_path)
            cached = True
        else:
            matrix = embed(
                texts, model=args.model, batch_size=args.batch_size, timeout=args.timeout,
                base_url=args.base_url, ssh_host=args.ssh_host or None,
            )
    else:
        matrix = embed(
            texts, model=args.model, batch_size=args.batch_size, timeout=args.timeout,
            base_url=args.base_url, ssh_host=args.ssh_host or None,
        )
    if not cached:
        np.save(vector_path, matrix)
        meta_path.write_text(json.dumps({
            "trade_date": day, "model": args.model, "text_hash": text_hash,
            "rows": len(records), "dimensions": int(matrix.shape[1]),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    timings["embedding_seconds"] = time.perf_counter() - embed_started
    timings["embedding_cached"] = cached

    cluster_started = time.perf_counter()
    neighbor_count = min(max(2, args.neighbors), len(records))
    nearest = NearestNeighbors(n_neighbors=neighbor_count, metric="cosine", algorithm="brute")
    nearest.fit(matrix)
    distances, indices = nearest.kneighbors(matrix, return_distance=True)
    title_grams = [grams(row.get("title") or row.get("content")) for row in records]
    number_sets = [numbers(f"{row.get('title') or ''} {str(row.get('content') or '')[:420]}") for row in records]
    subjects = [subject(row.get("title") or row.get("content")) for row in records]
    union = UnionFind(len(records))
    pair_rows: list[dict[str, Any]] = []
    accepted: list[tuple[int, int, float, str]] = []
    seen_pairs: set[tuple[int, int]] = set()
    for left in range(len(records)):
        for distance, right_value in zip(distances[left, 1:], indices[left, 1:]):
            right = int(right_value)
            pair = (min(left, right), max(left, right))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            similarity = 1.0 - float(distance)
            if similarity < 0.78:
                continue
            lexical = jaccard(title_grams[left], title_grams[right])
            same = bool(subjects[left] and subjects[left] == subjects[right])
            merge, reason = should_merge(
                similarity, lexical, same, number_sets[left], number_sets[right],
            )
            pair_rows.append({
                "left_index": left, "right_index": right,
                "left_event_id": records[left]["event_id"],
                "right_event_id": records[right]["event_id"],
                "similarity": round(similarity, 6), "lexical_jaccard": round(lexical, 6),
                "same_subject": same, "merge": merge, "reason": reason,
                "left_title": records[left].get("title") or "",
                "right_title": records[right].get("title") or "",
            })
            if merge:
                union.union(left, right)
                accepted.append((left, right, similarity, reason))

    raw_components: dict[int, list[int]] = defaultdict(list)
    for index in range(len(records)):
        raw_components[union.find(index)].append(index)

    # 控制单链传递造成的过度合并：每个成员还必须与子簇代表满足合并条件。
    final_clusters: list[list[int]] = []
    for component in raw_components.values():
        ordered = sorted(
            component,
            key=lambda index: (
                float(records[index].get("representative_score") or 0),
                int(records[index].get("source_count") or 0),
            ),
            reverse=True,
        )
        subclusters: list[list[int]] = []
        for index in ordered:
            best: tuple[float, list[int]] | None = None
            for group in subclusters:
                head = group[0]
                similarity = float(matrix[index] @ matrix[head])
                lexical = jaccard(title_grams[index], title_grams[head])
                same = bool(subjects[index] and subjects[index] == subjects[head])
                merge, _ = should_merge(
                    similarity, lexical, same, number_sets[index], number_sets[head],
                )
                if merge and (best is None or similarity > best[0]):
                    best = (similarity, group)
            if best is None:
                subclusters.append([index])
            else:
                best[1].append(index)
        final_clusters.extend(subclusters)

    final_clusters.sort(key=lambda group: min(pd.Timestamp(records[index]["first_published_at"]) for index in group))
    members: list[dict[str, Any]] = []
    clusters: list[dict[str, Any]] = []
    for cluster_number, group in enumerate(final_clusters, start=1):
        head = group[0]
        cluster_id = f"SE-{day.replace('-', '')}-{cluster_number:05d}"
        source_names: set[str] = set()
        article_count = 0
        for index in group:
            source_names.update(json.loads(records[index].get("sources_json") or "[]"))
            article_count += int(records[index].get("item_count") or 0)
            members.append({
                "semantic_event_id": cluster_id,
                "event_id": records[index]["event_id"],
                "is_representative": index == head,
                "similarity_to_representative": round(float(matrix[index] @ matrix[head]), 6),
                "first_published_at": records[index]["first_published_at"],
                "source": records[index].get("representative_src") or "",
                "title": records[index].get("title") or "",
                "content": records[index].get("content") or "",
                "item_count": int(records[index].get("item_count") or 0),
                "source_count": int(records[index].get("source_count") or 0),
            })
        clusters.append({
            "semantic_event_id": cluster_id,
            "event_date": day,
            "first_published_at": min(records[index]["first_published_at"] for index in group),
            "last_published_at": max(records[index]["last_published_at"] for index in group),
            "representative_event_id": records[head]["event_id"],
            "representative_src": records[head].get("representative_src") or "",
            "title": records[head].get("title") or "",
            "content": records[head].get("content") or "",
            "member_event_count": len(group),
            "raw_article_count": article_count,
            "source_count": len(source_names),
            "sources_json": json.dumps(sorted(source_names), ensure_ascii=False),
        })

    pair_frame = pd.DataFrame(pair_rows)
    if not pair_frame.empty:
        pair_frame = pair_frame.sort_values(["merge", "similarity"], ascending=[False, False])
    pair_frame.to_csv(out / "candidate_pairs.csv", index=False)
    pd.DataFrame(members).to_csv(out / "semantic_event_members.csv", index=False)
    pd.DataFrame(clusters).to_csv(out / "semantic_events.csv", index=False)
    timings["clustering_seconds"] = time.perf_counter() - cluster_started
    timings["total_seconds"] = time.perf_counter() - started
    summary = {
        "trade_date": day,
        "input_rule_events": len(records),
        "semantic_events": len(clusters),
        "multi_member_events": sum(1 for group in final_clusters if len(group) > 1),
        "merged_rule_events": len(records) - len(clusters),
        "semantic_reduction_rate": round(1 - len(clusters) / len(records), 4),
        "raw_articles_represented": int(sum(int(row.get("item_count") or 0) for row in records)),
        "accepted_pair_edges": len(accepted),
        "model": args.model,
        "embedding_dimensions": int(matrix.shape[1]),
        "timings": {key: round(value, 4) if isinstance(value, float) else value for key, value in timings.items()},
    }
    (out / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()

import hashlib
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


SCRIPT = Path(__file__).with_name("correct_tdx_zero_amount_sentinel.py")
SPEC = importlib.util.spec_from_file_location("correct_tdx_zero_amount_sentinel", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_backup_file_checks_source_hash_and_existing_backup_collision(tmp_path):
    src = tmp_path / "src.bin"
    dst = tmp_path / "archive" / "src.bin"
    src.write_bytes(b"verified source bytes")
    expected = _hash(src)
    assert module.backup_file(src, dst) == expected
    assert _hash(dst) == expected
    dst.write_bytes(b"different older archive")
    collision_hash = _hash(dst)
    with pytest.raises(RuntimeError, match="existing backup collision"):
        module.backup_file(src, dst)
    assert _hash(dst) == collision_hash
    with pytest.raises(RuntimeError, match="source hash mismatch"):
        module.backup_file(src, tmp_path / "other.bin", "0" * 64)


def test_amount_transform_changes_only_exact_sentinel():
    values = pa.array([module.SENTINEL, 0.0, 1.25, module.SENTINEL * 2], type=pa.float64())
    corrected, count = module.correct_amount_chunk(values)
    assert count == 1
    assert corrected.to_pylist() == [0.0, 0.0, 1.25, module.SENTINEL * 2]


def test_failed_multi_file_run_restores_replaced_file_and_schema(tmp_path, monkeypatch):
    root = tmp_path / "db"
    base = root / "parquet" / "schema_v2"
    manifest_root = base / "_manifest"
    catalog = root / "catalog.duckdb"
    files = []
    records = {}
    for day, part, expected_sentinels in [
        ("20260512", "part-a.parquet", 1),
        ("20260513", "part-b.parquet", 2),
    ]:
        rel = Path(f"year=2026/month=05/trade_date={day}/{part}")
        path = base / rel
        path.parent.mkdir(parents=True)
        table = pa.table({
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": [int(day), int(day)],
            "amount": [module.SENTINEL, 1.5],
        })
        pq.write_table(table, path)
        files.append(path)
        manifest_root.mkdir(parents=True, exist_ok=True)
        manifest = manifest_root / f"trade_date={day}.json"
        manifest.write_text(json.dumps({"status": "ok", "trade_date": day}))
        stat = path.stat()
        records[str(rel)] = {
            "trade_date": day,
            "row_count": 2,
            "pytdx_zero_amount_sentinel_rows": expected_sentinels,
            "amount_nonzero_when_vol_zero_rows": 0,
            "input_before": {"size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns},
        }
    before_hash = [_hash(path) for path in files]
    before_schemas = [pq.ParquetFile(path).schema_arrow for path in files]
    catalog.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(catalog) as conn:
        conn.execute("CREATE TABLE one_min_ingest_catalog (trade_date TEXT, parquet_size INTEGER)")
        conn.executemany("INSERT INTO one_min_ingest_catalog VALUES (?,?)", [("20260512", 1), ("20260513", 2)])
    audit = tmp_path / "audit.json"
    audit.write_text(json.dumps({"partitions": records}))
    backup = tmp_path / "private-backup"
    receipt = tmp_path / "receipt.json"
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--root", str(root), "--audit", str(audit),
                                       "--backup-root", str(backup), "--receipt", str(receipt), "--apply"])
    with pytest.raises(RuntimeError, match="sentinel count changed"):
        module.main()
    assert [_hash(path) for path in files] == before_hash
    assert [pq.ParquetFile(path).schema_arrow.equals(schema, check_metadata=True)
            for path, schema in zip(files, before_schemas)] == [True, True]
    assert json.loads((manifest_root / "trade_date=20260512.json").read_text()) == {"status": "ok", "trade_date": "20260512"}
    with sqlite3.connect(catalog) as conn:
        assert conn.execute("SELECT trade_date,parquet_size FROM one_min_ingest_catalog ORDER BY trade_date").fetchall() == [("20260512", 1), ("20260513", 2)]
    assert json.loads(receipt.read_text())["state"] == "rolled_back"

import importlib.util
import json
import tempfile
import unittest
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

SCRIPT = TOOLS_DIR / "audit_one_min_code_day_coverage.py"
SPEC = importlib.util.spec_from_file_location("audit_one_min_code_day_coverage", SCRIPT)
AUDIT = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(AUDIT)


class OneMinuteCodeDayCoverageTests(unittest.TestCase):
    def test_exact_manifest_partition_and_bidirectional_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "schema_v2"
            part = root / "year=2026" / "month=05" / "trade_date=20260512" / "part-000.parquet"
            part.parent.mkdir(parents=True)
            pq.write_table(pa.table({
                "ts_code": ["000001.SZ", "600000.SH", "600000.SH"],
                "trade_date": [20260512, 20260512, 20260512],
                "time_idx": [0, 0, 1],
            }), part)
            manifest_dir = root / "_manifest"
            manifest_dir.mkdir()
            (manifest_dir / "trade_date=20260512.json").write_text(json.dumps({
                "trade_date": "20260512",
                "target_file": str(part),
                "target_size": part.stat().st_size,
                "rows": 3,
                "stock_count": 2,
                "status": "ok",
            }))
            key_db = base / "keys.duckdb"
            con = duckdb.connect(str(key_db))
            con.execute("CREATE TABLE daily(ts_code VARCHAR,trade_date VARCHAR)")
            con.execute("INSERT INTO daily VALUES ('000001.SZ','20260512'),('000002.SZ','20260512')")
            con.close()

            result = AUDIT.run(root, key_db, base / "progress.json", "20260512", "20260512", "128MB")
            self.assertEqual(result["status"], "complete")
            self.assertEqual(result["inventory"]["canonical_glob_part_files"], 1)
            self.assertEqual(result["physical_rows_scanned"], 3)
            self.assertEqual(result["code_day_rows_inserted"], 2)
            self.assertEqual(result["comparison"]["daily_code_days_missing_one_min"], 1)
            self.assertEqual(result["comparison"]["one_min_code_days_missing_daily"], 1)


if __name__ == "__main__":
    unittest.main()

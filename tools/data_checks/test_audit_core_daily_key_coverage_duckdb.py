import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("audit_core_daily_key_coverage_duckdb.py")
SPEC = importlib.util.spec_from_file_location("audit_core_daily_key_coverage_duckdb", SCRIPT)
AUDIT = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(AUDIT)


class DailyKeyCoverageAuditTests(unittest.TestCase):
    def test_streamed_keys_report_all_four_directions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "source.sqlite"
            con = sqlite3.connect(db)
            for table in ("tbl_cn_day", "tbl_cn_daily_basic", "tbl_cn_adj_factor"):
                con.execute(f"CREATE TABLE {table}(ts_code TEXT, trade_date TEXT)")
            con.executemany("INSERT INTO tbl_cn_day VALUES (?,?)", [
                ("000001.SZ", "20260105"), ("000002.SZ", "20260105"),
            ])
            con.executemany("INSERT INTO tbl_cn_daily_basic VALUES (?,?)", [
                ("000001.SZ", "20260105"),
            ])
            con.executemany("INSERT INTO tbl_cn_adj_factor VALUES (?,?)", [
                ("000001.SZ", "20260105"), ("000002.SZ", "20260105"),
                ("000003.SZ", "20260105"),
            ])
            con.commit()
            con.close()

            output = root / "progress.json"
            temp_dir = root / "ducktmp"
            result = AUDIT.run(db, output, temp_dir, "20260101", "20261231", 1, "128MB")

            self.assertEqual(result["status"], "complete")
            self.assertTrue(result["interrupted_then_resumed"])
            checks = result["checks"]
            self.assertEqual(checks["daily_missing_daily_basic"]["missing_key_count"], 1)
            self.assertEqual(checks["daily_missing_adj_factor"]["missing_key_count"], 0)
            self.assertEqual(checks["daily_basic_missing_daily"]["missing_key_count"], 0)
            self.assertEqual(checks["adj_factor_missing_daily"]["missing_key_count"], 1)
            self.assertEqual(checks["daily_missing_daily_basic"]["sample"], [
                {"ts_code": "000002.SZ", "trade_date": "20260105"},
            ])
            self.assertTrue(output.is_file())


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import hashlib
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd


SCRIPT = Path(__file__).with_name("repair_minute_quality_ledger.py")
SPEC = importlib.util.spec_from_file_location("repair_minute_quality_ledger", SCRIPT)
repair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repair)


class QualityLedgerRepairTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "schema_v2"
        self.db = Path(self.tmp.name) / "online.db"
        self.receipt = Path(self.tmp.name) / "receipt.json"
        self.day = "20260922"
        self.root.mkdir()
        target = self.root / "year=2026" / "month=09" / f"trade_date={self.day}" / "part-000.parquet"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"fixture-primary-partition")
        sidecar = self.root / "_quality" / f"trade_date={self.day}" / "part-000.parquet"
        sidecar.parent.mkdir(parents=True)
        payload = json.dumps({"price": 10.0, "vol": 0, "amount": 0.0}, sort_keys=True)
        rows = [
            {
                "trade_date": self.day,
                "ts_code": "000001.SZ",
                "time_idx": 1,
                "datetime": "2026-09-22 09:32:00",
                "quality_flags": json.dumps(["pytdx_zero_amount_sentinel_corrected", "pytdx_zero_volume_sentinel_corrected"]),
                "source": "tdx",
                "source_payload_json": payload,
                "source_payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
            },
            {
                "trade_date": self.day,
                "ts_code": "000002.SZ",
                "time_idx": 2,
                "datetime": "2026-09-22 09:33:00",
                "quality_flags": json.dumps(["amount_nonzero_with_zero_volume"]),
                "source": "tdx",
                "source_payload_json": payload,
                "source_payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
            },
        ]
        pd.DataFrame(rows).to_parquet(sidecar, index=False)
        manifest = {
            "status": "ok", "trade_date": self.day,
            "target_file": str(target), "target_size": target.stat().st_size,
            "quality_sidecar": str(sidecar), "quality_sidecar_rows": 2,
            "quality_sidecar_sha256": repair.sha256(sidecar),
        }
        (self.root / "_manifest").mkdir()
        (self.root / "_manifest" / f"trade_date={self.day}.json").write_text(json.dumps(manifest))
        conn = sqlite3.connect(self.db)
        conn.execute(f"CREATE TABLE minute_{self.day} (trade_date TEXT, ts_code TEXT, time_idx INTEGER, datetime TEXT, price REAL, vol INTEGER)")
        conn.execute(f"INSERT INTO minute_{self.day} VALUES (?, ?, ?, ?, ?, ?)", (self.day, "000001.SZ", 1, "2026-09-22 09:32:00", 10.0, 0))
        conn.execute(repair.QUALITY_SQL)
        conn.execute("INSERT INTO minute_source_quality VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     (self.day, "000001.SZ", 1, "old", "old_flag", "old_source", "{}", None, "old"))
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def run_tool(self, apply=False):
        args = ["--db", str(self.db), "--trade-date", self.day,
                "--parquet-root", str(self.root), "--receipt", str(self.receipt)]
        if apply:
            args.append("--apply")
        old_argv = __import__("sys").argv
        __import__("sys").argv = [str(SCRIPT), *args]
        try:
            repair.main()
        finally:
            __import__("sys").argv = old_argv

    def test_dry_run_does_not_change_ledger(self):
        self.run_tool()
        conn = sqlite3.connect(self.db)
        rows = conn.execute("SELECT quality_code FROM minute_source_quality WHERE trade_date=?", (self.day,)).fetchall()
        conn.close()
        self.assertEqual(rows, [("old_flag",)])
        self.assertFalse(json.loads(self.receipt.read_text())["minute_bars_modified"])

    def test_apply_preserves_existing_rows_and_appends_only_matching_bar_keys(self):
        self.run_tool(apply=True)
        conn = sqlite3.connect(self.db)
        rows = conn.execute("SELECT quality_code FROM minute_source_quality WHERE trade_date=? ORDER BY quality_code", (self.day,)).fetchall()
        bars = conn.execute(f"SELECT count(*) FROM minute_{self.day}").fetchone()[0]
        conn.close()
        self.assertEqual(rows, [
            ("old_flag",),
            ("pytdx_zero_amount_sentinel_corrected",),
            ("pytdx_zero_volume_sentinel_corrected",),
        ])
        self.assertEqual(bars, 1)
        receipt = json.loads(self.receipt.read_text())
        self.assertEqual(receipt["status"], "committed")
        self.assertEqual(receipt["sidecar_bar_keys_in_target"], 1)
        self.assertEqual(receipt["sidecar_bar_keys_excluded_not_in_target"], 1)
        self.assertEqual(receipt["new_ledger_rows"], 2)

    def test_repeated_apply_is_idempotent_and_preserves_original_provenance(self):
        self.run_tool(apply=True)
        conn = sqlite3.connect(self.db)
        before = conn.execute(
            "SELECT datetime,source,source_payload_json,source_timestamp,detected_at "
            "FROM minute_source_quality WHERE quality_code='old_flag'"
        ).fetchone()
        conn.close()
        self.run_tool(apply=True)
        conn = sqlite3.connect(self.db)
        after = conn.execute(
            "SELECT datetime,source,source_payload_json,source_timestamp,detected_at "
            "FROM minute_source_quality WHERE quality_code='old_flag'"
        ).fetchone()
        count = conn.execute("SELECT count(*) FROM minute_source_quality WHERE trade_date=?", (self.day,)).fetchone()[0]
        conn.close()
        receipt = json.loads(self.receipt.read_text())
        self.assertEqual(before, after)
        self.assertEqual(count, 3)
        self.assertEqual(receipt["new_ledger_rows"], 0)
        self.assertEqual(receipt["already_present_identical_rows"], 2)

    def test_same_key_conflict_is_reported_without_overwriting_existing_row(self):
        conn = sqlite3.connect(self.db)
        conn.execute(
            "INSERT INTO minute_source_quality VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (self.day, "000001.SZ", 1, "old-datetime", "pytdx_zero_amount_sentinel_corrected",
             "legacy-source", '{"old":true}', None, "old-detected-at"),
        )
        conn.commit()
        conn.close()
        self.run_tool(apply=True)
        conn = sqlite3.connect(self.db)
        old = conn.execute(
            "SELECT datetime,source,source_payload_json,detected_at FROM minute_source_quality "
            "WHERE trade_date=? AND ts_code='000001.SZ' AND time_idx=1 "
            "AND quality_code='pytdx_zero_amount_sentinel_corrected'", (self.day,),
        ).fetchone()
        conn.close()
        receipt = json.loads(self.receipt.read_text())
        self.assertEqual(old, ("old-datetime", "legacy-source", '{"old":true}', "old-detected-at"))
        self.assertEqual(receipt["same_key_payload_conflicts"], 1)
        self.assertEqual(receipt["new_ledger_rows"], 1)

    def test_sidecar_datetime_must_match_the_target_bar_datetime(self):
        sidecar = self.root / "_quality" / f"trade_date={self.day}" / "part-000.parquet"
        df = pd.read_parquet(sidecar)
        df.loc[df["ts_code"] == "000001.SZ", "datetime"] = "2026-09-22 09:33:00"
        df.to_parquet(sidecar, index=False)
        manifest_path = self.root / "_manifest" / f"trade_date={self.day}.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["quality_sidecar_sha256"] = repair.sha256(sidecar)
        manifest_path.write_text(json.dumps(manifest))

        self.run_tool(apply=True)

        conn = sqlite3.connect(self.db)
        rows = conn.execute(
            "SELECT quality_code FROM minute_source_quality WHERE trade_date=? ORDER BY quality_code",
            (self.day,),
        ).fetchall()
        conn.close()
        receipt = json.loads(self.receipt.read_text())
        self.assertEqual(rows, [("old_flag",)])
        self.assertEqual(receipt["sidecar_bar_keys_excluded_datetime_conflict"], 1)
        self.assertEqual(receipt["new_ledger_rows"], 0)
        self.assertEqual(receipt["datetime_conflict_samples"][0]["reason"],
                         "invalid_or_mismatched_bar_datetime")

    def test_hash_mismatch_fails_before_database_write(self):
        manifest_path = self.root / "_manifest" / f"trade_date={self.day}.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["quality_sidecar_sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "SHA256"):
            repair.load_verified_sidecar(self.root, self.day)
        conn = sqlite3.connect(self.db)
        rows = conn.execute("SELECT quality_code FROM minute_source_quality WHERE trade_date=?", (self.day,)).fetchall()
        conn.close()
        self.assertEqual(rows, [("old_flag",)])


if __name__ == "__main__":
    unittest.main()

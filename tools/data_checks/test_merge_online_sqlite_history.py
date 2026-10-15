import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("merge_online_sqlite_history", HERE / "merge_online_sqlite_history.py")
MERGER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MERGER)


def make_db(path, events, *, with_business_key=True, ambiguous=False):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, day TEXT, value TEXT, snap_ts TEXT)")
    if with_business_key:
        con.execute("CREATE UNIQUE INDEX uq_events_code_day ON events(code, day)")
    if ambiguous:
        con.execute("CREATE UNIQUE INDEX uq_events_value ON events(value)")
    con.executemany("INSERT INTO events(id,code,day,value,snap_ts) VALUES (?,?,?,?,?)", events)
    con.commit()
    con.close()


class OnlineSQLiteMergeTests(unittest.TestCase):
    def test_surrogate_id_collision_does_not_drop_local_only_and_mini_wins_payload_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [(7, "A", "20260901", "same", "mini-time"),
                           (8, "B", "20260901", "mini-value", "mini-time")])
            make_db(local, [(7, "C", "20260901", "new-local", "local-time"),
                            (80, "A", "20260901", "same", "different-snapshot"),
                            (90, "B", "20260901", "local-value", "local-time")])

            report = MERGER.merge_databases(mini, local, output, {"events"})
            events = sqlite3.connect(output)
            rows = events.execute("SELECT id,code,value FROM events ORDER BY code").fetchall()
            events.close()

            stats = next(row for row in report["tables"] if row["table"] == "events")
            self.assertFalse(report["ready_to_promote"])  # semantic conflicts require review
            self.assertEqual(report["review_required_tables"], ["events"])
            self.assertEqual(stats["business_key"], ["code", "day"])
            self.assertEqual(stats["same_payload_duplicates"], 1)
            self.assertEqual(stats["business_key_payload_conflicts_kept_mini"], 1)
            self.assertEqual(stats["conflict_samples"][0]["business_key"], {"code": "B", "day": "20260901"})
            self.assertEqual(stats["conflict_samples"][0]["differing_columns"]["value"],
                             {"mini": "mini-value", "local": "local-value"})
            self.assertEqual(stats["local_only_inserted"], 1)
            self.assertEqual(rows[0], (7, "A", "same"))
            self.assertEqual(rows[1], (8, "B", "mini-value"))
            self.assertEqual(rows[2][1:], ("C", "new-local"))
            self.assertGreater(rows[2][0], 8)  # local id=7 did not collide/drop the source-only row
            self.assertTrue(local.exists())

    def test_no_business_key_is_quarantined_and_never_promotable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [(1, "A", "20260901", "mini", "t")], with_business_key=False)
            make_db(local, [(1, "B", "20260901", "local", "t")], with_business_key=False)
            report = MERGER.merge_databases(mini, local, output, {"events"})
            stats = next(row for row in report["tables"] if row["table"] == "events")
            self.assertFalse(report["ready_to_promote"])
            self.assertEqual(stats["status"], "quarantined")
            self.assertEqual(stats["reason"], "no_business_unique_index")
            con = sqlite3.connect(output)
            self.assertEqual(con.execute("SELECT code FROM events").fetchall(), [("A",)])
            con.close()
            self.assertEqual(sqlite3.connect(local).execute("SELECT code FROM events").fetchall(), [("B",)])

    def test_empty_local_table_needs_no_guessed_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [(1, "A", "20260901", "mini", "t")], with_business_key=False)
            make_db(local, [], with_business_key=False)
            report = MERGER.merge_databases(mini, local, output, {"events"})
            stats = next(row for row in report["tables"] if row["table"] == "events")
            self.assertTrue(report["ready_to_promote"])
            self.assertEqual(stats["status"], "local_empty_passthrough")
            con = sqlite3.connect(output)
            self.assertEqual(con.execute("SELECT code FROM events").fetchall(), [("A",)])
            con.close()

    def test_text_primary_key_is_not_dropped_as_an_auto_increment_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            for path, key_id, value in ((mini, "mini-event", "A"), (local, "local-event", "A")):
                con = sqlite3.connect(path)
                con.execute("CREATE TABLE events (id TEXT PRIMARY KEY, code TEXT NOT NULL, day TEXT NOT NULL, value TEXT, UNIQUE(code,day))")
                con.execute("INSERT INTO events VALUES (?,?,?,?)", (key_id, value, "20260901", "payload"))
                con.commit()
                con.close()
            report = MERGER.merge_databases(mini, local, output, {"events"})
            stats = next(row for row in report["tables"] if row["table"] == "events")
            self.assertFalse(report["ready_to_promote"])
            self.assertEqual(stats["reason"], "ambiguous_business_unique_indexes")
            con = sqlite3.connect(output)
            self.assertEqual(con.execute("SELECT id FROM events").fetchall(), [("mini-event",)])
            con.close()

    def test_ambiguous_business_unique_indexes_are_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [(1, "A", "20260901", "V1", "t")], ambiguous=True)
            make_db(local, [(1, "B", "20260901", "V2", "t")], ambiguous=True)
            report = MERGER.merge_databases(mini, local, output, {"events"})
            stats = next(row for row in report["tables"] if row["table"] == "events")
            self.assertFalse(report["ready_to_promote"])
            self.assertEqual(stats["reason"], "ambiguous_business_unique_indexes")

    def test_null_business_key_values_are_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [(1, None, "20260901", "V1", "t")])
            make_db(local, [(2, "B", "20260901", "V2", "t")])
            report = MERGER.merge_databases(mini, local, output, {"events"})
            stats = next(row for row in report["tables"] if row["table"] == "events")
            self.assertFalse(report["ready_to_promote"])
            self.assertEqual(stats["reason"], "null_business_key_values")

    def test_local_only_table_is_preserved_with_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [(1, "A", "20260901", "V1", "t")])
            make_db(local, [(1, "A", "20260901", "V1", "t")])
            con = sqlite3.connect(local)
            con.execute("CREATE TABLE minute_source_quality (trade_date TEXT NOT NULL, code TEXT NOT NULL, flag TEXT, PRIMARY KEY(trade_date,code))")
            con.execute("INSERT INTO minute_source_quality VALUES ('20260901','A','quality')")
            con.commit()
            con.close()
            report = MERGER.merge_databases(mini, local, output, {"events", "minute_source_quality"})
            self.assertTrue(report["ready_to_promote"])
            con = sqlite3.connect(output)
            self.assertEqual(con.execute("SELECT * FROM minute_source_quality").fetchall(), [("20260901", "A", "quality")])
            con.close()

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [])
            make_db(local, [])
            output.write_bytes(b"keep")
            with self.assertRaises(FileExistsError):
                MERGER.merge_databases(mini, local, output, {"events"})
            self.assertEqual(output.read_bytes(), b"keep")

    def test_non_allowlisted_control_table_is_preserved_from_mini(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            for path, value in ((mini, "mini-cursor"), (local, "local-cursor")):
                con = sqlite3.connect(path)
                con.execute("CREATE TABLE online_stream_cursor (name TEXT PRIMARY KEY, value TEXT)")
                con.execute("INSERT INTO online_stream_cursor VALUES ('minute', ?)", (value,))
                con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, code TEXT, day TEXT, value TEXT, UNIQUE(code,day))")
                con.commit()
                con.close()
            report = MERGER.merge_databases(mini, local, output, {"events"})
            self.assertTrue(report["ready_to_promote"])
            self.assertIn("online_stream_cursor", report["excluded_mini_tables_preserved"])
            con = sqlite3.connect(output)
            self.assertEqual(con.execute("SELECT value FROM online_stream_cursor").fetchone(), ("mini-cursor",))
            con.close()

    def test_control_table_cannot_be_allowlisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [])
            make_db(local, [])
            for table in ("online_stream_cursor", "online_stream_timestamp_quarantine"):
                with self.assertRaisesRegex(ValueError, "control tables cannot be merged"):
                    MERGER.merge_databases(mini, local, output, {table})
            self.assertFalse(output.exists())

    def test_allowlist_is_required_and_unknown_tables_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local = root / "mini.db", root / "local.db"
            make_db(mini, [])
            make_db(local, [])
            with self.assertRaisesRegex(ValueError, "explicit reviewed domain-table allowlist"):
                MERGER.merge_databases(mini, local, root / "empty.db", set())
            with self.assertRaisesRegex(ValueError, "absent from both sources"):
                MERGER.merge_databases(mini, local, root / "unknown.db", {"not_a_table"})

    def test_integrity_and_foreign_key_checks_are_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            make_db(mini, [])
            make_db(local, [])
            report = MERGER.merge_databases(mini, local, output, {"events"})
            self.assertTrue(report["integrity_check"]["ok"])
            self.assertEqual(report["integrity_check"]["results"], ["ok"])
            self.assertTrue(report["foreign_key_check"]["ok"])
            self.assertEqual(report["foreign_key_check"]["violation_count"], 0)

    def test_foreign_key_violation_blocks_promotion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mini, local, output = root / "mini.db", root / "local.db", root / "merged.db"
            for path in (mini, local):
                con = sqlite3.connect(path)
                con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, code TEXT, day TEXT, value TEXT, UNIQUE(code,day))")
                con.execute("CREATE TABLE event_detail (event_id INTEGER REFERENCES events(id), note TEXT)")
                con.execute("INSERT INTO event_detail VALUES (999, 'orphan')")
                con.commit()
                con.close()
            report = MERGER.merge_databases(mini, local, output, {"events"})
            self.assertTrue(report["integrity_check"]["ok"])
            self.assertFalse(report["foreign_key_check"]["ok"])
            self.assertEqual(report["foreign_key_check"]["violation_count"], 1)
            self.assertFalse(report["ready_to_promote"])


if __name__ == "__main__":
    unittest.main()

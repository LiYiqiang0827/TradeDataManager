from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from tools.data_checks.refresh_tradecal_window import refresh_calendar, validate_calendar_rows


def response(start="20261015", days=8, *, closed=()):
    begin = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
    rows = {"SSE": [], "SZSE": []}
    prev = "20261014"
    for offset in range(days):
        d = (begin + timedelta(days=offset)).strftime("%Y%m%d")
        is_open = "0" if d in closed else "1"
        for exchange in rows:
            rows[exchange].append({
                "cal_date": d, "exchange": exchange, "is_open": is_open,
                "pretrade_date": prev,
            })
        if is_open == "1":
            prev = d
    return rows


class RefreshCalendarTests(unittest.TestCase):
    def test_validates_weekend_from_provider_instead_of_weekday(self):
        rows = response()
        accepted, end = validate_calendar_rows(rows, "20261015", "20261031")
        self.assertEqual(end, "20261022")
        self.assertEqual(len(accepted), 16)

    def test_rejects_gaps_exchange_disagreement_and_short_coverage(self):
        rows = response()
        del rows["SSE"][3]
        with self.assertRaisesRegex(ValueError, "gaps"):
            validate_calendar_rows(rows, "20261015", "20261031")
        rows = response(days=8)
        rows["SZSE"][0]["is_open"] = "0"
        with self.assertRaisesRegex(ValueError, "flags disagree"):
            validate_calendar_rows(rows, "20261015", "20261031")
        with self.assertRaisesRegex(ValueError, "next-week"):
            validate_calendar_rows(response(days=5), "20261015", "20261031")

    def test_refresh_upserts_only_forward_window_and_records_before_after(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "db_cn_basic.db"
            receipt = root / "receipt.json"
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE tbl_cn_tradecal(cal_date TEXT NOT NULL, exchange TEXT NOT NULL, is_open INTEGER, pretrade_date TEXT, snap_ts TEXT, PRIMARY KEY(cal_date,exchange))")
            conn.execute("INSERT INTO tbl_cn_tradecal VALUES('20261014','SSE',1,'20261013','old')")
            conn.commit()
            conn.close()
            first = refresh_calendar(
                db_path=db, receipt_path=receipt, start_date="20261015", requested_end="20261031",
                provider_rows=response(), fetched_at="2026-10-15T12:00:00+08:00", provider_request_sha256="abc",
            )
            self.assertEqual(first["status"], "committed")
            self.assertEqual(first["upserted_rows"], 16)
            self.assertEqual(first["missing_keys_before"].__len__(), 16)
            revised = response(closed=("20261015",))
            second = refresh_calendar(
                db_path=db, receipt_path=receipt, start_date="20261015", requested_end="20261031",
                provider_rows=revised, fetched_at="2026-10-15T12:01:00+08:00", provider_request_sha256="def",
            )
            self.assertEqual(second["changed_existing_keys"], 4)
            conn = sqlite3.connect(db)
            self.assertEqual(conn.execute("SELECT is_open FROM tbl_cn_tradecal WHERE cal_date='20261015' AND exchange='SSE'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT snap_ts FROM tbl_cn_tradecal WHERE cal_date='20261014' AND exchange='SSE'").fetchone()[0], "old")
            conn.close()


if __name__ == "__main__":
    unittest.main()

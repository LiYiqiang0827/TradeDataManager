"""Offline tests: report schema, dated prompt input and distributable resources."""
import json
from pathlib import Path
import tempfile
import unittest
from coreClient.neo4j_client import normalize_trade_date
from theme_graph.daily_limitup_graph.db import initialize_database, integrity_report
from theme_graph.daily_limitup_graph.import_report import import_report
from theme_graph.neo4j.import_sqlite_snapshot import load
from theme_graph.scripts.generate_daily_report import build_prompt

ROOT=Path(__file__).resolve().parents[1]


class DeliveryTests(unittest.TestCase):
    def test_report_sqlite_and_idempotent_view(self):
        with tempfile.TemporaryDirectory() as tmp:
            db=Path(tmp)/'graph.sqlite3'
            for _ in range(2):
                result=import_report(ROOT/'theme_graph/examples/20261016.md',db)
                self.assertEqual(result['stock_edge_count'],2)
            info=integrity_report(db)
            self.assertEqual(info['integrity'],'ok');self.assertEqual(info['foreign_key_errors'],[])
            self.assertEqual(info['counts']['topic_date_stock_edges'],2)
    def test_prompt_time_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet=Path(tmp)/'packet.json';evidence=Path(tmp)/'evidence.json'
            packet.write_text(json.dumps({'trade_date':'20261016','limitups':[{'ts_code':'600000.SH'}]}))
            evidence.write_text(json.dumps([{'published_at':'2026-10-16T17:00:00+08:00','title':'当日盘后'}]))
            self.assertIn('20261016',build_prompt(packet,evidence,[ROOT/'theme_graph/examples/20261015.md']))
            with self.assertRaises(ValueError):build_prompt(packet,None,[ROOT/'theme_graph/examples/20261016.md'])
            evidence.write_text(json.dumps([{'published_at':'2026-10-17T00:01:00+08:00','title':'未来信息'}]))
            with self.assertRaises(ValueError):build_prompt(packet,evidence)
    def test_resources_and_relative_skill_links(self):
        import coreClient.neo4j_client as client
        import theme_graph.daily_limitup_graph.db as db
        self.assertTrue(Path(client.__file__).with_name('neo4j_schema.cypher').is_file())
        self.assertTrue(db.SCHEMA_PATH.is_file())
        self.assertTrue((ROOT/'skills/daily-limitup-analysis-theme-db/references/limit_attribution_v3_2.md').is_file())
    def test_date_validation(self):
        self.assertEqual(normalize_trade_date('2026-10-16'),'20261016')
        with self.assertRaises(ValueError):normalize_trade_date('20260230')
    def test_bad_snapshot_rejected_before_graph_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'bad.json';path.write_text('{}')
            with self.assertRaises(ValueError):load(path)


if __name__=='__main__':unittest.main()

import sqlite3

from core.offline_db_client import get_ctrl_kpl, update_ctrl_kpl


def test_historical_kpl_backfill_preserves_latest_checkpoint():
    with sqlite3.connect(':memory:') as conn:
        conn.execute('CREATE TABLE tbl_kpl_ctrl (key TEXT PRIMARY KEY, max_date TEXT, updated_at TEXT)')
        update_ctrl_kpl(conn, 'cn_kpl_list', '20260930')
        update_ctrl_kpl(conn, 'cn_kpl_list', '20260922')
        assert get_ctrl_kpl('cn_kpl_list', conn=conn) == '20260930'
        update_ctrl_kpl(conn, 'cn_kpl_list', '20261008')
        assert get_ctrl_kpl('cn_kpl_list', conn=conn) == '20261008'

"""
test_checkdb_cursor: STREAM 落盘游标(SQLite 视角)

AI 调用点:
    from test.test_checkdb_cursor import (
        check_cursors,             # 检查(导入名 = 原工具类函数)
        fetch_cursor,             # 拉数据(导入名 = 原工具类函数)
        fetch_all_cursors,             # 拉数据(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_db import check_cursors
from core.query_db import fetch_cursor
from core.query_db import fetch_all_cursors

__all__ = ['check_cursors', 'fetch_cursor', 'fetch_all_cursors']

if __name__ == "__main__":
    print("=== DB CURSORS ===")
    rows = check_cursors()
    print("  共 " + str(len(rows)) + " 条游标")
    for r in rows[:5]:
        print("    " + str(r.get("stream_key")) + ": lag=" + str(r.get("lag")))
    c = fetch_cursor("online:zt:stream")
    print("  fetch_cursor(zt) = " + str(c))
    cs = fetch_all_cursors()
    print("  fetch_all_cursors = " + str(len(cs)) + " 条")

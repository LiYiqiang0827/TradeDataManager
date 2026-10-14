"""
test_checkdb_snapshot: snapshot 当日表

AI 调用点:
    from test.test_checkdb_snapshot import (
        check_snapshot,             # 检查(导入名 = 原工具类函数)
        fetch_snapshot,             # 拉数据(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_db import check_snapshot
from core.query_db import fetch_snapshot

__all__ = ['check_snapshot', 'fetch_snapshot']

if __name__ == "__main__":
    print("=== DB SNAPSHOT ===")
    info = check_snapshot("20260911")
    import json as _json
    print(_json.dumps(info, ensure_ascii=False, indent=2, default=str))
    rows = fetch_snapshot("20260911", limit=3)
    print("  fetch 前 3 条: " + str(len(rows)))
    if rows:
        print("  字段: " + str(list(rows[0].keys())))

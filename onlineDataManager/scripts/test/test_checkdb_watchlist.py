"""
test_checkdb_watchlist: watchlist 当日表

AI 调用点:
    from test.test_checkdb_watchlist import (
        check_watchlist,             # 检查(导入名 = 原工具类函数)
        fetch_watchlist,             # 拉数据(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_db import check_watchlist
from core.query_db import fetch_watchlist

__all__ = ['check_watchlist', 'fetch_watchlist']

if __name__ == "__main__":
    print("=== DB WATCHLIST ===")
    info = check_watchlist("20260912")
    import json as _json
    print(_json.dumps(info, ensure_ascii=False, indent=2, default=str))
    rows = fetch_watchlist("20260912")
    print("  fetch " + str(len(rows)) + " 条")
    if rows:
        print("  字段: " + str(list(rows[0].keys())))

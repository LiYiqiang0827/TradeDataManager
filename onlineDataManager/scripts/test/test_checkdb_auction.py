"""
test_checkdb_auction: auction 当日表

AI 调用点:
    from test.test_checkdb_auction import (
        check_auction,             # 检查(导入名 = 原工具类函数)
        fetch_auction,             # 拉数据(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_db import check_auction
from core.query_db import fetch_auction

__all__ = ['check_auction', 'fetch_auction']

if __name__ == "__main__":
    print("=== DB AUCTION ===")
    info = check_auction("20260911")
    import json as _json
    print(_json.dumps(info, ensure_ascii=False, indent=2, default=str))
    rows = fetch_auction("20260911", limit=3)
    print("  fetch 前 3 条: " + str(len(rows)))

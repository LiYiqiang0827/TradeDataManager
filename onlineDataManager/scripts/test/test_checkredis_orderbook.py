"""
test_checkredis_orderbook: 5档盘口

AI 调用点:
    from test.test_checkredis_orderbook import (
        check_orderbook,             # 检查(导入名 = 原工具类函数)
        check_orderbook_for_stock,             # 检查(导入名 = 原工具类函数)
        fetch_orderbook_window,             # 拉数据(导入名 = 原工具类函数)
        fetch_orderbook_history,             # 拉数据(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_orderbook
from core.check_redis import check_orderbook_for_stock
from core.query_redis import fetch_orderbook_window
from core.query_redis import fetch_orderbook_history

__all__ = ['check_orderbook', 'check_orderbook_for_stock', 'fetch_orderbook_window', 'fetch_orderbook_history']

if __name__ == "__main__":
    print("=== ORDERBOOK 全局 ===")
    check_orderbook()
    print()
    print("=== ORDERBOOK for 600519.SH ===")
    check_orderbook_for_stock("600519.SH")
    print()
    rows = fetch_orderbook_window("600519.SH")
    print("  600519.SH window: " + str(len(rows)) + " bars")
    rows = fetch_orderbook_history(3)
    print("  latest 3: " + str(len(rows)) + " 条")

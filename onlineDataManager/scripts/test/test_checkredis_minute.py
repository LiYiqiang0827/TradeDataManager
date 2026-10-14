"""
test_checkredis_minute: 分时 K 线

AI 调用点:
    from test.test_checkredis_minute import (
        check_minute,             # 检查(导入名 = 原工具类函数)
        check_minute_for_stock,             # 检查(导入名 = 原工具类函数)
        fetch_minute_bars,             # 拉数据(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_minute
from core.check_redis import check_minute_for_stock
from core.query_redis import fetch_minute_bars

__all__ = ['check_minute', 'check_minute_for_stock', 'fetch_minute_bars']

if __name__ == "__main__":
    print("=== MINUTE ===")
    check_minute()
    check_minute_for_stock("600519.SH")
    rows = fetch_minute_bars("600519.SH")
    print("  600519.SH bars: " + str(len(rows)) + " 条")

"""
test_checkdb_overview: db 总览(年/月/kind 列表)

AI 调用点:
    from test.test_checkdb_overview import (
        check_overview,             # 检查(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_db import check_overview

__all__ = ['check_overview']

if __name__ == "__main__":
    print("=== DB OVERVIEW ===")
    info = check_overview()
    import json as _json
    print(_json.dumps(info, ensure_ascii=False, indent=2, default=str))

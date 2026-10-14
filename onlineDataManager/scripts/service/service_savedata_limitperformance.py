"""
service/service_savedata_limitperformance.py
============================================

【2026-09-14 v5 新增】原子化:1 个 service 只落盘 1 个 kind

savedata daemon — 把 Redis online:limitperformance:stream 落盘到 SQLite。
表:limitperformance_<YYYYMMDD>(注意无 `_` 分隔)
PK:UNIQUE(ts_code, limitperformance_timestamp)
15 min/轮(用户需求)
"""


from __future__ import annotations


import sys
from pathlib import Path


SERVICE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SERVICE_DIR.parent))


from service.savedata_loop import SavedataDaemon, main  # noqa: E402


class SavedataLimitperformance(SavedataDaemon):
    KIND = "limitperformance"
    DEFAULT_INTERVAL = 900.0   # 15 分钟(用户要求)


if __name__ == "__main__":
    sys.exit(main(SavedataLimitperformance))
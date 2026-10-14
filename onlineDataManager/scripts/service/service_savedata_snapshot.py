"""
service/service_savedata_snapshot.py
====================================

savedata daemon — 把 Redis online:snapshot:stream STREAM 落盘到 SQLite。

2026-09-16 v6.15 重构后:
- 独立 daemon,scheduler 在 morning_savedata / afternoon_savedata 阶段 spawn
- 15:16 scheduler 阶段切到 post_savedata 时 kill
- 内部 while True + sleep(interval_sec=900,15 分钟)

2026-09-14 v6.3:
- snapshot 新增 timeline + archive,落盘逻辑**不变**,仍只走 STREAM
- archive + timeline 是**查询层**,不参与落盘
- 游标机制(Stream ID)自然兼容 — 午休无新数据则无新落盘
"""

from __future__ import annotations

import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SERVICE_DIR.parent))

from service.savedata_loop import SavedataDaemon, main  # noqa: E402


class SavedataSnapshot(SavedataDaemon):
    KIND = "snapshot"
    DEFAULT_INTERVAL = 900.0   # 15 分钟
    DEFAULT_DRAIN = True       # 2026-09-15 v6.8:daemon 默认 drain,治本(snapshot 数据量大,15 分钟 1 轮太慢)


if __name__ == "__main__":
    sys.exit(main(SavedataSnapshot))

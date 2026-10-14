"""
service/service_savedata_snapshot_index.py
===========================================

snapshot_index 落盘 service(v6.10 2026-09-16 新增)。
纯基类复用:继承 SavedataDaemon + 走 savedata_loop 基类,只设 KIND 常量。

启动:
    python3 -m service.service_savedata_snapshot_index --once

⚠️ SCHEDULER 调度窗口:
    snapshot_index 走独立 4 阶段(snapshot_index_morning/lunch/stopped/closed)
    落盘窗口 09:29-11:45 跑 / 11:45-12:45 sleep / 12:45-15:01 跑(用户原话,2026-09-16)
    间隔由 scheduler 决定,内部循环读 STREAM 累积条目
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from core.logger import setup_logger                  # noqa: E402
from service.savedata_loop import SavedataDaemon, main  # noqa: E402


class SavedataSnapshotIndex(SavedataDaemon):
    """snapshot_index 落盘 service — 纯基类复用,只设 KIND"""
    KIND = "snapshot_index"                            # 2026-09-16 v6.10 新增


if __name__ == "__main__":
    sys.exit(main(SavedataSnapshotIndex))
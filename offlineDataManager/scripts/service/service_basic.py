"""
service_basic.py - 股票基本信息增量更新
用法: python3 service_basic.py [--full]
"""
import sys
import os
import argparse
import time
from pathlib import Path

# 让 service/ 当成包 import core
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from loguru import logger


def main():
    parser = argparse.ArgumentParser(description="Update basic stock info")
    parser.add_argument("--full", action="store_true", help="force full reload")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_basic.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        n = down.update_basic(bFull=args.full)
        logger.info(f"[service_basic] 完成: +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_basic] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

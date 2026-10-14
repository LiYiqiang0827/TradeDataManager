"""
service_cctv_news.py - CCTV 新闻联播增量更新(每天 1 次)
用法: python3 service_cctv_news.py
"""
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from core.news_event_store import repair_cctv_news
from loguru import logger


def main():
    log_file = PROJECT_ROOT / "logs" / "service_cctv_news.log"
    logger.add(str(log_file), rotation="10 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        repair = repair_cctv_news()
        logger.info(f"[service_cctv_news] 历史主键/重复检查: {repair}")
        down = CNDataDown()
        result = down.update_cctv_news()
        logger.info(f"[service_cctv_news] 完成: +{result.get('inserted', 0):,} 行, 用时 {result.get('elapsed', 0):.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_cctv_news] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

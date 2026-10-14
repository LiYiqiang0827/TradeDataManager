"""
service_major_news.py - 长新闻增量更新(每 10 分钟)
用法: python3 service_major_news.py
"""
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from service.service_news_events import main as update_news_events
from loguru import logger


def main():
    log_file = PROJECT_ROOT / "logs" / "service_major_news.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        result = down.update_major_news()
        logger.info(f"[service_major_news] 完成: +{result.get('inserted', 0):,} 行, 用时 {result.get('elapsed', 0):.1f}s")
        # 每日新闻分析只使用 Major News；每次更新后增量刷新派生事件层。
        clean_rc = update_news_events([])
        if clean_rc:
            raise RuntimeError(f"Major News 事件清洗失败, rc={clean_rc}")
        return 0
    except Exception as e:
        logger.error(f"[service_major_news] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

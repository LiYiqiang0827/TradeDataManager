"""
service_news.py - 新闻快讯增量更新(每 10 分钟)
用法: python3 service_news.py
"""
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from loguru import logger


def main():
    log_file = PROJECT_ROOT / "logs" / "service_news.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        # update_news 默认 1 天前起(增量)
        result = down.update_news()
        total = sum(v for v in result.values() if isinstance(v, int))
        logger.info(f"[service_news] 完成: +{total:,} 行, 各源: {result}, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_news] 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

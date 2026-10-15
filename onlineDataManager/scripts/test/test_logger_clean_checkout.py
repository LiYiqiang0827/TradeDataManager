"""The online logger must create ignored runtime directories on a clean clone."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from core import logger as logger_module  # noqa: E402

_REAL_SETUP_LOGGER = logger_module.setup_logger


def test_setup_logger_creates_missing_log_directory(monkeypatch, tmp_path):
    log_dir = tmp_path / "onlineDataManager" / "logs"
    monkeypatch.setattr(logger_module, "LOG_DIR", log_dir)
    name = f"clean-checkout-{tmp_path.name}"
    logger = _REAL_SETUP_LOGGER(name)
    try:
        assert log_dir.is_dir()
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        assert Path(file_handlers[0].baseFilename).is_file()
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()

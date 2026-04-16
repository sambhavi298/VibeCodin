from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def configure_logging(logs_dir: str) -> logging.Logger:
    Path(logs_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("flowpilot")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    file_handler = RotatingFileHandler(Path(logs_dir) / "app.log", maxBytes=1_000_000, backupCount=3)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger

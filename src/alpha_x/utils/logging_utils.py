from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(
    log_dir: Path,
    log_level: str = "INFO",
    logger_name: str = "alpha_x",
) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{logger_name}.log"

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level.upper())
    logger.propagate = False

    if logger.handlers:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

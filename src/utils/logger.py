"""
Centralized logging utility for the Muller AI Pipeline project.
"""
import logging
import os
from datetime import datetime

LOG_DIR = "/tmp/manufacturing_etl_logs"
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y%m%d')}_pipeline.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError as e:
        logger.warning(f"Could not create file log handler, using console only: {e}")

    return logger
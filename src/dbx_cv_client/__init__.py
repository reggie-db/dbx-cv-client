"""Client library for streaming data to Databricks ingestion tables."""

import logging
import os

from dbx_cv_client import models

LEVEL_COLORS = {
    logging.DEBUG: "\033[36m",  # cyan
    logging.INFO: None,  # default (no color)
    logging.WARNING: "\033[33m",  # yellow
    logging.ERROR: "\033[31m",  # red
    logging.CRITICAL: "\033[1;31m",
}
RESET = "\033[0m"


class ColorFormatter(logging.Formatter):
    def format(self, record):
        color = LEVEL_COLORS.get(record.levelno)
        msg = super().format(record)
        return f"{color}{msg}{RESET}" if color else msg


_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def logger(name: str) -> logging.Logger:
    """Returns a configured logger for the given module name."""
    log = logging.getLogger(name or __package__)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO"))

    # Install handler only once
    if not any(isinstance(h, logging.StreamHandler) for h in log.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(ColorFormatter(_LOG_FORMAT))
        log.addHandler(handler)
        log.propagate = False

    return log


__all__ = ["logger", "models"]

"""Client library for streaming data to Databricks ingestion tables."""

import logging
import os

from dbx_cv_client import models

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def logger(name: str) -> logging.Logger:
    """Returns a configured logger for the given module name."""
    log = logging.getLogger(name or __package__)
    log.setLevel(logging.INFO)
    return log


__all__ = ["logger", "models"]

"""Databricks Change Vault client library for streaming data ingestion."""

import logging
import os
import pathlib

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def logger(name: str) -> logging.Logger:
    """Returns a configured logger for the given module name."""
    log = logging.getLogger(name or __package__)
    log.setLevel(logging.INFO)
    return log


def generated_dir() -> pathlib.Path:
    """Returns the path to the generated protobuf directory."""
    return pathlib.Path(__file__).resolve().parent / "generated"

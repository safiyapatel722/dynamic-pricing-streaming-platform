"""
utils/logger.py

Centralized JSON logger for GCP Cloud Logging compatibility.
Logs are written to stdout — Cloud Run captures them automatically.
"""

import logging
import json
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """formats log records as JSON for GCP Cloud Logging."""

    def format(self, record) -> str:
        return json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "message": record.getMessage(),
            "module": record.name
        })


def get_logger(name: str) -> logging.Logger:
    """
    Returns a JSON-formatted logger for the given module.
    Usage: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    # avoid adding duplicate handlers on repeated calls
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
"""Logging setup: human-readable text by default, JSON lines on request."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from riskstream.config import PROJECT_ROOT, get_settings

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JSONFormatter(logging.Formatter):
    """One JSON object per line. Extra fields passed via ``extra=`` are included."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_configured = False


def setup_logging(
    log_level: str | None = None,
    log_format: str | None = None,
    log_file: Path | None = None,
) -> None:
    """Configure the ``riskstream`` logger. Safe to call more than once."""
    global _configured
    settings = get_settings()
    level = (log_level or settings.log_level).upper()
    fmt = log_format or settings.log_format
    log_file = log_file or PROJECT_ROOT / "logs" / "riskstream.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter: logging.Formatter
    if fmt == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-7s %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"
        )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger = logging.getLogger("riskstream")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.propagate = False

    for noisy in ("urllib3", "matplotlib", "mlflow", "alembic", "PIL"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a module logger under the ``riskstream`` namespace."""
    return logging.getLogger(name)

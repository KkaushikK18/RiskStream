"""Logging setup: human-readable text by default, JSON lines on request.

Every record from the ``riskstream`` loggers carries ``request_id`` and
``correlation_id`` (``-`` outside a request). Request payloads, amounts and
account ids are never logged by the service; ``RedactingFilter`` additionally
masks any field whose name looks sensitive if someone passes it via ``extra=``.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from riskstream.config import PROJECT_ROOT, get_settings

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}
SENSITIVE_KEYS = ("api_key", "authorization", "password", "secret", "token", "account_id", "orig_id",
                  "dest_id", "balance", "amount", "database_url")


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        from riskstream.api.context import correlation_id_var, request_id_var

        if not hasattr(record, "request_id"):
            record.request_id = request_id_var.get() or "-"
        if not hasattr(record, "correlation_id"):
            record.correlation_id = correlation_id_var.get() or "-"
        return True


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key in list(record.__dict__):
            if key not in _RESERVED and any(s in key.lower() for s in SENSITIVE_KEYS):
                record.__dict__[key] = "[redacted]"
        return True


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


def setup_logging(
    log_level: str | None = None,
    log_format: str | None = None,
    log_file: Path | None = None,
) -> None:
    """Configure the ``riskstream`` logger. Safe to call more than once."""
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
            "%(asctime)s %(levelname)-7s %(name)s [%(request_id)s] - %(message)s", "%Y-%m-%d %H:%M:%S"
        )

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout),
                                       logging.FileHandler(log_file, encoding="utf-8")]
    handlers[0].setLevel(level)
    handlers[1].setLevel(logging.DEBUG)

    logger = logging.getLogger("riskstream")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    for handler in handlers:
        handler.setFormatter(formatter)
        handler.addFilter(ContextFilter())
        handler.addFilter(RedactingFilter())
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    for noisy in ("urllib3", "matplotlib", "mlflow", "alembic", "PIL"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # Our access log replaces uvicorn's, which would log full paths with ids.
    logging.getLogger("uvicorn.access").disabled = True


def get_logger(name: str) -> logging.Logger:
    """Return a module logger under the ``riskstream`` namespace."""
    return logging.getLogger(name)

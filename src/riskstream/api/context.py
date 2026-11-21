"""Per-request context shared with logging."""

from __future__ import annotations

from contextvars import ContextVar

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)
client_var: ContextVar[str | None] = ContextVar("client", default=None)

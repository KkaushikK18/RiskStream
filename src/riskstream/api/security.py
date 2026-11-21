"""API-key authentication, role checks and per-client rate limiting.

Keys are random 256-bit tokens. The service stores only their SHA-256 hashes
(``API_KEYS`` env var), so a leaked config does not leak usable keys. A fast
hash is appropriate here because keys are high-entropy random values, unlike
passwords. Comparison uses ``hmac.compare_digest``.

Roles:
* ``scorer``  - POST /v1/score
* ``reader``  - GET /v1/transactions/..., /v1/predictions, /v1/model
* ``labeler`` - POST /v1/labels
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import threading
import time
from dataclasses import dataclass

from riskstream.api.errors import AppError
from riskstream.config import ApiKeyEntry
from riskstream.monitoring import metrics

ROLES = ("scorer", "reader", "labeler")


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def generate_key() -> str:
    return "rsk_" + secrets.token_urlsafe(32)


@dataclass(frozen=True)
class Principal:
    name: str
    roles: frozenset[str]
    rate_per_second: float | None


class ApiKeyAuthenticator:
    def __init__(self, entries: list[ApiKeyEntry]) -> None:
        for e in entries:
            unknown = set(e.roles) - set(ROLES)
            if unknown:
                raise ValueError(f"API key {e.name}: unknown roles {sorted(unknown)}")
        self._entries = [(bytes.fromhex(e.sha256.lower()), Principal(e.name, frozenset(e.roles), e.rate_per_second))
                         for e in entries]

    def authenticate(self, key: str | None, role: str) -> Principal:
        if not key:
            metrics.AUTH_FAILURES.labels("missing_key").inc()
            raise AppError(401, "unauthorized", "Missing X-API-Key header",
                           headers={"WWW-Authenticate": "ApiKey"})
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        match = None
        for stored, principal in self._entries:  # constant work regardless of which entry matches
            if hmac.compare_digest(stored, digest):
                match = principal
        if match is None:
            metrics.AUTH_FAILURES.labels("invalid_key").inc()
            raise AppError(401, "unauthorized", "Invalid API key", headers={"WWW-Authenticate": "ApiKey"})
        if role not in match.roles:
            metrics.AUTH_FAILURES.labels("missing_role").inc()
            raise AppError(403, "forbidden", f"API key lacks the '{role}' role")
        return match


class RateLimiter:
    """Token bucket per client, per process.

    With N replicas each enforces its own bucket, so the effective limit is up
    to N times higher. A shared Redis limiter can replace this in Phase 4.
    """

    def __init__(self, per_second: float, burst: int) -> None:
        self.per_second, self.burst = per_second, burst
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def check(self, principal: Principal) -> None:
        rate = principal.rate_per_second or self.per_second
        burst = max(self.burst * rate / self.per_second, 1.0)
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(principal.name, (burst, now))
            tokens = min(burst, tokens + (now - last) * rate)
            if tokens < 1.0:
                self._buckets[principal.name] = (tokens, now)
                retry = max(1, int((1.0 - tokens) / rate + 0.999))
                metrics.RATE_LIMITED.labels(principal.name).inc()
                raise AppError(429, "rate_limited", "Rate limit exceeded", headers={"Retry-After": str(retry)})
            self._buckets[principal.name] = (tokens - 1.0, now)

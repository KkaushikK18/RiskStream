"""FastAPI dependencies: authentication + rate limit, and access to the scoring service."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Request, Security
from fastapi.security import APIKeyHeader

from riskstream.api.context import client_var
from riskstream.api.errors import AppError
from riskstream.api.security import Principal
from riskstream.inference.predictor import ScoringService, ServiceError

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False,
                              description="API key issued with `python -m riskstream.cli create-api-key`")


def require_role(role: str) -> Callable[..., Coroutine[Any, Any, Principal]]:
    # async: authentication and the token bucket are microseconds of CPU, and running
    # in the event-loop task lets the access log see which client made the request.
    async def dependency(request: Request, api_key: str | None = Security(api_key_header)) -> Principal:
        principal = request.app.state.auth.authenticate(api_key, role)
        client_var.set(principal.name)
        request.app.state.limiter.check(principal)
        return principal

    return dependency


def get_service(request: Request) -> ScoringService:
    service = request.app.state.service
    if service is None:
        reason = request.app.state.unavailable_reason or "service is not ready"
        raise AppError(503, "service_unavailable", f"Scoring is unavailable: {reason}",
                       headers={"Retry-After": "5"})
    return service


def raise_service_error(exc: ServiceError) -> None:
    raise AppError(exc.status, exc.code, exc.message) from exc

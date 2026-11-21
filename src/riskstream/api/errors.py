"""Consistent error envelope: ``{"error": {"code", "message", "request_id", "details"}}``.

Internal details (stack traces, SQL, file paths) never reach the client; they
go to the log with the request id, which the client can quote.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from riskstream.api.context import request_id_var
from riskstream.logging_config import get_logger
from riskstream.monitoring import metrics
from riskstream.storage.database import DATABASE_ERRORS

logger = get_logger("riskstream.api.errors")


class AppError(Exception):
    def __init__(self, status: int, code: str, message: str, *, details: list[dict[str, Any]] | None = None,
                 headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message
        self.details, self.headers = details, headers


def error_response(status: int, code: str, message: str, details: list[dict[str, Any]] | None = None,
                   headers: dict[str, str] | None = None) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "request_id": request_id_var.get(), "details": details}}
    return JSONResponse(body, status_code=status, headers=headers)


_HTTP_CODES = {401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed",
               413: "payload_too_large", 415: "unsupported_media_type", 429: "rate_limited"}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError) -> JSONResponse:
        return error_response(exc.status, exc.code, exc.message, exc.details, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        # Echo field locations and messages, never the submitted values.
        details = [{"loc": list(e.get("loc", ())), "msg": e.get("msg"), "type": e.get("type")}
                   for e in exc.errors()]
        return error_response(422, "validation_error", "Request failed validation", details)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _HTTP_CODES.get(exc.status_code, "http_error")
        message = exc.detail if isinstance(exc.detail, str) else code
        return error_response(exc.status_code, code, message, headers=getattr(exc, "headers", None))

    # Handlers for specific classes run inside the app (within the request
    # context). A handler for bare ``Exception`` would run in Starlette's
    # outermost layer and lose the request id, so other unexpected errors are
    # answered by RequestContextMiddleware instead.
    async def _database(_: Request, exc: Exception) -> JSONResponse:
        metrics.DEPENDENCY_ERRORS.labels("database").inc()
        logger.error("Database unavailable: %s", type(exc).__name__)
        return error_response(503, "dependency_unavailable",
                              "The database is unavailable; no decision was made",
                              headers={"Retry-After": "2"})

    for exc_class in DATABASE_ERRORS:
        app.add_exception_handler(exc_class, _database)

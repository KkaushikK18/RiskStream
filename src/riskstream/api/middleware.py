"""Pure-ASGI middleware (no BaseHTTPMiddleware, so streaming and contextvars behave).

Order, outermost first:
1. RequestContextMiddleware - request/correlation ids, access log, HTTP metrics
2. BodySizeLimitMiddleware  - rejects oversized bodies before they are parsed
3. LoadSheddingMiddleware   - caps concurrent /v1 requests; excess gets 503 at once
"""

from __future__ import annotations

import json
import re
import time
import uuid

from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from riskstream.api.context import client_var, correlation_id_var, request_id_var
from riskstream.logging_config import get_logger
from riskstream.monitoring import metrics

logger = get_logger("riskstream.api.access")
_SAFE_ID = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")


def _header(scope: Scope, name: bytes) -> str | None:
    for k, v in scope.get("headers", []):
        if k == name:
            return v.decode("latin-1")
    return None


def _error_body(status: int, code: str, message: str) -> bytes:
    return json.dumps({"error": {"code": code, "message": message, "request_id": request_id_var.get(),
                                 "details": None}}).encode()


async def _send_error(send: Send, status: int, code: str, message: str,
                      extra_headers: list[tuple[bytes, bytes]] | None = None) -> None:
    body = _error_body(status, code, message)
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
    await send({"type": "http.response.start", "status": status, "headers": headers + (extra_headers or [])})
    await send({"type": "http.response.body", "body": body})


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    @staticmethod
    def _route(scope: Scope) -> str:
        # The router stores the matched route in the scope. Using its template
        # (not the raw path) keeps metric label cardinality bounded.
        return getattr(scope.get("route"), "path", None) or "unmatched"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = _header(scope, b"x-request-id")
        request_id = incoming if incoming and _SAFE_ID.match(incoming) else uuid.uuid4().hex
        corr_in = _header(scope, b"x-correlation-id")
        correlation_id = corr_in if corr_in and _SAFE_ID.match(corr_in) else request_id
        t_rid, t_cid, t_cl = request_id_var.set(request_id), correlation_id_var.set(correlation_id), client_var.set(None)
        status_holder = {"status": 500}
        start = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                status_holder["started"] = True
                headers = list(message.get("headers", []))
                headers += [(b"x-request-id", request_id.encode()), (b"x-correlation-id", correlation_id.encode())]
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            # Unhandled error: answer here, inside the request context, so the
            # client still gets the envelope and its request id. Details go to the log only.
            logger.exception("Unhandled error")
            if status_holder["status"] == 500 and not status_holder.get("started"):
                await _send_error(send_wrapper, 500, "internal_error", "Internal server error")
        finally:
            duration = time.perf_counter() - start
            route = self._route(scope)
            method = scope.get("method", "")
            status = status_holder["status"]
            metrics.HTTP_REQUESTS.labels(method, route, str(status)).inc()
            metrics.HTTP_LATENCY.labels(method, route).observe(duration)
            if route not in ("/metrics", "/health/live", "/health/ready"):
                logger.info(
                    "%s %s -> %d (%.1f ms)", method, route, status, duration * 1000,
                    extra={"event": "http_request", "method": method, "route": route, "status": status,
                           "duration_ms": round(duration * 1000, 2), "client": client_var.get()},
                )
            request_id_var.reset(t_rid)
            correlation_id_var.reset(t_cid)
            client_var.reset(t_cl)


class BodySizeLimitMiddleware:
    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app, self.max_bytes = app, max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        declared = _header(scope, b"content-length")
        if declared is not None:
            try:
                too_big = int(declared) > self.max_bytes
            except ValueError:
                await _send_error(send, 400, "bad_request", "Invalid Content-Length")
                return
            if too_big:
                await _send_error(send, 413, "payload_too_large", f"Request body exceeds {self.max_bytes} bytes")
                return

        # No (or a lying) Content-Length, e.g. chunked upload: count while reading.
        received = 0
        started = False

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise _BodyTooLarge
            return message

        async def tracking_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracking_send)
        except _BodyTooLarge:
            if not started:
                await _send_error(send, 413, "payload_too_large", f"Request body exceeds {self.max_bytes} bytes")


class _BodyTooLarge(HTTPException):
    """An HTTPException so FastAPI's body parser re-raises it unchanged (as 413)
    instead of converting it into a generic 400."""

    def __init__(self) -> None:
        super().__init__(413, "Request body too large")


class LoadSheddingMiddleware:
    """Bound concurrent /v1 work. Each request runs in a worker thread, and an
    unbounded queue in front of those threads would turn overload into ever
    growing latency. Rejecting early keeps latency of admitted requests stable."""

    def __init__(self, app: ASGIApp, max_inflight: int) -> None:
        self.app, self.max_inflight = app, max_inflight
        self.inflight = 0  # only touched on the event-loop thread

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not scope.get("path", "").startswith("/v1/"):
            await self.app(scope, receive, send)
            return
        if self.inflight >= self.max_inflight:
            metrics.LOAD_SHED.inc()
            await _send_error(send, 503, "overloaded", "Server is at capacity; retry shortly",
                              [(b"retry-after", b"1")])
            return
        self.inflight += 1
        metrics.HTTP_INFLIGHT.set(self.inflight)
        try:
            await self.app(scope, receive, send)
        finally:
            self.inflight -= 1
            metrics.HTTP_INFLIGHT.set(self.inflight)

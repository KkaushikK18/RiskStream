from __future__ import annotations

import time

from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from riskstream.api.schemas.health import DependencyStatus, LivenessResponse, ReadinessResponse
from riskstream.features.online_store import FeatureStoreError, assess_freshness
from riskstream.monitoring import metrics
from riskstream.storage.database import DATABASE_ERRORS, ping

router = APIRouter(tags=["operations"])


@router.get("/health/live", response_model=LivenessResponse)
async def live(request: Request) -> LivenessResponse:
    """The process is running and the event loop responds. Checks no dependencies,
    so a database outage never makes the orchestrator restart healthy containers."""
    return LivenessResponse(status="alive", instance_id=request.app.state.settings.instance_id)


@router.get("/health/ready", response_model=ReadinessResponse,
            responses={503: {"model": ReadinessResponse, "description": "An essential dependency is down"}})
def ready(request: Request, response: Response) -> ReadinessResponse:
    """Ready to score: a valid model + policy are loaded and the database answers.

    Essential dependencies make the service ``not_ready`` (HTTP 503) when down.
    Optional ones (none yet; Redis in Phase 4) would only make it ``degraded``.
    """
    state = request.app.state
    deps: list[DependencyStatus] = []
    if state.model is not None:
        deps.append(DependencyStatus(name="model", essential=True, status="ok",
                                     detail=f"{state.model.model_version} / policy {state.model.policy.policy_version}"))
    else:
        deps.append(DependencyStatus(name="model", essential=True, status="down", detail=state.model_error))
    if state.engine is None:
        deps.append(DependencyStatus(name="database", essential=True, status="down", detail="DATABASE_URL not set"))
    else:
        try:
            latency = ping(state.engine)
            deps.append(DependencyStatus(name="database", essential=True, status="ok", latency_ms=round(latency, 2)))
        except DATABASE_ERRORS as exc:
            metrics.DEPENDENCY_ERRORS.labels("database").inc()
            deps.append(DependencyStatus(name="database", essential=True, status="down", detail=type(exc).__name__))

    # Optional: Redis online features. If unusable, scoring falls back to PostgreSQL
    # (slower, same semantics), so the service is degraded, not unready.
    store = state.feature_store
    if state.stream_cfg.serving_feature_source == "redis":
        if store is None:
            deps.append(DependencyStatus(name="online_features", essential=False, status="degraded",
                                         detail="REDIS_URL not set; serving PostgreSQL features"))
        else:
            t = time.perf_counter()
            try:
                fresh = assess_freshness(store.read_meta(), state.stream_cfg)
                latency = round((time.perf_counter() - t) * 1000, 2)
                deps.append(DependencyStatus(
                    name="online_features", essential=False, status="ok" if fresh.usable else "degraded",
                    detail=None if fresh.usable else f"falling back to PostgreSQL: {fresh.reason} {fresh.detail}".strip(),
                    latency_ms=latency))
            except FeatureStoreError as exc:
                metrics.DEPENDENCY_ERRORS.labels("redis").inc()
                deps.append(DependencyStatus(name="online_features", essential=False, status="down",
                                             detail=f"Redis unavailable ({type(exc.__cause__).__name__}); "
                                                    "falling back to PostgreSQL"))

    if any(d.essential and d.status != "ok" for d in deps):
        status = "not_ready"
        response.status_code = 503
    elif any(d.status != "ok" for d in deps):
        status = "degraded"
    else:
        status = "ready"
    model = state.model
    return ReadinessResponse(status=status, instance_id=state.settings.instance_id,
                             model_version=model.model_version if model else None,
                             policy_version=model.policy.policy_version if model else None, dependencies=deps)


FRESHNESS_REASONS = ("ok", "not_bootstrapped", "layout_mismatch", "relay_heartbeat_stale",
                     "consumer_heartbeat_stale", "consumer_not_running", "relay_backlog", "consumer_lag",
                     "consumer_stalled", "redis_unavailable", "not_configured")


def _update_online_store_gauges(request: Request) -> None:
    """Freshness verdict as metrics (one HMGET per scrape), so dashboards and alerts
    see exactly what the scoring path decides."""
    state = request.app.state
    if state.stream_cfg.serving_feature_source != "redis":
        return
    reason = "not_configured"
    if state.feature_store is not None:
        try:
            reason = assess_freshness(state.feature_store.read_meta(), state.stream_cfg).reason
        except FeatureStoreError:
            reason = "redis_unavailable"
            metrics.ONLINE_STORE_CHECK_ERRORS.inc()
    metrics.ONLINE_STORE_USABLE.set(1 if reason == "ok" else 0)
    for r in FRESHNESS_REASONS:  # fixed label set: no stale series, no cardinality growth
        metrics.ONLINE_STORE_STATE.labels(r).set(1 if r == reason else 0)


@router.get("/metrics", include_in_schema=False)
def prometheus_metrics(request: Request) -> Response:
    engine = request.app.state.engine
    if engine is not None and hasattr(engine.pool, "checkedout"):
        metrics.DB_POOL_IN_USE.set(engine.pool.checkedout())
        metrics.DB_POOL_SIZE.set(engine.pool.size())
        metrics.DB_POOL_CAPACITY.set(engine.pool.size() + max(0, getattr(engine.pool, "_max_overflow", 0)))
    _update_online_store_gauges(request)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

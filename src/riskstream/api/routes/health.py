from __future__ import annotations

from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from riskstream.api.schemas.health import DependencyStatus, LivenessResponse, ReadinessResponse
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


@router.get("/metrics", include_in_schema=False)
def prometheus_metrics(request: Request) -> Response:
    engine = request.app.state.engine
    if engine is not None and hasattr(engine.pool, "checkedout"):
        metrics.DB_POOL_IN_USE.set(engine.pool.checkedout())
        metrics.DB_POOL_SIZE.set(engine.pool.size())
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

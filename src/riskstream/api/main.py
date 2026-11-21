"""Application factory.

Run locally:
    python -m riskstream.cli serve
    # or: uvicorn riskstream.api.main:app_factory --factory --port 8000

Startup (lifespan) loads the model and policy once, connects the database
pool and records the deployment. If the model cannot be loaded the process
still starts, so ``/health/live`` answers and the failure is visible in
``/health/ready`` (503) and the logs, but ``/v1/score`` returns 503.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import anyio
from fastapi import FastAPI
from sqlalchemy import Engine

from riskstream import __version__
from riskstream.api.errors import install_error_handlers
from riskstream.api.middleware import (
    BodySizeLimitMiddleware,
    LoadSheddingMiddleware,
    RequestContextMiddleware,
)
from riskstream.api.routes import health, labels, scoring, transactions
from riskstream.api.security import ApiKeyAuthenticator, RateLimiter
from riskstream.config import AppConfig, Settings, StreamingConfig, get_settings
from riskstream.features.online_store import RedisFeatureStore
from riskstream.features.registry import get_registry
from riskstream.inference.feature_source import FeatureProvider
from riskstream.inference.model_loader import LoadedModel, ModelLoadError, load_serving_model
from riskstream.inference.predictor import ScoringService
from riskstream.logging_config import get_logger, setup_logging
from riskstream.monitoring import metrics
from riskstream.storage.database import DATABASE_ERRORS, create_db_engine, session_factory, session_scope
from riskstream.storage.repositories.deployments import record_loaded_model

logger = get_logger("riskstream.api")

DESCRIPTION = """
Real-time fraud scoring for a **simulated** payment platform (PaySim-based).
Decisions are a simulated policy, not real payment processing.

Authenticate with an `X-API-Key` header. Roles: `scorer`, `reader`, `labeler`.
Every response carries `X-Request-ID` and `X-Correlation-ID` headers.
"""


def _load_model(app: FastAPI) -> None:
    state = app.state
    try:
        state.model = load_serving_model(state.settings.model_bundle_path, state.settings.policy_path,
                                         state.registry, state.cfg.require_approved_policy)
        state.model_error = None
        metrics.MODEL_LOADED.set(1)
        metrics.MODEL_INFO.labels(state.model.model_version, state.model.policy.policy_version,
                                  state.model.feature_version).set(1)
        logger.info("Loaded model %s with policy %s", state.model.model_version, state.model.policy.policy_version)
    except ModelLoadError as exc:
        state.model, state.model_error = None, str(exc)
        metrics.MODEL_LOADED.set(0)
        logger.error("Model not loaded: %s", exc)


def _record_deployment(app: FastAPI) -> None:
    state, m = app.state, app.state.model
    try:
        with session_scope(state.sessions) as s:
            changed = record_loaded_model(
                s, model_version=m.model_version, policy_version=m.policy.policy_version,
                feature_version=m.feature_version, bundle_path=m.bundle_path, policy_path=m.policy.path,
                actor=state.settings.instance_id)
            s.commit()
        if changed:
            logger.info("Recorded new active deployment %s", m.model_version)
    except DATABASE_ERRORS as exc:
        # Not fatal: readiness reports the database separately.
        logger.warning("Could not record deployment (%s); database unavailable at startup", type(exc).__name__)


def _feature_provider(app: FastAPI) -> FeatureProvider:
    state = app.state
    windows = state.registry.config.windows
    mode = state.stream_cfg.serving_feature_source
    if state.feature_store is None and mode == "redis" and state.settings.redis_url:
        state.feature_store = RedisFeatureStore.from_url(state.settings.redis_url, state.stream_cfg, windows,
                                                         request_path=True)
    if mode == "redis" and state.feature_store is None:
        logger.warning("serving.feature_source=redis but REDIS_URL is not set; using PostgreSQL features")
    return FeatureProvider(windows, mode, state.feature_store)


def _wire(app: FastAPI) -> None:
    state = app.state
    if state.sessions is not None and state.model is not None:
        state.service = ScoringService(state.sessions, state.model, state.registry, state.cfg,
                                       state.settings.instance_id, _feature_provider(app))
        state.unavailable_reason = None
    else:
        state.service = None
        state.unavailable_reason = state.model_error if state.model is None else "database is not configured"


def create_app(settings: Settings | None = None, cfg: AppConfig | None = None, *,
               engine: Engine | None = None, model: LoadedModel | None = None,
               stream_cfg: StreamingConfig | None = None, feature_store: RedisFeatureStore | None = None) -> FastAPI:
    settings = settings or get_settings()
    cfg = cfg or AppConfig.load()
    stream_cfg = stream_cfg or StreamingConfig.load()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        anyio.to_thread.current_default_thread_limiter().total_tokens = cfg.worker_threads
        state = app.state
        if state.engine is None and settings.database_url:
            state.engine = create_db_engine(settings.database_url, cfg)
            state.owns_engine = True
        state.sessions = session_factory(state.engine) if state.engine is not None else None
        if state.model is None:
            await anyio.to_thread.run_sync(_load_model, app)
        if state.model is not None and state.sessions is not None:
            await anyio.to_thread.run_sync(_record_deployment, app)
        _wire(app)
        yield
        if state.owns_engine and state.engine is not None:
            state.engine.dispose()

    app = FastAPI(title="RiskStream Scoring API", version=__version__, description=DESCRIPTION,
                  lifespan=lifespan,
                  openapi_tags=[{"name": "scoring"}, {"name": "transactions"}, {"name": "labels"},
                                {"name": "operations"}])
    state: Any = app.state
    state.settings, state.cfg, state.registry = settings, cfg, get_registry()
    state.stream_cfg, state.feature_store = stream_cfg, feature_store
    state.engine, state.owns_engine, state.sessions = engine, False, None
    state.model, state.model_error = model, None if model else "model not loaded yet"
    state.service, state.unavailable_reason = None, "starting"
    state.auth = ApiKeyAuthenticator(settings.api_keys)
    state.limiter = RateLimiter(cfg.rate_limit_per_second, cfg.rate_limit_burst)
    if model is not None:
        metrics.MODEL_LOADED.set(1)

    install_error_handlers(app)
    for router in (scoring.router, transactions.router, labels.router, health.router):
        app.include_router(router)
    # add_middleware wraps: the last added is the outermost.
    app.add_middleware(LoadSheddingMiddleware, max_inflight=cfg.max_inflight_requests)
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=cfg.max_body_bytes)
    app.add_middleware(RequestContextMiddleware)
    if not settings.api_keys:
        logger.warning("API_KEYS is empty: every /v1 request will be rejected with 401")
    return app


def app_factory() -> FastAPI:
    """Entry point for ``uvicorn riskstream.api.main:app_factory --factory``."""
    setup_logging()
    return create_app()

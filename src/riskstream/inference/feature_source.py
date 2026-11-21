"""Where the scoring path gets feature primitives.

* ``postgres``: the Phase 3 history query (always consistent with committed data).
* ``redis``: the online store, if it is reachable, bootstrapped and the pipeline
  is demonstrably live (heartbeats, backlog, lag). Otherwise the request falls
  back to PostgreSQL. Both sources produce the same primitives for the same
  history, so the fallback changes latency, not feature semantics. Missing
  data is never replaced by zeros.

Every prediction records which source was used and, for fallbacks, why.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from sqlalchemy.orm import Session

from riskstream.features.online_store import FeatureStoreError, FeatureStoreNotUsable, RedisFeatureStore
from riskstream.monitoring import metrics
from riskstream.storage.repositories.feature_history import history_primitives


@dataclass
class PrimitiveResult:
    primitives: dict
    source: str            # redis | postgres | postgres_fallback
    fallback_reason: str | None
    redis_ms: float | None


class FeatureProvider:
    def __init__(self, windows: tuple[int, ...], mode: str = "postgres",
                 store: RedisFeatureStore | None = None) -> None:
        if mode == "redis" and store is None:
            mode = "postgres"
        self.windows, self.mode, self.store = windows, mode, store

    def primitives(self, session: Session, dest_id: str, orig_id: str, step: int) -> PrimitiveResult:
        reason, redis_ms = None, None
        if self.mode == "redis":
            t = time.perf_counter()
            try:
                prims = self.store.primitives(dest_id, orig_id, step)
                redis_ms = (time.perf_counter() - t) * 1000
                metrics.FEATURE_STORE_SECONDS.observe(redis_ms / 1000)
                metrics.FEATURE_SOURCE.labels("redis").inc()
                return PrimitiveResult(prims, "redis", None, redis_ms)
            except FeatureStoreNotUsable as exc:
                reason = exc.reason
            except FeatureStoreError:
                reason = "redis_unavailable"
                metrics.DEPENDENCY_ERRORS.labels("redis").inc()
            redis_ms = (time.perf_counter() - t) * 1000
            metrics.FEATURE_FALLBACKS.labels(reason).inc()
        prims = history_primitives(session, dest_id, orig_id, step, self.windows)
        source = "postgres_fallback" if self.mode == "redis" else "postgres"
        metrics.FEATURE_SOURCE.labels(source).inc()
        return PrimitiveResult(prims, source, reason, redis_ms)

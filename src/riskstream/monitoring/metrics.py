"""Service metrics, exposed at ``GET /metrics``.

Labels are bounded: ``route`` is the route template (``/v1/transactions/{transaction_id}``),
never the raw path, so ids cannot explode label cardinality. One process per
container is assumed (scale with replicas, not uvicorn workers); with several
workers per container, prometheus_client multiprocess mode would be needed.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

LATENCY_BUCKETS = (0.002, 0.005, 0.01, 0.02, 0.035, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0, 2.5, 5.0)
STAGE_BUCKETS = (0.0005, 0.001, 0.002, 0.004, 0.008, 0.015, 0.03, 0.06, 0.125, 0.25, 0.5, 1.0)

HTTP_REQUESTS = Counter(
    "riskstream_http_requests_total", "HTTP requests by route template and status",
    ["method", "route", "status"],
)
HTTP_LATENCY = Histogram(
    "riskstream_http_request_duration_seconds", "Server-side request duration",
    ["method", "route"], buckets=LATENCY_BUCKETS,
)
HTTP_INFLIGHT = Gauge("riskstream_http_inflight_requests", "Requests currently being handled under /v1")
LOAD_SHED = Counter("riskstream_load_shed_total", "Requests rejected because the in-flight limit was reached")
RATE_LIMITED = Counter("riskstream_rate_limited_total", "Requests rejected by the per-client rate limit", ["client"])
AUTH_FAILURES = Counter("riskstream_auth_failures_total", "Rejected API keys or missing roles", ["reason"])

SCORE_STAGE = Histogram(
    "riskstream_score_stage_seconds", "Time per scoring stage", ["stage"], buckets=STAGE_BUCKETS,
)
DECISIONS = Counter(
    "riskstream_decisions_total", "Scoring decisions", ["decision", "model_version"],
)
FRAUD_PROBABILITY = Histogram(
    "riskstream_fraud_probability", "Calibrated fraud probability of scored transactions",
    ["model_version"], buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 0.9, 0.99, 1.0),
)
IDEMPOTENT_REPLAYS = Counter("riskstream_idempotent_replays_total", "Repeated requests answered from storage",
                             ["endpoint"])
CONFLICTS = Counter("riskstream_conflicts_total", "Requests rejected because the id exists with different data",
                    ["endpoint"])
LABELS = Counter("riskstream_labels_total", "Labels recorded", ["source", "is_fraud"])
DEPENDENCY_ERRORS = Counter("riskstream_dependency_errors_total", "Failed calls to dependencies", ["dependency"])
DB_POOL_IN_USE = Gauge("riskstream_db_pool_checked_out", "Database connections currently checked out")
DB_POOL_SIZE = Gauge("riskstream_db_pool_size", "Configured database pool size (excluding overflow)")
DB_POOL_CAPACITY = Gauge("riskstream_db_pool_capacity", "Maximum connections this process can hold (pool + overflow)")
ONLINE_STORE_USABLE = Gauge("riskstream_online_store_usable",
                            "1 when the API may serve Redis features (freshness checks pass), else 0")
ONLINE_STORE_STATE = Gauge("riskstream_online_store_state",
                           "Current online-store freshness verdict (value 1 on the active reason)", ["reason"])
ONLINE_STORE_CHECK_ERRORS = Counter("riskstream_online_store_check_errors_total",
                                    "Failed online-store freshness checks during /metrics scrapes")
MODEL_LOADED = Gauge("riskstream_model_loaded", "1 when a valid model and policy are loaded")
FEATURE_SOURCE = Counter("riskstream_feature_source_total", "Feature primitives served per source",
                         ["source"])  # redis | postgres | postgres_fallback
FEATURE_FALLBACKS = Counter("riskstream_feature_fallbacks_total",
                            "Requests that could not use Redis features, by reason", ["reason"])
FEATURE_STORE_SECONDS = Histogram("riskstream_feature_store_read_seconds", "Redis feature read (one round trip)",
                                  buckets=STAGE_BUCKETS)
MODEL_INFO = Gauge("riskstream_model_info", "Loaded model and policy versions (value is always 1)",
                   ["model_version", "policy_version", "feature_version"])

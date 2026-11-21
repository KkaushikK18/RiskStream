# ADR 0007: Inference service design (Phase 3)

Status: accepted (Phase 3)

## Decisions

### Synchronous handlers in a bounded thread pool
Database and model work runs in plain `def` handlers, which FastAPI executes
in a worker-thread pool (`api.worker_threads`, default 16), so blocking calls
never stall the event loop. One code path serves PostgreSQL and the SQLite
used in unit tests.

*Alternative:* async SQLAlchemy with an async driver. Model inference is CPU
work and would still need a thread, so async adds a second driver and more
complexity for little gain at this scale. Revisit if Phase 9 load tests show
thread-pool saturation before CPU saturation.

### Behavioral features from PostgreSQL until Phase 4
The service reads primitives with one indexed range query per entity
(`storage/repositories/feature_history.py`) and passes them to the shared
`features.derive` formulas. A parity test checks these primitives against the
offline as-of-join implementation. In Phase 4, Redis buckets replace the SQL,
and the `derive` step stays the same.

Consistency in Phase 3: a scored transaction is visible to later feature reads
as soon as its database transaction commits. Features ignore same-hour events,
so concurrent requests for one account never affect each other.

### Fail closed when an essential dependency is down
The database (idempotency, history, persistence) and the model are essential.
If either is unavailable, `/v1/score` returns 503 with `Retry-After` and makes
no decision. The service never approves an unscored payment. What the caller
does with a 503 (hold, route to manual review, retry) is the caller's policy.
`/health/ready` returns 503 so load balancers stop routing to the instance;
`/health/live` stays 200, so the orchestrator does not restart it in a loop.

### Idempotency by `transaction_id` and request hash
`transactions.transaction_id` is unique. The service stores a SHA-256 of the
normalized request. A repeat with the same hash returns the stored decision
(200, `idempotent_replay: true`). A repeat with a different hash returns 409.
A race between two identical requests is resolved by the unique constraint:
the loser rolls back and returns the winner's stored result.

### Transactional outbox
The transaction row, prediction row and a `transactions.scored` event are
written in one database transaction. A relay (Phase 4) publishes outbox rows
to Redpanda, so an event exists if and only if the decision was stored.

### Explicit model and policy versions
`MODEL_BUNDLE_PATH` and `POLICY_PATH` must be set. Startup refuses a bundle
whose feature schema hash differs from the running feature registry, and a
policy tuned for a different model version. Optionally
(`model.require_approved_policy`) it also refuses an unapproved policy.

### API keys stored as hashes, with roles
Keys are 256-bit random tokens. `API_KEYS` holds only SHA-256 hashes, roles
and optional per-client rate limits. Roles are `scorer`, `reader` and
`labeler`. Health endpoints and `/metrics` are unauthenticated and must be
network-restricted in deployment.

### Overload protection
* Per-client token-bucket rate limit (429 with `Retry-After`), per process.
  With N replicas the effective limit is up to N times higher until a shared
  Redis limiter exists.
* Load shedding: more than `api.max_inflight_requests` concurrent `/v1`
  requests get an immediate 503. Waiting in an unbounded queue would only turn
  overload into growing latency for everyone.
* Body limit of 16 KB (413), timeouts on database connections, statements and
  locks, and a 2 s pool wait.

### Data minimization
The API accepts only fields the model uses. Post-transaction and destination
balances are not accepted. Responses mask account ids (`C***0001`). Logs never
contain amounts, balances, account ids or keys, and a test asserts this.

## Consequences
* Each scored request costs about four indexed queries and one write
  transaction. In the parity run (11,805 PaySim transactions, one client),
  total time was p50 31 ms. Feature lookup plus derivation took p50 11.6 ms
  and the model p50 1.6 ms. Most of the rest is the idempotency check and the
  write transaction, so database round trips dominate. Redis features
  (Phase 4) and fewer round trips are the first optimization targets.
* Rate limits are approximate across replicas.
* The history table grows with every transaction. Retention and partitioning
  of `transactions` by step are not implemented yet.

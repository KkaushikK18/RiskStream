# Incident runbooks

One section per alert in `infrastructure/prometheus/alerts.yml`. Each alert's
`runbook_url` annotation points to its section here (anchor = alert name in lower case).
`tests/unit/test_observability_config.py` fails if an alert has no section.

Each section answers: what it means, what users see, how to confirm, how to fix,
and how to verify recovery. Commands assume the repository root in PowerShell and
the local Compose stack (`docker compose --profile app --profile observability ...`).

Useful everywhere:

```powershell
docker compose --profile app --profile observability --profile mlflow ps   # container state
docker compose logs --since 10m <service>                                   # JSON logs (service, instance, request_id)
python -m riskstream.cli stream-status                                       # outbox, lag, topics, online-store verdict
curl.exe -s http://127.0.0.1:8000/health/ready                              # readiness + dependency detail
```

Prometheus alerts page: http://127.0.0.1:9090/alerts. Grafana: http://127.0.0.1:3000
(folder RiskStream).

Severity: **critical** means decisions are failing or at risk now; **warning** means degraded
or trending bad. Local Prometheus has no Alertmanager, so alerts are viewed in Prometheus
and Grafana; routing to a pager is a deployment concern (Phase 9+).

---

## ServiceDown

**Meaning.** Prometheus could not scrape an application process (`tier="app"`: api,
outbox-relay, feature-consumer, scoring-worker) for 1 minute.

**Impact.** Depends on the job. `api`: scoring unavailable. `outbox-relay`: events stop
flowing, Redis features go stale, API falls back to PostgreSQL. `feature-consumer`: same
fallback. `scoring-worker`: replayed transactions are not scored.

**Confirm.** `docker compose ps <service>`; look for `Restarting`, `Exited (137)` (OOM kill)
or `Exited (3)` (feature consumer saw the Redis store disappear; it restarts and rebuilds by design).
`docker compose logs --since 10m <service>`.

**Fix.** Exit 137: see [ProcessMemoryNearLimit](#processmemorynearlimit). Exit 3: wait for the
rebuild (about 2 min) and check `online-bootstrap` progress in the logs. Otherwise
`docker compose --profile app up -d <service>`. If the `app` profile is intentionally not running,
this alert is expected.

**Verify.** `up{job="<job>"}` is 1 in Prometheus; the alert resolves within one scrape.

## InfraExporterDown

**Meaning.** An infrastructure scrape target (`tier="infra"`: redpanda, redis-exporter,
postgres-exporter, prometheus) is unreachable for 1 minute.

**Impact.** Monitoring only: the corresponding dashboard panels go empty and the
dependency alerts for it (PostgresDown, RedisDown, Redpanda*) cannot fire. Scoring is unaffected.

**Confirm.** Prometheus *Status > Targets* shows the error. `docker compose ps redis-exporter postgres-exporter`.

**Fix.** `docker compose --profile observability up -d <exporter>`. postgres-exporter needs
`RISKSTREAM_MONITOR_PASSWORD` in `.env` and the `riskstream_monitor` role
(`infrastructure/postgres/roles.sql`); an authentication error in its log means the
password and the role disagree.

**Verify.** Target is UP; `pg_up` / `redis_up` series are present again.

## PostgresDown

**Meaning.** postgres-exporter reports `pg_up == 0` for 1 minute: PostgreSQL does not accept connections.

**Impact.** Critical. The API fails closed: `/v1/score` returns 503 and readiness is `not_ready`.
The outbox relay cannot read events. No decision is made without being stored.

**Confirm.** `docker compose ps postgres`; `docker compose logs --since 10m postgres`;
`curl.exe -s http://127.0.0.1:8000/health/ready` shows `database: down`.

**Fix.** Restart: `docker compose up -d postgres`. If the container is OOM-killed, check host
memory (`docker stats --no-stream`). If the volume is full, free disk space. Do **not**
delete the `pgdata` volume: it holds the 6.24M-row history.

**Verify.** `pg_up` is 1, readiness is `ready`, a test score returns 201. Clients that got 503
retry with the same `transaction_id`; idempotency makes the retry safe.

## RedisDown

**Meaning.** redis-exporter reports `redis_up == 0` for 1 minute.

**Impact.** Warning. Scoring continues using PostgreSQL features (`feature_source=postgres_fallback`,
higher latency). The feature consumer retries and blocks (no messages lost). When Redis returns
empty (no persistence), the feature consumer rebuilds it from PostgreSQL.

**Confirm.** `docker compose ps redis`; the *Online features usable* panel is NO with reason
`redis_unavailable`.

**Fix.** `docker compose up -d redis`. The feature-consumer container detects the empty store,
exits with code 3, restarts and runs `online-bootstrap --if-needed` (about 2 min), then resumes
consuming from its committed offsets.

**Verify.** `redis_up` is 1; after the rebuild `riskstream_online_store_usable` is 1 and
`riskstream_feature_source_total{source="redis"}` grows again.

## ModelNotLoaded

**Meaning.** An API instance reports `riskstream_model_loaded == 0` for 1 minute.

**Impact.** Critical for that instance: readiness 503, `/v1/score` unavailable.

**Confirm.** `docker compose logs api` shows the load error (missing file, checksum mismatch,
feature-version mismatch, unapproved policy when `require_approved_policy` is true).
`/health/ready` has the detail.

**Fix.** Check `MODEL_BUNDLE_PATH` and `POLICY_PATH` in `.env` point to existing files under
`models/` (mounted read-only into the container). Re-run `tune-policy` if the policy is for a
different model. Restart: `docker compose up -d api`.

**Verify.** `riskstream_model_loaded` is 1 and `riskstream_model_info` shows the expected versions.

## ApiHighErrorRate

**Meaning.** More than 5% of `/v1` requests returned 5xx for 5 minutes (with at least 0.1 req/s).

**Impact.** Critical. Clients see failures; scoring requests that failed must be retried.

**Confirm.** *API & Infrastructure* dashboard, *responses by status*. 503 with
`dependency_unavailable` points at the database ([PostgresDown](#postgresdown),
[DatabaseErrors](#databaseerrors)); 503 from load shedding points at
[ApiLoadShedding](#apiloadshedding); 500 is a bug: find the `request_id` in
`docker compose logs api` (search `"level": "ERROR"`).

**Fix.** Fix the dependency or roll back the last change (model, policy or image).

**Verify.** 5xx ratio panel back below 1%.

## ApiHighLatency

**Meaning.** `/v1/score` server-side p95 above 250 ms for 10 minutes (measured baseline about 40 ms).

**Impact.** Warning. Clients with tight timeouts may give up and retry.

**Confirm.** *Scoring stages p95* shows which stage is slow: `history_query` (PostgreSQL fallback
features or a slow database), `persist` (database writes, locks), `model_inference` (CPU).
Check *Feature source / s*: a high `postgres_fallback` share explains most slowdowns
([FeatureFallbackHigh](#featurefallbackhigh)). Check CPU per service.

**Fix.** Restore Redis features; reduce load; check PostgreSQL for long queries
(`log_min_duration_statement=250` logs them in `docker compose logs postgres`).

**Verify.** p95 panel back near the baseline.

## ApiLoadShedding

**Meaning.** The API rejected requests with 503 because 64 requests were already in flight
(`api.max_inflight_requests`).

**Impact.** Warning. Shed requests are not scored; clients should retry with backoff.

**Confirm.** *In-flight /v1 requests* at the limit; latency usually rises first.

**Fix.** Find what is slow (see [ApiHighLatency](#apihighlatency)). Reduce the sender's rate
(`replay --rate`) or add API replicas. Raising the limit without more capacity only moves the queue.

**Verify.** `riskstream_load_shed_total` stops increasing.

## DatabaseErrors

**Meaning.** API database calls failed in the last 5 minutes (`riskstream_dependency_errors_total{dependency="database"}`).

**Impact.** Critical. Each failure is a 503 to the client (fail closed).

**Confirm.** `docker compose logs api` for `OperationalError` / statement timeout (2 s) / lock
timeout (1 s). If `pg_up` is 0, follow [PostgresDown](#postgresdown). Pool timeouts
(2 s) point at [DbPoolSaturated](#dbpoolsaturated).

**Fix.** Restore the database, end long-running sessions (`pg_stat_activity`), reduce load.

**Verify.** The counter stops increasing; the alert resolves 5 minutes later.

## DbPoolSaturated

**Meaning.** More than 90% of the API's connection pool (pool 8 + overflow 8 = 16) is checked
out for 5 minutes.

**Impact.** Warning. New requests wait up to 2 s for a connection, then fail with 503.

**Confirm.** *API connection pool* panel. Slow queries hold connections longer: check
`history_query` and `persist` stage latency and `docker compose logs postgres`.

**Fix.** Remove the cause of slow queries; reduce load. Increase `database.pool_size` only if
PostgreSQL has headroom (`max_connections=100`; see [PostgresConnectionsHigh](#postgresconnectionshigh)).

**Verify.** Pool usage under 70%.

## OnlineStoreUnusable

**Meaning.** The API has not been allowed to use Redis features for 10 minutes.
`riskstream_online_store_state` shows the reason: `not_bootstrapped`, `relay_heartbeat_stale`,
`consumer_heartbeat_stale`, `consumer_not_running`, `relay_backlog`, `consumer_lag`,
`consumer_stalled`, `redis_unavailable`, `layout_mismatch`, `not_configured`.

**Impact.** Warning. Scoring is correct but uses PostgreSQL features (slower). A rebuild takes
about 2-3.5 min, so 10 minutes means something is stuck.

**Confirm.** *Current freshness verdict* table on the *Streaming* dashboard;
`python -m riskstream.cli stream-status`.

**Fix by reason.**
- `redis_unavailable`: [RedisDown](#redisdown).
- `not_bootstrapped`: the feature consumer should be rebuilding; check its logs. If it is not
  running, `docker compose --profile app up -d feature-consumer`. Manual rebuild:
  `docker compose exec feature-consumer python -m riskstream.cli online-bootstrap`.
- `relay_heartbeat_stale` / `relay_backlog`: [OutboxBacklogStale](#outboxbacklogstale), check the relay.
- `consumer_*`: [FeatureConsumerStalled](#featureconsumerstalled), [ConsumerLagHigh](#consumerlaghigh).
- `layout_mismatch`: the store was built with another key layout; rebuild it.

**Verify.** `riskstream_online_store_usable` is 1. Optionally
`python -m riskstream.cli check-online-store --limit 200` (Redis vs PostgreSQL, expect 0 mismatches).

## FeatureFallbackHigh

**Meaning.** Over 20% of scoring requests used PostgreSQL fallback features for 10 minutes,
while traffic exists.

**Impact.** Warning. Higher latency and database load; decisions are unchanged (same feature logic).

**Confirm.** *Fallbacks / s by reason* panel. A short spike during a rebuild is normal.

**Fix.** By reason, as in [OnlineStoreUnusable](#onlinestoreunusable). Reason `redis_unavailable` while
`redis_up` is 1 usually means the 50 ms request-path timeout was exceeded under load: check
`redis_commands_processed_total` and host CPU.

**Verify.** Fallback ratio near 0.

## ConsumerBlocked

**Meaning.** A consumer has been retrying the same message for 2 minutes after a transient error
(Redis down, API down). By design it does not skip the message.

**Impact.** Critical. Its lag grows. For the feature consumer, Redis features go stale and the
API falls back; for the scoring worker, replayed transactions wait.

**Confirm.** `docker compose logs --since 5m <consumer>` names the error. *Blocked / stalled* panel.

**Fix.** Restore the dependency. The consumer resumes by itself, without restarts or data loss.
Do not reset offsets.

**Verify.** `riskstream_consumer_blocked` is 0 and lag drains.

## FeatureConsumerStalled

**Meaning.** The relay measured from the broker that the feature consumer group has lag but
committed no progress for over 60 s. This works even if the consumer process is dead.

**Impact.** Critical. Redis features are stale; the API already stopped using them after 3 s
(`max_consumer_stall_s`) and falls back to PostgreSQL.

**Confirm.** `docker compose ps feature-consumer`; its logs; `stream-status`.

**Fix.** Dead or hung: `docker compose restart feature-consumer`. Blocked on Redis:
[ConsumerBlocked](#consumerblocked). Bootstrapping: wait for it to finish.

**Verify.** `riskstream_feature_consumer_stalled_seconds` returns to 0.

## ConsumerLagHigh

**Meaning.** A consumer is more than 1000 messages behind for 5 minutes.

**Impact.** Warning. Above 1000 messages of feature-consumer lag the API stops using Redis features.

**Confirm.** *Consumer lag by consumer* and *Messages / s by outcome*. Lag right after a fast
replay that drains steadily is normal.

**Fix.** If it is not draining, see [ConsumerBlocked](#consumerblocked) /
[FeatureConsumerStalled](#featureconsumerstalled). If it drains too slowly, reduce the
replay rate; add consumer instances only up to the partition count (6).

**Verify.** Lag under 100 and falling.

## OutboxBacklogStale

**Meaning.** The oldest unpublished outbox event is older than 60 s for 2 minutes.

**Impact.** Critical. Scored and labeled events are not reaching Redpanda; Redis features
fall behind (API already falls back after 5 s). Nothing is lost: events stay in PostgreSQL.

**Confirm.** `docker compose ps relay`, `docker compose logs --since 10m relay` (publish
failures name the Kafka error). `riskstream_outbox_publish_failures_total` on the *Streaming* dashboard.

**Fix.** Relay down: `docker compose --profile app up -d relay`. Broker errors:
[RedpandaUnavailablePartitions](#redpandaunavailablepartitions), [RedpandaDiskLow](#redpandadisklow).
Several relays may run at once (`SKIP LOCKED`), so a second one is safe.

**Verify.** Backlog drains to 0; oldest-unpublished age under 5 s.

## DeadLetters

**Meaning.** A consumer sent at least one message to `transactions.dlq` in the last 15 minutes.
Only permanent errors go there (invalid payload or schema version, API 400/409/413/422). API 401/403 are treated as
configuration problems: the worker blocks instead of dead-lettering.

**Impact.** Warning. Those events were skipped; for the feature consumer this can mean a
missing feature update for one account.

**Confirm.** `python -m riskstream.cli dlq-inspect --limit 20` shows the error class and payload.

**Fix.** Fix the producer or the bug. After the fix, re-publish:
`python -m riskstream.cli dlq-replay --limit 20 --yes` (consumers deduplicate by event id,
so replays are safe). If features may be affected, compare with
`python -m riskstream.cli check-online-store`.

**Verify.** No new dead letters; `check-online-store` shows 0 mismatches.

## RedpandaUnavailablePartitions

**Meaning.** Redpanda reports partitions without a leader for 1 minute.

**Impact.** Critical. Producers and consumers on those partitions stop; the relay backlog grows.

**Confirm.** `docker compose exec redpanda rpk cluster health`, `docker compose logs redpanda`.

**Fix.** `docker compose restart redpanda` (single node, so restart is the only recovery).
Check its memory limit and disk.

**Verify.** `rpk cluster health` shows `Healthy: true`; relay backlog drains.

## PostgresConnectionsHigh

**Meaning.** Over 80% of `max_connections` (100) are in use for 5 minutes.

**Impact.** Warning. At 100% new connections fail: API 503s, relay and exporter errors.

**Confirm.** *PostgreSQL connections by state*. Many `idle in transaction` sessions mean a
client holds transactions open.

**Fix.** Find the client in `pg_stat_activity` (`application_name`, `client_addr`) and fix or
restart it. Leftover local test or notebook sessions are a common cause.

**Verify.** Usage under 60%.

## RedisMemoryHigh

**Meaning.** Redis uses more than 85% of `maxmemory` (640 MB) for 10 minutes.

**Impact.** Warning. With `noeviction`, writes fail at 100%: the feature consumer blocks and
the API falls back to PostgreSQL. Measured size of the full PaySim store is about 289 MB.

**Confirm.** *Redis memory* panel and `redis_db_keys`. Unexpected keys (for example
`rs:test:*` from killed test runs) are visible with `redis-cli --scan --pattern 'rs:test:*'`.

**Fix.** Remove leftover test keys (the test suite also cleans them after 10 minutes).
Raise `--maxmemory` and the container limit together if the dataset really grew.

**Verify.** Memory under 70% of maxmemory.

## ProcessMemoryNearLimit

**Meaning.** A RiskStream process is above about 85% of its container memory limit for 5 minutes
(api 768 MB, feature-consumer 1536 MB, relay and scoring-worker 384 MB).

**Impact.** Warning. At the limit the kernel kills the container (exit 137) and Compose restarts it.

**Confirm.** *Resident memory by service*; `docker stats --no-stream`. The feature consumer
peaks around 850 MB during a bootstrap; that is expected.

**Fix.** Find what grew (load, a bootstrap, a leak after many hours). Raise the limit in
`docker-compose.yml` only if the host has free memory.

**Verify.** Memory stable below the threshold.

## RedpandaDiskLow

**Meaning.** Redpanda's low-disk alert is on for 5 minutes.

**Impact.** Warning; at the hard limit producers are rejected and the relay backlog grows.

**Confirm.** `docker system df`, `docker compose exec redpanda df -h /var/lib/redpanda/data`.

**Fix.** Free Docker disk space (`docker image prune`, old volumes). Topic retention is 7 days;
lower it for a local stack if needed.

**Verify.** `redpanda_storage_disk_free_space_alert` is 0.

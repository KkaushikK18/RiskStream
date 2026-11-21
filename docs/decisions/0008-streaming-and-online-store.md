# ADR 0008: Outbox relay, Redpanda and a Redis online store with PostgreSQL fallback

Status: accepted (Phase 4)

## Decisions

1. **Transactional outbox + polling relay** instead of publishing from the
   request handler. The decision and its event commit together. A
   fire-and-forget publish could lose events on a crash, or publish events
   for decisions that were rolled back. Alternative: change data capture
   (Debezium). It is more moving parts than this project needs; polling
   with `SKIP LOCKED` is enough at these rates.
2. **confluent-kafka (librdkafka)** client: an idempotent producer, consumer
   groups with cooperative rebalancing, and wheels for Python 3.14 on Windows.
3. **Redis primitives, not features.** Redis stores sufficient statistics.
   The formulas stay in `features/derive.py`, which is shared with training,
   so there is still one implementation of each feature.
4. **One Lua script per event** applies dedup, lifetime stats, window buckets
   and pruning atomically. Concurrent consumers are safe without locks.
5. **Hash-bucketed lifetime stats.** One key per account would need about
   1 GB for 8.9M accounts. Packing them into listpack hashes needed 289 MB
   (measured).
6. **Snapshot bootstrap with event-id dedup**, so a rebuild needs no downtime
   and does not double-count.
7. **Redis is optional; PostgreSQL is the source of truth.** The API uses Redis
   only when freshness checks pass, records the source on every prediction,
   and never substitutes zeros.
8. **Scoring worker calls the HTTP API**, so there is one scoring
   implementation and retries rely on its idempotency.
9. **Transient errors block; permanent errors go to the DLQ.** Dead-lettering
   during an outage would turn a temporary problem into manual repair work.

## Measured (2026-11-21, laptop, single instances)

| check | result |
|---|---|
| tests | 184 passed (incl. 9 against live Redis + Redpanda) |
| bootstrap | 6,257,213 rows, 2,675,105 destination + 6,248,175 origin accounts, 229,755 window hashes in 276 s; Redis 289 MB |
| backlog published by relay | 12,739 existing events; all recognised as duplicates of the snapshot |
| Redis vs PostgreSQL, step 599 (read only) | 25,424 primitives, 0 mismatches; read p50 2.9 ms vs 10.2 ms |
| end to end, steps 599-600 (`replay --barrier`) | 1,834 transactions via worker -> API -> outbox -> relay -> consumer; 62,356 features vs offline, 0 mismatches; 1,833 Redis, 1 fallback |
| feature time per request (stored) | Redis p50 5.75 ms; PostgreSQL fallback p50 75-97 ms during these runs |
| Redis paused, step 601 | 865 requests scored with PostgreSQL fallback, 0 mismatches; consumer blocked, then applied all 865 after resume |
| consumer killed, steps 605-606 | 5 fallbacks after detection, 0 mismatches; restarted consumer applied the 14 pending events |
| poison message | 1 record in `transactions.dlq`, stream continued |

The barrier run published at 15 events/s because it waited for each hour
(108 s of 120 s were barrier waits). Throughput is not measured here (Phase 9).

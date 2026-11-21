"""Event streaming: schemas, Kafka helpers, outbox relay, consumers, replay, dead letters.

Delivery guarantees (see docs/streaming.md):
* API -> outbox: a decision and its event commit in one PostgreSQL transaction.
* outbox -> Kafka: at-least-once (relay may republish after a crash).
* Kafka -> consumers: at-least-once; offsets are committed after processing.
* Effects are idempotent: the feature consumer dedups by event_id inside the
  same Redis script as the update; the scoring worker relies on the API's
  transaction_id idempotency. There is no exactly-once across systems.
"""

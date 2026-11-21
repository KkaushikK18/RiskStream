"""Metrics for streaming workers (relay, consumers, replay) and the online store.

Each worker process exposes its own ``/metrics`` (``--metrics-port``).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

MESSAGES = Counter("riskstream_stream_messages_total", "Messages handled by a consumer",
                   ["consumer", "outcome"])  # applied | duplicate | dead_lettered | retried | blocked
PROCESS_SECONDS = Histogram("riskstream_stream_process_seconds", "Processing time per message", ["consumer"],
                            buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
END_TO_END_SECONDS = Histogram(
    "riskstream_stream_event_age_seconds", "Event age when processed (now - event creation time)", ["consumer"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300))
LAG = Gauge("riskstream_consumer_lag_messages", "Consumer lag per partition", ["consumer", "partition"])
STATE = Gauge("riskstream_consumer_blocked", "1 while the consumer is blocked on a transient error", ["consumer"])
DLQ = Counter("riskstream_dlq_messages_total", "Messages sent to the dead-letter topic", ["consumer", "error_class"])

RELAY_PUBLISHED = Counter("riskstream_outbox_published_total", "Outbox events published", ["topic"])
RELAY_FAILED = Counter("riskstream_outbox_publish_failures_total", "Outbox publish failures", ["topic"])
RELAY_BACKLOG = Gauge("riskstream_outbox_backlog_events", "Unpublished outbox events")
RELAY_OLDEST_AGE = Gauge("riskstream_outbox_oldest_unpublished_age_seconds", "Age of the oldest unpublished event")
RELAY_BATCH_SECONDS = Histogram("riskstream_outbox_batch_seconds", "Time to publish and mark one batch",
                                buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))

FEATURE_CONSUMER_BROKER_LAG = Gauge("riskstream_feature_consumer_broker_lag_messages",
                                    "Feature consumer group lag measured from the broker by the relay")
FEATURE_CONSUMER_STALLED = Gauge("riskstream_feature_consumer_stalled_seconds",
                                 "Seconds the feature consumer group has lagged without committing progress")

REPLAY_PUBLISHED = Counter("riskstream_replay_published_total", "Events published by the replay tool")

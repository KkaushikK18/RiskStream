"""Thin wrappers around confluent-kafka (librdkafka): producer, consumer, topic admin, lag."""

from __future__ import annotations

import time
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from riskstream.config import StreamingConfig
from riskstream.logging_config import get_logger

logger = get_logger(__name__)


def producer(bootstrap: str, cfg: StreamingConfig, client_id: str) -> Producer:
    p = cfg.producer
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "acks": p["acks"],
        "enable.idempotence": bool(p["enable_idempotence"]),
        "linger.ms": int(p["linger_ms"]),
        "delivery.timeout.ms": int(p["delivery_timeout_ms"]),
        "compression.type": "lz4",
    })


def consumer(bootstrap: str, cfg: StreamingConfig, group_id: str, client_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "client.id": client_id,
        # At-least-once: offsets are stored explicitly AFTER processing and
        # auto-committed from that store (never before processing).
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": cfg.session_timeout_ms,
        "max.poll.interval.ms": cfg.max_poll_interval_ms,
        "partition.assignment.strategy": "cooperative-sticky",
    })


def all_topics(cfg: StreamingConfig) -> list[str]:
    return [cfg.topic_ingested, cfg.topic_scored, cfg.topic_labeled, cfg.topic_dlq]


def ensure_topics(bootstrap: str, cfg: StreamingConfig, timeout_s: float = 20.0) -> dict[str, str]:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    wanted = [NewTopic(t, num_partitions=cfg.partitions, replication_factor=cfg.replication_factor,
                       config={"retention.ms": str(cfg.retention_ms), "cleanup.policy": "delete"})
              for t in all_topics(cfg) if t not in existing]
    result = {t: "exists" for t in all_topics(cfg) if t in existing}
    if wanted:
        for topic, fut in admin.create_topics(wanted, operation_timeout=timeout_s).items():
            try:
                fut.result()
                result[topic] = "created"
            except KafkaException as exc:
                result[topic] = f"error: {exc}"
    return result


def consumer_lag(c: Consumer, timeout_s: float = 5.0) -> dict[int, int]:
    """Lag per assigned partition = high watermark - committed offset."""
    assigned = c.assignment()
    if not assigned:
        return {}
    committed = c.committed(assigned, timeout=timeout_s)
    lag: dict[int, int] = {}
    for tp in committed:
        _, high = c.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=timeout_s, cached=False)
        offset = tp.offset if tp.offset >= 0 else 0
        lag[tp.partition] = max(0, high - offset)
    return lag


class GroupLagProbe:
    """Measures a consumer group's lag from the broker (committed offsets vs high
    watermarks), so a dead or stuck consumer is visible even though it can no
    longer report anything itself. Also tracks how long the group's committed
    offsets have not advanced while lag > 0 (stall time)."""

    def __init__(self, bootstrap: str, group_id: str, topic: str, timeout_s: float = 5.0) -> None:
        self.topic, self.timeout_s = topic, timeout_s
        self.c = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id, "enable.auto.commit": False})
        self._last_committed: int | None = None
        self._progress_at = time.monotonic()

    def measure(self) -> tuple[int, float]:
        """(total lag, seconds since the group last made progress while lagging)."""
        md = self.c.list_topics(self.topic, timeout=self.timeout_s).topics[self.topic]
        tps = [TopicPartition(self.topic, p) for p in md.partitions]
        committed_total, lag_total = 0, 0
        for tp in self.c.committed(tps, timeout=self.timeout_s):
            low, high = self.c.get_watermark_offsets(TopicPartition(self.topic, tp.partition),
                                                     timeout=self.timeout_s, cached=False)
            offset = tp.offset if tp.offset >= 0 else low
            committed_total += offset
            lag_total += max(0, high - offset)
        now = time.monotonic()
        if lag_total == 0 or committed_total != self._last_committed:
            self._progress_at = now
        self._last_committed = committed_total
        return lag_total, now - self._progress_at

    def close(self) -> None:
        self.c.close()


def group_lag(bootstrap: str, group_id: str, topic: str, timeout_s: float = 10.0) -> dict[str, Any]:
    """Lag of a consumer group without joining it (for status commands)."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id, "enable.auto.commit": False})
    try:
        md = c.list_topics(topic, timeout=timeout_s).topics[topic]
        tps = [TopicPartition(topic, p) for p in md.partitions]
        committed = c.committed(tps, timeout=timeout_s)
        per = {}
        for tp in committed:
            low, high = c.get_watermark_offsets(tp, timeout=timeout_s)
            per[tp.partition] = {"committed": tp.offset, "high": high,
                                 "lag": high - (tp.offset if tp.offset >= 0 else low)}
        return {"group": group_id, "topic": topic, "total_lag": sum(v["lag"] for v in per.values()),
                "partitions": per}
    finally:
        c.close()

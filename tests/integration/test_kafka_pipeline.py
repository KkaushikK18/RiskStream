"""Feature consumer against real Redpanda + Redis: delivery, redelivery after
restart, republished duplicates, and the dead-letter topic. Uses unique topic
names and consumer groups per test.

    $env:RISKSTREAM_TEST_KAFKA = "127.0.0.1:19092"
    $env:RISKSTREAM_TEST_REDIS_URL = "redis://:<password>@127.0.0.1:6379/15"
"""

from __future__ import annotations

import json
import os
import time
import uuid

import polars as pl
import pytest

KAFKA = os.environ.get("RISKSTREAM_TEST_KAFKA")
REDIS = os.environ.get("RISKSTREAM_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not (KAFKA and REDIS), reason="RISKSTREAM_TEST_KAFKA / _REDIS_URL not set")


@pytest.fixture
def env(small_canonical):
    from riskstream.config import StreamingConfig
    from riskstream.features.online_bootstrap import bootstrap
    from riskstream.features.online_store import RedisFeatureStore
    from riskstream.streaming.kafka import ensure_topics

    tag = uuid.uuid4().hex[:8]
    cfg = StreamingConfig.load(namespace=f"rs:test:{tag}", stat_buckets=64, partitions=3,
                               topic_scored=f"test.{tag}.scored", topic_dlq=f"test.{tag}.dlq",
                               topic_ingested=f"test.{tag}.ingested", topic_labeled=f"test.{tag}.labeled",
                               max_attempts=2, backoff_initial_s=0.05, pause_on_failure_s=0.1)
    ensure_topics(KAFKA, cfg)
    store = RedisFeatureStore.from_url(REDIS, cfg, (24, 168))
    bootstrap(store, frame=small_canonical.filter(pl.col("step") <= 1))
    yield cfg, store, tag
    store.clear_namespace()
    from confluent_kafka.admin import AdminClient

    AdminClient({"bootstrap.servers": KAFKA}).delete_topics(
        [cfg.topic_scored, cfg.topic_dlq, cfg.topic_ingested, cfg.topic_labeled])


def scored(i: int, step: int = 10, dest: str = "CK") -> dict:
    return {"event_id": f"evt-{i:06d}-abcdef", "schema_version": 1, "transaction_id": f"tx{i}",
            "event_time": "2026-01-01T09:00:00+00:00", "step": step, "type": "TRANSFER", "amount": 10.0,
            "orig_account_id": f"O{i}", "dest_account_id": dest, "dest_type": "CUSTOMER", "fraud_probability": 0.1,
            "decision": "APPROVE", "model_version": "m", "policy_version": "p",
            "scored_at": "2026-11-21T00:00:00+00:00"}


def publish(cfg, payloads, raw: list[bytes] = ()):
    from riskstream.streaming.kafka import producer

    p = producer(KAFKA, cfg, "test")
    for pl_ in payloads:
        p.produce(cfg.topic_scored, key=pl_["dest_account_id"].encode(), value=json.dumps(pl_).encode())
    for r in raw:
        p.produce(cfg.topic_scored, key=b"CK", value=r)
    assert p.flush(30) == 0


def consume(cfg, store, group, **run_kw):
    from riskstream.streaming.consumers import ConsumerRunner, FeatureUpdateProcessor
    from riskstream.streaming.dead_letter import DeadLetterPublisher
    from riskstream.streaming.kafka import consumer, producer

    c = consumer(KAFKA, cfg, group, "test-consumer")
    c.subscribe([cfg.topic_scored])
    runner = ConsumerRunner(c, FeatureUpdateProcessor(store),
                            DeadLetterPublisher(producer(KAFKA, cfg, "t-dlq"), cfg.topic_dlq, "feature-updater",
                                                group), cfg)
    return runner.run(**run_kw)


def dlq_count(cfg) -> int:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"x-{uuid.uuid4().hex}"})
    md = c.list_topics(cfg.topic_dlq, timeout=10).topics[cfg.topic_dlq]
    n = sum(c.get_watermark_offsets(TopicPartition(cfg.topic_dlq, p), timeout=10)[1] for p in md.partitions)
    c.close()
    return n


def test_events_applied_once_across_restart_and_republish(env):
    cfg, store, tag = env
    group = f"g-{tag}"
    events = [scored(i) for i in range(50)]
    publish(cfg, events)
    first = consume(cfg, store, group, max_messages=20)            # "crash" after 20, offsets committed on close
    assert first.get("applied") == 20
    second = consume(cfg, store, group, idle_exit_s=3)             # restart: continues from committed offset
    assert second.get("applied", 0) == 30
    publish(cfg, events[:10])                                      # relay republishes 10 after a crash
    third = consume(cfg, store, group, idle_exit_s=3)
    assert third.get("duplicate") == 10 and "applied" not in third
    p = store.primitives("CK", "Onew", 11, check_freshness=False)
    assert p["dest_p_cnt_24"] == 50 and store.applied_count(10) == 50


def test_poison_message_goes_to_dlq_and_stream_continues(env):
    cfg, store, tag = env
    publish(cfg, [scored(1)], raw=[b"{not json", json.dumps({"event_id": "x" * 10}).encode()])
    publish(cfg, [scored(2)])
    out = consume(cfg, store, f"g-{tag}", idle_exit_s=3)
    assert out.get("applied") == 2 and out.get("dead_lettered") == 2
    time.sleep(0.5)
    assert dlq_count(cfg) == 2


def test_redis_outage_blocks_without_losing_events(env, monkeypatch):
    """While Redis is failing the consumer retries and rewinds; nothing is committed
    or dead-lettered. When Redis recovers the same events are applied."""
    from riskstream.features.online_store import FeatureStoreError

    cfg, store, tag = env
    publish(cfg, [scored(i) for i in range(5)])
    real = store.apply
    state = {"fail": 4}

    def flaky(**kw):
        if state["fail"] > 0:
            state["fail"] -= 1
            raise FeatureStoreError("connection refused")
        return real(**kw)

    monkeypatch.setattr(store, "apply", flaky)
    out = consume(cfg, store, f"g-{tag}", idle_exit_s=4)
    assert out.get("blocked", 0) >= 1 and out.get("applied") == 5 and "dead_lettered" not in out
    assert dlq_count(cfg) == 0

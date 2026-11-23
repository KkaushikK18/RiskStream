"""Leftover cleanup against the real broker and Redis: stale test resources are
deleted, fresh test resources and every real topic / key survive."""

from __future__ import annotations

import os
import time

import pytest

from tests.integration import leftovers as lo

KAFKA = os.environ.get("RISKSTREAM_TEST_KAFKA")
REDIS = os.environ.get("RISKSTREAM_TEST_REDIS_URL")


@pytest.mark.skipif(not KAFKA, reason="RISKSTREAM_TEST_KAFKA not set")
def test_live_topic_cleanup():
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA})
    old = lo.topic_names(lo.new_tag(now=time.time() - lo.MAX_AGE_S - 60))["scored"]
    fresh = lo.topic_names(lo.new_tag())["scored"]
    for fut in admin.create_topics([NewTopic(t, 1, 1) for t in (old, fresh)]).values():
        fut.result()
    before = set(admin.list_topics(timeout=10).topics)
    real = {t for t in before if t.startswith("transactions.")}
    try:
        deleted = lo.delete_stale_topics(KAFKA)
        time.sleep(1)
        after = set(admin.list_topics(timeout=10).topics)
        assert old in deleted and old not in after
        assert fresh in after and fresh not in deleted
        assert real <= after and not any(t.startswith("transactions.") for t in deleted)
    finally:
        for t in (old, fresh):
            if t in set(admin.list_topics(timeout=10).topics):
                admin.delete_topics([t])[t].result()


@pytest.mark.skipif(not REDIS, reason="RISKSTREAM_TEST_REDIS_URL not set")
def test_live_redis_cleanup():
    import redis

    r = redis.Redis.from_url(REDIS, decode_responses=True)
    old_ns = lo.namespace(lo.new_tag(now=time.time() - lo.MAX_AGE_S - 60))
    fresh_ns = lo.namespace(lo.new_tag())
    keys = {f"{old_ns}:meta": "x", f"{old_ns}:dw:C1": "x", f"{fresh_ns}:meta": "x", "rs:v1:meta": "probe"}
    for k, v in keys.items():
        r.set(k, v)
    try:
        assert lo.delete_stale_keys(REDIS) == 2
        assert not r.exists(f"{old_ns}:meta") and not r.exists(f"{old_ns}:dw:C1")
        assert r.exists(f"{fresh_ns}:meta") and r.get("rs:v1:meta") == "probe"
    finally:
        r.delete(*keys)
        r.close()

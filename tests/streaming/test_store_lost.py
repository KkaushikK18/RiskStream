"""The feature consumer exits (so its supervisor can re-bootstrap) when the store is wiped."""

import pytest

pytest.importorskip("confluent_kafka")

from riskstream.config import StreamingConfig  # noqa: E402
from riskstream.features.online_store import FeatureStoreNotUsable  # noqa: E402
from riskstream.streaming.consumers import (  # noqa: E402
    ConsumerRunner,
    FeatureUpdateProcessor,
    StoreLost,
    TransientError,
)
from riskstream.streaming.dead_letter import DeadLetterPublisher  # noqa: E402
from tests.streaming.fakes import FakeConsumer, FakeMessage, FakeProducer  # noqa: E402

CFG = StreamingConfig.load(max_attempts=2, backoff_initial_s=0.0, pause_on_failure_s=0.0, heartbeat_interval_s=0.0)


class FakeRedis:
    def __init__(self, lock=False):
        self.lock = lock

    def exists(self, key):
        return 1 if self.lock else 0


class WipedStore:
    ns = "rs:v1"

    def __init__(self, lock=False):
        self.r = FakeRedis(lock)
        self.meta = {}

    def apply(self, **kw):
        raise FeatureStoreNotUsable("not_bootstrapped")

    def is_bootstrapped(self):
        return False

    def write_meta(self, **kw):
        self.meta |= kw


def scored():
    return FakeMessage({"event_id": "e" * 16, "schema_version": 1, "transaction_id": "t", "step": 5,
                        "event_time": "2026-01-01T00:00:00+00:00", "type": "TRANSFER", "amount": 1.0,
                        "orig_account_id": "O", "dest_account_id": "D", "dest_type": "CUSTOMER",
                        "fraud_probability": 0.1, "decision": "APPROVE", "model_version": "m",
                        "policy_version": "p", "scored_at": "2026-11-21T00:00:00+00:00"})


def test_wiped_store_exits_and_commits_nothing_unprocessed():
    consumer = FakeConsumer([scored()])
    runner = ConsumerRunner(consumer, FeatureUpdateProcessor(WipedStore(), exit_on_store_lost=True),
                            DeadLetterPublisher(FakeProducer(), "dlq", "f", "g"), CFG, sleep=lambda s: None)
    with pytest.raises(StoreLost):
        runner.run(idle_exit_s=0)
    assert consumer.stored == [] and consumer.closed


def test_idle_consumer_detects_wipe_on_heartbeat():
    consumer = FakeConsumer([])
    runner = ConsumerRunner(consumer, FeatureUpdateProcessor(WipedStore(), exit_on_store_lost=True),
                            DeadLetterPublisher(FakeProducer(), "dlq", "f", "g"), CFG, sleep=lambda s: None)
    with pytest.raises(StoreLost):
        runner.run(idle_exit_s=5)
    assert consumer.closed


def test_running_bootstrap_means_wait_not_exit():
    p = FeatureUpdateProcessor(WipedStore(lock=True), exit_on_store_lost=True)
    with pytest.raises(TransientError):
        p.handle(scored())
    p.heartbeat("running", 0)  # no StoreLost while another process rebuilds


def test_without_flag_it_only_retries():
    p = FeatureUpdateProcessor(WipedStore(), exit_on_store_lost=False)
    with pytest.raises(TransientError):
        p.handle(scored())

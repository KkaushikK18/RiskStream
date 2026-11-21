"""Outbox relay on SQLite with a fake producer (no broker)."""

import json

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from riskstream.config import StreamingConfig
from riskstream.storage.admin import upgrade
from riskstream.storage.repositories.events import add_outbox_event
from riskstream.streaming.outbox_relay import OutboxRelay
from tests.streaming.fakes import FakeProducer

CFG = StreamingConfig.load(relay_batch_size=3)


@pytest.fixture
def engine(tmp_path):
    url = f"sqlite:///{(tmp_path / 'o.db').as_posix()}"
    upgrade(url)
    e = create_engine(url)
    with Session(e) as s:
        for i in range(5):
            add_outbox_event(s, "transactions.scored", f"D{i % 2}", {"transaction_id": f"t{i}", "step": 1})
        s.commit()
    yield e
    e.dispose()


def unpublished(engine) -> int:
    with engine.connect() as c:
        return c.execute(text("SELECT count(*) FROM outbox_events WHERE published_at IS NULL")).scalar_one()


def test_publishes_in_id_order_with_headers(engine):
    p = FakeProducer()
    relay = OutboxRelay(engine, p, CFG)
    assert relay.publish_batch() == {"published": 3, "failed": 0}
    assert relay.publish_batch() == {"published": 2, "failed": 0}
    assert relay.publish_batch() == {"published": 0, "failed": 0}
    assert [json.loads(v)["transaction_id"] for _, _, v, _ in p.sent] == [f"t{i}" for i in range(5)]
    topic, key, value, headers = p.sent[0]
    assert topic == "transactions.scored" and key == b"D0"
    assert dict(headers)["event_id"].decode() == json.loads(value)["event_id"]
    assert unpublished(engine) == 0
    assert relay.backlog()[0] == 0


def test_failed_delivery_stays_unpublished(engine):
    relay = OutboxRelay(engine, FakeProducer(fail_keys={b"D1"}), CFG)
    out = relay.publish_batch()
    assert out == {"published": 2, "failed": 1}
    with engine.connect() as c:
        row = c.execute(text("SELECT attempts, last_error FROM outbox_events WHERE event_key='D1' ORDER BY id")).first()
    assert row.attempts == 1 and "broker down" in row.last_error
    assert unpublished(engine) == 3


def test_crash_before_marking_republishes_same_event_ids(engine, monkeypatch):
    """At-least-once: a crash after produce but before the UPDATE re-sends the rows,
    with the same event_id, so consumers can deduplicate."""
    p = FakeProducer()
    relay = OutboxRelay(engine, p, CFG)
    real_flush = p.flush

    def crash(timeout=None):
        real_flush(timeout)
        raise RuntimeError("relay killed")

    monkeypatch.setattr(p, "flush", crash)
    with pytest.raises(RuntimeError):
        relay.publish_batch()
    first = [json.loads(v)["event_id"] for _, _, v, _ in p.sent]
    assert unpublished(engine) == 5  # transaction rolled back, nothing marked
    monkeypatch.setattr(p, "flush", real_flush)
    relay.publish_batch()
    second = [json.loads(v)["event_id"] for _, _, v, _ in p.sent[3:]]
    assert second == first


def test_run_until_empty(engine):
    from riskstream.streaming.consumers import StopFlag

    totals = OutboxRelay(engine, FakeProducer(), CFG).run(StopFlag(), until_empty=True)
    assert totals["published"] == 5 and unpublished(engine) == 0

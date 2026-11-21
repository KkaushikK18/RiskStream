"""Retry, dead-letter and commit rules of ConsumerRunner (no broker needed)."""

import json

import httpx
import pytest

pytest.importorskip("confluent_kafka")

from riskstream.config import StreamingConfig  # noqa: E402
from riskstream.streaming.consumers import (  # noqa: E402
    ConsumerRunner,
    PermanentError,
    ScoringProcessor,
    TransientError,
)
from riskstream.streaming.dead_letter import DeadLetterPublisher, original_value  # noqa: E402
from tests.streaming.fakes import FakeConsumer, FakeMessage, FakeProducer  # noqa: E402

CFG = StreamingConfig.load(max_attempts=3, backoff_initial_s=0.0, backoff_max_s=0.0, pause_on_failure_s=0.0,
                           heartbeat_interval_s=0.0)


class Scripted:
    name = "test"

    def __init__(self, script):
        self.script = list(script)  # per call: "ok" | "dup" | "perm" | "trans"
        self.calls = 0
        self.beats = []

    def handle(self, msg):
        self.calls += 1
        action = self.script.pop(0) if self.script else "ok"
        if action == "perm":
            raise PermanentError("bad message")
        if action == "trans":
            raise TransientError("redis down")
        return "applied" if action == "ok" else "duplicate"

    def heartbeat(self, state, lag):
        self.beats.append(state)


def run(messages, script, **kw):
    consumer, producer = FakeConsumer(messages), FakeProducer()
    proc = Scripted(script)
    runner = ConsumerRunner(consumer, proc, DeadLetterPublisher(producer, "dlq", "test", "g"), CFG,
                            sleep=lambda s: None, **kw)
    counts = runner.run(idle_exit_s=0)
    return consumer, producer, proc, counts


def msgs(n):
    return [FakeMessage({"n": i}, offset=i) for i in range(n)]


def test_success_stores_every_offset_and_closes():
    c, p, proc, counts = run(msgs(3), ["ok", "dup", "ok"])
    assert c.stored == [0, 1, 2] and c.closed
    assert counts == {"applied": 2, "duplicate": 1} and p.sent == []


def test_permanent_error_dead_letters_then_continues():
    c, p, proc, counts = run(msgs(3), ["ok", "perm", "ok"])
    assert c.stored == [0, 1, 2]
    assert len(p.sent) == 1
    topic, key, value, _ = p.sent[0]
    record = json.loads(value)
    assert topic == "dlq" and record["original_offset"] == 1 and record["error_class"] == "PermanentError"
    assert json.loads(original_value(record)) == {"n": 1}
    assert proc.calls == 3  # no retries for permanent errors


def test_transient_error_retries_then_succeeds():
    c, p, proc, counts = run(msgs(1), ["trans", "trans", "ok"])
    assert c.stored == [0] and counts["retried"] == 2 and counts["applied"] == 1 and p.sent == []


def test_transient_exhaustion_rewinds_and_never_skips():
    # 3 failures exhaust attempts -> blocked, rewind; next delivery succeeds.
    c, p, proc, counts = run(msgs(2), ["trans", "trans", "trans", "ok", "ok"])
    assert c.seeks == [0]
    assert c.stored == [0, 1]  # message 0 processed before message 1, nothing skipped
    assert counts["blocked"] == 1 and p.sent == []
    assert "blocked" in proc.beats


def test_dlq_failure_does_not_commit():
    consumer = FakeConsumer(msgs(1))
    runner = ConsumerRunner(consumer, Scripted(["perm"]),
                            DeadLetterPublisher(FakeProducer(fail_all=True), "dlq", "t", "g"), CFG,
                            sleep=lambda s: None)
    with pytest.raises(RuntimeError, match="DLQ publish failed"):
        runner.run(idle_exit_s=0)
    assert consumer.stored == [] and consumer.closed


def test_invalid_bytes_are_preserved_in_dlq():
    c, p, proc, counts = run([FakeMessage(b"\xff\x00not-json", offset=0)], ["perm"])
    record = json.loads(p.sent[0][2])
    assert record["payload_encoding"] == "base64" and original_value(record) == b"\xff\x00not-json"


# ---------------------------------------------------------------- scoring processor


def ingested(**kw):
    body = {"event_id": "e" * 16, "schema_version": 1, "transaction_id": "ps_1", "timestamp": "2026-01-25T10:00:00Z",
            "type": "TRANSFER", "amount": 10.0, "orig_account_id": "C1", "orig_balance_before": 10.0,
            "dest_account_id": "C2", "dest_type": "CUSTOMER", "replay_run_id": "r"}
    return FakeMessage(body | kw)


def processor(status, body=None, headers=None, raise_exc=None):
    seen = {}

    def handler(request):
        seen["json"] = json.loads(request.content)
        seen["headers"] = request.headers
        if raise_exc:
            raise raise_exc
        return httpx.Response(status, json=body or {}, headers=headers or {})

    client = httpx.Client(base_url="http://api", transport=httpx.MockTransport(handler))
    return ScoringProcessor(client, "k"), seen


def test_scoring_success_and_replay():
    p, seen = processor(201)
    assert p.handle(ingested()) == "applied"
    assert "replay_run_id" not in seen["json"] and "event_id" not in seen["json"]
    assert seen["headers"]["x-api-key"] == "k"
    assert processor(200)[0].handle(ingested()) == "duplicate"


@pytest.mark.parametrize("status", [409, 422, 400, 413])
def test_scoring_permanent_statuses(status):
    with pytest.raises(PermanentError):
        processor(status, {"error": {"code": "x"}})[0].handle(ingested())


@pytest.mark.parametrize("status", [401, 403, 429, 500, 503])
def test_scoring_transient_statuses(status):
    with pytest.raises(TransientError) as exc:
        processor(status, {"error": {"code": "x"}}, {"retry-after": "3"})[0].handle(ingested())
    assert exc.value.retry_after_s == 3.0


def test_scoring_connection_error_is_transient():
    with pytest.raises(TransientError):
        processor(0, raise_exc=httpx.ConnectError("refused"))[0].handle(ingested())


def test_scoring_invalid_event_is_permanent():
    with pytest.raises(PermanentError):
        processor(201)[0].handle(ingested(amount=-1))

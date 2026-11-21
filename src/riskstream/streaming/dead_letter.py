"""Dead-letter topic: messages that can never succeed (invalid schema, rejected by
the API as a conflict or invalid). Transient failures are never dead-lettered.

Each DLQ record keeps the original payload and where it came from, so an
operator can inspect it and, after fixing the cause, re-publish it with
``python -m riskstream.cli dlq-replay``.
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from riskstream.monitoring import stream_metrics as sm


def dlq_record(msg: Any, consumer: str, group: str, error: Exception, attempts: int) -> dict:
    raw = msg.value() or b""
    try:
        payload: Any = json.loads(raw)
        encoding = "json"
    except (ValueError, UnicodeDecodeError):
        payload, encoding = base64.b64encode(raw).decode(), "base64"
    key = msg.key()
    return {
        "dlq_id": str(uuid.uuid4()),
        "failed_at": datetime.now(UTC).isoformat(),
        "consumer": consumer, "group": group,
        "original_topic": msg.topic(), "original_partition": msg.partition(), "original_offset": msg.offset(),
        "original_key": key.decode("utf-8", "replace") if isinstance(key, bytes) else key,
        "error_class": type(error).__name__, "error": str(error)[:1000], "attempts": attempts,
        "payload_encoding": encoding, "payload": payload,
    }


class DeadLetterPublisher:
    def __init__(self, producer: Any, topic: str, consumer: str, group: str) -> None:
        self.producer, self.topic, self.consumer, self.group = producer, topic, consumer, group

    def send(self, msg: Any, error: Exception, attempts: int, timeout_s: float = 10.0) -> dict:
        """Publish synchronously: the source offset is committed only after this succeeds."""
        record = dlq_record(msg, self.consumer, self.group, error, attempts)
        delivered: dict[str, Any] = {}

        def on_delivery(err, _m):
            delivered["err"] = err

        self.producer.produce(self.topic, key=msg.key(), value=json.dumps(record).encode(), on_delivery=on_delivery)
        deadline = time.monotonic() + timeout_s
        while "err" not in delivered and time.monotonic() < deadline:
            self.producer.poll(0.1)
        if delivered.get("err") is not None or "err" not in delivered:
            raise RuntimeError(f"DLQ publish failed: {delivered.get('err', 'timeout')}")
        sm.DLQ.labels(self.consumer, record["error_class"]).inc()
        return record


def original_value(record: dict) -> bytes:
    if record["payload_encoding"] == "json":
        return json.dumps(record["payload"]).encode()
    return base64.b64decode(record["payload"])

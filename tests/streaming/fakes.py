"""In-memory stand-ins for Kafka messages, consumers and producers."""

from __future__ import annotations

import json


class FakeMessage:
    def __init__(self, value, key=b"k", topic="t", partition=0, offset=0, error=None):
        self._v = value if isinstance(value, (bytes, type(None))) else json.dumps(value).encode()
        self._k, self._t, self._p, self._o, self._e = key, topic, partition, offset, error

    def value(self):
        return self._v

    def key(self):
        return self._k

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def error(self):
        return self._e


class FakeConsumer:
    """Delivers a list of messages; seek() rewinds; tracks stored offsets."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.pos = 0
        self.stored: list[int] = []
        self.closed = False
        self.seeks: list[int] = []

    def poll(self, timeout):
        if self.pos >= len(self.messages):
            return None
        m = self.messages[self.pos]
        self.pos += 1
        return m

    def store_offsets(self, message):
        self.stored.append(message.offset())

    def seek(self, tp):
        self.seeks.append(tp.offset)
        self.pos = next(i for i, m in enumerate(self.messages) if m.offset() == tp.offset)

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, fail_keys=(), fail_all=False):
        self.sent: list[tuple] = []
        self._pending = []
        self.fail_keys, self.fail_all = set(fail_keys), fail_all

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self._pending.append((topic, key, value, headers, on_delivery))

    def poll(self, timeout=0):
        self._deliver()
        return 0

    def flush(self, timeout=None):
        self._deliver()
        return 0

    def _deliver(self):
        pending, self._pending = self._pending, []
        for topic, key, value, headers, cb in pending:
            err = "broker down" if self.fail_all or key in self.fail_keys else None
            if err is None:
                self.sent.append((topic, key, value, headers))
            if cb:
                cb(err, None)

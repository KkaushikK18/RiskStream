"""Clean up resources that killed test runs leave behind.

Integration tests create temporary Kafka topics and Redis namespaces. Their
fixture teardown deletes them, but a run killed with Ctrl+C or a crash skips
teardown. Left alone, the topics fill Redpanda's partition limit, and the
Redis keys use memory shared with the real online store (database 0) under
one maxmemory cap with noeviction.

Safety rules. Each rule alone prevents deleting real data:
1. Exact name patterns. Test resources are named
   ``test.<unix seconds>.<8 hex>.<kind>`` and ``rs:test:<unix seconds>:<8 hex>``.
   Real topics (``transactions.*``) and the real namespace (``rs:v1``) can never
   match. Names that only look similar (e.g. ``test.foo``) are left alone.
2. Age. Only resources created more than ``MAX_AGE_S`` ago are deleted, using
   the timestamp in the name, so a test run still in progress is never touched.
3. Redis database 0 is refused outright, even if the test URL points at it.
"""

from __future__ import annotations

import re
import time
import uuid
from urllib.parse import urlparse

# A full test run takes ~2.5 min and one Kafka test well under a minute, so 10 min
# protects any run still in progress (e.g. a second terminal) while letting the
# next run clean up after a killed one soon.
MAX_AGE_S = 600
KINDS = ("scored", "dlq", "ingested", "labeled")
# Used with fullmatch(): "$" alone would also accept a trailing newline.
TOPIC_RE = re.compile(r"test\.(\d{10})\.[0-9a-f]{8}\.(?:scored|dlq|ingested|labeled)")
# Namespace keys look like "rs:test:<ts>:<hex>:<anything>"; the bootstrap lock is
# "rs:test:<ts>:<hex>-lock:bootstrap".
NS_KEY_RE = re.compile(r"^rs:test:(\d{10}):[0-9a-f]{8}(?::|-lock:)")


def new_tag(now: float | None = None) -> str:
    """'<unix seconds>.<8 hex>': unique per test, with its creation time inside."""
    return f"{int(now if now is not None else time.time())}.{uuid.uuid4().hex[:8]}"


def topic_names(tag: str) -> dict[str, str]:
    return {kind: f"test.{tag}.{kind}" for kind in KINDS}


def namespace(tag: str) -> str:
    return "rs:test:" + tag.replace(".", ":")


def _is_stale(created: str, now: float, max_age_s: float) -> bool:
    return now - int(created) > max_age_s


def stale_topics(names, now: float | None = None, max_age_s: float = MAX_AGE_S) -> list[str]:
    now = now if now is not None else time.time()
    return sorted(n for n in names if (m := TOPIC_RE.fullmatch(n)) and _is_stale(m.group(1), now, max_age_s))


def stale_keys(keys, now: float | None = None, max_age_s: float = MAX_AGE_S) -> list[str]:
    now = now if now is not None else time.time()
    return [k for k in keys if (m := NS_KEY_RE.match(k)) and _is_stale(m.group(1), now, max_age_s)]


def redis_db(url: str) -> int:
    path = urlparse(url).path.strip("/")
    return int(path) if path else 0


def delete_stale_topics(bootstrap: str, now: float | None = None, max_age_s: float = MAX_AGE_S,
                        timeout_s: float = 20.0) -> list[str]:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    doomed = stale_topics(admin.list_topics(timeout=timeout_s).topics, now, max_age_s)
    if doomed:
        for fut in admin.delete_topics(doomed, operation_timeout=timeout_s).values():
            fut.result()
    return doomed


def delete_stale_keys(url: str, now: float | None = None, max_age_s: float = MAX_AGE_S) -> int:
    import redis

    if redis_db(url) == 0:
        raise ValueError("refusing to clean Redis database 0 (the real online store); use a test database")
    r = redis.Redis.from_url(url, decode_responses=True)
    try:
        removed = 0
        batch: list[str] = []
        for key in r.scan_iter(match="rs:test:*", count=1000):
            batch.append(key)
            if len(batch) >= 1000:
                removed += _unlink(r, stale_keys(batch, now, max_age_s))
                batch = []
        removed += _unlink(r, stale_keys(batch, now, max_age_s))
        return removed
    finally:
        r.close()


def _unlink(r, keys: list[str]) -> int:
    return r.unlink(*keys) if keys else 0

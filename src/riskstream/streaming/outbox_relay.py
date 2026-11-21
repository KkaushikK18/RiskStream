"""Transactional outbox relay: PostgreSQL ``outbox_events`` -> Kafka.

Loop:
1. Lock the oldest unpublished rows (``FOR UPDATE SKIP LOCKED``), so several
   relay instances can run without publishing the same row concurrently.
2. Produce them in id order (key = event_key, so one account's events stay
   in one partition and in order) and wait for broker acknowledgements.
3. Mark acknowledged rows ``published_at = now()``; failed rows get
   ``attempts + 1`` and ``last_error``, and are retried next cycle.
4. Commit, which releases the locks.

Guarantee: at-least-once. A crash between step 2 and step 4 republishes
those rows with the same ``event_id``; consumers deduplicate on it.

The relay also reports its heartbeat and backlog age into the online store's
meta hash, which the API uses to decide whether Redis features are fresh.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, bindparam, text

from riskstream.config import StreamingConfig
from riskstream.logging_config import get_logger
from riskstream.monitoring import stream_metrics as sm
from riskstream.streaming.schemas import dumps

logger = get_logger(__name__)


def _select_sql(dialect: str) -> str:
    lock = " FOR UPDATE SKIP LOCKED" if dialect == "postgresql" else ""
    return ("SELECT id, event_id, topic, event_key, payload FROM outbox_events "
            f"WHERE published_at IS NULL ORDER BY id LIMIT :n{lock}")


class OutboxRelay:
    def __init__(self, engine: Engine, producer: Any, cfg: StreamingConfig, *, store: Any = None,
                 flush_timeout_s: float = 30.0, lag_probe: Any = None) -> None:
        self.engine, self.producer, self.cfg, self.store = engine, producer, cfg, store
        self.flush_timeout_s = flush_timeout_s
        # Measures the feature consumer group from the broker (see GroupLagProbe).
        self.lag_probe = lag_probe
        self.select_sql = text(_select_sql(engine.dialect.name))

    def publish_batch(self) -> dict[str, int]:
        t0 = time.perf_counter()
        published = failed = 0
        with self.engine.begin() as conn:
            if self.engine.dialect.name == "postgresql":
                conn.execute(text("SET LOCAL statement_timeout = '60s'"))
            rows = conn.execute(self.select_sql, {"n": self.cfg.relay_batch_size}).mappings().all()
            if not rows:
                return {"published": 0, "failed": 0}
            results: dict[int, str | None] = {}

            def callback(row_id: int):
                def on_delivery(err, _msg):
                    results[row_id] = None if err is None else str(err)
                return on_delivery

            for row in rows:
                payload = row["payload"]
                value = payload.encode() if isinstance(payload, str) else dumps(payload)
                headers = [("event_id", row["event_id"].encode()), ("outbox_id", str(row["id"]).encode())]
                try:
                    self.producer.produce(row["topic"], key=row["event_key"].encode(), value=value,
                                          headers=headers, on_delivery=callback(row["id"]))
                except BufferError:
                    self.producer.poll(0.5)
                    self.producer.produce(row["topic"], key=row["event_key"].encode(), value=value,
                                          headers=headers, on_delivery=callback(row["id"]))
                self.producer.poll(0)
            remaining = self.producer.flush(self.flush_timeout_s)
            ok_ids = [rid for rid, err in results.items() if err is None]
            bad = {rid: (results.get(rid) or "not acknowledged before flush timeout")
                   for rid in (r["id"] for r in rows) if results.get(rid, "missing") is not None}
            now = datetime.now(UTC)
            if ok_ids:
                conn.execute(text("UPDATE outbox_events SET published_at = :t, attempts = attempts + 1 "
                                  "WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                             {"t": now, "ids": ok_ids})
            for rid, err in bad.items():
                conn.execute(text("UPDATE outbox_events SET attempts = attempts + 1, last_error = :e "
                                  "WHERE id = :i"), {"e": err[:500], "i": rid})
            topic_of = {r["id"]: r["topic"] for r in rows}
            for rid in ok_ids:
                sm.RELAY_PUBLISHED.labels(topic_of[rid]).inc()
            for rid in bad:
                sm.RELAY_FAILED.labels(topic_of[rid]).inc()
            published, failed = len(ok_ids), len(bad)
            if remaining:
                logger.warning("%d messages still in flight after flush timeout", remaining)
        sm.RELAY_BATCH_SECONDS.observe(time.perf_counter() - t0)
        if failed:
            logger.warning("outbox batch: %d published, %d failed", published, failed)
        return {"published": published, "failed": failed}

    def backlog(self) -> tuple[int, float]:
        with self.engine.connect() as conn:
            n, oldest = conn.execute(text(
                "SELECT count(*), min(created_at) FROM outbox_events WHERE published_at IS NULL")).one()
        if oldest is not None and oldest.tzinfo is None:
            oldest = oldest.replace(tzinfo=UTC)
        age = 0.0 if oldest is None else max(0.0, (datetime.now(UTC) - oldest).total_seconds())
        return int(n), age

    def report(self) -> tuple[int, float]:
        n, age = self.backlog()
        sm.RELAY_BACKLOG.set(n)
        sm.RELAY_OLDEST_AGE.set(age)
        fields: dict[str, object] = {"relay_heartbeat_ms": int(time.time() * 1000),
                                     "relay_oldest_unpublished_age_s": round(age, 3), "relay_backlog": n}
        if self.lag_probe is not None:
            try:
                lag, stalled = self.lag_probe.measure()
                fields |= {"scored_lag_broker": lag, "consumer_stalled_s": round(stalled, 2)}
                sm.FEATURE_CONSUMER_BROKER_LAG.set(lag)
                sm.FEATURE_CONSUMER_STALLED.set(stalled)
            except Exception as exc:
                logger.warning("feature consumer lag probe failed: %s", exc)
        if self.store is not None:
            try:
                self.store.write_meta(**fields)
            except Exception as exc:
                logger.warning("could not report relay status to the online store: %s", exc)
        return n, age

    def run(self, stop: Any, *, report_interval_s: float = 1.0, until_empty: bool = False) -> dict[str, int]:
        totals = {"published": 0, "failed": 0, "batches": 0}
        last_report = 0.0
        while not stop:
            try:
                out = self.publish_batch()
            except Exception as exc:  # database or broker trouble: back off, keep the process alive
                logger.error("relay cycle failed: %s", exc)
                stop.wait(min(5.0, self.cfg.relay_poll_interval_s * 10))
                continue
            totals["published"] += out["published"]
            totals["failed"] += out["failed"]
            totals["batches"] += 1 if out["published"] or out["failed"] else 0
            if time.monotonic() - last_report >= report_interval_s or not out["published"]:
                try:
                    n, _ = self.report()
                except Exception as exc:
                    logger.error("relay backlog check failed: %s", exc)
                    n = -1
                last_report = time.monotonic()
                if until_empty and n == 0:
                    break
            if not out["published"]:
                stop.wait(self.cfg.relay_poll_interval_s)
        return totals

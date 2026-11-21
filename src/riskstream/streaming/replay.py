"""Replay an ordered dataset into ``transactions.ingested``.

Uses:
* simulate incoming traffic at a controlled rate (``--rate`` events/s, or
  ``--step-seconds`` wall-clock seconds per simulated hour),
* exercise the full pipeline: replay -> scoring worker -> API -> outbox ->
  relay -> feature consumer -> Redis,
* reproduce exact training-time features with ``--barrier``: before
  publishing hour h+1, wait until every event of hour h has been applied to
  the online store. Without the barrier, events still in flight when the next
  hour starts are missing from its features (the documented consistency gap).

Only rows of steps after the stored history should be replayed with
``--barrier``: transactions that already exist are answered by the API from
storage and produce no new event, so the barrier would wait for them.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import polars as pl

from riskstream.config import AppConfig, StreamingConfig
from riskstream.logging_config import get_logger
from riskstream.monitoring import stream_metrics as sm
from riskstream.streaming.schemas import SCHEMA_VERSION, dumps

logger = get_logger(__name__)


class BarrierTimeout(RuntimeError):
    pass


def events_for(rows: pl.DataFrame, epoch, run_id: str) -> Iterator[tuple[int, str, dict]]:
    """(step, key, payload) per row, in event order. Real transaction ids are kept."""
    for r in rows.iter_rows(named=True):
        yield r["step"], r["dest_id"], {
            "event_id": str(uuid.uuid4()), "schema_version": SCHEMA_VERSION, "event_type": "transaction.ingested",
            "transaction_id": r["transaction_id"],
            "timestamp": (epoch + timedelta(hours=r["step"] - 1)).isoformat(),
            "type": r["type"], "amount": r["amount"], "orig_account_id": r["orig_id"],
            "orig_balance_before": r["orig_balance_before"], "dest_account_id": r["dest_id"],
            "dest_type": r["dest_type"], "replay_run_id": run_id,
        }


class Pacer:
    def __init__(self, rate: float | None) -> None:
        self.interval = 1.0 / rate if rate else 0.0
        self.next = time.perf_counter()

    def wait(self) -> None:
        if not self.interval:
            return
        self.next += self.interval
        delay = self.next - time.perf_counter()
        if delay > 0:
            time.sleep(delay)


def replay(rows: pl.DataFrame, producer: Any, cfg: StreamingConfig, app_cfg: AppConfig, *,
           rate: float | None = None, step_seconds: float | None = None, barrier_store: Any = None,
           barrier_timeout_s: float | None = None, run_id: str | None = None) -> dict:
    run_id = run_id or uuid.uuid4().hex[:12]
    per_step = dict(rows.group_by("step").len().iter_rows())
    sent: dict[int, int] = {}
    t0 = time.perf_counter()
    current: int | None = None
    pacer = Pacer(rate)
    step_started = time.perf_counter()
    waited = 0.0

    def finish_step(step: int) -> None:
        nonlocal waited
        producer.flush(30)
        if barrier_store is not None:
            w0 = time.perf_counter()
            deadline = w0 + (barrier_timeout_s or cfg.replay_barrier_timeout_s)
            while barrier_store.applied_count(step) < sent[step]:
                if time.perf_counter() > deadline:
                    raise BarrierTimeout(
                        f"step {step}: {barrier_store.applied_count(step)} of {sent[step]} events applied "
                        f"after {barrier_timeout_s or cfg.replay_barrier_timeout_s}s. Check the scoring worker, "
                        "relay, feature consumer and the DLQ (stream-status, dlq-inspect)")
                time.sleep(0.05)
            waited += time.perf_counter() - w0
        if step_seconds:
            remaining = step_seconds - (time.perf_counter() - step_started)
            if remaining > 0:
                time.sleep(remaining)
        logger.info("replay step %d done: %d events", step, sent[step])

    for step, key, payload in events_for(rows, app_cfg.simulation_epoch, run_id):
        if current is not None and step != current:
            finish_step(current)
            step_started = time.perf_counter()
        if current != step and step_seconds:
            pacer = Pacer(per_step[step] / step_seconds)
        current = step
        pacer.wait()
        while True:
            try:
                producer.produce(cfg.topic_ingested, key=key.encode(), value=dumps(payload))
                break
            except BufferError:
                producer.poll(0.2)
        producer.poll(0)
        sent[step] = sent.get(step, 0) + 1
        sm.REPLAY_PUBLISHED.inc()
    if current is not None:
        finish_step(current)
    elapsed = time.perf_counter() - t0
    total = sum(sent.values())
    return {"run_id": run_id, "events": total, "steps": sorted(sent), "seconds": round(elapsed, 2),
            "publish_rate_per_s": round(total / elapsed, 1) if elapsed else None,
            "barrier": barrier_store is not None, "barrier_wait_seconds": round(waited, 2)}

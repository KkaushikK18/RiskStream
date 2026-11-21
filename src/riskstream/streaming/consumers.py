"""Consumer runner and the two processors.

Processing rules (one message at a time per process, in partition order):

* success          -> store the offset; librdkafka auto-commits stored offsets
                      every second and on close (at-least-once).
* PermanentError   -> publish to the DLQ (synchronously), then store the offset.
                      Invalid schema, API 409/422, and similar.
* TransientError   -> retry with exponential backoff up to ``max_attempts``.
                      If still failing: do NOT skip. Rewind to the message,
                      mark the consumer blocked, wait, and try again. Nothing
                      is lost; lag grows and alerts fire instead (backpressure).

Graceful shutdown (Ctrl+C / SIGTERM): finish the current message, store its
offset, close the consumer (final commit, leave the group).
"""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from riskstream.config import StreamingConfig
from riskstream.features.online_store import FeatureStoreError, FeatureStoreNotUsable, RedisFeatureStore
from riskstream.logging_config import get_logger
from riskstream.monitoring import stream_metrics as sm
from riskstream.streaming.dead_letter import DeadLetterPublisher
from riskstream.streaming.schemas import IngestedTransaction, InvalidEvent, ScoredTransaction, parse

logger = get_logger(__name__)


class PermanentError(Exception):
    """The message can never succeed as is."""


class TransientError(Exception):
    """A dependency failed; the same message may succeed later."""

    def __init__(self, message: str, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


# ------------------------------------------------------------------ processors


class StoreLost(Exception):
    """The online store was wiped (e.g. Redis restarted) and must be rebuilt before
    any event can be applied. Not caught by the runner: the process exits, stored
    offsets are committed, and the container restart runs ``online-bootstrap --if-needed``
    before consuming again from the last committed offset."""


class FeatureUpdateProcessor:
    """transactions.scored -> Redis online features."""

    name = "feature-updater"

    def __init__(self, store: RedisFeatureStore, exit_on_store_lost: bool = False) -> None:
        self.store = store
        self.exit_on_store_lost = exit_on_store_lost

    def handle(self, msg: Any) -> str:
        try:
            ev = parse(ScoredTransaction, msg.value())
        except InvalidEvent as exc:
            raise PermanentError(str(exc)) from exc
        try:
            applied = self.store.apply(event_id=ev.event_id, dest_id=ev.dest_account_id,
                                       orig_id=ev.orig_account_id, step=ev.step, amount=ev.amount, txn_type=ev.type)
        except FeatureStoreNotUsable as exc:
            if self.exit_on_store_lost and exc.reason == "not_bootstrapped" and not self._bootstrap_running():
                raise StoreLost("online store is not bootstrapped") from exc
            raise TransientError(f"online store: {exc}") from exc
        except FeatureStoreError as exc:
            raise TransientError(f"online store: {exc}") from exc
        sm.END_TO_END_SECONDS.labels(self.name).observe(
            max(0.0, (datetime.now(UTC) - ev.scored_at).total_seconds()))
        return "applied" if applied else "duplicate"

    def _bootstrap_running(self) -> bool:
        from riskstream.features.online_bootstrap import lock_key

        try:
            return bool(self.store.r.exists(lock_key(self.store)))
        except Exception:
            return True  # unsure: keep retrying rather than exiting

    def heartbeat(self, state: str, lag: int | None) -> None:
        # Detect a wiped store even when no events arrive (e.g. Redis restarted while idle).
        if self.exit_on_store_lost and not self.store.is_bootstrapped() and not self._bootstrap_running():
            raise StoreLost("online store is not bootstrapped")
        fields: dict[str, object] = {"consumer_heartbeat_ms": int(time.time() * 1000), "consumer_state": state}
        if lag is not None:
            fields["consumer_lag"] = lag
        self.store.write_meta(**fields)


class ScoringProcessor:
    """transactions.ingested -> POST /v1/score. Safe to retry: the API is idempotent
    by transaction_id (an identical retry returns the stored decision)."""

    name = "scoring-worker"
    PERMANENT_STATUS = {400, 409, 413, 422}

    def __init__(self, client: httpx.Client, api_key: str) -> None:
        self.client, self.headers = client, {"X-API-Key": api_key}

    def handle(self, msg: Any) -> str:
        try:
            ev = parse(IngestedTransaction, msg.value())
        except InvalidEvent as exc:
            raise PermanentError(str(exc)) from exc
        try:
            r = self.client.post("/v1/score", json=ev.score_request(),
                                 headers=self.headers | {"X-Correlation-ID": ev.event_id[:64]})
        except httpx.HTTPError as exc:
            raise TransientError(f"API unreachable: {type(exc).__name__}") from exc
        if r.status_code in (200, 201):
            return "applied" if r.status_code == 201 else "duplicate"
        code = _error_code(r)
        if r.status_code in self.PERMANENT_STATUS:
            raise PermanentError(f"API {r.status_code} {code}")
        retry_after = _retry_after(r)
        # 401/403 are configuration problems: block and alert, never dead-letter every message.
        raise TransientError(f"API {r.status_code} {code}", retry_after)

    def heartbeat(self, state: str, lag: int | None) -> None:  # metrics only
        return None


def _error_code(r: httpx.Response) -> str:
    try:
        return str(r.json()["error"]["code"])
    except (ValueError, KeyError, TypeError):
        return "unknown"


def _retry_after(r: httpx.Response) -> float | None:
    try:
        return float(r.headers["retry-after"])
    except (KeyError, ValueError):
        return None


# ------------------------------------------------------------------ runner


class StopFlag:
    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self, *_: object) -> None:
        self._event.set()

    def __bool__(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        return self._event.wait(seconds)

    def install(self) -> None:
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is not None:
                try:
                    signal.signal(sig, self.set)
                except (ValueError, OSError):  # not in main thread
                    pass


class ConsumerRunner:
    def __init__(self, consumer: Any, processor: Any, dlq: DeadLetterPublisher, cfg: StreamingConfig, *,
                 stop: StopFlag | None = None, lag_fn: Callable[[], dict[int, int]] | None = None,
                 sleep: Callable[[float], None] | None = None, poll_timeout_s: float = 0.5) -> None:
        self.c, self.p, self.dlq, self.cfg = consumer, processor, dlq, cfg
        self.stop = stop or StopFlag()
        self.lag_fn = lag_fn
        self._sleep = sleep or (lambda s: self.stop.wait(s))
        self.poll_timeout_s = poll_timeout_s
        self.state = "running"
        self._last_hb = self._last_lag = 0.0
        self._lag_total: int | None = None
        self.counts: dict[str, int] = {}

    def _count(self, outcome: str) -> None:
        self.counts[outcome] = self.counts.get(outcome, 0) + 1
        sm.MESSAGES.labels(self.p.name, outcome).inc()

    def _maintenance(self, force: bool = False) -> None:
        now = time.monotonic()
        if self.lag_fn and (force or now - self._last_lag >= self.cfg.lag_report_interval_s):
            self._last_lag = now
            try:
                lag = self.lag_fn()
                self._lag_total = sum(lag.values())
                for part, v in lag.items():
                    sm.LAG.labels(self.p.name, str(part)).set(v)
            except Exception as exc:  # lag is diagnostic; never stop consuming for it
                logger.warning("lag check failed: %s", exc)
        if force or now - self._last_hb >= self.cfg.heartbeat_interval_s:
            self._last_hb = now
            try:
                self.p.heartbeat(self.state, self._lag_total)
            except StoreLost:
                raise
            except Exception as exc:
                logger.warning("heartbeat failed: %s", exc)

    def process_one(self, msg: Any) -> bool:
        """Handle one message. Returns True when its offset may be stored."""
        delay = self.cfg.backoff_initial_s
        for attempt in range(1, self.cfg.max_attempts + 1):
            t = time.perf_counter()
            try:
                outcome = self.p.handle(msg)
                sm.PROCESS_SECONDS.labels(self.p.name).observe(time.perf_counter() - t)
                self._count(outcome)
                if self.state != "running":
                    logger.info("%s recovered", self.p.name)
                self.state = "running"
                sm.STATE.labels(self.p.name).set(0)
                return True
            except PermanentError as exc:
                self.dlq.send(msg, exc, attempt)
                self._count("dead_lettered")
                logger.warning("dead-lettered %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), exc)
                return True
            except TransientError as exc:
                self._count("retried")
                if attempt == self.cfg.max_attempts:
                    break
                wait = min(exc.retry_after_s or delay, self.cfg.backoff_max_s)
                logger.info("transient failure (attempt %d/%d), retry in %.2fs: %s", attempt,
                            self.cfg.max_attempts, wait, exc)
                self._sleep(wait)
                delay = min(delay * 2, self.cfg.backoff_max_s)
                if self.stop:
                    return False
        self.state = "blocked"
        sm.STATE.labels(self.p.name).set(1)
        self._count("blocked")
        logger.error("%s blocked on %s[%d]@%d after %d attempts; will retry the same message",
                     self.p.name, msg.topic(), msg.partition(), msg.offset(), self.cfg.max_attempts)
        self._maintenance(force=True)
        return False

    def run(self, max_messages: int | None = None, idle_exit_s: float | None = None) -> dict[str, int]:
        from confluent_kafka import KafkaError, TopicPartition

        handled = 0
        idle_since = time.monotonic()
        try:
            while not self.stop:
                self._maintenance()
                msg = self.c.poll(self.poll_timeout_s)
                if msg is None:
                    if idle_exit_s is not None and time.monotonic() - idle_since >= idle_exit_s:
                        break
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("consumer error: %s", msg.error())
                    continue
                idle_since = time.monotonic()
                if self.process_one(msg):
                    self.c.store_offsets(message=msg)
                    handled += 1
                    if max_messages is not None and handled >= max_messages:
                        break
                else:
                    # rewind so the same message is delivered again, then back off
                    self.c.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    self._sleep(self.cfg.pause_on_failure_s)
        finally:
            self.state = "stopped"
            try:
                self._maintenance(force=True)
            except StoreLost:
                pass  # already exiting
            self.c.close()  # commits stored offsets and leaves the group
            logger.info("%s stopped: %s", self.p.name, self.counts)
        return self.counts

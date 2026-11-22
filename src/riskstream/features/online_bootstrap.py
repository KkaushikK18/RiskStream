"""Build the Redis online store from a consistent PostgreSQL snapshot.

Steps:
1. Remove the bootstrap marker. From that moment the apply script refuses
   events (returns -1), so consumers retry instead of racing the rebuild.
2. In one REPEATABLE READ transaction, export ``transactions`` (only the
   columns features need) and the event ids of ``transactions.scored`` outbox
   rows. A transaction and its outbox event commit together, so the snapshot
   contains both or neither.
3. Clear the namespace, then write lifetime stats, window buckets for steps
   the next hours can query, and a dedup key for every snapshotted event id.
   When the relay later publishes those events, the consumer skips them.
   Events committed after the snapshot are not deduped and get applied.
4. Write meta (``window_floor``, layout) and the marker last.

Correctness therefore does not depend on stopping the relay or consumers.
The API falls back to PostgreSQL for the duration (no marker = not usable).
"""

from __future__ import annotations

import tempfile
import threading
import time
import zlib
from pathlib import Path

import polars as pl
from sqlalchemy import Engine

from riskstream.features.online_store import RedisFeatureStore
from riskstream.logging_config import get_logger

logger = get_logger(__name__)

EXPORT_SQL = ("COPY (SELECT dest_id, orig_id, step, amount::float8 AS amount, type FROM transactions) "
              "TO STDOUT WITH (FORMAT csv, HEADER true)")
EVENTS_SQL = ("COPY (SELECT event_id FROM outbox_events WHERE topic = 'transactions.scored' "
              "AND created_at > now() - make_interval(secs => %s)) TO STDOUT WITH (FORMAT csv)")
SCHEMA = {"dest_id": pl.String, "orig_id": pl.String, "step": pl.Int32, "amount": pl.Float64, "type": pl.String}


def export_snapshot(engine: Engine, workdir: Path, dedupe_ttl_s: int) -> tuple[Path, Path]:
    tx_path, ev_path = workdir / "transactions.csv", workdir / "events.csv"
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("SET LOCAL statement_timeout = 0")
        with open(tx_path, "wb") as f, cur.copy(EXPORT_SQL) as copy:
            for chunk in copy:
                f.write(chunk)
        with open(ev_path, "wb") as f, cur.copy(EVENTS_SQL.replace("%s", str(int(dedupe_ttl_s)))) as copy:
            for chunk in copy:
                f.write(chunk)
        raw.commit()
    finally:
        raw.close()
    return tx_path, ev_path


def lifetime_frame(lf: pl.LazyFrame, key: str) -> pl.DataFrame:
    """Per account: total, newest step, events at newest step, newest earlier step (-1 = none)."""
    per_step = lf.group_by(key, "step").agg(pl.len().alias("n"))
    steps = pl.col("step").sort(descending=True)
    return (
        per_step.group_by(key)
        .agg(pl.col("n").sum().alias("total"), pl.col("step").max().alias("last"),
             pl.col("n").sort_by("step").last().alias("cnt_last"),
             steps.slice(1, 1).first().fill_null(-1).alias("prev"))
        .collect()
    )


def window_frame(lf: pl.LazyFrame, floor: int) -> pl.DataFrame:
    return (
        lf.filter(pl.col("step") >= floor)
        .group_by("dest_id", "step")
        .agg(pl.len().cast(pl.Float64).alias("c"), pl.col("amount").sum().alias("s"),
             pl.col("amount").log1p().sum().alias("l"), (pl.col("amount").log1p() ** 2).sum().alias("q"),
             (pl.col("type") == "TRANSFER").sum().cast(pl.Float64).alias("t"))
        .collect()
    )


# ---------------------------------------------------------------- bulk writes
#
# Redis commands are built as raw RESP protocol text by Polars expressions
# (vectorized, in Rust) and streamed over one connection in large chunks, like
# `redis-cli --pipe`. Python only handles one string per command, never one per
# account: per-entry Python formatting and redis-py argument packing took
# 161 of 197 s in the previous implementation.
#
# RESP bulk string: "$<byte length>\r\n<bytes>\r\n". Command: "*<argc>\r\n" + bulk strings.

CHUNK_BYTES = 8 * 1024 * 1024


def _bulk(col: pl.Expr) -> pl.Expr:
    return pl.format("${}\r\n{}\r\n", col.str.len_bytes(), col)


def _send(store: RedisFeatureStore, commands: pl.Series, replies_per_command: int = 1) -> int:
    """Stream pre-encoded commands; read every reply (errors such as OOM raise)."""
    conn = store.r.connection_pool.get_connection()
    sent = 0
    try:
        buf: list[str] = []
        size = 0

        def flush() -> None:
            nonlocal buf, size, sent
            if not buf:
                return
            payload = "".join(buf).encode("utf-8")
            conn.send_packed_command([payload], check_health=False)
            del payload
            for _ in range(len(buf) * replies_per_command):
                conn.read_response()
            sent += len(buf)
            buf, size = [], 0

        for cmd in commands:
            buf.append(cmd)
            size += len(cmd)
            if size >= CHUNK_BYTES:
                flush()
        flush()
    except BaseException:
        conn.disconnect()  # protocol state unknown; never reuse this connection
        raise
    finally:
        store.r.connection_pool.release(conn)
    return sent


# Commands are built for 1/GROUPS of the buckets at a time. Building all 6.25M
# origin accounts at once peaked above the feature-consumer's 1.5 GB limit and
# the process was OOM-killed inside the container (measured).
GROUPS = 8
ID_SLICE = 500_000


def _bucket_column(df: pl.DataFrame, key: str, buckets: int) -> pl.Series:
    # stat_bucket() (crc32) has no Polars equivalent. Slicing keeps the temporary
    # Python strings to ID_SLICE at a time instead of millions.
    parts = []
    for sl in df.select(key).iter_slices(ID_SLICE):
        parts.append(pl.Series([zlib.crc32(i.encode("utf-8")) % buckets for i in sl[key].to_list()],
                               dtype=pl.UInt32))
    return (pl.concat(parts) if parts else pl.Series([], dtype=pl.UInt32)).alias("b")


def _lifetime_commands(df: pl.DataFrame, key: str, prefix: str, buckets: int):
    """Yield one Series of HSET commands per bucket group."""
    value = pl.format("{}|{}|{}|{}", "total", "last", "cnt_last", "prev")
    base = df.select(pl.col(key), value.alias("val")).with_columns(_bucket_column(df, key, buckets))
    k = pl.format("{}:{}", pl.lit(prefix), pl.col("b"))
    for g in range(GROUPS):
        part = (base.filter(pl.col("b") % GROUPS == g)
                .group_by("b").agg(pl.len().alias("n"),
                                   pl.concat_str(_bulk(pl.col(key)), _bulk(pl.col("val"))).str.join("").alias("pairs")))
        yield part.select(pl.format("*{}\r\n$4\r\nHSET\r\n{}{}", 2 + 2 * pl.col("n"), _bulk(k),
                                    pl.col("pairs"))).to_series()
        del part


def _write_lifetime(store: RedisFeatureStore, df: pl.DataFrame, key: str, kind: str, batch: int = 0) -> int:
    """HSET {ns}:{kind}:{bucket} id "total|last|cnt_last|prev" for every account."""
    for commands in _lifetime_commands(df, key, f"{store.ns}:{kind}", store.cfg.stat_buckets):
        _send(store, commands)
    return len(df)


def _window_commands(df: pl.DataFrame, ns: str, ttl_s: int) -> pl.Series:
    step = pl.col("step").cast(pl.String)
    pairs = pl.concat_str(*[
        pl.concat_str(_bulk(pl.concat_str(step, pl.lit(f":{s}"))), _bulk(pl.col(s).cast(pl.String)))
        for s in ("c", "s", "l", "q", "t")
    ])
    per_dest = (df.select("dest_id", pairs.alias("p"))
                .group_by("dest_id").agg(pl.len().alias("n"), pl.col("p").str.join("")))
    k = _bulk(pl.concat_str(pl.lit(f"{ns}:dw:"), pl.col("dest_id")))
    ttl = str(int(ttl_s))
    # Two commands per account: HSET with all its buckets, then EXPIRE (safety TTL).
    return per_dest.select(pl.format(
        "*{}\r\n$4\r\nHSET\r\n{}{}*3\r\n$6\r\nEXPIRE\r\n{}" + f"${len(ttl)}\r\n{ttl}\r\n",
        2 + 10 * pl.col("n"), k, pl.col("p"), k,
    )).to_series()


def _write_windows(store: RedisFeatureStore, df: pl.DataFrame, batch: int = 0) -> int:
    """One HSET (all buckets) + EXPIRE per destination with activity in the window."""
    return _send(store, _window_commands(df, store.ns, store.cfg.window_key_ttl_s), replies_per_command=2)


def _write_dedupe(store: RedisFeatureStore, event_ids: list[str], batch: int) -> None:
    pipe = store.r.pipeline(transaction=False)
    for i, ev in enumerate(event_ids, 1):
        pipe.set(store.k_event(ev), "1", ex=store.cfg.dedupe_ttl_s)
        if i % batch == 0:
            pipe.execute()
    pipe.execute()


class BootstrapInProgress(RuntimeError):
    pass


# Short lease, renewed while the bootstrap runs: if the process dies (e.g. it is
# OOM-killed) the lock disappears within LOCK_TTL_S instead of blocking recovery.
LOCK_TTL_S = 30
LOCK_RENEW_S = 10
WAIT_TIMEOUT_S = 1800

_RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def lock_key(store: RedisFeatureStore) -> str:
    # Outside the namespace pattern ("{ns}:*"), so clear_namespace() never deletes it.
    return f"{store.ns}-lock:bootstrap"


class _Lease:
    """Redis lock owned by a token; a daemon thread renews it until release()."""

    def __init__(self, store: RedisFeatureStore) -> None:
        self.store, self.key = store, lock_key(store)
        self.token = f"{time.time_ns()}-{threading.get_ident()}"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def acquire(self) -> bool:
        if not self.store.r.set(self.key, self.token, nx=True, ex=LOCK_TTL_S):
            return False
        self._thread = threading.Thread(target=self._renew, name="bootstrap-lease", daemon=True)
        self._thread.start()
        return True

    def _renew(self) -> None:
        while not self._stop.wait(LOCK_RENEW_S):
            try:
                if not self.store.r.eval(_RENEW_LUA, 1, self.key, self.token, LOCK_TTL_S):
                    logger.error("bootstrap lease lost")
                    return
            except Exception as exc:  # Redis hiccup: try again on the next tick
                logger.warning("bootstrap lease renewal failed: %s", exc)

    def release(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        try:
            self.store.r.eval(_RELEASE_LUA, 1, self.key, self.token)
        except Exception as exc:
            logger.warning("bootstrap lease release failed (expires in %ss): %s", LOCK_TTL_S, exc)


def wait_for_bootstrap(store: RedisFeatureStore, timeout_s: float = WAIT_TIMEOUT_S) -> bool:
    """Wait for another process's bootstrap to finish. True if the store became usable."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if store.is_bootstrapped():
            return True
        if not store.r.exists(lock_key(store)):
            return store.is_bootstrapped()
        time.sleep(2)
    return False


def bootstrap(store: RedisFeatureStore, engine: Engine | None = None, *, frame: pl.LazyFrame | None = None,
              event_ids: list[str] | None = None, batch: int = 500) -> dict:
    """Rebuild the namespace. ``frame``/``event_ids`` replace the PostgreSQL export (tests).

    A Redis lock makes concurrent rebuilds impossible (e.g. an operator's manual run
    while a restarted consumer container runs ``--if-needed``)."""
    lease = _Lease(store)
    if not lease.acquire():
        raise BootstrapInProgress("another bootstrap holds the lock")
    try:
        return _bootstrap(store, engine, frame=frame, event_ids=event_ids, batch=batch)
    finally:
        lease.release()


def _bootstrap(store: RedisFeatureStore, engine: Engine | None, *, frame: pl.LazyFrame | None,
               event_ids: list[str] | None, batch: int) -> dict:
    t0 = time.perf_counter()
    store.r.hdel(store.k_meta, "bootstrap_completed_at_ms")  # stop applies first
    with tempfile.TemporaryDirectory() as tmp:
        if frame is None:
            assert engine is not None
            tx_path, ev_path = export_snapshot(engine, Path(tmp), store.cfg.dedupe_ttl_s)
            lf = pl.scan_csv(tx_path, schema=SCHEMA)
            event_ids = (pl.read_csv(ev_path, has_header=False, new_columns=["event_id"])["event_id"].to_list()
                         if ev_path.stat().st_size else [])
        else:
            lf = frame.select(list(SCHEMA)).cast(SCHEMA)
            event_ids = event_ids or []
        t_export = time.perf_counter()
        stages: dict[str, float] = {}

        def mark(name: str, since: float) -> float:
            now = time.perf_counter()
            stages[name] = round(now - since, 1)
            return now

        # Stay lazy: each stage scans the snapshot file again (~1 s). Materializing all
        # rows once pushed the container past its 1.5 GB limit (measured: killed).
        rows, max_step = lf.select(pl.len(), pl.col("step").max()).collect().row(0)
        t = mark("load", t_export)
        removed = store.clear_namespace()
        max_step = int(max_step or 0)
        floor = max_step + 1 - store.max_window
        t = mark("clear", t)
        dest = lifetime_frame(lf, "dest_id")
        n_dest = _write_lifetime(store, dest, "dest_id", "da", batch)
        del dest
        t = mark("dest_lifetime", t)
        orig = lifetime_frame(lf, "orig_id")
        n_orig = _write_lifetime(store, orig, "orig_id", "oa", batch)
        del orig
        t = mark("orig_lifetime", t)
        n_windows = _write_windows(store, window_frame(lf, floor), batch)
        t = mark("windows", t)
        _write_dedupe(store, event_ids, 5000)
        mark("dedupe", t)
    now_ms = int(time.time() * 1000)
    store.r.hset(store.k_meta, mapping={
        "layout": store.ns, "window_floor": str(floor), "bootstrap_rows": str(rows),
        "bootstrap_max_step": str(max_step), "bootstrap_dedupe_events": str(len(event_ids)),
        "max_applied_step": str(max_step),
    })
    store.r.hset(store.k_meta, "bootstrap_completed_at_ms", str(now_ms))  # marker last
    out = {"rows": int(rows), "max_step": max_step, "window_floor": floor, "dest_accounts": n_dest,
           "orig_accounts": n_orig, "window_hashes": n_windows, "dedupe_event_ids": len(event_ids),
           "keys_removed_first": removed, "export_seconds": round(t_export - t0, 1),
           "stage_seconds": stages, "total_seconds": round(time.perf_counter() - t0, 1)}
    logger.info("Online store bootstrapped: %s", out)
    return out

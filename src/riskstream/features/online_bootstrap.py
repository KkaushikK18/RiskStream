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
import time
from pathlib import Path

import polars as pl
from sqlalchemy import Engine

from riskstream.features.online_store import RedisFeatureStore, stat_bucket
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


def _write_lifetime(store: RedisFeatureStore, df: pl.DataFrame, key: str, kind: str, batch: int) -> int:
    buckets: dict[int, dict[str, str]] = {}
    n = 0
    for sl in df.iter_slices(200_000):
        for ent, total, last, cl, prev in sl.select(key, "total", "last", "cnt_last", "prev").iter_rows():
            buckets.setdefault(stat_bucket(ent, store.cfg.stat_buckets), {})[ent] = f"{total}|{last}|{cl}|{prev}"
            n += 1
    pipe = store.r.pipeline(transaction=False)
    for i, (b, mapping) in enumerate(buckets.items(), 1):
        pipe.hset(f"{store.ns}:{kind}:{b}", mapping=mapping)
        if i % batch == 0:
            pipe.execute()
    pipe.execute()
    return n


def _write_windows(store: RedisFeatureStore, df: pl.DataFrame, batch: int) -> int:
    pipe = store.r.pipeline(transaction=False)
    keys = 0
    for i, (dest, rows) in enumerate(df.sort("dest_id").partition_by("dest_id", as_dict=True).items(), 1):
        dest_id = dest[0] if isinstance(dest, tuple) else dest
        mapping = {}
        for step, c, s, lg, q, t in rows.select("step", "c", "s", "l", "q", "t").iter_rows():
            mapping |= {f"{step}:c": repr(c), f"{step}:s": repr(s), f"{step}:l": repr(lg), f"{step}:q": repr(q),
                        f"{step}:t": repr(t)}
        k = store.k_window(dest_id)
        pipe.hset(k, mapping=mapping)
        pipe.expire(k, store.cfg.window_key_ttl_s)
        keys += 1
        if i % batch == 0:
            pipe.execute()
    pipe.execute()
    return keys


def _write_dedupe(store: RedisFeatureStore, event_ids: list[str], batch: int) -> None:
    pipe = store.r.pipeline(transaction=False)
    for i, ev in enumerate(event_ids, 1):
        pipe.set(store.k_event(ev), "1", ex=store.cfg.dedupe_ttl_s)
        if i % batch == 0:
            pipe.execute()
    pipe.execute()


def bootstrap(store: RedisFeatureStore, engine: Engine | None = None, *, frame: pl.LazyFrame | None = None,
              event_ids: list[str] | None = None, batch: int = 500) -> dict:
    """Rebuild the namespace. ``frame``/``event_ids`` replace the PostgreSQL export (tests)."""
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
        rows, max_step = lf.select(pl.len(), pl.col("step").max()).collect().row(0)
        removed = store.clear_namespace()
        max_step = int(max_step or 0)
        floor = max_step + 1 - store.max_window
        dest = lifetime_frame(lf, "dest_id")
        n_dest = _write_lifetime(store, dest, "dest_id", "da", batch)
        del dest
        orig = lifetime_frame(lf, "orig_id")
        n_orig = _write_lifetime(store, orig, "orig_id", "oa", batch)
        del orig
        n_windows = _write_windows(store, window_frame(lf, floor), batch)
        _write_dedupe(store, event_ids, 5000)
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
           "total_seconds": round(time.perf_counter() - t0, 1)}
    logger.info("Online store bootstrapped: %s", out)
    return out

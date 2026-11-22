"""Time each online-store bootstrap stage on the real PostgreSQL snapshot.

Writes into a separate namespace (default ``rs:bench``) one stage at a time
and deletes it after each stage, so the live ``rs:v1`` store is untouched and
peak extra Redis memory is one stage's worth.

    python scripts/bench_bootstrap.py --impl current
    python scripts/bench_bootstrap.py --impl current --compare reference

``--compare reference`` writes each stage with both the given implementation
and the original (pre-optimisation) one and checks that a random sample of the
written hashes is byte-for-byte identical.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path

import polars as pl

from riskstream.config import PROJECT_ROOT, StreamingConfig, get_settings
from riskstream.features import online_bootstrap as ob
from riskstream.features.online_store import RedisFeatureStore
from riskstream.features.registry import get_registry
from riskstream.storage.admin import admin_engine

CACHE = PROJECT_ROOT / "logs" / "bench_snapshot"


def snapshot(store: RedisFeatureStore) -> tuple[Path, Path, float]:
    CACHE.mkdir(parents=True, exist_ok=True)
    tx, ev = CACHE / "transactions.csv", CACHE / "events.csv"
    t = time.perf_counter()
    if not tx.exists():
        engine = admin_engine(get_settings().admin_database_url)
        try:
            ob.export_snapshot(engine, CACHE, store.cfg.dedupe_ttl_s)
        finally:
            engine.dispose()
    return tx, ev, time.perf_counter() - t


def sample_hashes(store: RedisFeatureStore, keys: list[str]) -> dict[str, dict]:
    pipe = store.r.pipeline(transaction=False)
    for k in keys:
        pipe.hgetall(k)
    return dict(zip(keys, pipe.execute(), strict=True))


def run_stages(store: RedisFeatureStore, lf: pl.LazyFrame, impl, timings: dict, label: str,
               sample_keys: dict[str, list[str]] | None = None) -> dict[str, dict]:
    floor = lf.select(pl.col("step").max()).collect().item() + 1 - store.max_window
    samples: dict[str, dict] = {}
    for role, key, kind in (("dest", "dest_id", "da"), ("orig", "orig_id", "oa")):
        t = time.perf_counter()
        df = impl.lifetime_frame(lf, key)
        timings[f"{label}.{role}_lifetime_compute"] = time.perf_counter() - t
        t = time.perf_counter()
        impl.write_lifetime(store, df, key, kind)
        timings[f"{label}.{role}_lifetime_write"] = time.perf_counter() - t
        if sample_keys is not None:
            samples |= sample_hashes(store, sample_keys[kind])
        del df
        store.clear_namespace()
    t = time.perf_counter()
    wf = impl.window_frame(lf, floor)
    timings[f"{label}.window_compute"] = time.perf_counter() - t
    t = time.perf_counter()
    n = impl.write_windows(store, wf)
    timings[f"{label}.window_write"] = time.perf_counter() - t
    timings[f"{label}.window_hashes"] = n
    if sample_keys is not None:
        samples |= sample_hashes(store, sample_keys["dw"])
    store.clear_namespace()
    return samples


class Reference:
    """The original implementation, kept verbatim for timing and equality checks."""

    @staticmethod
    def lifetime_frame(lf, key):
        per_step = lf.group_by(key, "step").agg(pl.len().alias("n"))
        steps = pl.col("step").sort(descending=True)
        return (per_step.group_by(key)
                .agg(pl.col("n").sum().alias("total"), pl.col("step").max().alias("last"),
                     pl.col("n").sort_by("step").last().alias("cnt_last"),
                     steps.slice(1, 1).first().fill_null(-1).alias("prev"))
                .collect())

    @staticmethod
    def write_lifetime(store, df, key, kind, batch=500):
        from riskstream.features.online_store import stat_bucket

        buckets: dict[int, dict[str, str]] = {}
        for sl in df.iter_slices(200_000):
            for ent, total, last, cl, prev in sl.select(key, "total", "last", "cnt_last", "prev").iter_rows():
                buckets.setdefault(stat_bucket(ent, store.cfg.stat_buckets), {})[ent] = f"{total}|{last}|{cl}|{prev}"
        pipe = store.r.pipeline(transaction=False)
        for i, (b, mapping) in enumerate(buckets.items(), 1):
            pipe.hset(f"{store.ns}:{kind}:{b}", mapping=mapping)
            if i % batch == 0:
                pipe.execute()
        pipe.execute()

    @staticmethod
    def window_frame(lf, floor):
        return (lf.filter(pl.col("step") >= floor).group_by("dest_id", "step")
                .agg(pl.len().cast(pl.Float64).alias("c"), pl.col("amount").sum().alias("s"),
                     pl.col("amount").log1p().sum().alias("l"), (pl.col("amount").log1p() ** 2).sum().alias("q"),
                     (pl.col("type") == "TRANSFER").sum().cast(pl.Float64).alias("t"))
                .collect())

    @staticmethod
    def write_windows(store, df, batch=500):
        pipe = store.r.pipeline(transaction=False)
        keys = 0
        for i, (dest, rows) in enumerate(df.sort("dest_id").partition_by("dest_id", as_dict=True).items(), 1):
            dest_id = dest[0] if isinstance(dest, tuple) else dest
            mapping = {}
            for step, c, s, lg, q, t in rows.select("step", "c", "s", "l", "q", "t").iter_rows():
                mapping |= {f"{step}:c": repr(c), f"{step}:s": repr(s), f"{step}:l": repr(lg),
                            f"{step}:q": repr(q), f"{step}:t": repr(t)}
            k = store.k_window(dest_id)
            pipe.hset(k, mapping=mapping)
            pipe.expire(k, store.cfg.window_key_ttl_s)
            keys += 1
            if i % batch == 0:
                pipe.execute()
        pipe.execute()
        return keys


class Current:
    """Whatever riskstream.features.online_bootstrap provides now."""

    lifetime_frame = staticmethod(ob.lifetime_frame)
    window_frame = staticmethod(ob.window_frame)

    @staticmethod
    def write_lifetime(store, df, key, kind):
        return ob._write_lifetime(store, df, key, kind, 500)

    @staticmethod
    def write_windows(store, df):
        return ob._write_windows(store, df, 500)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--impl", choices=["current", "reference"], default="current")
    ap.add_argument("--compare", choices=["reference"])
    ap.add_argument("--namespace", default="rs:bench")
    ap.add_argument("--sample", type=int, default=400)
    args = ap.parse_args()

    cfg = StreamingConfig.load(namespace=args.namespace)
    store = RedisFeatureStore.from_url(get_settings().redis_url, cfg, get_registry().config.windows)
    store.clear_namespace()
    tx, _, export_s = snapshot(store)
    lf = pl.scan_csv(tx, schema=ob.SCHEMA)
    timings: dict = {"export_s_(0_if_cached)": export_s}
    impl = Current if args.impl == "current" else Reference

    sample_keys = None
    if args.compare:
        rng = random.Random(0)
        dests = lf.select(pl.col("dest_id").unique()).collect()["dest_id"].to_list()
        recent = (lf.filter(pl.col("step") >= lf.select(pl.col("step").max()).collect().item() - 168)
                  .select(pl.col("dest_id").unique()).collect()["dest_id"].to_list())
        sample_keys = {
            "da": [f"{store.ns}:da:{b}" for b in rng.sample(range(cfg.stat_buckets), args.sample)],
            "oa": [f"{store.ns}:oa:{b}" for b in rng.sample(range(cfg.stat_buckets), args.sample)],
            "dw": [store.k_window(d) for d in rng.sample(recent, min(args.sample * 5, len(recent)))],
        }
        del dests

    t0 = time.perf_counter()
    got = run_stages(store, lf, impl, timings, args.impl, sample_keys)
    timings[f"{args.impl}.total_stages_s"] = time.perf_counter() - t0
    if args.compare:
        t0 = time.perf_counter()
        ref = run_stages(store, lf, Reference, timings, "reference", sample_keys)
        timings["reference.total_stages_s"] = time.perf_counter() - t0
        def parsed(k: str, h: dict) -> dict:
            # lifetime values are exact strings; window values are floats parsed by the read path
            return h if ":dw:" not in k else {f: float(v) for f, v in h.items()}

        diff = [k for k in got if parsed(k, got[k]) != parsed(k, ref[k])]
        text_diff = sum(1 for k in got for f in got[k] if got[k][f] != ref[k].get(f))
        nonempty = sum(1 for k in got if got[k])
        fields = sum(len(v) for v in got.values())
        timings["compare"] = {"keys_compared": len(got), "nonempty": nonempty, "fields_compared": fields,
                              "keys_with_different_values": len(diff), "examples": diff[:3],
                              "fields_with_different_text_only": text_diff}
    store.clear_namespace()
    print(json.dumps({k: (round(v, 2) if isinstance(v, float) else v) for k, v in timings.items()}, indent=2))


if __name__ == "__main__":
    main()

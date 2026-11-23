"""Online store against a real Redis (database 15, a unique namespace per test).

    $env:RISKSTREAM_TEST_REDIS_URL = "redis://:<password>@127.0.0.1:6379/15"
"""

from __future__ import annotations

import math
import os
import threading
import time

import polars as pl
import pytest

URL = os.environ.get("RISKSTREAM_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not URL, reason="RISKSTREAM_TEST_REDIS_URL not set")

WINDOWS = (24, 168)


@pytest.fixture
def store():
    from riskstream.config import StreamingConfig
    from riskstream.features.online_store import RedisFeatureStore
    from tests.integration.leftovers import namespace, new_tag

    cfg = StreamingConfig.load(namespace=namespace(new_tag()), stat_buckets=64)
    s = RedisFeatureStore.from_url(URL, cfg, WINDOWS)
    yield s
    s.clear_namespace()


def live(store):
    now = int(time.time() * 1000)
    store.write_meta(relay_heartbeat_ms=now, consumer_heartbeat_ms=now, consumer_state="running",
                     relay_oldest_unpublished_age_s=0, consumer_lag=0)


def same(a, b) -> bool:
    if a is None or (isinstance(a, float) and math.isnan(a)):
        return b is None or (isinstance(b, float) and math.isnan(b))
    return b is not None and math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-6)


def _reference_hashes(store, lf):
    """The original per-entry implementation (redis-py HSET per bucket / per account)."""
    from riskstream.features import online_bootstrap as ob
    from riskstream.features.online_store import stat_bucket

    pipe = store.r.pipeline(transaction=False)
    for key, kind in (("dest_id", "da"), ("orig_id", "oa")):
        buckets: dict[int, dict[str, str]] = {}
        for ent, total, last, cl, prev in ob.lifetime_frame(lf, key).select(
                key, "total", "last", "cnt_last", "prev").iter_rows():
            buckets.setdefault(stat_bucket(ent, store.cfg.stat_buckets), {})[ent] = f"{total}|{last}|{cl}|{prev}"
        for b, mapping in buckets.items():
            pipe.hset(f"{store.ns}:{kind}:{b}", mapping=mapping)
    floor = lf.select(pl.col("step").max()).collect().item() + 1 - store.max_window
    for dest, rows in ob.window_frame(lf, floor).partition_by("dest_id", as_dict=True).items():
        mapping = {}
        for step, c, s, lg, q, t in rows.select("step", "c", "s", "l", "q", "t").iter_rows():
            mapping |= {f"{step}:c": repr(c), f"{step}:s": repr(s), f"{step}:l": repr(lg), f"{step}:q": repr(q),
                        f"{step}:t": repr(t)}
        pipe.hset(store.k_window(dest[0]), mapping=mapping)
    pipe.execute()
    return _dump(store)


def _dump(store):
    keys = sorted(k for k in store.r.scan_iter(f"{store.ns}:*") if ":d" in k or ":o" in k)
    return {k: store.r.hgetall(k) for k in keys if store.r.type(k) == "hash" and not k.endswith(":meta")}


def test_bulk_writer_matches_reference_byte_for_byte(store, small_canonical):
    """The RESP bulk writer must produce exactly the hashes the original code wrote."""
    from riskstream.features import online_bootstrap as ob

    lf = small_canonical.select(list(ob.SCHEMA)).cast(ob.SCHEMA)
    ref = _reference_hashes(store, lf)
    store.clear_namespace()
    ob.bootstrap(store, frame=lf)
    got = _dump(store)
    assert set(got) == set(ref) and len(got) > 100
    assert got == ref
    ttls = [store.r.ttl(k) for k in got if ":dw:" in k]
    assert ttls and all(0 < t <= store.cfg.window_key_ttl_s for t in ttls)


def test_bulk_writer_handles_non_ascii_ids(store):
    from riskstream.features import online_bootstrap as ob

    lf = pl.LazyFrame({"dest_id": ["CÄ€é漢", "C2"], "orig_id": ["O€", "O2"], "step": [5, 6],
                       "amount": [1.5, 2.0], "type": ["TRANSFER", "PAYMENT"]}).cast(ob.SCHEMA)
    ob.bootstrap(store, frame=lf)
    live(store)
    p = store.primitives("CÄ€é漢", "O€", 7)
    assert p["dest_p_cnt_all"] == 1 and p["dest_p_transfer_cnt_24"] == 1 and p["orig_p_cnt_all"] == 1


def test_lock_of_crashed_bootstrap_expires(store, monkeypatch):
    """A bootstrap that dies without releasing its lock must not block recovery for long."""
    from riskstream.features import online_bootstrap as ob

    monkeypatch.setattr(ob, "LOCK_TTL_S", 2)
    store.r.set(ob.lock_key(store), "dead-process", ex=ob.LOCK_TTL_S)  # what a killed process leaves
    with pytest.raises(ob.BootstrapInProgress):
        ob.bootstrap(store, frame=pl.LazyFrame({"dest_id": ["C1"], "orig_id": ["O1"], "step": [1], "amount": [1.0],
                                                "type": ["PAYMENT"]}).cast(ob.SCHEMA))
    time.sleep(2.5)
    ob.bootstrap(store, frame=pl.LazyFrame({"dest_id": ["C1"], "orig_id": ["O1"], "step": [1], "amount": [1.0],
                                            "type": ["PAYMENT"]}).cast(ob.SCHEMA))
    assert store.is_bootstrapped() and not store.r.exists(ob.lock_key(store))


def test_lease_is_renewed_during_long_bootstrap(store, monkeypatch, small_canonical):
    from riskstream.features import online_bootstrap as ob

    monkeypatch.setattr(ob, "LOCK_TTL_S", 2)
    monkeypatch.setattr(ob, "LOCK_RENEW_S", 0.5)
    real = ob._write_windows
    seen = {}

    def slow(store_, df, batch=0):
        time.sleep(3)  # longer than the TTL: survives only if renewed
        seen["lock_alive"] = bool(store_.r.exists(ob.lock_key(store_)))
        return real(store_, df, batch)

    monkeypatch.setattr(ob, "_write_windows", slow)
    ob.bootstrap(store, frame=small_canonical)
    assert seen["lock_alive"] and not store.r.exists(ob.lock_key(store))


def test_apply_refused_before_bootstrap(store):
    from riskstream.features.online_store import FeatureStoreNotUsable

    with pytest.raises(FeatureStoreNotUsable, match="not_bootstrapped"):
        store.apply(event_id="e1", dest_id="D", orig_id="O", step=1, amount=1.0, txn_type="TRANSFER")


def test_bootstrap_then_stream_matches_offline(store, small_canonical):
    """Bootstrap from steps 1-20, stream steps 21-48 event by event, and compare every
    transaction's primitives (read before its own event is applied) with the offline
    as-of-join computation."""
    from riskstream.features.derive import primitive_columns
    from riskstream.features.offline import compute_primitives
    from riskstream.features.online_bootstrap import bootstrap

    df = small_canonical.collect()
    bootstrap(store, frame=df.filter(pl.col("step") <= 20).lazy())
    live(store)
    offline = {r["transaction_id"]: r for r in compute_primitives(small_canonical, WINDOWS).collect()
               .iter_rows(named=True)}
    cols = primitive_columns(WINDOWS)
    compared = 0
    for r in df.filter(pl.col("step") > 20).sort("step", "event_seq").iter_rows(named=True):
        got = store.primitives(r["dest_id"], r["orig_id"], r["step"])
        ref = offline[r["transaction_id"]]
        for c in cols:
            assert same(got[c], ref[c]), (r["transaction_id"], c, got[c], ref[c])
        compared += 1
        assert store.apply(event_id=f"ev-{r['transaction_id']}", dest_id=r["dest_id"], orig_id=r["orig_id"],
                           step=r["step"], amount=r["amount"], txn_type=r["type"])
    assert compared > 1000


def test_duplicate_events_are_ignored(store, small_canonical):
    from riskstream.features.online_bootstrap import bootstrap

    bootstrap(store, frame=small_canonical.filter(pl.col("step") <= 1))
    live(store)
    args = dict(event_id="dup-1", dest_id="CX", orig_id="OX", step=30, amount=50.0, txn_type="TRANSFER")
    assert store.apply(**args) is True
    assert store.apply(**args) is False
    p = store.primitives("CX", "OY", 31)
    assert p["dest_p_cnt_24"] == 1 and p["dest_p_cnt_all"] == 1 and store.applied_count(30) == 1


def test_bootstrap_dedupes_snapshotted_events(store, small_canonical):
    from riskstream.features.online_bootstrap import bootstrap

    bootstrap(store, frame=small_canonical.filter(pl.col("step") <= 5), event_ids=["already-in-snapshot"])
    assert store.apply(event_id="already-in-snapshot", dest_id="CZ", orig_id="O", step=6, amount=1.0,
                       txn_type="PAYMENT") is False


def test_pruning_and_late_queries(store, small_canonical):
    from riskstream.features.online_bootstrap import bootstrap
    from riskstream.features.online_store import FeatureStoreNotUsable

    bootstrap(store, frame=small_canonical.filter(pl.col("step") <= 1))
    live(store)
    for i, step in enumerate([10, 20, 400]):
        store.apply(event_id=f"p{i}", dest_id="CP", orig_id=f"O{i}", step=step, amount=10.0, txn_type="CASH_OUT")
    fields = store.r.hkeys(store.k_window("CP"))
    assert {f.split(":")[0] for f in fields} == {"400"}  # older buckets pruned
    p = store.primitives("CP", "Onew", 401)
    assert p["dest_p_cnt_all"] == 3 and p["dest_p_last_step"] == 400 and p["dest_p_cnt_168"] == 1
    with pytest.raises(FeatureStoreNotUsable, match="late_query"):
        store.primitives("CP", "Onew", 300)


def test_freshness_gate_and_window_floor(store, small_canonical):
    from riskstream.features.online_bootstrap import bootstrap
    from riskstream.features.online_store import FeatureStoreNotUsable

    out = bootstrap(store, frame=small_canonical.filter(pl.col("step") <= 40))
    assert out["window_floor"] == 41 - 168
    with pytest.raises(FeatureStoreNotUsable, match="heartbeat"):
        store.primitives("C1", "C2", 41)  # no relay/consumer heartbeat yet
    live(store)
    store.primitives("C1", "C2", 41)
    # step 20 would need window buckets older than the bootstrap kept
    with pytest.raises(FeatureStoreNotUsable, match="before_window_floor"):
        store.primitives("C1", "C2", 20)


def test_concurrent_applies_are_atomic(store, small_canonical):
    from riskstream.features.online_bootstrap import bootstrap

    bootstrap(store, frame=small_canonical.filter(pl.col("step") <= 1))
    live(store)

    def worker(w):
        for i in range(100):
            store.apply(event_id=f"c{w}-{i}", dest_id="CHOT", orig_id=f"O{w}", step=50, amount=1.0,
                        txn_type="TRANSFER")

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    p = store.primitives("CHOT", "Oq", 51)
    assert p["dest_p_cnt_24"] == 800 and p["dest_p_sum_24"] == pytest.approx(800.0)
    assert p["dest_p_transfer_cnt_24"] == 800 and p["dest_p_cnt_all"] == 800

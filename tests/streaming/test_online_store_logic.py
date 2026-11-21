"""Pure read-side logic and bootstrap aggregates of the online store (no Redis needed)."""

import polars as pl
import pytest

from riskstream.config import StreamingConfig
from riskstream.features.online_bootstrap import lifetime_frame, window_frame
from riskstream.features.online_store import (
    FeatureStoreNotUsable,
    assess_freshness,
    lifetime_before,
    parse_lifetime,
    stat_bucket,
    window_primitives,
)

CFG = StreamingConfig.load()
NOW = 1_000_000_000


def test_stat_bucket_is_stable():
    assert stat_bucket("C1231006815", 32768) == stat_bucket("C1231006815", 32768)
    assert 0 <= stat_bucket("x", 7) < 7


def test_lifetime_before_excludes_same_step():
    stats = parse_lifetime("5|10|2|7")  # 5 events, newest step 10 (2 events), previous step 7
    assert lifetime_before(stats, 11) == (5.0, 10)
    assert lifetime_before(stats, 10) == (3.0, 7)
    assert lifetime_before(None, 10) == (0.0, None)
    assert lifetime_before(parse_lifetime("2|10|2|-1"), 10) == (0.0, None)
    with pytest.raises(FeatureStoreNotUsable, match="late_query"):
        lifetime_before(stats, 9)


def test_window_primitives_bounds():
    h = {"1:c": "1", "1:s": "10", "2:c": "2", "2:s": "50", "2:t": "2", "26:c": "1", "26:s": "5"}
    p = window_primitives(h, 26, (24, 168))
    assert p["dest_p_cnt_24"] == 2 and p["dest_p_sum_24"] == 50  # step 1 out, step 26 same-step out
    assert p["dest_p_cnt_168"] == 3 and p["dest_p_transfer_cnt_168"] == 2


def meta(**kw):
    base = {"bootstrap_completed_at_ms": "1", "layout": CFG.namespace, "relay_heartbeat_ms": str(NOW),
            "consumer_heartbeat_ms": str(NOW), "consumer_state": "running",
            "relay_oldest_unpublished_age_s": "0.1", "consumer_lag": "0"}
    return base | kw


@pytest.mark.parametrize(("change", "reason"), [
    ({"bootstrap_completed_at_ms": None}, "not_bootstrapped"),
    ({"layout": "rs:v0"}, "layout_mismatch"),
    ({"relay_heartbeat_ms": str(NOW - 60_000)}, "relay_heartbeat_stale"),
    ({"consumer_heartbeat_ms": None}, "consumer_heartbeat_stale"),
    ({"consumer_state": "blocked"}, "consumer_not_running"),
    ({"relay_oldest_unpublished_age_s": "30"}, "relay_backlog"),
    ({"consumer_lag": "5000"}, "consumer_lag"),
    ({"scored_lag_broker": "5000"}, "consumer_lag"),
    ({"consumer_stalled_s": "10"}, "consumer_stalled"),
])
def test_freshness_rules(change, reason):
    assert assess_freshness(meta(), CFG, NOW).usable
    f = assess_freshness(meta(**change), CFG, NOW)
    assert not f.usable and f.reason == reason


def test_bootstrap_aggregates():
    lf = pl.LazyFrame({"dest_id": ["D", "D", "D", "E"], "orig_id": ["A", "B", "A", "C"], "step": [3, 7, 7, 5],
                       "amount": [1.0, 2.0, 3.0, 4.0], "type": ["TRANSFER", "CASH_OUT", "TRANSFER", "PAYMENT"]})
    d = {r["dest_id"]: r for r in lifetime_frame(lf, "dest_id").iter_rows(named=True)}
    assert (d["D"]["total"], d["D"]["last"], d["D"]["cnt_last"], d["D"]["prev"]) == (3, 7, 2, 3)
    assert (d["E"]["total"], d["E"]["last"], d["E"]["cnt_last"], d["E"]["prev"]) == (1, 5, 1, -1)
    w = window_frame(lf, floor=5).sort("dest_id", "step")
    assert w.select("dest_id", "step", "c", "t").rows() == [("D", 7, 2.0, 1.0), ("E", 5, 1.0, 0.0)]

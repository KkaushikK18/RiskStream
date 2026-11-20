import math

import polars as pl
import pytest

from riskstream.features.offline import build_feature_frame, compute_primitives
from riskstream.features.online import EntityHistory
from riskstream.features.parity import compare
from riskstream.features.registry import EXCLUDED_COLUMNS, FeatureConfig, FeatureRegistry, build_specs
from tests.conftest import raw_rows, to_canonical

WINDOWS = (24, 168)


@pytest.fixture
def registry() -> FeatureRegistry:
    cfg = FeatureConfig("test", WINDOWS, {"full": ("transaction", "balance", "dest_behavior", "orig_behavior"),
                                          "no_balance": ("transaction", "dest_behavior", "orig_behavior")}, 0.1, 0)
    return FeatureRegistry(cfg, build_specs(WINDOWS))


def _feats(rows: list[dict], tmp_path, registry) -> pl.DataFrame:
    return build_feature_frame(to_canonical(raw_rows(rows), tmp_path), registry).collect().sort("event_seq")


def test_window_counts_and_boundaries(tmp_path, registry):
    # Destination D receives at steps 1, 2, 2, 26, 200. Query rows are those events themselves.
    rows = [{"step": s, "nameDest": "CD", "type": "TRANSFER", "amount": a}
            for s, a in [(1, 10.0), (2, 20.0), (2, 30.0), (26, 40.0), (200, 50.0)]]
    f = _feats(rows, tmp_path, registry)
    # step 1: no history
    assert f["dest_txn_count_24h"][0] == 0
    assert math.isnan(f["dest_amount_mean_24h"][0])
    # step 2: only step-1 event counts; the other step-2 event is same-step -> excluded
    assert f["dest_txn_count_24h"][1] == 1 and f["dest_txn_count_24h"][2] == 1
    assert f["dest_amount_sum_24h"][2] == pytest.approx(10.0)
    # step 26: window [2, 25] -> both step-2 events, step 1 has dropped out
    assert f["dest_txn_count_24h"][3] == 2
    assert f["dest_amount_sum_24h"][3] == pytest.approx(50.0)
    assert f["dest_txn_count_168h"][3] == 3
    assert f["dest_hours_since_last_txn"][3] == 24
    # step 200: window_168 = [32, 199] -> nothing; all-time count still 4
    assert f["dest_txn_count_168h"][4] == 0
    assert f["dest_txn_count_all"][4] == 4
    assert f["dest_hours_since_last_txn"][4] == 174


def test_future_events_never_leak(tmp_path, registry):
    base = [{"step": 5, "nameDest": "CD", "amount": 10.0}]
    with_future = base + [{"step": 6 + i, "nameDest": "CD", "amount": 1e6} for i in range(5)]
    a = _feats(base, tmp_path, registry).row(0, named=True)
    b = _feats(with_future, tmp_path, registry).row(0, named=True)
    for k in a:
        if isinstance(a[k], float) and math.isnan(a[k]):
            assert math.isnan(b[k]), k
        else:
            assert a[k] == b[k], k


def test_labels_do_not_affect_features(tmp_path, registry):
    rows = [{"step": s, "nameDest": "CD", "type": "TRANSFER", "amount": 100.0 + s} for s in range(1, 30)]
    flipped = [r | {"isFraud": 1} for r in rows]
    a = _feats(rows, tmp_path, registry).drop("is_fraud")
    b = _feats(flipped, tmp_path, registry).drop("is_fraud")
    assert a.equals(b, null_equal=True)


def test_std_and_zscore(tmp_path, registry):
    rows = [{"step": 1, "nameDest": "CD", "amount": 99.0}, {"step": 1, "nameDest": "CD", "amount": 999.0},
            {"step": 2, "nameDest": "CD", "amount": 9999.0}]
    f = _feats(rows, tmp_path, registry).row(2, named=True)
    logs = [math.log1p(99.0), math.log1p(999.0)]
    mean = sum(logs) / 2
    std = math.sqrt(sum((x - mean) ** 2 for x in logs) / 1)
    assert f["dest_log_amount_std_24h"] == pytest.approx(std, rel=1e-5)
    assert f["dest_log_amount_zscore_24h"] == pytest.approx((math.log1p(9999.0) - mean) / std, rel=1e-5)


def test_constant_history_gives_undefined_zscore(tmp_path, registry):
    rows = [{"step": 1, "nameDest": "CD", "amount": 5.0}, {"step": 1, "nameDest": "CD", "amount": 5.0 + 0},
            {"step": 3, "nameDest": "CD", "amount": 7.0}]
    rows[1]["nameOrig"] = "C2"  # avoid exact-duplicate semantics in the test data
    f = _feats(rows, tmp_path, registry).row(2, named=True)
    assert math.isnan(f["dest_log_amount_zscore_24h"])


def test_balance_artifact_features(tmp_path, registry):
    rows = [{"type": "TRANSFER", "nameDest": "C9", "amount": 500.0, "oldbalanceOrg": 500.0},
            {"type": "TRANSFER", "nameDest": "C9", "amount": 10.0, "oldbalanceOrg": 0.0}]
    f = _feats(rows, tmp_path, registry)
    assert f["amount_drains_orig_balance"].to_list() == [1.0, 0.0]
    assert f["orig_balance_is_zero"].to_list() == [0.0, 1.0]
    assert f["amount_exceeds_orig_balance"].to_list() == [0.0, 1.0]


def test_excluded_columns_not_features(registry):
    assert not set(EXCLUDED_COLUMNS) & set(registry.all_names()) - {"orig_balance_before"}
    for name in ("orig_balance_after", "dest_balance_before", "dest_balance_after", "is_flagged_fraud"):
        assert name not in registry.all_names()


def test_no_balance_set_drops_balance_group(registry):
    assert "amount_drains_orig_balance" not in registry.names("no_balance")
    assert "amount_drains_orig_balance" in registry.names("full")


def test_offline_online_parity_on_synthetic(small_canonical, registry):
    # Restrict to a few busy destinations so windows and pruning are exercised.
    df = small_canonical.collect()
    busy = df.group_by("dest_id").len().sort("len", descending=True).head(40)["dest_id"]
    sub = df.filter(pl.col("dest_id").is_in(busy.implode())).head(1500)
    offline = build_feature_frame(sub.lazy(), registry).collect()
    result = compare(sub, offline, registry)
    assert result["mismatches"] == [], result["mismatches"][:5]
    assert result["rows"] == len(sub)


def test_online_duplicate_event_ignored():
    h = EntityHistory(WINDOWS)
    assert h.update("t1", "D", "O", 1, 10.0, "TRANSFER")
    assert not h.update("t1", "D", "O", 1, 10.0, "TRANSFER")
    assert h.primitives("D", "O", 2)["dest_p_cnt_24"] == 1


def test_online_pruning_keeps_all_time_counts():
    h = EntityHistory(WINDOWS)
    for i, step in enumerate([1, 2, 400, 800]):
        h.update(f"t{i}", "D", "O", step, 10.0, "CASH_OUT")
    p = h.primitives("D", "O", 801)
    assert p["dest_p_cnt_all"] == 4
    assert p["dest_p_last_step"] == 800
    assert p["dest_p_cnt_168"] == 1


def test_primitive_columns_present(small_canonical):
    out = compute_primitives(small_canonical, WINDOWS).collect_schema().names()
    assert "dest_p_cnt_24" in out and "orig_p_last_step" in out

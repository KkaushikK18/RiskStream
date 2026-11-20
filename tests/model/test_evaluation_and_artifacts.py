import numpy as np
import pytest
from sklearn.metrics import brier_score_loss

from riskstream.models.artifacts import BUNDLE_FORMAT, FeatureSchemaMismatch, ModelBundle, load_bundle
from riskstream.models.calibration import Calibrator, select_calibrator
from riskstream.models.estimators import SUPERVISED, build_estimator, predict_scores
from riskstream.models.evaluation import (
    NEVER,
    CostMatrix,
    DecisionThresholds,
    binary_metrics,
    optimize_thresholds,
    policy_cost,
    precision_at_k,
    recall_at_fpr,
)
from riskstream.training.tuning import trial_params

CM = CostMatrix(0.0, 1.0, 50.0, 5.0, 0.95)


def test_binary_metrics_hand_example():
    y = np.array([1, 1, 0, 0, 0])
    m = binary_metrics(y, np.array([1, 0, 1, 0, 0]))
    assert (m["tp"], m["fp"], m["fn"], m["tn"]) == (1, 1, 1, 2)
    assert m["precision"] == 0.5 and m["recall"] == 0.5
    assert m["fpr"] == pytest.approx(1 / 3) and m["fnr"] == 0.5


def test_precision_at_k_and_recall_at_fpr():
    y = np.array([1, 0, 1, 0, 0, 0])
    s = np.array([0.9, 0.8, 0.7, 0.3, 0.2, 0.1])
    assert precision_at_k(y, s, 1) == 1.0
    assert precision_at_k(y, s, 3) == pytest.approx(2 / 3)
    r = recall_at_fpr(y, s, 0.0)
    assert r["recall"] == 0.5 and r["fpr"] == 0.0


def test_policy_cost_hand_example():
    y = np.array([1, 1, 0, 0])
    p = np.array([0.9, 0.5, 0.6, 0.1])
    amount = np.array([1000.0, 200.0, 50.0, 10.0])
    out = policy_cost(y, p, amount, DecisionThresholds(review=0.4, decline=0.8), CM)
    # fraud 1 declined: 0; fraud 2 reviewed: 5 + 0.05 * 200 = 15; legit 3 reviewed: 5; legit 4 approved: 0
    assert out["total_cost"] == pytest.approx(20.0)
    assert out["approve_everything_cost"] == pytest.approx(1200.0)
    assert out["fraud_outcomes"] == {"approved": 0, "reviewed": 1, "declined": 1}


def test_threshold_optimizer_respects_review_cap_and_beats_baseline():
    rng = np.random.default_rng(0)
    n = 20_000
    y = (rng.random(n) < 0.01).astype(int)
    p = np.clip(rng.normal(0.05, 0.03, n) + 0.6 * y * rng.random(n), 0, 1)
    amount = rng.lognormal(9, 1, n)
    th = optimize_thresholds(y, p, amount, CM, max_review_rate=0.01, grid_size=120)
    out = policy_cost(y, p, amount, th, CM)
    assert out["rates"]["review"] <= 0.01 + 1e-9
    assert out["total_cost"] < out["approve_everything_cost"]
    assert th.review <= th.decline


def test_threshold_optimizer_can_choose_never():
    y = np.zeros(1000, dtype=int)
    p = np.linspace(0, 1, 1000)
    th = optimize_thresholds(y, p, np.ones(1000), CM, 0.05, 50)
    assert th.decline == NEVER and th.review == NEVER


def test_calibration_fixes_inflated_scores():
    rng = np.random.default_rng(1)
    true_p = rng.beta(0.5, 30, 30_000)
    y = (rng.random(len(true_p)) < true_p).astype(int)
    inflated = np.clip(true_p * 8, 0, 1)  # what undersampling does to scores
    cal, info = select_calibrator(inflated, y, ["none", "sigmoid", "isotonic"], 5, 0)
    assert info["selected"] != "none"
    assert brier_score_loss(y, cal.transform(inflated)) < brier_score_loss(y, inflated)


def test_identity_calibrator():
    s = np.array([0.1, 0.5])
    assert np.array_equal(Calibrator("none").fit(s, np.array([0, 1])).transform(s), s)


def test_tuning_is_deterministic():
    space = {"a": {"int": [1, 9]}, "b": {"loguniform": [0.01, 1.0]}, "c": {"choice": [None, "x"]}}
    assert trial_params(space, {"f": 1}, 4, 3) == trial_params(space, {"f": 1}, 4, 3)
    assert all(t["f"] == 1 for t in trial_params(space, {"f": 1}, 4, 3))


@pytest.fixture
def toy_xy():
    rng = np.random.default_rng(2)
    X = rng.normal(size=(2000, 5)).astype(np.float32)
    X[rng.random(X.shape) < 0.05] = np.nan
    y = ((np.nan_to_num(X[:, 0]) + rng.normal(0, 0.5, 2000)) > 1.5).astype(int)
    return X, y


@pytest.mark.parametrize("kind", SUPERVISED)
def test_every_estimator_handles_nan_and_returns_probabilities(kind, toy_xy):
    X, y = toy_xy
    params = {"n_estimators": 10} if kind in ("random_forest", "xgboost", "lightgbm") else {}
    model = build_estimator(kind, params, seed=0, n_jobs=1).fit(X, y)
    s = predict_scores(model, X)
    assert s.shape == (len(X),) and np.all((s >= 0) & (s <= 1))


def _bundle(toy_xy, schema_hash="abc"):
    X, y = toy_xy
    model = build_estimator("lightgbm", {"n_estimators": 10}, 0, 1).fit(X, y)
    cal = Calibrator("sigmoid").fit(predict_scores(model, X), y)
    meta = {"bundle_format": BUNDLE_FORMAT, "model_version": "t1", "features": list("abcde"),
            "feature_schema_hash": schema_hash}
    return ModelBundle(model, cal, meta), X


def test_bundle_round_trip_identical_predictions(toy_xy, tmp_path):
    bundle, X = _bundle(toy_xy)
    bundle.save(tmp_path / "v1")
    loaded = load_bundle(tmp_path / "v1", expected_schema_hash="abc")
    assert np.array_equal(loaded.predict_proba(X), bundle.predict_proba(X))
    assert loaded.feature_names == list("abcde")


def test_bundle_rejects_schema_mismatch_and_wrong_width(toy_xy, tmp_path):
    bundle, X = _bundle(toy_xy)
    bundle.save(tmp_path / "v1")
    with pytest.raises(FeatureSchemaMismatch):
        load_bundle(tmp_path / "v1", expected_schema_hash="different")
    with pytest.raises(ValueError):
        bundle.predict_proba(X[:, :3])


def test_bundle_never_overwrites_a_version(toy_xy, tmp_path):
    bundle, _ = _bundle(toy_xy)
    bundle.save(tmp_path / "v1")
    with pytest.raises(FileExistsError):
        bundle.save(tmp_path / "v1")


def test_missing_bundle_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_bundle(tmp_path / "nothing")

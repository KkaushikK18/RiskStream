"""Shared fixtures. Tests use small synthetic or hand-built data only."""

from __future__ import annotations

import importlib.util
import json
from datetime import timedelta

import numpy as np
import polars as pl
import pytest

from riskstream.config import AppConfig
from riskstream.data.adapters.paysim import PAYSIM_RAW_SCHEMA, PaySimAdapter
from riskstream.data.synthetic_generator import SyntheticConfig, generate

API_DEPS = ("httpx", "prometheus_client")
MISSING_API_DEPS = [m for m in API_DEPS if importlib.util.find_spec(m) is None]
# The API suite imports FastAPI's TestClient (needs httpx) and the metrics module.
collect_ignore = ["api", "integration"] if MISSING_API_DEPS else []


def pytest_report_header(config):
    if collect_ignore:
        return (f"API and integration tests NOT collected: missing {MISSING_API_DEPS}. "
                "Run: python -m pip install -r requirements.txt")
    return None


def raw_rows(rows: list[dict]) -> pl.DataFrame:
    """Build a PaySim-format raw frame from partial row dicts."""
    defaults = {
        "step": 1, "type": "PAYMENT", "amount": 10.0, "nameOrig": "C1",
        "oldbalanceOrg": 100.0, "newbalanceOrig": 90.0, "nameDest": "M1",
        "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0, "isFlaggedFraud": 0,
    }
    return pl.DataFrame([defaults | r for r in rows]).cast(PAYSIM_RAW_SCHEMA)


def to_canonical(df: pl.DataFrame, tmp_path, name: str = "t") -> pl.LazyFrame:
    path = tmp_path / f"{name}.parquet"
    df.write_parquet(path)
    adapter = PaySimAdapter(path, id_prefix="t")
    return adapter.to_canonical(adapter.scan_raw())


@pytest.fixture
def small_synthetic_raw() -> pl.DataFrame:
    return generate(SyntheticConfig(seed=7, num_transactions=3_000, num_steps=48))


@pytest.fixture
def small_canonical(small_synthetic_raw, tmp_path) -> pl.LazyFrame:
    return to_canonical(small_synthetic_raw, tmp_path, "syn")


# ---------------------------------------------------------------- serving fixtures

MODEL_VERSION = "test-model-v1"
REVIEW_T, DECLINE_T = 0.2, 0.6
EPOCH = AppConfig.load().simulation_epoch


def ts(step: int, minute: int = 0) -> str:
    return (EPOCH + timedelta(hours=step - 1, minutes=minute)).isoformat().replace("+00:00", "Z")


def txn(transaction_id: str = "t1", step: int = 10, **kw) -> dict:
    body = {"transaction_id": transaction_id, "timestamp": ts(step), "type": "TRANSFER", "amount": 1500.0,
            "orig_account_id": "C1000001", "orig_balance_before": 1500.0, "dest_account_id": "C2000002",
            "dest_type": "CUSTOMER"}
    body.update(kw)
    return body


@pytest.fixture(scope="session")
def artifacts(tmp_path_factory):
    """A tiny LightGBM bundle trained on synthetic features, plus a matching policy file."""
    from riskstream.features.offline import build_feature_frame
    from riskstream.features.registry import get_registry
    from riskstream.models.artifacts import BUNDLE_FORMAT, ModelBundle
    from riskstream.models.calibration import Calibrator
    from riskstream.models.estimators import build_estimator, predict_scores

    d = tmp_path_factory.mktemp("artifacts")
    registry = get_registry()
    canon = to_canonical(generate(SyntheticConfig(seed=5, num_transactions=6000, num_steps=48)), d)
    feats = build_feature_frame(canon, registry).collect()
    names = registry.names("full")
    X = feats.select(names).to_numpy().astype(np.float32)
    y = feats["is_fraud"].to_numpy()
    model = build_estimator("lightgbm", {"n_estimators": 30, "num_leaves": 7}, seed=0, n_jobs=2).fit(X, y)
    cal = Calibrator("sigmoid").fit(predict_scores(model, X), y)
    meta = {"bundle_format": BUNDLE_FORMAT, "model_version": MODEL_VERSION, "model_name": "lightgbm",
            "estimator": "lightgbm", "features": names, "feature_set": "full",
            "feature_version": registry.version, "feature_schema_hash": registry.schema_hash("full"),
            "calibration_method": "sigmoid", "dataset": "synthetic", "is_synthetic_data": True,
            "mlflow_run_id": None}
    bundle_dir = d / "bundle"
    ModelBundle(model, cal, meta).save(bundle_dir)
    policy = {"policy_version": "test-1", "model_version": MODEL_VERSION, "feature_version": registry.version,
              "thresholds": {"review": REVIEW_T, "decline": DECLINE_T}, "approval": {"status": "unapproved"}}
    policy_path = d / "policy.json"
    policy_path.write_text(json.dumps(policy))
    return {"bundle": bundle_dir, "policy": policy_path, "dir": d}

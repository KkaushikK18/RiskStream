"""End-to-end training on small synthetic data, isolated in a temp directory."""

import json

import numpy as np
import polars as pl
import pytest

from riskstream.data.splitting import SplitBoundaries, assign_split
from riskstream.data.synthetic_generator import SyntheticConfig, generate
from riskstream.features.offline import build_feature_frame
from riskstream.features.registry import get_registry
from riskstream.models.artifacts import load_bundle
from tests.conftest import to_canonical


@pytest.mark.slow
def test_training_run_reads_test_once_after_selection(tmp_path, monkeypatch):
    import riskstream.training.pipeline as tp

    registry = get_registry()
    canon = to_canonical(generate(SyntheticConfig(seed=11, num_transactions=12_000, num_steps=60)), tmp_path)
    feats = assign_split(build_feature_frame(canon, registry), SplitBoundaries(40, 50)).collect()
    tables = {s: feats.filter(pl.col("split") == s).drop("split") for s in ("train", "val", "test")}
    tables["train_sampled"] = tables["train"]
    fdir = tmp_path / "features"
    fdir.mkdir()
    (fdir / "feature_manifest.json").write_text(json.dumps({
        "feature_version": registry.version, "source_raw_sha256": "test",
        "rows": {k: len(v) for k, v in tables.items()},
        "train_sampled": {"rows": len(tables["train"]), "fraud": int(tables["train"]["is_fraud"].sum()),
                          "negative_sample_rate": 1.0},
    }))

    loads: list[str] = []

    def fake_load(dataset, split):
        loads.append(split)
        return tables[split]

    monkeypatch.setattr(tp, "load_features", fake_load)
    monkeypatch.setattr(tp, "features_dir", lambda dataset: fdir)
    monkeypatch.setattr(tp, "PROJECT_ROOT", tmp_path)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}")
    tp.get_settings.cache_clear()

    out = tp.run_training("synthetic", models=["logistic_regression", "lightgbm"], n_trials=1)
    tp.get_settings.cache_clear()

    assert loads.count("test") == 1 and loads[-1] == "test"
    bundle = load_bundle(out["candidate_bundle"], registry.schema_hash("full"))
    assert bundle.metadata["is_synthetic_data"] is True
    assert bundle.metadata["mlflow_run_id"] == out["mlflow_run_id"]
    assert bundle.metadata["model_name"] in ("logistic_regression", "lightgbm")
    X = tables["val"].select(bundle.feature_names).to_numpy().astype(np.float32)
    p = bundle.predict_proba(X)
    assert np.all((p >= 0) & (p <= 1))
    results = json.loads((tmp_path / "reports" / "model_evaluation").glob("*/results.json").__next__().read_text())
    assert results["selection"]["split"] == "val"
    assert "Synthetic generator data" in next((tmp_path / "reports" / "model_evaluation").glob("*/report.md")).read_text()

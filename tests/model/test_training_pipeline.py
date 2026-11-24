"""End-to-end training on small synthetic data, isolated in a temp directory."""

import json
from pathlib import Path

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


@pytest.mark.slow
def test_ieee_cis_training_end_to_end(tmp_path, monkeypatch):
    """Dataset-specific registry, model config and disclaimer flow through training and policy."""
    import riskstream.training.pipeline as tp
    import riskstream.training.policy as pol
    from riskstream.features import ieee_cis as feats
    from tests.unit.test_ieee_cis import _tiny_splits

    _tiny_splits(tmp_path)
    fdir = tmp_path / "features"
    feats.run_build_features(splits_dir=tmp_path / "splits", out_dir=fdir)
    registry = feats.IEEERegistry.load(fdir)

    def load(dataset, split):
        assert dataset == "ieee_cis"
        return pl.read_parquet(fdir / f"{split}.parquet")

    for mod in (tp, pol):
        monkeypatch.setattr(mod, "load_features", load)
        monkeypatch.setattr(mod, "registry_for", lambda dataset: registry)
        monkeypatch.setattr(mod, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(tp, "features_dir", lambda dataset: fdir)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}")
    tp.get_settings.cache_clear()
    try:
        assert "lightgbm_no_vesta_v" in tp.model_config("ieee_cis")["models"]
        assert "lightgbm_no_balance" in tp.model_config("paysim")["models"]
        out = tp.run_training("ieee_cis", models=["logistic_regression", "lightgbm"], n_trials=1)
        policy = pol.run_policy_tuning("ieee_cis", Path(out["candidate_bundle"]))
    finally:
        tp.get_settings.cache_clear()

    fs = json.loads((Path(out["candidate_bundle"]) / "metadata.json").read_text())["feature_set"]
    bundle = load_bundle(out["candidate_bundle"], registry.schema_hash(fs))
    meta = bundle.metadata
    assert meta["feature_set"] == ("linear" if meta["model_name"] == "logistic_regression" else "full")
    assert meta["dataset"] == "ieee_cis" and meta["feature_version"] == registry.version
    assert meta["is_synthetic_data"] is False
    lr = json.loads(next((tmp_path / "reports" / "model_evaluation").glob("*/results.json")).read_text())
    assert lr["supervised"]["logistic_regression"]["feature_set"] == "linear"
    report = next((tmp_path / "reports" / "model_evaluation").glob("*/report.md")).read_text()
    assert "IEEE-CIS is real but anonymized" in report and "PaySim" not in report
    assert "IEEE-CIS is real but anonymized" in Path(policy["report"]).read_text()

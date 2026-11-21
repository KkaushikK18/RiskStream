"""Load an explicit model bundle + policy pair for serving.

Rules:
* Paths are explicit (``MODEL_BUNDLE_PATH``, ``POLICY_PATH``). There is no
  "latest" lookup, so a restart can never silently pick up a different model.
* The bundle's feature schema hash must match the running feature registry.
* The policy must have been tuned for this exact model version.
* Estimators are switched to a single thread: multi-threaded prediction on one
  row costs more in thread start-up than it saves.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from riskstream.config import resolve_path
from riskstream.features.registry import FeatureRegistry
from riskstream.inference.decision_policy import DecisionPolicy, PolicyError
from riskstream.models.artifacts import ModelBundle, load_bundle


class ModelLoadError(RuntimeError):
    pass


def _single_thread(model: object) -> None:
    targets = [model]
    steps = getattr(model, "named_steps", None)
    if steps:
        targets.append(steps.get("model"))
    for est in targets:
        if est is None:
            continue
        params = est.get_params() if hasattr(est, "get_params") else {}
        if "n_jobs" in params:
            est.set_params(n_jobs=1)


@dataclass
class LoadedModel:
    bundle: ModelBundle
    policy: DecisionPolicy
    bundle_path: str
    # Serializes predict calls; prediction is ~1 ms, so this costs little and
    # removes any doubt about estimator thread-safety.
    _lock: threading.Lock

    @property
    def model_version(self) -> str:
        return self.bundle.model_version

    @property
    def feature_names(self) -> list[str]:
        return self.bundle.feature_names

    @property
    def feature_version(self) -> str:
        return str(self.bundle.metadata["feature_version"])

    def predict_one(self, features: dict[str, float]) -> float:
        x = np.array([[features[n] for n in self.feature_names]], dtype=np.float32)
        with self._lock:
            p = float(self.bundle.predict_proba(x)[0])
        if not np.isfinite(p) or not (0.0 <= p <= 1.0):
            raise ModelLoadError(f"model returned an invalid probability: {p}")
        return p


def load_serving_model(bundle_path: Path | None, policy_path: Path | None, registry: FeatureRegistry,
                       require_approved_policy: bool) -> LoadedModel:
    if bundle_path is None or policy_path is None:
        raise ModelLoadError("MODEL_BUNDLE_PATH and POLICY_PATH must both be set")
    bundle_path, policy_path = resolve_path(bundle_path), resolve_path(policy_path)
    try:
        meta_probe = load_bundle(Path(bundle_path))
        feature_set = meta_probe.metadata.get("feature_set", "full")
        bundle = load_bundle(Path(bundle_path), registry.schema_hash(feature_set))
    except Exception as exc:  # file missing, corrupt pickle, schema mismatch ...
        raise ModelLoadError(f"cannot load model bundle {bundle_path}: {exc}") from exc
    if bundle.metadata["feature_version"] != registry.version:
        raise ModelLoadError(
            f"bundle feature version {bundle.metadata['feature_version']} != registry {registry.version}")
    try:
        policy = DecisionPolicy.load(Path(policy_path))
    except PolicyError as exc:
        raise ModelLoadError(str(exc)) from exc
    if policy.model_version != bundle.model_version:
        raise ModelLoadError(
            f"policy {policy.path} was tuned for {policy.model_version}, not {bundle.model_version}")
    if require_approved_policy and policy.approval_status != "approved":
        raise ModelLoadError(f"policy {policy.policy_version} is '{policy.approval_status}', not approved")
    _single_thread(bundle.model)
    loaded = LoadedModel(bundle, policy, str(bundle_path), threading.Lock())
    # Fail at startup, not on the first request, if the artifact cannot predict.
    loaded.predict_one({n: float("nan") for n in loaded.feature_names})
    return loaded

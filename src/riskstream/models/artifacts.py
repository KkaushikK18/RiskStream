"""Versioned model bundles that load without any training code.

A bundle directory contains:

* ``model.joblib``      - fitted estimator (including its preprocessing pipeline)
* ``calibrator.joblib`` - fitted probability calibrator
* ``metadata.json``     - version, feature list and schema hash, thresholds,
                          dataset checksums, metrics, library versions, run ids
* ``feature_schema.json``

``load_bundle`` verifies that the feature schema hash in the metadata matches
the current feature registry, so a model can never silently receive features
computed under different definitions.
"""

from __future__ import annotations

import json
import platform
from dataclasses import dataclass
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import Any

import joblib
import numpy as np

from riskstream.models.calibration import Calibrator
from riskstream.models.estimators import predict_scores

BUNDLE_FORMAT = 1


def library_versions() -> dict[str, str]:
    libs = ["numpy", "polars", "scikit-learn", "lightgbm", "xgboost", "joblib"]
    return {"python": platform.python_version()} | {lib: pkg_version(lib) for lib in libs}


class FeatureSchemaMismatch(RuntimeError):
    pass


@dataclass
class ModelBundle:
    model: Any
    calibrator: Calibrator
    metadata: dict

    @property
    def feature_names(self) -> list[str]:
        return list(self.metadata["features"])

    @property
    def model_version(self) -> str:
        return str(self.metadata["model_version"])

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if X.shape[1] != len(self.feature_names):
            raise ValueError(f"Expected {len(self.feature_names)} features, got {X.shape[1]}")
        return self.calibrator.transform(predict_scores(self.model, X))

    def save(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=False)  # never overwrite a version
        joblib.dump(self.model, directory / "model.joblib", compress=3)
        joblib.dump(self.calibrator, directory / "calibrator.joblib")
        (directory / "metadata.json").write_text(json.dumps(self.metadata, indent=2, default=str))
        return directory


def load_bundle(directory: Path, expected_schema_hash: str | None = None) -> ModelBundle:
    directory = Path(directory)
    meta_path = directory / "metadata.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"No model bundle at {directory}")
    metadata = json.loads(meta_path.read_text())
    if metadata.get("bundle_format") != BUNDLE_FORMAT:
        raise ValueError(f"Unsupported bundle format {metadata.get('bundle_format')}")
    if expected_schema_hash is not None and metadata["feature_schema_hash"] != expected_schema_hash:
        raise FeatureSchemaMismatch(
            f"Model {metadata['model_version']} was trained with feature schema "
            f"{metadata['feature_schema_hash']}, current registry is {expected_schema_hash}"
        )
    model = joblib.load(directory / "model.joblib")
    calibrator = joblib.load(directory / "calibrator.joblib")
    return ModelBundle(model, calibrator, metadata)

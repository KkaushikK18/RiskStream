"""Estimator factory.

One small factory instead of a module per model: each model is a few lines of
configuration, and keeping them together makes the differences easy to see.
Every estimator takes a float32 matrix whose columns follow the feature
registry order, with NaN meaning "undefined" (see ``features.registry``).
"""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.tree import DecisionTreeClassifier


def signed_log1p(x: np.ndarray) -> np.ndarray:
    """Compress heavy tails (amounts span 7 orders of magnitude) for linear models."""
    return np.sign(x) * np.log1p(np.abs(x))


def _linear_preprocessing() -> list[tuple[str, Any]]:
    # Median imputation plus missing-indicator columns, all fitted on training data
    # and serialized with the model, so serving applies exactly the same transform.
    return [
        ("impute", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
        ("log", FunctionTransformer(signed_log1p, feature_names_out="one-to-one")),
        ("scale", StandardScaler()),
    ]


SUPERVISED = ("logistic_regression", "decision_tree", "random_forest", "xgboost", "lightgbm")


def build_estimator(kind: str, params: dict[str, Any], seed: int, n_jobs: int) -> Any:
    params = dict(params)
    if kind == "logistic_regression":
        return Pipeline(
            [*_linear_preprocessing(), ("model", LogisticRegression(random_state=seed, **params))]
        )
    if kind == "decision_tree":
        return DecisionTreeClassifier(random_state=seed, **params)
    if kind == "random_forest":
        return RandomForestClassifier(random_state=seed, n_jobs=n_jobs, **params)
    if kind == "xgboost":
        from xgboost import XGBClassifier

        return XGBClassifier(
            random_state=seed, n_jobs=n_jobs, eval_metric="aucpr", missing=np.nan, **params
        )
    if kind == "lightgbm":
        from lightgbm import LGBMClassifier

        return LGBMClassifier(
            random_state=seed, n_jobs=n_jobs, verbose=-1, subsample_freq=1, **params
        )
    raise ValueError(f"Unknown estimator kind {kind}")


def build_isolation_forest(params: dict[str, Any], seed: int, n_jobs: int) -> Pipeline:
    return Pipeline(
        [
            *_linear_preprocessing()[:2],
            ("model", IsolationForest(random_state=seed, n_jobs=n_jobs, **params)),
        ]
    )


def predict_scores(model: Any, X: np.ndarray) -> np.ndarray:
    """Positive-class score in [0, 1] for classifiers."""
    return np.asarray(model.predict_proba(X)[:, 1], dtype=np.float64)


def anomaly_scores(model: Pipeline, X: np.ndarray) -> np.ndarray:
    """Higher = more anomalous. Not a probability."""
    return -np.asarray(model.score_samples(X), dtype=np.float64)


def feature_importance(model: Any, names: list[str]) -> dict[str, float] | None:
    est = model.named_steps["model"] if isinstance(model, Pipeline) else model
    if hasattr(est, "feature_importances_"):
        vals = np.asarray(est.feature_importances_, dtype=float)
    elif hasattr(est, "coef_") and len(est.coef_[0]) >= len(names):
        # First len(names) coefficients map to features; the rest are missing indicators.
        vals = np.abs(est.coef_[0][: len(names)])
    else:
        return None
    total = vals.sum() or 1.0
    return dict(sorted(((n, float(v / total)) for n, v in zip(names, vals, strict=False)),
                       key=lambda kv: -kv[1]))

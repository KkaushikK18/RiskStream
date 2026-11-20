"""Post-hoc probability calibration fitted on validation scores.

Why it is needed here: models are trained on undersampled negatives and some
use class weights, so raw scores overstate fraud probability. A calibrator maps
raw scores to probabilities at the validation period's fraud rate.

Method choice uses K-fold cross-fitting on the validation set: each fold's
calibrator is scored on rows it did not see, which gives an honest Brier
estimate for "none", "sigmoid" (Platt) and "isotonic". The chosen method is
then refit on all validation rows for serving. Sigmoid is strictly monotone
and leaves ranking metrics unchanged. Isotonic is monotone but piecewise
constant, so it merges nearby scores into ties and can move PR-AUC slightly.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss
from sklearn.model_selection import StratifiedKFold

_EPS = 1e-7


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, _EPS, 1 - _EPS)
    return np.log(p / (1 - p))


@dataclass
class Calibrator:
    method: str  # none | sigmoid | isotonic
    model: object | None = None

    def fit(self, scores: np.ndarray, y: np.ndarray) -> Calibrator:
        if self.method == "sigmoid":
            m = LogisticRegression(C=1e6, max_iter=1000)
            m.fit(_logit(scores).reshape(-1, 1), y)
            self.model = m
        elif self.method == "isotonic":
            m = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
            m.fit(scores, y)
            self.model = m
        elif self.method != "none":
            raise ValueError(f"Unknown calibration method {self.method}")
        return self

    def transform(self, scores: np.ndarray) -> np.ndarray:
        if self.method == "none":
            return np.asarray(scores, dtype=np.float64)
        if self.method == "sigmoid":
            return self.model.predict_proba(_logit(scores).reshape(-1, 1))[:, 1]  # type: ignore[union-attr]
        return np.asarray(self.model.predict(scores), dtype=np.float64)  # type: ignore[union-attr]


def cross_fitted(scores: np.ndarray, y: np.ndarray, method: str, folds: int, seed: int) -> np.ndarray:
    out = np.empty_like(scores, dtype=np.float64)
    skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=seed)
    for tr, te in skf.split(scores.reshape(-1, 1), y):
        out[te] = Calibrator(method).fit(scores[tr], y[tr]).transform(scores[te])
    return out


def select_calibrator(scores: np.ndarray, y: np.ndarray, methods: list[str], folds: int,
                      seed: int) -> tuple[Calibrator, dict]:
    """Pick the method with the lowest cross-fitted Brier score; refit it on all rows."""
    results = {}
    for m in methods:
        oof = cross_fitted(scores, y, m, folds, seed)
        results[m] = {"cv_brier": float(brier_score_loss(y, oof)), "cv_mean_predicted": float(oof.mean())}
    best = min(results, key=lambda m: results[m]["cv_brier"])
    return Calibrator(best).fit(scores, y), {"selected": best, "observed_rate": float(y.mean()),
                                             "methods": results}

"""Metrics for imbalanced fraud scoring and the cost-based three-outcome policy."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score, roc_curve

from riskstream.config import load_config

# ----------------------------------------------------------------- ranking metrics


def precision_at_k(y: np.ndarray, s: np.ndarray, k: int) -> float:
    k = min(k, len(y))
    top = np.argsort(-s, kind="stable")[:k]
    return float(y[top].mean()) if k else float("nan")


def recall_at_fpr(y: np.ndarray, s: np.ndarray, max_fpr: float) -> dict:
    fpr, tpr, thr = roc_curve(y, s)
    ok = np.where(fpr <= max_fpr)[0]
    i = ok[-1]
    return {"max_fpr": max_fpr, "recall": float(tpr[i]), "fpr": float(fpr[i]),
            "threshold": float(thr[i]) if np.isfinite(thr[i]) else None}


def ranking_metrics(y: np.ndarray, s: np.ndarray, ks: list[int], fprs: list[float]) -> dict:
    has_both = 0 < y.sum() < len(y)
    return {
        "rows": int(len(y)),
        "positives": int(y.sum()),
        "prevalence": float(y.mean()),
        "pr_auc": float(average_precision_score(y, s)) if has_both else None,
        "roc_auc": float(roc_auc_score(y, s)) if has_both else None,
        "precision_at_k": {str(k): precision_at_k(y, s, k) for k in ks},
        "recall_at_fpr": [recall_at_fpr(y, s, f) for f in fprs] if has_both else [],
    }


# ----------------------------------------------------------------- threshold metrics


def binary_metrics(y: np.ndarray, flagged: np.ndarray) -> dict:
    tp = int(((flagged == 1) & (y == 1)).sum())
    fp = int(((flagged == 1) & (y == 0)).sum())
    fn = int(((flagged == 0) & (y == 1)).sum())
    tn = int(((flagged == 0) & (y == 0)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": precision, "recall": recall, "f1": f1,
        "fpr": fp / (fp + tn) if fp + tn else 0.0,
        "fnr": fn / (fn + tp) if fn + tp else 0.0,
        "flag_rate": float(flagged.mean()) if len(flagged) else 0.0,
    }


def best_f1_threshold(y: np.ndarray, s: np.ndarray) -> float:
    from sklearn.metrics import precision_recall_curve

    p, r, t = precision_recall_curve(y, s)
    f1 = np.where(p + r > 0, 2 * p * r / np.maximum(p + r, 1e-12), 0)[:-1]
    return float(t[int(np.argmax(f1))])


# ----------------------------------------------------------------- calibration metrics


def calibration_metrics(y: np.ndarray, p: np.ndarray, n_bins: int) -> dict:
    """Brier score plus quantile-binned reliability (equal-count bins suit skewed scores)."""
    edges = np.unique(np.quantile(p, np.linspace(0, 1, n_bins + 1)))
    idx = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, len(edges) - 2)
    bins = []
    ece = 0.0
    for b in range(len(edges) - 1):
        m = idx == b
        if not m.any():
            continue
        mean_p, frac = float(p[m].mean()), float(y[m].mean())
        bins.append({"count": int(m.sum()), "mean_predicted": mean_p, "observed_rate": frac})
        ece += m.mean() * abs(mean_p - frac)
    return {"brier": float(brier_score_loss(y, p)), "ece": float(ece), "bins": bins,
            "mean_predicted": float(p.mean()), "observed_rate": float(y.mean())}


# ----------------------------------------------------------------- cost policy


@dataclass(frozen=True)
class CostMatrix:
    fraud_loss_fixed: float
    fraud_loss_amount_fraction: float
    false_decline_cost: float  # fixed part of declining a legitimate payment
    review_cost: float
    review_fraud_catch_rate: float
    # Share of the amount lost when a legitimate payment is declined
    # (lost fees, customer churn). Zero reproduces the old flat-cost model.
    false_decline_amount_fraction: float = 0.0

    @classmethod
    def from_config(cls, overrides: dict | None = None) -> CostMatrix:
        return cls(**(load_config("thresholds")["cost_matrix"] | (overrides or {})))

    def fraud_loss(self, amount: np.ndarray) -> np.ndarray:
        return self.fraud_loss_fixed + self.fraud_loss_amount_fraction * amount

    def false_decline_loss(self, amount: np.ndarray) -> np.ndarray:
        return self.false_decline_cost + self.false_decline_amount_fraction * amount


@dataclass(frozen=True)
class DecisionThresholds:
    review: float
    decline: float

    def decide(self, p: np.ndarray) -> np.ndarray:
        """0 = APPROVE, 1 = REVIEW, 2 = DECLINE."""
        return np.where(p >= self.decline, 2, np.where(p >= self.review, 1, 0))


def policy_cost(y: np.ndarray, p: np.ndarray, amount: np.ndarray, th: DecisionThresholds,
                cm: CostMatrix) -> dict:
    d = th.decide(p)
    loss = cm.fraud_loss(amount)
    fraud = y == 1
    approved_fraud = float(loss[(d == 0) & fraud].sum())
    reviewed_fraud_loss = float((1 - cm.review_fraud_catch_rate) * loss[(d == 1) & fraud].sum())
    review = float(cm.review_cost * (d == 1).sum())
    declines = float(cm.false_decline_loss(amount)[(d == 2) & ~fraud].sum())
    total = approved_fraud + reviewed_fraud_loss + review + declines
    no_model = float(loss[fraud].sum())  # approve everything
    return {
        "thresholds": asdict(th),
        "total_cost": total,
        "cost_per_transaction": total / len(y),
        "approve_everything_cost": no_model,
        "cost_reduction_vs_approve_all": 1 - total / no_model if no_model else None,
        "components": {"approved_fraud_loss": approved_fraud, "reviewed_fraud_leakage": reviewed_fraud_loss,
                       "review_cost": review, "false_decline_cost": declines},
        "rates": {"approve": float((d == 0).mean()), "review": float((d == 1).mean()),
                  "decline": float((d == 2).mean())},
        "fraud_outcomes": {"approved": int(((d == 0) & fraud).sum()), "reviewed": int(((d == 1) & fraud).sum()),
                           "declined": int(((d == 2) & fraud).sum())},
        "fraud_amount_caught_share": (
            1 - (approved_fraud + reviewed_fraud_loss) / no_model if no_model else None
        ),
    }


def optimize_thresholds(y: np.ndarray, p: np.ndarray, amount: np.ndarray, cm: CostMatrix,
                        max_review_rate: float, grid_size: int) -> DecisionThresholds:
    """Minimize expected cost over (review, decline) pairs from a score-quantile grid.

    Vectorized with cumulative sums over rows sorted by descending score: for a
    threshold ``t``, the rows with ``p >= t`` are a prefix of that order.
    """
    order = np.argsort(-p, kind="stable")
    ps, ys, loss = p[order], y[order].astype(float), cm.fraud_loss(amount[order])
    decline_loss = cm.false_decline_loss(amount[order])
    n = len(ps)
    z = np.zeros(1)
    # cumulative cost of declining every legitimate row in the prefix
    c_legit = np.concatenate([z, np.cumsum((1 - ys) * decline_loss)])
    c_loss = np.concatenate([z, np.cumsum(loss * ys)])
    total_loss = c_loss[-1]

    qs = np.linspace(0.5, 1.0, grid_size)  # thresholds above the median score
    cand = np.unique(np.concatenate([np.quantile(p, qs), [np.inf]]))
    # number of rows with score >= t (prefix length in descending order)
    k = np.searchsorted(-ps, -cand, side="right")

    R = k[:, None]  # review threshold -> prefix length r (rows reviewed or declined)
    D = k[None, :]  # decline threshold -> prefix length d
    valid = (cand[None, :] >= cand[:, None]) & ((R - D) / n <= max_review_rate)
    cost = (
        c_legit[D]
        + (R - D) * cm.review_cost
        + (1 - cm.review_fraud_catch_rate) * (c_loss[R] - c_loss[D])
        + (total_loss - c_loss[R])
    )
    cost = np.where(valid, cost, np.inf)
    i, j = np.unravel_index(int(np.argmin(cost)), cost.shape)
    return DecisionThresholds(review=_finite(cand[i]), decline=_finite(cand[j]))


# Threshold meaning "never": just above the largest possible probability.
NEVER = float(np.nextafter(1.0, 2.0))


def _finite(t: float) -> float:
    return NEVER if not np.isfinite(t) else float(t)


def full_evaluation(y: np.ndarray, p: np.ndarray, amount: np.ndarray, th: DecisionThresholds,
                    cm: CostMatrix, eval_cfg: dict) -> dict:
    """Everything reported for one model on one split (thresholds fixed beforehand)."""
    return {
        "ranking": ranking_metrics(y, p, eval_cfg["precision_at_k"], eval_cfg["recall_at_fpr"]),
        "calibration": calibration_metrics(y, p, eval_cfg["calibration_bins"]),
        "at_review_threshold": binary_metrics(y, (p >= th.review).astype(int)),
        "at_decline_threshold": binary_metrics(y, (p >= th.decline).astype(int)),
        "policy": policy_cost(y, p, amount, th, cm),
    }

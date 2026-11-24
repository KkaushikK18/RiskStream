"""Decision policy tuning, versioned separately from the model.

Given a saved model bundle, this:

1. scores the validation set with the bundle (model + calibrator),
2. chooses REVIEW/DECLINE thresholds that minimize expected cost under
   ``configs/thresholds.yaml``, within the review-capacity limit,
3. repeats the choice for each sensitivity scenario, to show how much the
   outcome depends on the cost assumptions,
4. evaluates the baseline policy on the test set, with thresholds fixed from
   validation,
5. writes ``models/policies/<model_version>/policy-<policy_version>-<ts>.json``
   plus a Markdown report.

The model bundle is never modified. Serving (Phase 3) loads one model bundle
and one policy file; the policy records which model version it was tuned for.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from riskstream.config import PROJECT_ROOT, dataset_config, load_config
from riskstream.features.offline import load_features, registry_for
from riskstream.logging_config import get_logger
from riskstream.models.artifacts import load_bundle
from riskstream.models.evaluation import CostMatrix, DecisionThresholds, optimize_thresholds, policy_cost

logger = get_logger(__name__)


def _score(bundle, df) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    X = df.select(bundle.feature_names).to_numpy().astype(np.float32, copy=False)
    return (bundle.predict_proba(X), df["is_fraud"].to_numpy().astype(np.int8),
            df["amount"].to_numpy().astype(np.float64))


def tune_for_scores(y: np.ndarray, p: np.ndarray, amount: np.ndarray, cfg: dict) -> dict:
    """Baseline thresholds plus every sensitivity scenario, all on the same scores."""
    base_cm = CostMatrix.from_config()
    pol = cfg["policy"]
    scenarios = []
    for sc in cfg.get("sensitivity", [{"name": "baseline"}]):
        overrides = {k: v for k, v in sc.items() if k != "name"}
        cm = CostMatrix(**(asdict(base_cm) | overrides))
        th = optimize_thresholds(y, p, amount, cm, pol["max_review_rate"], int(pol["threshold_grid_size"]))
        out = policy_cost(y, p, amount, th, cm)
        scenarios.append({"name": sc["name"], "overrides": overrides, "cost_matrix": asdict(cm), **out})
    return {"scenarios": scenarios}


def run_policy_tuning(dataset: str, bundle_dir: Path) -> dict:
    cfg = load_config("thresholds")
    registry = registry_for(dataset)
    bundle = load_bundle(bundle_dir)
    feature_set = bundle.metadata.get("feature_set", "full")
    bundle = load_bundle(bundle_dir, registry.schema_hash(feature_set))

    p_val, y_val, a_val = _score(bundle, load_features(dataset, "val"))
    tuned = tune_for_scores(y_val, p_val, a_val, cfg)
    base = next(s for s in tuned["scenarios"] if s["name"] == "baseline")
    th = DecisionThresholds(**base["thresholds"])
    cm = CostMatrix.from_config()

    p_te, y_te, a_te = _score(bundle, load_features(dataset, "test"))
    test = policy_cost(y_te, p_te, a_te, th, cm)

    created = datetime.now(UTC)
    cfg_hash = hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()[:12]
    policy = {
        "policy_version": str(cfg["policy_version"]),
        "owner": cfg.get("owner"),
        "model_version": bundle.model_version,
        "feature_version": bundle.metadata["feature_version"],
        "created_at": created.isoformat(),
        "config_sha256_12": cfg_hash,
        "thresholds": base["thresholds"],
        "cost_matrix": asdict(cm),
        "max_review_rate": cfg["policy"]["max_review_rate"],
        "tuned_on": {"dataset": dataset, "split": "val", "rows": int(len(y_val)), "fraud": int(y_val.sum())},
        "val": {k: base[k] for k in ("total_cost", "rates", "fraud_outcomes", "components",
                                     "cost_reduction_vs_approve_all", "fraud_amount_caught_share")},
        "test": {k: test[k] for k in ("total_cost", "rates", "fraud_outcomes", "components",
                                      "cost_reduction_vs_approve_all", "fraud_amount_caught_share")},
        "sensitivity_val": [
            {k: s[k] for k in ("name", "overrides", "thresholds", "total_cost", "rates", "fraud_outcomes")}
            for s in tuned["scenarios"]
        ],
        "approval": {"status": "unapproved", "note": "requires sign-off by the policy owner before use"},
    }
    out_dir = PROJECT_ROOT / "models" / "policies" / bundle.model_version
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"policy-{policy['policy_version']}-{created:%Y%m%d%H%M%S}"
    path = out_dir / f"{stem}.json"
    path.write_text(json.dumps(policy, indent=2))
    report = PROJECT_ROOT / "reports" / "policy" / f"{bundle.model_version}-{stem}.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(render_policy_report(policy, bundle.metadata.get("is_synthetic_data", False),
                                           dataset_config(dataset).get("disclaimer", "")))
    logger.info("Wrote policy %s and report %s", path, report)
    return {"policy": str(path), "report": str(report), "thresholds": policy["thresholds"],
            "val_rates": policy["val"]["rates"], "test_rates": policy["test"]["rates"]}


def _pct(x: float | None) -> str:
    return "n/a" if x is None else f"{100 * x:.2f}%"


def _th(t: float) -> str:
    return "never" if t > 1.0 else f"{t:.4f}"


def render_policy_report(p: dict, is_synthetic: bool, disclaimer: str = "") -> str:
    cm = p["cost_matrix"]
    L = [f"# Decision policy {p['policy_version']} for model {p['model_version']}", ""]
    L += ["> " + ("Synthetic data: illustrates the mechanics only." if is_synthetic else
                  f"{disclaimer} The costs are assumptions, not business-approved figures.".strip()), ""]
    L += [f"- Owner: {p['owner']}. Approval status: **{p['approval']['status']}**.",
          f"- Thresholds chosen on validation ({p['tuned_on']['rows']:,} rows, {p['tuned_on']['fraud']:,} fraud): "
          f"REVIEW at p >= {_th(p['thresholds']['review'])}, DECLINE at p >= {_th(p['thresholds']['decline'])}.",
          f"- Costs: fraud approved = {cm['fraud_loss_fixed']:g} + {cm['fraud_loss_amount_fraction']:g} x amount; "
          f"legitimate declined = {cm['false_decline_cost']:g} + {cm['false_decline_amount_fraction']:g} x amount; "
          f"review = {cm['review_cost']:g}; reviews stop {cm['review_fraud_catch_rate']:.0%} of fraud; "
          f"review queue <= {p['max_review_rate']:.0%} of traffic.", "",
          "## Outcomes", "", "| | validation (tuning) | test (thresholds fixed) |", "|---|---:|---:|"]
    v, t = p["val"], p["test"]
    for label, key in (("approve", "approve"), ("review", "review"), ("decline", "decline")):
        L.append(f"| {label} rate | {_pct(v['rates'][key])} | {_pct(t['rates'][key])} |")
    for label, key in (("fraud approved", "approved"), ("fraud reviewed", "reviewed"), ("fraud declined", "declined")):
        L.append(f"| {label} | {v['fraud_outcomes'][key]:,} | {t['fraud_outcomes'][key]:,} |")
    L.append(f"| legitimate payments declined, cost | {v['components']['false_decline_cost']:,.0f} | "
             f"{t['components']['false_decline_cost']:,.0f} |")
    L.append(f"| total policy cost | {v['total_cost']:,.0f} | {t['total_cost']:,.0f} |")
    L.append(f"| share of fraud amount stopped | {_pct(v['fraud_amount_caught_share'])} | "
             f"{_pct(t['fraud_amount_caught_share'])} |")
    L += ["", "## Sensitivity to the cost assumptions (validation)", "",
          "Each row re-optimizes thresholds under one changed assumption. If outcomes swing a lot, "
          "the business needs to pin that number down before the policy is trusted.", "",
          "| scenario | change | REVIEW at | DECLINE at | approve | review | decline | fraud approved |",
          "|---|---|---:|---:|---:|---:|---:|---:|"]
    for s in p["sensitivity_val"]:
        change = ", ".join(f"{k}={v}" for k, v in s["overrides"].items()) or "none"
        L.append(f"| {s['name']} | {change} | {_th(s['thresholds']['review'])} | {_th(s['thresholds']['decline'])} | "
                 f"{_pct(s['rates']['approve'])} | {_pct(s['rates']['review'])} | {_pct(s['rates']['decline'])} | "
                 f"{s['fraud_outcomes']['approved']} |")
    return "\n".join(L) + "\n"

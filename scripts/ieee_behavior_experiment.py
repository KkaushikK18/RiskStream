"""IEEE-CIS behaviour-feature experiment (validation only; the test split is never read).

Question: why did removing our behaviour features RAISE validation PR-AUC, and what
behaviour design should feature version ieee-1.1.0 use?

Hypothesis (from the PSI check, train vs validation): the cumulative "since history
start" features (``*_txn_count_all``), the 1d/30d activity ratio and uncapped
``*_hours_since_last_txn`` grow with elapsed time, so their meaning shifts between
periods. The first 30 days also have incomplete 30-day windows (warm-up).

Each variant is fitted with the SAME LightGBM hyperparameters (taken from the
ieee-1.0.0 run's tuning trials) and compared on validation PR-AUC, with a paired
bootstrap of the difference against the baseline.

    python scripts/ieee_behavior_experiment.py [--params-from <results.json>] [--bootstrap 300]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.metrics import average_precision_score

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from riskstream.features.ieee_cis import IEEERegistry, features_dir  # noqa: E402
from riskstream.models.estimators import build_estimator, predict_scores  # noqa: E402

WARMUP_DAYS = 30
CAP_HOURS = 30 * 24


def is_cumulative(n: str) -> bool:
    return n.endswith("_txn_count_all") or "_activity_ratio_" in n


def is_recency(n: str) -> bool:
    return n.endswith("_hours_since_last_txn")


def variants(reg: IEEERegistry) -> dict[str, dict]:
    full = reg.names("full")
    beh = [n for n in full if n.startswith(("card1_", "uid_")) and not n.endswith("_freq")]
    no_beh = [n for n in full if n not in beh]
    windowed = [n for n in full if not is_cumulative(n)]
    uid_only = [n for n in windowed if not (n in beh and n.startswith("card1_"))]
    card1_beh = {n for n in beh if n.startswith("card1_")}
    beh_no_novelty = {n for n in beh if "_prior_share_same_" not in n and "_is_new_" not in n}
    vcols = set(reg.names("full")) - set(reg.names("no_vesta_v"))
    return {
        "A_full": {"features": full, "warmup": False, "cap": False},
        "B_no_behavior": {"features": no_beh, "warmup": False, "cap": False},
        "C_windowed_capped": {"features": windowed, "warmup": False, "cap": True},
        "D_windowed_capped_warmup": {"features": windowed, "warmup": True, "cap": True},
        "E_no_behavior_warmup": {"features": no_beh, "warmup": True, "cap": False},
        "F_uid_only_windowed_warmup": {"features": uid_only, "warmup": True, "cap": True},
        "G_full_warmup": {"features": full, "warmup": True, "cap": False},
        # Round 3 (feature version 1.1.0 tables): does novelty help on top of uid behaviour?
        "M_no_novelty": {"features": [n for n in full if not n.startswith("uid_") or n in beh_no_novelty],
                         "warmup": False, "cap": False},
        # Round 2: are the behaviour features redundant with Vesta's V columns?
        "H_no_v_no_behavior": {"features": [n for n in no_beh if n not in vcols], "warmup": False, "cap": False},
        "I_no_v_uid_behavior": {"features": [n for n in full if n not in vcols and n not in card1_beh],
                                "warmup": False, "cap": False},
        "J_no_v_full_behavior": {"features": [n for n in full if n not in vcols], "warmup": False, "cap": False},
        "K_uid_behavior": {"features": [n for n in full if n not in card1_beh], "warmup": False, "cap": False},
    }


def matrix(df: pl.DataFrame, names: list[str], cap: bool) -> np.ndarray:
    exprs = []
    for n in names:
        e = pl.col(n)
        if cap and is_recency(n):
            # No previous transaction within 30 days -> undefined (same as never seen).
            e = pl.when(e <= CAP_HOURS).then(e).otherwise(None)
        exprs.append(e.alias(n))
    return df.select(exprs).to_numpy().astype(np.float32)


def paired_bootstrap(y: np.ndarray, a: np.ndarray, b: np.ndarray, n: int, seed: int) -> dict:
    """PR-AUC(b) - PR-AUC(a) on resampled validation rows."""
    rng = np.random.default_rng(seed)
    d = []
    for _ in range(n):
        i = rng.integers(0, len(y), len(y))
        d.append(average_precision_score(y[i], b[i]) - average_precision_score(y[i], a[i]))
    d = np.array(d)
    return {"mean": float(d.mean()), "ci95": [float(np.quantile(d, 0.025)), float(np.quantile(d, 0.975))],
            "share_positive": float((d > 0).mean())}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--params-from", default=str(sorted((ROOT / "reports" / "model_evaluation").glob("*-ieee_cis"))[-1]
                                                  / "results.json"))
    ap.add_argument("--trials", default="5,3", help="lightgbm trial indices whose params are reused")
    ap.add_argument("--bootstrap", type=int, default=300)
    ap.add_argument("--n-jobs", type=int, default=6)
    ap.add_argument("--only", help="comma-separated variant names (A is always included as the baseline)")
    ap.add_argument("--tag", default="v1_1", help="report file suffix")
    args = ap.parse_args()

    reg = IEEERegistry.load()
    d = features_dir()
    src = json.loads(Path(args.params_from).read_text())
    trials = src["supervised"]["lightgbm"]["trials"]
    param_sets = [trials[int(i)]["params"] for i in args.trials.split(",")]

    train = pl.read_parquet(d / "train_sampled.parquet")
    val = pl.read_parquet(d / "val.parquet")
    yv = val["is_fraud"].to_numpy()
    out: dict = {"created_at": datetime.now(UTC).isoformat(), "feature_version": reg.version,
                 "params_from": args.params_from, "param_sets": param_sets, "warmup_days": WARMUP_DAYS,
                 "cap_hours": CAP_HOURS, "split_used": "train_sampled for fitting, val for scoring; test not read",
                 "variants": {}}
    scores: dict[str, list[np.ndarray]] = {}
    chosen = variants(reg)
    if args.only:
        keep = {"A_full", *args.only.split(",")}
        chosen = {k: v for k, v in chosen.items() if k in keep}
    for name, v in chosen.items():
        tr = train.filter(pl.col("day") >= WARMUP_DAYS) if v["warmup"] else train
        Xt, yt = matrix(tr, v["features"], v["cap"]), tr["is_fraud"].to_numpy()
        Xv = matrix(val, v["features"], v["cap"])
        res = []
        scores[name] = []
        for k, params in enumerate(param_sets):
            t = time.perf_counter()
            m = build_estimator("lightgbm", params, 42, args.n_jobs)
            m.fit(Xt, yt)
            s = predict_scores(m, Xv)
            scores[name].append(s)
            res.append({"params": k, "val_pr_auc": float(average_precision_score(yv, s)),
                        "fit_seconds": round(time.perf_counter() - t, 1)})
            print(f"{name:28} params {k}: val PR-AUC {res[-1]['val_pr_auc']:.4f} ({res[-1]['fit_seconds']}s)", flush=True)
        out["variants"][name] = {"n_features": len(v["features"]), "warmup": v["warmup"], "cap": v["cap"],
                                 "train_rows": tr.height, "train_fraud": int(yt.sum()), "runs": res,
                                 "mean_val_pr_auc": float(np.mean([r["val_pr_auc"] for r in res]))}

    base = "A_full"
    for name in out["variants"]:
        if name == base:
            continue
        # Average the scores over the parameter sets, then bootstrap the difference.
        a, b = np.mean(scores[base], axis=0), np.mean(scores[name], axis=0)
        out["variants"][name]["vs_A_bootstrap"] = paired_bootstrap(yv, a, b, args.bootstrap, 7)

    rep = ROOT / "reports" / "experiments"
    rep.mkdir(parents=True, exist_ok=True)
    (rep / f"ieee_behavior_{args.tag}.json").write_text(json.dumps(out, indent=2))
    L = ["# IEEE-CIS behaviour-feature experiment", "",
         "Validation only (the test split is not read). Same LightGBM hyperparameters for every "
         f"variant ({len(param_sets)} sets from the ieee-1.0.0 tuning run). Warm-up = training rows with "
         f"day < {WARMUP_DAYS} removed (their 30-day windows are incomplete); cap = hours since last "
         f"transaction undefined beyond {CAP_HOURS} h.", "",
         "| variant | features | train rows | mean val PR-AUC | vs A (paired bootstrap, 95% CI) |",
         "|---|---:|---:|---:|---|"]
    for name, v in out["variants"].items():
        b = v.get("vs_A_bootstrap")
        cmp = "baseline" if b is None else f"{b['mean']:+.4f} [{b['ci95'][0]:+.4f}, {b['ci95'][1]:+.4f}]"
        L.append(f"| {name} | {v['n_features']} | {v['train_rows']:,} | {v['mean_val_pr_auc']:.4f} | {cmp} |")
    (rep / f"ieee_behavior_{args.tag}.md").write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))
    return 0


if __name__ == "__main__":
    sys.exit(main())

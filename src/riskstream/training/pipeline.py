"""End-to-end experiment: tune -> calibrate -> threshold -> select -> test once -> save.

Data use, by split (the order is enforced by the code structure):

* train_sampled - model fitting only (all fraud + a deterministic 10% of legit rows)
* val           - hyperparameter selection, calibration fitting, threshold
                  optimization and model selection. Because all four reuse the
                  same rows, validation metrics are optimistic; they are used
                  for choices, not for claims.
* test          - read only after the candidate, its calibrator and its
                  thresholds are fixed, and only for that one candidate.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import mlflow  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
from sklearn.metrics import average_precision_score, precision_recall_curve  # noqa: E402

from riskstream.config import CONFIG_DIR, PROJECT_ROOT, dataset_config, get_settings, load_config  # noqa: E402
from riskstream.features.offline import features_dir, load_features, registry_for  # noqa: E402
from riskstream.logging_config import get_logger  # noqa: E402
from riskstream.models.artifacts import BUNDLE_FORMAT, ModelBundle, library_versions, load_bundle  # noqa: E402
from riskstream.models.calibration import select_calibrator  # noqa: E402
from riskstream.models.estimators import (  # noqa: E402
    anomaly_scores,
    build_estimator,
    build_isolation_forest,
    feature_importance,
    predict_scores,
)
from riskstream.models.evaluation import (  # noqa: E402
    CostMatrix,
    best_f1_threshold,
    binary_metrics,
    full_evaluation,
    optimize_thresholds,
    ranking_metrics,
)
from riskstream.training.tuning import trial_params  # noqa: E402

logger = get_logger(__name__)


def git_info() -> dict[str, Any]:
    def run(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=PROJECT_ROOT, capture_output=True, text=True,
                              check=False).stdout.strip()
    commit = run("rev-parse", "HEAD")
    return {"commit": commit or None, "dirty": bool(run("status", "--porcelain")) if commit else None}


def _xy(df: pl.DataFrame, names: list[str]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    X = df.select(names).to_numpy().astype(np.float32, copy=False)
    return X, df["is_fraud"].to_numpy().astype(np.int8), df["amount"].to_numpy().astype(np.float64)


def _latency(model: Any, X: np.ndarray, repeats: int = 200) -> dict:
    one = X[:1]
    predict_scores(model, one)  # warm-up
    times = []
    for _ in range(repeats):
        t = time.perf_counter()
        predict_scores(model, one)
        times.append((time.perf_counter() - t) * 1000)
    batch = X[: min(1000, len(X))]
    t = time.perf_counter()
    predict_scores(model, batch)
    batch_ms = (time.perf_counter() - t) * 1000
    return {"single_row_p50_ms": float(np.percentile(times, 50)),
            "single_row_p95_ms": float(np.percentile(times, 95)),
            "batch_1000_ms": batch_ms, "note": "measured in-process on the training machine"}


def _setup_mlflow(experiment: str) -> str:
    import os

    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    mlflow.set_tracking_uri(get_settings().mlflow_tracking_uri)
    exp = mlflow.get_experiment_by_name(experiment)
    if exp is None:
        if get_settings().mlflow_tracking_uri.startswith(("http://", "https://")):
            # Tracking server with --serve-artifacts: it chooses and proxies artifact storage.
            return mlflow.create_experiment(experiment)
        artifacts = (PROJECT_ROOT / "mlruns").resolve()
        artifacts.mkdir(exist_ok=True)
        return mlflow.create_experiment(experiment, artifact_location=artifacts.as_uri())
    return exp.experiment_id


def _flat(prefix: str, d: dict) -> dict[str, float]:
    out = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out |= _flat(key, v)
        elif isinstance(v, (int, float)) and not isinstance(v, bool) and v is not None and np.isfinite(v):
            out[key] = float(v)
    return out


def tune_model(name: str, spec: dict, Xtr: np.ndarray, ytr: np.ndarray, Xva: np.ndarray,
               yva: np.ndarray, seed: int, n_jobs: int, n_trials: int | None) -> dict:
    kind = spec.get("estimator", name)
    trials = trial_params(spec.get("search", {}), spec.get("fixed", {}),
                          n_trials or int(spec.get("n_trials", 5)), seed)
    best: dict | None = None
    history = []
    for i, params in enumerate(trials):
        with mlflow.start_run(run_name=f"{name}-trial-{i}", nested=True):
            mlflow.set_tags({"model": name, "role": "tuning_trial"})
            mlflow.log_params({k: str(v) for k, v in params.items()})
            t = time.perf_counter()
            model = build_estimator(kind, params, seed, n_jobs)
            model.fit(Xtr, ytr)
            fit_s = time.perf_counter() - t
            scores = predict_scores(model, Xva)
            pr_auc = float(average_precision_score(yva, scores))
            mlflow.log_metrics({"val_pr_auc": pr_auc, "fit_seconds": fit_s})
        history.append({"trial": i, "params": params, "val_pr_auc": pr_auc, "fit_seconds": fit_s})
        logger.info("%s trial %d/%d: val PR-AUC %.4f (%.1fs)", name, i + 1, len(trials), pr_auc, fit_s)
        if best is None or pr_auc > best["val_pr_auc"]:
            best = {"model": model, "params": params, "val_pr_auc": pr_auc, "fit_seconds": fit_s,
                    "val_scores": scores}
        else:
            del model
    assert best is not None
    best["trials"] = history
    best["kind"] = kind
    return best


def _plot_pr(curves: dict[str, tuple[np.ndarray, np.ndarray]], prevalence: float, path: Path,
             title: str) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    for name, (y, s) in curves.items():
        p, r, _ = precision_recall_curve(y, s)
        ax.plot(r, p, lw=1.2, label=f"{name} (AP={average_precision_score(y, s):.3f})")
    ax.axhline(prevalence, ls="--", c="grey", lw=0.8, label=f"prevalence {prevalence:.4f}")
    ax.set_xlabel("recall")
    ax.set_ylabel("precision")
    ax.set_title(title)
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _plot_calibration(evals: dict[str, dict], path: Path) -> None:
    fig, ax = plt.subplots(figsize=(5.5, 5))
    for label, ev in evals.items():
        bins = ev["calibration"]["bins"]
        ax.plot([b["mean_predicted"] for b in bins], [b["observed_rate"] for b in bins], "o-",
                ms=3, label=f"{label} (Brier {ev['calibration']['brier']:.5f})")
    ax.plot([0, 1], [0, 1], "--", c="grey", lw=0.8)
    ax.set_xscale("symlog", linthresh=1e-3)
    ax.set_yscale("symlog", linthresh=1e-3)
    ax.set_xlabel("mean predicted probability (bin)")
    ax.set_ylabel("observed fraud rate (bin)")
    ax.set_title("Reliability (equal-count bins)")
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _plot_importance(imp: dict[str, float], path: Path, title: str) -> None:
    top = list(imp.items())[:20][::-1]
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.barh([k for k, _ in top], [v for _, v in top], color="#4c72b0")
    ax.set_title(title)
    ax.set_xlabel("normalized importance")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def model_config(dataset: str) -> dict:
    """configs/model.yaml, with keys replaced by configs/model_<dataset>.yaml when it exists."""
    cfg = load_config("model")
    if (CONFIG_DIR / f"model_{dataset}.yaml").exists():
        cfg |= load_config(f"model_{dataset}")
    return cfg


def run_training(dataset: str, models: list[str] | None = None, n_trials: int | None = None,
                 run_name: str | None = None) -> dict:
    mcfg = model_config(dataset)
    tcfg = load_config("thresholds")
    registry = registry_for(dataset)
    cm = CostMatrix.from_config()
    seed, n_jobs = int(mcfg["random_seed"]), int(mcfg["n_jobs"])
    eval_cfg = mcfg["evaluation"]
    fmanifest = json.loads((features_dir(dataset) / "feature_manifest.json").read_text())
    if fmanifest["feature_version"] != registry.version:
        raise RuntimeError("Feature tables were built with a different feature version; rebuild them")

    chosen = models or list(mcfg["models"])
    unknown = [m for m in chosen if m not in mcfg["models"]]
    if unknown:
        raise ValueError(f"Unknown models {unknown}")

    train = load_features(dataset, "train_sampled")
    val = load_features(dataset, "val")
    logger.info("train_sampled %s rows (%s fraud); val %s rows (%s fraud)", f"{len(train):,}",
                int(train["is_fraud"].sum()), f"{len(val):,}", int(val["is_fraud"].sum()))

    exp_id = _setup_mlflow(mcfg["experiment_name"])
    started = datetime.now(UTC)
    version = f"{started:%Y%m%d%H%M%S}-{dataset}"
    report_dir = PROJECT_ROOT / "reports" / "model_evaluation" / version
    report_dir.mkdir(parents=True, exist_ok=True)
    git = git_info()

    results: dict[str, dict] = {}
    fitted: dict[str, dict] = {}
    with mlflow.start_run(experiment_id=exp_id, run_name=run_name or f"experiment-{version}") as parent:
        mlflow.set_tags({"dataset": dataset, "feature_version": registry.version,
                         "git_commit": str(git["commit"]), "git_dirty": str(git["dirty"]),
                         "is_synthetic_data": str(dataset == "synthetic")})
        mlflow.log_params({"dataset_raw_sha256": fmanifest["source_raw_sha256"],
                           "negative_sample_rate": fmanifest["train_sampled"]["negative_sample_rate"],
                           "models": ",".join(chosen)})
        for name in chosen:
            spec = mcfg["models"][name]
            names = registry.names(spec.get("feature_set", "full"))
            Xtr, ytr, _ = _xy(train, names)
            Xva, yva, ava = _xy(val, names)
            with mlflow.start_run(run_name=name, nested=True):
                mlflow.set_tags({"model": name, "role": "model_best",
                                 "feature_set": spec.get("feature_set", "full")})
                best = tune_model(name, spec, Xtr, ytr, Xva, yva, seed, n_jobs, n_trials)
                calibrator, cal_info = select_calibrator(
                    best["val_scores"], yva, mcfg["calibration"]["methods"],
                    int(mcfg["calibration"]["folds"]), seed)
                p_val = calibrator.transform(best["val_scores"])
                th = optimize_thresholds(yva, p_val, ava, cm, tcfg["policy"]["max_review_rate"],
                                         int(tcfg["policy"]["threshold_grid_size"]))
                ev = full_evaluation(yva, p_val, ava, th, cm, eval_cfg)
                f1_t = best_f1_threshold(yva, p_val)
                ev["at_best_f1_threshold"] = binary_metrics(yva, (p_val >= f1_t).astype(int)) | {"threshold": f1_t}
                with tempfile.TemporaryDirectory() as tmp:
                    import joblib

                    joblib.dump(best["model"], Path(tmp) / "m.joblib", compress=3)
                    size = (Path(tmp) / "m.joblib").stat().st_size
                lat = _latency(best["model"], Xva)
                res = {
                    "estimator": best["kind"], "feature_set": spec.get("feature_set", "full"),
                    "n_features": len(names), "candidate_eligible": bool(spec.get("candidate_eligible", True)),
                    "best_params": best["params"], "fit_seconds": best["fit_seconds"],
                    "trials": best["trials"], "calibration": cal_info, "thresholds": asdict(th),
                    "val": ev, "latency": lat, "serialized_size_bytes": size,
                    "feature_importance": feature_importance(best["model"], names),
                }
                mlflow.log_params({k: str(v) for k, v in best["params"].items()})
                mlflow.log_metrics(_flat("val", {"pr_auc": ev["ranking"]["pr_auc"],
                                                 "roc_auc": ev["ranking"]["roc_auc"],
                                                 "brier": ev["calibration"]["brier"],
                                                 "ece": ev["calibration"]["ece"],
                                                 "cost": ev["policy"]["total_cost"],
                                                 "review_threshold": ev["at_review_threshold"]}))
                mlflow.log_metrics({"latency_single_p50_ms": lat["single_row_p50_ms"],
                                    "serialized_size_bytes": size})
            results[name] = res
            fitted[name] = {"model": best["model"], "calibrator": calibrator, "names": names,
                            "p_val": p_val, "thresholds": th}
            logger.info("%s: val PR-AUC %.4f, calibration=%s, cost %.0f", name,
                        ev["ranking"]["pr_auc"], cal_info["selected"], ev["policy"]["total_cost"])

        # ---------------- unsupervised anomaly detection (reported separately)
        anomaly = {}
        for aname, aspec in mcfg.get("anomaly_detection", {}).items():
            names = registry.names(aspec.get("feature_set", "full"))
            Xtr, ytr, _ = _xy(train, names)
            Xva, yva, _ = _xy(val, names)
            params = {k: v for k, v in aspec.items() if k not in ("feature_set", "train_on")}
            t = time.perf_counter()
            iso = build_isolation_forest(params, seed, n_jobs)
            iso.fit(Xtr[ytr == 0] if aspec.get("train_on") == "legit_only" else Xtr)
            fit_s = time.perf_counter() - t
            s = anomaly_scores(iso, Xva)
            rk = ranking_metrics(yva, s, eval_cfg["precision_at_k"], eval_cfg["recall_at_fpr"])
            anomaly[aname] = {"params": params, "fit_seconds": fit_s, "val_ranking": rk,
                              "note": "anomaly score, not a probability; never a candidate"}
            fitted[aname] = {"val_scores": s}
            with mlflow.start_run(run_name=aname, nested=True):
                mlflow.set_tags({"model": aname, "role": "anomaly_detection"})
                mlflow.log_metrics({"val_pr_auc": rk["pr_auc"], "val_roc_auc": rk["roc_auc"]})
            logger.info("%s: val PR-AUC %.4f (anomaly score)", aname, rk["pr_auc"])

        # ---------------- selection on validation only
        metric = mcfg["selection_metric"]
        eligible = {n: r for n, r in results.items() if r["candidate_eligible"]}
        if not eligible:
            raise RuntimeError("No candidate-eligible model was trained")
        cand = max(eligible, key=lambda n: eligible[n]["val"]["ranking"][metric])
        logger.info("Selected candidate on validation %s: %s", metric, cand)

        # ---------------- test: once, for the selected candidate only
        test = load_features(dataset, "test")
        c = fitted[cand]
        Xte, yte, ate = _xy(test, c["names"])
        p_test = c["calibrator"].transform(predict_scores(c["model"], Xte))
        test_eval = full_evaluation(yte, p_test, ate, c["thresholds"], cm, eval_cfg)
        mlflow.log_metrics(_flat("candidate_test", {"pr_auc": test_eval["ranking"]["pr_auc"],
                                                    "roc_auc": test_eval["ranking"]["roc_auc"],
                                                    "brier": test_eval["calibration"]["brier"],
                                                    "cost": test_eval["policy"]["total_cost"],
                                                    "review_threshold": test_eval["at_review_threshold"]}))

        # ---------------- save, then prove the saved bundle reproduces predictions
        model_version = f"{version}-{cand}"
        bundle_dir = PROJECT_ROOT / "models" / "candidates" / model_version
        metadata = {
            "bundle_format": BUNDLE_FORMAT,
            "model_version": model_version,
            "model_name": cand,
            "estimator": results[cand]["estimator"],
            "params": results[cand]["best_params"],
            "features": c["names"],
            "feature_set": results[cand]["feature_set"],
            "feature_version": registry.version,
            "feature_schema_hash": registry.schema_hash(results[cand]["feature_set"]),
            "calibration_method": c["calibrator"].method,
            "decision_thresholds": asdict(c["thresholds"]),
            "cost_matrix": asdict(cm),
            "dataset": dataset,
            "is_synthetic_data": dataset == "synthetic",
            "dataset_raw_sha256": fmanifest["source_raw_sha256"],
            "training_rows": len(train),
            "trained_at": started.isoformat(),
            "mlflow_run_id": parent.info.run_id,
            "mlflow_experiment_id": exp_id,
            "git": git,
            "libraries": library_versions(),
            "metrics": {"val": results[cand]["val"]["ranking"] | {"brier": results[cand]["val"]["calibration"]["brier"]},
                        "test": test_eval["ranking"] | {"brier": test_eval["calibration"]["brier"]}},
        }
        ModelBundle(c["model"], c["calibrator"], metadata).save(bundle_dir)
        (bundle_dir / "feature_schema.json").write_text(
            json.dumps(registry.schema_dict(results[cand]["feature_set"]), indent=2))
        reloaded = load_bundle(bundle_dir, registry.schema_hash(results[cand]["feature_set"]))
        Xva_c, _, _ = _xy(val, c["names"])
        same = bool(np.array_equal(reloaded.predict_proba(Xva_c), c["p_val"]))
        if not same:
            raise RuntimeError("Reloaded bundle predictions differ from in-memory model")
        logger.info("Saved candidate bundle %s (reload check passed)", bundle_dir)

        # ---------------- reports
        yva_all = val["is_fraud"].to_numpy()
        curves = {n: (yva_all, fitted[n]["p_val"]) for n in results}
        curves |= {n: (yva_all, fitted[n]["val_scores"]) for n in anomaly}
        _plot_pr(curves, float(yva_all.mean()), report_dir / "pr_curves_val.png",
                 "Validation precision-recall (used for selection)")
        _plot_calibration({f"{cand} val": results[cand]["val"], f"{cand} test": test_eval},
                          report_dir / "calibration_candidate.png")
        if results[cand]["feature_importance"]:
            _plot_importance(results[cand]["feature_importance"], report_dir / "importance_candidate.png",
                             f"{cand} feature importance")
        for n in [m for m, r in results.items() if not r["candidate_eligible"]]:
            if results[n]["feature_importance"]:
                _plot_importance(results[n]["feature_importance"], report_dir / f"importance_{n}.png",
                                 f"{n} feature importance")

        summary = {
            "version": version, "dataset": dataset, "is_synthetic_data": dataset == "synthetic",
            "feature_version": registry.version, "git": git, "mlflow_run_id": parent.info.run_id,
            "selection": {"metric": metric, "split": "val", "candidate": cand},
            "candidate_bundle": str(bundle_dir), "reload_predictions_identical": same,
            "supervised": results, "anomaly_detection": anomaly, "candidate_test": test_eval,
            "data": {"train_sampled": fmanifest["train_sampled"], "rows": fmanifest["rows"]},
            "disclaimer": dataset_config(dataset).get("disclaimer", ""),
        }
        (report_dir / "results.json").write_text(json.dumps(summary, indent=2, default=str))
        (report_dir / "report.md").write_text(render_report(summary))
        mlflow.log_artifacts(str(report_dir), artifact_path="report")
        mlflow.log_artifacts(str(bundle_dir), artifact_path="candidate_bundle")

    return {"report": str(report_dir / "report.md"), "candidate": cand,
            "candidate_bundle": str(bundle_dir), "mlflow_run_id": parent.info.run_id,
            "val_pr_auc": results[cand]["val"]["ranking"]["pr_auc"],
            "test_pr_auc": test_eval["ranking"]["pr_auc"]}


def _pct(x: float | None) -> str:
    return "n/a" if x is None else f"{100 * x:.2f}%"


def render_report(s: dict) -> str:
    sup, cand = s["supervised"], s["selection"]["candidate"]
    L = [f"# Model evaluation {s['version']}", ""]
    L += ["> " + (s.get("disclaimer") or ("Synthetic generator data: these numbers only show the pipeline runs."
                                          if s["is_synthetic_data"] else "")), ""]
    L += [f"- Dataset `{s['dataset']}`, feature version `{s['feature_version']}`, git "
          f"`{(s['git']['commit'] or 'n/a')[:10]}`{' (uncommitted changes)' if s['git']['dirty'] else ''}, "
          f"MLflow run `{s['mlflow_run_id']}`.",
          f"- Training rows (undersampled): {s['data']['train_sampled']['rows']:,} "
          f"({s['data']['train_sampled']['fraud']:,} fraud). Validation rows: {s['data']['rows']['val']:,}. "
          f"Test rows: {s['data']['rows']['test']:,}.",
          f"- Candidate selected by validation {s['selection']['metric']}: **{cand}**. The test set was "
          "used once, after selection, for this candidate only.", "",
          "## Validation comparison (used for every choice; optimistic)", "",
          "Thresholds, calibrator and hyperparameters were all fitted on validation, so these values "
          "overstate what to expect on new data.", "",
          "| model | features | PR-AUC | ROC-AUC | recall @ FPR 0.1% | Brier | calib. | precision | recall | FPR | policy cost | p50 latency ms | size KB |",
          "|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|"]
    for n, r in sorted(sup.items(), key=lambda kv: -(kv[1]["val"]["ranking"]["pr_auc"] or 0)):
        v = r["val"]
        rf = next((x["recall"] for x in v["ranking"]["recall_at_fpr"] if x["max_fpr"] == 0.001), None)
        b = v["at_review_threshold"]
        L.append(f"| {n}{' (ablation)' if not r['candidate_eligible'] else ''} | {r['n_features']} | "
                 f"{v['ranking']['pr_auc']:.4f} | {v['ranking']['roc_auc']:.4f} | {_pct(rf)} | "
                 f"{v['calibration']['brier']:.5f} | {r['calibration']['selected']} | {_pct(b['precision'])} | "
                 f"{_pct(b['recall'])} | {b['fpr']:.4%} | {v['policy']['total_cost']:,.0f} | "
                 f"{r['latency']['single_row_p50_ms']:.2f} | {r['serialized_size_bytes'] / 1024:,.0f} |")
    L += ["", "Precision, recall and FPR are at the REVIEW threshold (anything not approved counts as "
          "flagged). Policy cost uses `configs/thresholds.yaml`.", ""]
    for n, a in s["anomaly_detection"].items():
        rk = a["val_ranking"]
        L.append(f"- **{n}** (unsupervised, trained on legitimate rows only): validation PR-AUC "
                 f"{rk['pr_auc']:.4f}, ROC-AUC {rk['roc_auc']:.4f}. It outputs an anomaly score, not a "
                 "fraud probability, and was not eligible for selection.")
    t = s["candidate_test"]
    c = sup[cand]
    L += ["", f"## Candidate on the untouched test set: {cand}", "",
          f"- Best parameters: `{json.dumps(c['best_params'])}`",
          f"- Calibration: {c['calibration']['selected']} (cross-fitted validation Brier: "
          + ", ".join(f"{m} {x['cv_brier']:.5f}" for m, x in c["calibration"]["methods"].items()) + ")",
          f"- Thresholds (chosen on validation): REVIEW at p >= {c['thresholds']['review']:.4f}, "
          f"DECLINE at p >= {c['thresholds']['decline']:.4f}", "",
          "| metric | validation | test |", "|---|---:|---:|"]
    v = c["val"]
    rows = [
        ("fraud prevalence", _pct(v["ranking"]["prevalence"]), _pct(t["ranking"]["prevalence"])),
        ("PR-AUC", f"{v['ranking']['pr_auc']:.4f}", f"{t['ranking']['pr_auc']:.4f}"),
        ("ROC-AUC", f"{v['ranking']['roc_auc']:.4f}", f"{t['ranking']['roc_auc']:.4f}"),
        ("Brier", f"{v['calibration']['brier']:.5f}", f"{t['calibration']['brier']:.5f}"),
        ("ECE", f"{v['calibration']['ece']:.5f}", f"{t['calibration']['ece']:.5f}"),
    ]
    for k in v["ranking"]["precision_at_k"]:
        rows.append((f"precision@{k}", _pct(v["ranking"]["precision_at_k"][k]), _pct(t["ranking"]["precision_at_k"][k])))
    for a, b in zip(v["ranking"]["recall_at_fpr"], t["ranking"]["recall_at_fpr"], strict=False):
        rows.append((f"recall @ FPR <= {a['max_fpr']:.1%}", _pct(a["recall"]), _pct(b["recall"])))
    for label, key in (("REVIEW threshold", "at_review_threshold"), ("DECLINE threshold", "at_decline_threshold")):
        for m in ("precision", "recall", "f1"):
            rows.append((f"{m} at {label}", _pct(v[key][m]), _pct(t[key][m])))
        rows.append((f"FPR at {label}", f"{v[key]['fpr']:.4%}", f"{t[key]['fpr']:.4%}"))
        rows.append((f"FNR at {label}", _pct(v[key]["fnr"]), _pct(t[key]["fnr"])))
    pv, pt = v["policy"], t["policy"]
    rows += [
        ("approve / review / decline rate", f"{_pct(pv['rates']['approve'])} / {_pct(pv['rates']['review'])} / {_pct(pv['rates']['decline'])}",
         f"{_pct(pt['rates']['approve'])} / {_pct(pt['rates']['review'])} / {_pct(pt['rates']['decline'])}"),
        ("fraud approved / reviewed / declined", f"{pv['fraud_outcomes']['approved']} / {pv['fraud_outcomes']['reviewed']} / {pv['fraud_outcomes']['declined']}",
         f"{pt['fraud_outcomes']['approved']} / {pt['fraud_outcomes']['reviewed']} / {pt['fraud_outcomes']['declined']}"),
        ("policy cost", f"{pv['total_cost']:,.0f}", f"{pt['total_cost']:,.0f}"),
        ("cost if everything approved", f"{pv['approve_everything_cost']:,.0f}", f"{pt['approve_everything_cost']:,.0f}"),
        ("cost reduction vs approve-all", _pct(pv["cost_reduction_vs_approve_all"]), _pct(pt["cost_reduction_vs_approve_all"])),
    ]
    L += [f"| {a} | {b} | {c_} |" for a, b, c_ in rows]
    tm = t["at_review_threshold"]
    L += ["", f"Test confusion at the REVIEW threshold: TP {tm['tp']:,}, FP {tm['fp']:,}, FN {tm['fn']:,}, TN {tm['tn']:,}.",
          "", "## Figures", "", "- `pr_curves_val.png`", "- `calibration_candidate.png`",
          "- `importance_candidate.png`", "- `importance_<ablation>.png` for each ablation model", ""]
    return "\n".join(L)

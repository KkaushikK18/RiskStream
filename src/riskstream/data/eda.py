"""Reproducible EDA on the canonical (validated) dataset.

Produces ``eda_report.json`` (all numbers), ``eda_summary.md`` (readable
findings) and PNG figures in the dataset's ``eda_dir``. Everything is computed
from data with Polars aggregations; nothing here is hand-written.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # file output only, no display needed
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402

from riskstream.data.schema import TARGET_COLUMN  # noqa: E402
from riskstream.logging_config import get_logger  # noqa: E402

logger = get_logger(__name__)

NUMERIC = [
    "amount",
    "orig_balance_before",
    "orig_balance_after",
    "dest_balance_before",
    "dest_balance_after",
]
QUANTILES = [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0]


def _f(x: object) -> float | None:
    if x is None:
        return None
    v = float(x)  # type: ignore[arg-type]
    return None if math.isnan(v) else v


def overview(lf: pl.LazyFrame) -> dict:
    r = lf.select(
        pl.len().alias("rows"),
        pl.col(TARGET_COLUMN).sum().alias("fraud"),
        pl.col("step").min().alias("step_min"),
        pl.col("step").max().alias("step_max"),
        pl.col("step").n_unique().alias("distinct_steps"),
        pl.col("orig_id").n_unique().alias("distinct_orig"),
        pl.col("dest_id").n_unique().alias("distinct_dest"),
        pl.col("is_flagged_fraud").sum().alias("flagged"),
    ).collect().row(0, named=True)
    rows, fraud = int(r["rows"]), int(r["fraud"])
    return {
        "rows": rows,
        "fraud": fraud,
        "legit": rows - fraud,
        "fraud_rate": fraud / rows if rows else 0.0,
        "imbalance_ratio_legit_per_fraud": (rows - fraud) / fraud if fraud else None,
        "step_min": int(r["step_min"]),
        "step_max": int(r["step_max"]),
        "distinct_steps": int(r["distinct_steps"]),
        "distinct_orig_accounts": int(r["distinct_orig"]),
        "distinct_dest_accounts": int(r["distinct_dest"]),
        "is_flagged_fraud_count": int(r["flagged"]),
    }


def missing_values(lf: pl.LazyFrame) -> dict:
    nulls = lf.null_count().collect().row(0, named=True)
    return {k: int(v) for k, v in nulls.items()}


def numeric_profile(lf: pl.LazyFrame) -> dict:
    out = {}
    for col in NUMERIC:
        exprs = [
            pl.col(col).mean().alias("mean"),
            pl.col(col).std().alias("std"),
            (pl.col(col) == 0).mean().alias("zero_share"),
        ] + [pl.col(col).quantile(q, "nearest").alias(f"q{q}") for q in QUANTILES]
        r = lf.select(exprs).collect().row(0, named=True)
        q1, q3 = r["q0.25"], r["q0.75"]
        iqr = q3 - q1
        outliers = lf.select(
            ((pl.col(col) < q1 - 1.5 * iqr) | (pl.col(col) > q3 + 1.5 * iqr)).mean()
        ).collect().item()
        out[col] = {k: _f(v) for k, v in r.items()} | {"iqr_outlier_share": _f(outliers)}
    return out


def fraud_by_type(lf: pl.LazyFrame) -> list[dict]:
    df = (
        lf.group_by("type")
        .agg(
            pl.len().alias("rows"),
            pl.col(TARGET_COLUMN).sum().cast(pl.Int64).alias("fraud"),
            pl.col("amount").median().alias("median_amount"),
        )
        .with_columns(
            (pl.col("fraud") / pl.col("rows")).alias("fraud_rate"),
            (pl.col("rows") / pl.col("rows").sum()).alias("row_share"),
        )
        .sort("rows", descending=True)
        .collect()
    )
    return df.to_dicts()


def fraud_by_dest_type(lf: pl.LazyFrame) -> list[dict]:
    return (
        lf.group_by("dest_type")
        .agg(pl.len().alias("rows"), pl.col(TARGET_COLUMN).sum().cast(pl.Int64).alias("fraud"))
        .sort("dest_type")
        .collect()
        .to_dicts()
    )


def amount_by_label(lf: pl.LazyFrame) -> dict:
    df = (
        lf.group_by(TARGET_COLUMN)
        .agg(
            pl.col("amount").mean().alias("mean"),
            *[pl.col("amount").quantile(q, "nearest").alias(f"q{q}") for q in (0.25, 0.5, 0.75, 0.99)],
            pl.col("amount").max().alias("max"),
        )
        .sort(TARGET_COLUMN)
        .collect()
    )
    return {str(r[TARGET_COLUMN]): {k: _f(v) for k, v in r.items() if k != TARGET_COLUMN}
            for r in df.to_dicts()}


def temporal(lf: pl.LazyFrame) -> dict:
    per_step = (
        lf.group_by("step")
        .agg(pl.len().alias("rows"), pl.col(TARGET_COLUMN).sum().cast(pl.Int64).alias("fraud"))
        .sort("step")
        .collect()
    )
    hour = (
        lf.with_columns(((pl.col("step") - 1) % 24).alias("hour_of_day"))
        .group_by("hour_of_day")
        .agg(pl.len().alias("rows"), pl.col(TARGET_COLUMN).sum().cast(pl.Int64).alias("fraud"))
        .with_columns((pl.col("fraud") / pl.col("rows")).alias("fraud_rate"))
        .sort("hour_of_day")
        .collect()
    )
    day = (
        lf.with_columns(((pl.col("step") - 1) // 24).alias("day"))
        .group_by("day")
        .agg(pl.len().alias("rows"), pl.col(TARGET_COLUMN).sum().cast(pl.Int64).alias("fraud"))
        .sort("day")
        .collect()
    )
    return {
        "note": "PaySim hour_of_day assumes step 1 is the first hour of a day; it is a simulated clock.",
        "per_step": per_step.to_dicts(),
        "per_hour_of_day": hour.to_dicts(),
        "per_day": day.to_dicts(),
    }


def entity_activity(lf: pl.LazyFrame) -> dict:
    """How often accounts repeat: decides which behavioral features are viable."""
    out = {}
    for col in ("orig_id", "dest_id"):
        counts = lf.group_by(col).agg(
            pl.len().alias("n"), pl.col(TARGET_COLUMN).sum().alias("fraud")
        )
        r = counts.select(
            pl.len().alias("entities"),
            (pl.col("n") == 1).sum().alias("single_txn_entities"),
            pl.col("n").mean().alias("mean_txn"),
            pl.col("n").quantile(0.5, "nearest").alias("median_txn"),
            pl.col("n").quantile(0.99, "nearest").alias("p99_txn"),
            pl.col("n").max().alias("max_txn"),
            (pl.col("fraud") > 0).sum().alias("entities_with_fraud"),
        ).collect().row(0, named=True)
        rows_in_repeat = (
            lf.join(counts.filter(pl.col("n") > 1).select(col), on=col, how="semi")
            .select(pl.len())
            .collect()
            .item()
        )
        total = lf.select(pl.len()).collect().item()
        out[col] = {k: _f(v) if isinstance(v, float) else int(v) for k, v in r.items()}
        out[col]["share_of_rows_with_repeat_entity"] = rows_in_repeat / total if total else 0.0
    return out


def leakage_and_balance_checks(lf: pl.LazyFrame) -> dict:
    """Signals that look predictive but are post-event or simulator artifacts."""
    tol = 0.01
    risky = lf.filter(pl.col("type").is_in(["TRANSFER", "CASH_OUT"]))
    df = (
        risky.group_by(TARGET_COLUMN)
        .agg(
            pl.len().alias("rows"),
            (pl.col("orig_balance_after") == 0).mean().alias("orig_after_zero"),
            ((pl.col("amount") - pl.col("orig_balance_before")).abs() <= tol).mean().alias("amount_equals_orig_balance"),
            ((pl.col("dest_balance_before") == 0) & (pl.col("dest_balance_after") == 0)).mean().alias("dest_both_zero"),
            (((pl.col("orig_balance_before") - pl.col("amount")).clip(lower_bound=0) - pl.col("orig_balance_after")).abs() > tol).mean().alias("orig_balance_mismatch"),
            ((pl.col("dest_balance_before") + pl.col("amount") - pl.col("dest_balance_after")).abs() > tol).mean().alias("dest_balance_mismatch"),
            (pl.col("orig_balance_before") == 0).mean().alias("orig_before_zero"),
        )
        .sort(TARGET_COLUMN)
        .collect()
    )
    flagged = lf.group_by("is_flagged_fraud").agg(
        pl.len().alias("rows"), pl.col(TARGET_COLUMN).sum().cast(pl.Int64).alias("fraud")
    ).sort("is_flagged_fraud").collect()
    return {
        "scope": "TRANSFER and CASH_OUT only (the only types containing fraud in PaySim)",
        "by_label": {str(r[TARGET_COLUMN]): {k: _f(v) for k, v in r.items() if k != TARGET_COLUMN}
                     for r in df.to_dicts()},
        "is_flagged_fraud_vs_label": flagged.to_dicts(),
    }


def correlations(lf: pl.LazyFrame, sample_rows: int, seed: int) -> dict:
    total = lf.select(pl.len()).collect().item()
    df = lf.select([*NUMERIC, "step", TARGET_COLUMN]).collect()
    if total > sample_rows:
        df = df.sample(sample_rows, seed=seed)
    df = df.cast({TARGET_COLUMN: pl.Float64, "step": pl.Float64})
    cols = df.columns
    mat = {a: {b: _f(df.select(pl.corr(a, b, method="spearman")).item()) for b in cols} for a in cols}
    return {"method": "spearman", "sample_rows": len(df), "matrix": mat}


def inference_availability() -> list[dict]:
    from riskstream.data.schema import CANONICAL_COLUMNS

    return [
        {"column": c.name, "available_at_inference": c.available_at_inference, "description": c.description}
        for c in CANONICAL_COLUMNS
    ]


# ----------------------------------------------------------------- figures


def _save(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def make_figures(report: dict, lf: pl.LazyFrame, out_dir: Path, seed: int) -> list[str]:
    files = []
    ov = report["overview"]

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(["legit", "fraud"], [ov["legit"], ov["fraud"]], color=["#4c72b0", "#c44e52"])
    ax.set_yscale("log")
    ax.set_ylabel("rows (log scale)")
    ax.set_title(f"Class balance (fraud rate {ov['fraud_rate']:.4%})")
    _save(fig, out_dir / "class_balance.png")
    files.append("class_balance.png")

    bt = report["fraud_by_type"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4))
    a1.bar([r["type"] for r in bt], [r["rows"] for r in bt], color="#4c72b0")
    a1.set_title("Rows by transaction type")
    a1.tick_params(axis="x", rotation=30)
    a2.bar([r["type"] for r in bt], [100 * r["fraud_rate"] for r in bt], color="#c44e52")
    a2.set_title("Fraud rate by type (%)")
    a2.tick_params(axis="x", rotation=30)
    _save(fig, out_dir / "type_distribution.png")
    files.append("type_distribution.png")

    sample = lf.select("amount", TARGET_COLUMN).collect()
    if len(sample) > 500_000:
        sample = sample.sample(500_000, seed=seed)
    fig, ax = plt.subplots(figsize=(7, 4))
    import numpy as np

    bins = np.logspace(0, np.log10(max(sample["amount"].max(), 10.0)), 60)
    for label, color in ((0, "#4c72b0"), (1, "#c44e52")):
        vals = sample.filter(pl.col(TARGET_COLUMN) == label)["amount"].to_numpy()
        vals = vals[vals > 0]
        if len(vals):
            ax.hist(vals, bins=bins, density=True, alpha=0.55, color=color,
                    label=f"{'fraud' if label else 'legit'} (n={len(vals):,})")
    ax.set_xscale("log")
    ax.set_xlabel("amount (log scale)")
    ax.set_ylabel("density")
    ax.set_title("Amount distribution by label (sample)")
    ax.legend()
    _save(fig, out_dir / "amount_by_label.png")
    files.append("amount_by_label.png")

    ps = report["temporal"]["per_step"]
    fig, a1 = plt.subplots(figsize=(11, 4))
    a1.plot([r["step"] for r in ps], [r["rows"] for r in ps], color="#4c72b0", lw=0.8)
    a1.set_ylabel("rows per step", color="#4c72b0")
    a1.set_xlabel("step (simulated hour)")
    a2 = a1.twinx()
    a2.plot([r["step"] for r in ps], [r["fraud"] for r in ps], color="#c44e52", lw=0.8)
    a2.set_ylabel("fraud per step", color="#c44e52")
    a1.set_title("Volume and fraud over time")
    _save(fig, out_dir / "volume_over_time.png")
    files.append("volume_over_time.png")

    hd = report["temporal"]["per_hour_of_day"]
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar([r["hour_of_day"] for r in hd], [100 * r["fraud_rate"] for r in hd], color="#c44e52")
    ax.set_xlabel("hour of day (step - 1) mod 24")
    ax.set_ylabel("fraud rate (%)")
    ax.set_title("Fraud rate by simulated hour of day")
    _save(fig, out_dir / "fraud_rate_by_hour.png")
    files.append("fraud_rate_by_hour.png")

    corr = report["correlations"]["matrix"]
    cols = list(corr)
    mat = [[corr[a][b] if corr[a][b] is not None else 0.0 for b in cols] for a in cols]
    fig, ax = plt.subplots(figsize=(7, 6))
    im = ax.imshow(mat, cmap="coolwarm", vmin=-1, vmax=1)
    ax.set_xticks(range(len(cols)), cols, rotation=60, ha="right", fontsize=8)
    ax.set_yticks(range(len(cols)), cols, fontsize=8)
    for i in range(len(cols)):
        for j in range(len(cols)):
            ax.text(j, i, f"{mat[i][j]:.2f}", ha="center", va="center", fontsize=6)
    fig.colorbar(im, ax=ax)
    ax.set_title("Spearman correlation (sample)")
    _save(fig, out_dir / "correlations.png")
    files.append("correlations.png")
    return files


# ----------------------------------------------------------------- summary


def summary_markdown(report: dict, dataset: str, is_synthetic: bool) -> str:
    ov = report["overview"]
    lk = report["leakage_checks"]["by_label"]
    ea = report["entity_activity"]
    lines = [f"# EDA summary: {dataset}", ""]
    if is_synthetic:
        lines += ["> Synthetic generator output. These numbers describe the generator's rules, "
                  "not real fraud.", ""]
    else:
        lines += ["> PaySim is itself a simulation. Findings describe the simulator, not real "
                  "payment fraud.", ""]
    lines += [
        "Generated by `python -m riskstream.cli prepare-data`. All values below are computed; "
        "see `eda_report.json` for the full numbers.",
        "",
        "## Overview",
        "",
        f"- Rows: {ov['rows']:,}; fraud: {ov['fraud']:,} ({ov['fraud_rate']:.4%}); "
        f"about {ov['imbalance_ratio_legit_per_fraud']:.0f} legitimate rows per fraud row."
        if ov["imbalance_ratio_legit_per_fraud"] else f"- Rows: {ov['rows']:,}; no fraud rows.",
        f"- Steps {ov['step_min']}-{ov['step_max']} ({ov['distinct_steps']} distinct). "
        "One step is one simulated hour, so behavioral windows are defined in hours.",
        f"- Missing values: {sum(report['missing_values'].values())} in total.",
        "",
        "## Fraud by transaction type",
        "",
        "| type | rows | share | fraud | fraud rate |",
        "|---|---:|---:|---:|---:|",
    ]
    for r in report["fraud_by_type"]:
        lines.append(f"| {r['type']} | {r['rows']:,} | {r['row_share']:.2%} | {r['fraud']:,} | {r['fraud_rate']:.4%} |")
    lines += ["", "## Account repetition", ""]
    for col, label in (("orig_id", "Origin"), ("dest_id", "Destination")):
        e = ea[col]
        lines.append(
            f"- {label} accounts: {e['entities']:,}; {e['single_txn_entities'] / e['entities']:.2%} appear "
            f"once; max {e['max_txn']} transactions; "
            f"{e['share_of_rows_with_repeat_entity']:.2%} of rows involve a repeat {label.lower()} account."
        )
    lines += ["", "## Leakage and simulator artifacts (TRANSFER and CASH_OUT rows)", "",
              "| signal | legit | fraud |", "|---|---:|---:|"]
    if "0" in lk and "1" in lk:
        for key in ("orig_after_zero", "amount_equals_orig_balance", "dest_both_zero",
                    "orig_balance_mismatch", "dest_balance_mismatch", "orig_before_zero"):
            lines.append(f"| {key} | {lk['0'][key]:.2%} | {lk['1'][key]:.2%} |")
    flagged = report["leakage_checks"]["is_flagged_fraud_vs_label"]
    lines += [
        "",
        f"- `is_flagged_fraud` rows: {sum(r['rows'] for r in flagged if r['is_flagged_fraud'] == 1)}. "
        "It is the output of the simulator's own rule, so it is excluded from features.",
        "- `orig_balance_after`, `dest_balance_before` and `dest_balance_after` are not "
        "available when a transaction is scored (post-event, or another institution's data). "
        "They are excluded from model features. `orig_balance_before` is treated as available "
        "(the sender's own institution knows it).",
        "",
        "## Figures",
        "",
    ]
    lines += [f"- `{f}`" for f in report["figures"]]
    return "\n".join(lines) + "\n"


def run_eda(lf: pl.LazyFrame, out_dir: Path, dataset: str, is_synthetic: bool,
            correlation_sample_rows: int, seed: int) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Running EDA for %s", dataset)
    report: dict = {
        "dataset": dataset,
        "is_synthetic_data": is_synthetic,
        "overview": overview(lf),
        "missing_values": missing_values(lf),
        "numeric_profile": numeric_profile(lf),
        "fraud_by_type": fraud_by_type(lf),
        "fraud_by_dest_type": fraud_by_dest_type(lf),
        "amount_by_label": amount_by_label(lf),
        "temporal": temporal(lf),
        "entity_activity": entity_activity(lf),
        "leakage_checks": leakage_and_balance_checks(lf),
        "correlations": correlations(lf, correlation_sample_rows, seed),
        "inference_availability": inference_availability(),
    }
    report["figures"] = make_figures(report, lf, out_dir, seed)
    (out_dir / "eda_report.json").write_text(json.dumps(report, indent=2, default=str))
    (out_dir / "eda_summary.md").write_text(summary_markdown(report, dataset, is_synthetic))
    logger.info("EDA written to %s", out_dir)
    return report

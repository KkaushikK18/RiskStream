"""IEEE-CIS data pipeline: validate, ingest, EDA summary, chronological split.

Same contract as the PaySim pipeline (``ingestion``, ``validation``, ``eda``,
``splitting``), with checks that fit this dataset:

* structural (fail the dataset): schema, unique ids, identity rows that match no
  transaction, time never going backwards in file order, non-empty;
* row (quarantined to ``rejected.parquet`` with a reason; ingestion fails above
  the configured tolerance): required nulls, non-positive or non-finite amount,
  invalid label, time before the reference, exact duplicate records;
* info (measured only): identity coverage, missing addr1 / D1 (uid undefined),
  very large amounts.

Outputs mirror PaySim: ``data/interim/ieee_cis/{transactions,rejected}.parquet``,
``manifest.json``, ``reports/eda/ieee_cis/*`` and ``data/processed/ieee_cis/splits/``.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from riskstream.config import dataset_config, load_config, resolve_path
from riskstream.data.adapters.ieee_cis import (
    CORE_SCHEMA,
    ID_STRING_COLUMNS,
    ORDER_COLUMNS,
    TX_STRING_COLUMNS,
    V_COLUMNS,
    IEEECISAdapter,
    canonical_schema,
)
from riskstream.data.ingestion import DataValidationError, file_sha256
from riskstream.data.validation import CheckResult, ValidationReport
from riskstream.logging_config import get_logger

logger = get_logger(__name__)

DATASET = "ieee_cis"
SPLITS: tuple[str, str, str] = ("train", "val", "test")
REQUIRED = ("transaction_id", "event_seq", "event_time_s", "day", "amount", "is_fraud")


def get_ieee_adapter(raw_path: Path | None = None, identity_path: Path | None = None) -> IEEECISAdapter:
    cfg = dataset_config(DATASET)
    return IEEECISAdapter(raw_path or resolve_path(cfg["raw_path"]),
                          identity_path or resolve_path(cfg["identity_path"]))


# ------------------------------------------------------------------ validation

def row_rule_exprs() -> dict[str, tuple[str, pl.Expr]]:
    """Each expression is True where the row VIOLATES the rule."""
    dup_cols = [c for c in canonical_schema() if c not in ("transaction_id", "event_seq")]
    return {
        "null_required_field": ("a required field is null",
                                pl.any_horizontal([pl.col(c).is_null() for c in REQUIRED])),
        "non_positive_amount": ("amount <= 0", pl.col("amount") <= 0),
        "non_finite_amount": ("amount is NaN or infinite", ~pl.col("amount").is_finite()),
        "invalid_label": ("is_fraud not in {0, 1}", ~pl.col("is_fraud").is_in([0, 1])),
        "time_before_reference": ("TransactionDT < 86400 (day < 0)", pl.col("day") < 0),
        "duplicate_record": ("exact repeat of an earlier record (all fields except the id)",
                             ~pl.struct(dup_cols).hash(seed=0).is_first_distinct()),
    }


def info_exprs() -> dict[str, tuple[str, pl.Expr]]:
    return {
        "no_identity_row": ("transaction has no train_identity row", pl.col("has_identity") == 0),
        "addr1_missing": ("addr1 is null", pl.col("addr1").is_null()),
        "d1_missing": ("D1 is null", pl.col("D1").is_null()),
        "uid_undefined": ("card1, addr1 or D1 null: the uid entity is undefined",
                          pl.any_horizontal(pl.col("card1").is_null(), pl.col("addr1").is_null(),
                                            pl.col("D1").is_null())),
        "amount_over_5000": ("amount > 5,000 USD", pl.col("amount") > 5000),
    }


def with_rejection_reason(lf: pl.LazyFrame) -> pl.LazyFrame:
    reason: pl.Expr = pl.lit(None, dtype=pl.String)
    for name, (_, expr) in reversed(list(row_rule_exprs().items())):
        reason = pl.when(expr.fill_null(True)).then(pl.lit(name)).otherwise(reason)
    return lf.with_columns(reason.alias("_reject_reason"))


def structural_problems(lf: pl.LazyFrame) -> list[str]:
    actual = lf.collect_schema()
    problems = []
    for name, dtype in canonical_schema().items():
        if name not in actual:
            problems.append(f"missing column {name}")
        elif actual[name] != dtype:
            problems.append(f"column {name}: expected {dtype}, found {actual[name]}")
    return problems


def validate(lf: pl.LazyFrame, identity: pl.LazyFrame, max_invalid_row_rate: float) -> ValidationReport:
    problems = structural_problems(lf)
    if problems:
        return ValidationReport(DATASET, 0, 0, 0, max_invalid_row_rate,
                                [CheckResult("canonical_schema", "structural", p, 0, False) for p in problems])
    rules, infos = row_rule_exprs(), info_exprs()
    c = lf.select(
        pl.len().alias("_total"),
        (pl.len() - pl.col("transaction_id").n_unique()).alias("_dup_ids"),
        (pl.col("event_time_s").diff() < 0).sum().alias("_order_violations"),
        *[e.fill_null(True).sum().alias(n) for n, (_, e) in rules.items()],
        *[e.fill_null(False).sum().alias(n) for n, (_, e) in infos.items()],
    ).collect().row(0, named=True)
    rejected = with_rejection_reason(lf).select(pl.col("_reject_reason").is_not_null().sum()).collect().item()
    tx_ids = lf.select(pl.col("event_seq").alias("TransactionID"))
    id_stats = identity.select(pl.len().alias("rows"), pl.col("TransactionID").n_unique().alias("unique")).collect().row(0)
    orphans = identity.select("TransactionID").join(tx_ids, on="TransactionID", how="anti").select(pl.len()).collect().item()
    total = int(c["_total"])
    checks = [
        CheckResult("canonical_schema", "structural", "columns and dtypes match", 0, True),
        CheckResult("unique_transaction_id", "structural", "transaction_id is unique", int(c["_dup_ids"]),
                    c["_dup_ids"] == 0),
        CheckResult("unique_identity_id", "structural", "train_identity TransactionID is unique",
                    int(id_stats[0] - id_stats[1]), id_stats[0] == id_stats[1]),
        CheckResult("identity_orphans", "structural", "every identity row matches a transaction",
                    int(orphans), orphans == 0),
        CheckResult("temporal_order", "structural", "TransactionDT never decreases in file order",
                    int(c["_order_violations"]), c["_order_violations"] == 0),
        CheckResult("non_empty", "structural", "dataset has rows", 0 if total else 1, total > 0),
    ]
    checks += [CheckResult(n, "row", d, int(c[n]), c[n] == 0) for n, (d, _) in rules.items()]
    checks += [CheckResult(n, "info", d, int(c[n]), True) for n, (d, _) in infos.items()]
    return ValidationReport(DATASET, total, total - int(rejected), int(rejected), max_invalid_row_rate, checks)


# ------------------------------------------------------------------ ingestion

def interim_paths() -> tuple[Path, Path, Path]:
    d = resolve_path(dataset_config(DATASET)["interim_dir"])
    return d / "transactions.parquet", d / "rejected.parquet", d / "manifest.json"


def ingest(adapter: IEEECISAdapter | None = None) -> dict:
    cfg = dataset_config(DATASET)
    quality = load_config("data")["quality"]
    adapter = adapter or get_ieee_adapter()
    out_path, rejected_path, manifest_path = interim_paths()
    eda_dir = resolve_path(cfg["eda_dir"])
    t0 = time.perf_counter()

    problems = adapter.check_raw_columns() + adapter.check_identity_columns()
    if problems:
        raise ValueError(f"Source schema mismatch for {DATASET}: {problems}")
    identity = adapter.scan_identity()
    canonical = adapter.to_canonical(adapter.scan_raw(), identity)
    report = validate(canonical, identity, quality["max_invalid_row_rate"])
    eda_dir.mkdir(parents=True, exist_ok=True)
    (eda_dir / "validation_report.json").write_text(json.dumps(report.to_dict(), indent=2))
    (eda_dir / "validation_report.md").write_text(report.to_markdown())
    logger.info("Validation %s: %s valid, %s rejected", "passed" if report.passed else "FAILED",
                f"{report.valid_rows:,}", f"{report.rejected_rows:,}")
    if not report.passed:
        raise DataValidationError(report)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    flagged = with_rejection_reason(canonical)
    # The source is already in time order (checked above: temporal_order), so no
    # sort of the 437-column table is needed; the left join keeps that order.
    flagged.filter(pl.col("_reject_reason").is_null()).drop("_reject_reason").sink_parquet(
        out_path, compression="zstd")
    flagged.filter(pl.col("_reject_reason").is_not_null()).select(
        "transaction_id", "event_time_s", "amount", "is_fraud", "_reject_reason").sink_parquet(
        rejected_path, compression="zstd")
    order_ok = pl.scan_parquet(out_path).select(
        (pl.col("event_time_s").diff() < 0).sum()).collect().item() == 0
    if not order_ok:
        raise RuntimeError("interim file is not in time order")

    manifest = {
        "dataset": DATASET, "is_synthetic_data": False,
        "raw_path": str(adapter.path), "raw_sha256": file_sha256(adapter.path),
        "raw_size_bytes": adapter.path.stat().st_size,
        "identity_path": str(adapter.identity_path), "identity_sha256": file_sha256(adapter.identity_path),
        "interim_path": str(out_path),
        "rows_total": report.total_rows, "rows_valid": report.valid_rows, "rows_rejected": report.rejected_rows,
        "core_schema": {k: str(v) for k, v in CORE_SCHEMA.items()},
        "n_columns": len(canonical_schema()),
        "order_columns": list(ORDER_COLUMNS),
        "created_at": datetime.now(UTC).isoformat(),
        "seconds": round(time.perf_counter() - t0, 1),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    logger.info("Wrote %s in %.1fs", out_path, manifest["seconds"])
    return manifest


def load_interim() -> pl.LazyFrame:
    path, _, _ = interim_paths()
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run: python -m riskstream.cli prepare-data --dataset ieee_cis")
    return pl.scan_parquet(path)


def load_manifest() -> dict:
    return json.loads(interim_paths()[2].read_text())


# ------------------------------------------------------------------ split

def split_bounds() -> tuple[int, int]:
    s = dataset_config(DATASET)["split"]
    train_end, val_end = int(s["train_end_day"]), int(s["val_end_day"])
    if not 0 <= train_end < val_end:
        raise ValueError(f"Need 0 <= train_end_day < val_end_day, got {train_end}, {val_end}")
    return train_end, val_end


def split_expr(train_end: int, val_end: int) -> pl.Expr:
    day = pl.col("day")
    return pl.when(day <= train_end).then(pl.lit("train")).when(day <= val_end).then(
        pl.lit("val")).otherwise(pl.lit("test"))


def run_split(lf: pl.LazyFrame) -> dict:
    """Write one Parquet file per split plus a summary (rows, fraud, card1 overlap)."""
    train_end, val_end = split_bounds()
    out_dir = resolve_path(dataset_config(DATASET)["processed_dir"]) / "splits"
    out_dir.mkdir(parents=True, exist_ok=True)
    labeled = lf.with_columns(split_expr(train_end, val_end).alias("split"))
    for name in SPLITS:
        labeled.filter(pl.col("split") == name).drop("split").sink_parquet(out_dir / f"{name}.parquet",
                                                                          compression="zstd")
    stats = labeled.group_by("split").agg(
        pl.len().alias("rows"), pl.col("is_fraud").sum().cast(pl.Int64).alias("fraud"),
        pl.col("day").min().alias("day_min"), pl.col("day").max().alias("day_max")).collect()
    total, fraud = int(stats["rows"].sum()), int(stats["fraud"].sum())
    summary: dict = {"boundaries": {"train_end_day": train_end, "val_end_day": val_end},
                     "total_rows": total, "total_fraud": fraud, "splits": {}}
    for r in stats.iter_rows(named=True):
        summary["splits"][r["split"]] = {
            "rows": int(r["rows"]), "row_share": r["rows"] / total, "fraud": int(r["fraud"]),
            "fraud_share": r["fraud"] / fraud, "fraud_rate": r["fraud"] / r["rows"],
            "day_min": int(r["day_min"]), "day_max": int(r["day_max"])}
    cards = {s: labeled.filter(pl.col("split") == s).select(pl.col("card1").unique()).collect()["card1"]
             for s in SPLITS}
    summary["card1_overlap"] = {
        f"{s}_card1_seen_in_train_share": float(cards[s].is_in(cards["train"].implode()).mean()) for s in ("val", "test")}
    (out_dir / "split_summary.json").write_text(json.dumps(summary, indent=2))
    for name in SPLITS:
        s = summary["splits"][name]
        logger.info("%-5s days %d-%d: %s rows (%.1f%%), %s fraud (rate %.2f%%)", name, s["day_min"], s["day_max"],
                    f"{s['rows']:,}", 100 * s["row_share"], f"{s['fraud']:,}", 100 * s["fraud_rate"])
    return summary


def load_split(name: str) -> pl.LazyFrame:
    path = resolve_path(dataset_config(DATASET)["processed_dir"]) / "splits" / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run prepare-data --dataset ieee_cis first.")
    return pl.scan_parquet(path)


# ------------------------------------------------------------------ EDA

def _rate_table(df: pl.DataFrame, key: str, label: str, limit: int = 15) -> list[str]:
    lines = [f"| {label} | rows | share | fraud rate |", "|---|---:|---:|---:|"]
    total = df["rows"].sum()
    for r in df.sort("rows", descending=True).head(limit).iter_rows(named=True):
        lines.append(f"| {r[key] if r[key] is not None else '(missing)'} | {r['rows']:,} | "
                     f"{r['rows'] / total:.1%} | {r['fraud_rate']:.2%} |")
    return lines


def run_eda(lf: pl.LazyFrame, out_dir: Path) -> dict:
    """Compact EDA: class balance over time, by product, amount, identity, missingness."""
    out_dir.mkdir(parents=True, exist_ok=True)
    agg = [pl.len().alias("rows"), pl.col("is_fraud").mean().alias("fraud_rate")]
    base = lf.select(pl.len().alias("rows"), pl.col("is_fraud").sum().alias("fraud"),
                     pl.col("day").max().alias("days"), pl.col("card1").n_unique().alias("card1_unique")).collect().row(0, named=True)
    weekly = lf.group_by((pl.col("day") // 7).alias("week")).agg(*agg).sort("week").collect()
    product = lf.group_by("ProductCD").agg(*agg).collect()
    identity = lf.group_by("has_identity").agg(*agg).collect()
    device = lf.group_by("DeviceType").agg(*agg).collect()
    email = lf.group_by("P_emaildomain").agg(*agg).collect()
    amount = lf.group_by("is_fraud").agg(
        *[pl.col("amount").quantile(q).alias(f"p{int(q * 100)}") for q in (0.1, 0.5, 0.9, 0.99)],
        pl.col("amount").mean().alias("mean")).sort("is_fraud").collect()
    cents = lf.group_by((pl.col("amount") % 1 != 0).alias("has_cents")).agg(*agg).collect()
    groups = {"C (counts)": [f"C{i}" for i in range(1, 15)], "D (time deltas)": [f"D{i}" for i in range(1, 16)],
              "M (match flags)": [f"M{i}" for i in range(1, 10)], "V (Vesta)": list(V_COLUMNS),
              "identity id_*": [f"id_{i:02d}" for i in range(1, 39)], "addr/dist": ["addr1", "addr2", "dist1", "dist2"]}
    miss = lf.select([pl.mean_horizontal([pl.col(c).is_null().mean() for c in cols]).alias(g)
                      for g, cols in groups.items()]).collect().row(0, named=True)

    L = [f"# EDA: {DATASET}", "",
         "> " + dataset_config(DATASET)["disclaimer"], "",
         f"- {base['rows']:,} transactions over {base['days'] + 1} days, {base['fraud']:,} fraud "
         f"({base['fraud'] / base['rows']:.2%}); {base['card1_unique']:,} distinct card1 values.",
         f"- String columns: {len(TX_STRING_COLUMNS) + len(ID_STRING_COLUMNS)}; V columns: {len(V_COLUMNS)}.", "",
         "## Fraud rate by week", "", "| week | rows | fraud rate |", "|---:|---:|---:|",
         *[f"| {r['week']} | {r['rows']:,} | {r['fraud_rate']:.2%} |" for r in weekly.iter_rows(named=True)],
         "", "## By product", "", *_rate_table(product, "ProductCD", "ProductCD"),
         "", "## Identity data present", "", *_rate_table(identity, "has_identity", "has_identity"),
         "", "## Device type", "", *_rate_table(device, "DeviceType", "DeviceType"),
         "", "## Purchaser email domain (top 15)", "", *_rate_table(email, "P_emaildomain", "P_emaildomain"),
         "", "## Amount (USD)", "", "| is_fraud | p10 | p50 | p90 | p99 | mean |", "|---:|---:|---:|---:|---:|---:|",
         *[f"| {r['is_fraud']} | {r['p10']:.2f} | {r['p50']:.2f} | {r['p90']:.2f} | {r['p99']:.2f} | {r['mean']:.2f} |"
           for r in amount.iter_rows(named=True)],
         "", "Amounts with cents (often a currency conversion):", "", *_rate_table(cents, "has_cents", "has cents"),
         "", "## Missing values (mean share per column group)", "", "| group | missing |", "|---|---:|",
         *[f"| {g} | {v:.1%} |" for g, v in miss.items()], ""]
    (out_dir / "eda_summary.md").write_text("\n".join(L), encoding="utf-8")
    summary = {"base": base, "weekly": weekly.to_dicts(), "missing_by_group": miss,
               "amount_by_label": amount.to_dicts()}
    (out_dir / "eda_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    logger.info("EDA written to %s", out_dir)
    return summary


def prepare(skip_eda: bool = False) -> dict:
    t0 = time.perf_counter()
    manifest = ingest()
    lf = load_interim()
    if not skip_eda:
        run_eda(lf, resolve_path(dataset_config(DATASET)["eda_dir"]))
    summary = run_split(lf)
    logger.info("prepare-data (ieee_cis) finished in %.1fs", time.perf_counter() - t0)
    return {"manifest": manifest, "split": summary}

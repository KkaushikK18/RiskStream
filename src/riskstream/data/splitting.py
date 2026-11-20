"""Chronological train / validation / test split.

Rows are assigned by ``step`` using configured inclusive upper bounds, so every
validation row is later than every training row and every test row is later
than every validation row. No row is shuffled across boundaries.

The split also reports *entity overlap*: destination accounts seen in training
that reappear later. Overlap is expected in a time-based split (it is what
behavioral features rely on), so it is measured and reported rather than
removed. The model never sees a later split's labels or events, because
features are computed point-in-time (see ``riskstream.features``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from riskstream.config import dataset_config, resolve_path
from riskstream.data.schema import ORDER_COLUMNS, TARGET_COLUMN, TEMPORAL_COLUMN
from riskstream.logging_config import get_logger

logger = get_logger(__name__)

SPLITS: tuple[str, str, str] = ("train", "val", "test")


@dataclass(frozen=True)
class SplitBoundaries:
    train_end_step: int
    val_end_step: int

    def __post_init__(self) -> None:
        if not 1 <= self.train_end_step < self.val_end_step:
            raise ValueError(
                f"Need 1 <= train_end_step < val_end_step, got "
                f"{self.train_end_step}, {self.val_end_step}"
            )

    @classmethod
    def from_config(cls, dataset: str) -> SplitBoundaries:
        s = dataset_config(dataset)["split"]
        return cls(int(s["train_end_step"]), int(s["val_end_step"]))

    def split_expr(self) -> pl.Expr:
        step = pl.col(TEMPORAL_COLUMN)
        return (
            pl.when(step <= self.train_end_step)
            .then(pl.lit("train"))
            .when(step <= self.val_end_step)
            .then(pl.lit("val"))
            .otherwise(pl.lit("test"))
        )


def assign_split(lf: pl.LazyFrame, bounds: SplitBoundaries) -> pl.LazyFrame:
    return lf.with_columns(bounds.split_expr().alias("split"))


def split_summary(lf: pl.LazyFrame, bounds: SplitBoundaries) -> dict:
    """Row, fraud and step statistics per split plus destination overlap."""
    labeled = assign_split(lf, bounds)
    stats = (
        labeled.group_by("split")
        .agg(
            pl.len().alias("rows"),
            pl.col(TARGET_COLUMN).sum().cast(pl.Int64).alias("fraud"),
            pl.col(TEMPORAL_COLUMN).min().alias("step_min"),
            pl.col(TEMPORAL_COLUMN).max().alias("step_max"),
        )
        .collect()
    )
    total_rows = int(stats["rows"].sum())
    total_fraud = int(stats["fraud"].sum())
    out: dict = {
        "boundaries": {
            "train_end_step": bounds.train_end_step,
            "val_end_step": bounds.val_end_step,
        },
        "total_rows": total_rows,
        "total_fraud": total_fraud,
        "splits": {},
    }
    for row in stats.iter_rows(named=True):
        out["splits"][row["split"]] = {
            "rows": int(row["rows"]),
            "row_share": row["rows"] / total_rows,
            "fraud": int(row["fraud"]),
            "fraud_share": row["fraud"] / total_fraud if total_fraud else 0.0,
            "fraud_rate": row["fraud"] / row["rows"],
            "step_min": int(row["step_min"]),
            "step_max": int(row["step_max"]),
        }

    # Destination entities shared across splits (computed on hashes to save memory).
    dest = labeled.select(pl.col("dest_id").hash(seed=0).alias("h"), "split")
    sets = {
        s: dest.filter(pl.col("split") == s).select(pl.col("h").unique()).collect()["h"]
        for s in SPLITS
    }
    overlap = {}
    for later in ("val", "test"):
        later_set = sets[later]
        if len(later_set) == 0:
            continue
        seen = later_set.is_in(sets["train"].implode()).sum()
        overlap[f"{later}_dest_seen_in_train_share"] = float(seen / len(later_set))
    out["dest_entity_overlap"] = overlap
    return out


def write_splits(
    lf: pl.LazyFrame, bounds: SplitBoundaries, output_dir: Path
) -> dict[str, Path]:
    """Write one sorted Parquet file per split."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    labeled = assign_split(lf, bounds)
    for name in SPLITS:
        path = output_dir / f"{name}.parquet"
        (
            labeled.filter(pl.col("split") == name)
            .drop("split")
            .sort(list(ORDER_COLUMNS))
            .sink_parquet(path, compression="zstd")
        )
        paths[name] = path
        logger.info("Wrote %s split to %s", name, path)
    return paths


def run_split(dataset: str, lf: pl.LazyFrame) -> dict:
    """Split the interim dataset, write files and a JSON summary."""
    cfg = dataset_config(dataset)
    bounds = SplitBoundaries.from_config(dataset)
    out_dir = resolve_path(cfg["processed_dir"]) / "splits"
    write_splits(lf, bounds, out_dir)
    summary = split_summary(lf, bounds)
    (out_dir / "split_summary.json").write_text(json.dumps(summary, indent=2))
    for name, s in summary["splits"].items():
        logger.info(
            "%-5s steps %d-%d: %s rows (%.2f%%), %s fraud (%.2f%% of fraud), fraud rate %.4f%%",
            name, s["step_min"], s["step_max"], f"{s['rows']:,}", 100 * s["row_share"],
            f"{s['fraud']:,}", 100 * s["fraud_share"], 100 * s["fraud_rate"],
        )
    return summary


def load_split(dataset: str, name: str) -> pl.LazyFrame:
    if name not in SPLITS:
        raise ValueError(f"Unknown split {name}")
    path = resolve_path(dataset_config(dataset)["processed_dir"]) / "splits" / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run prepare-data first.")
    return pl.scan_parquet(path)

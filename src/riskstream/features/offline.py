"""Offline, point-in-time-correct primitives for a whole dataset.

For a transaction at step ``s`` and window ``W`` the primitives cover the
entity's events with step in ``[s - W, s - 1]``. Events in step ``s`` itself
(including the transaction) are excluded, because PaySim's order inside one
hour is a file position, not a timestamp.

Method (no per-row Python loops):
1. aggregate events per (entity, step): count, sum, sum of log1p, ...
2. running totals per entity, in step order
3. for each transaction, look up the running total *as of* step ``s - 1`` and
   step ``s - W - 1`` with an as-of join; the window value is the difference.

Labels are never read here, so features cannot encode the target.
Entity ids are grouped through a 64-bit hash to save memory; with ~2.7M
destinations the collision probability is about 2e-7.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from riskstream.config import dataset_config, resolve_path
from riskstream.data.ingestion import load_interim, load_manifest
from riskstream.data.splitting import SplitBoundaries, assign_split
from riskstream.features.derive import derive_features, primitive_columns, window_primitive_names
from riskstream.features.registry import FeatureRegistry, get_registry
from riskstream.logging_config import get_logger

logger = get_logger(__name__)

META_COLUMNS: tuple[str, ...] = ("transaction_id", "step", "event_seq", "type", "amount", "is_fraud")
_STATS = ("cnt", "sum", "sumlog", "sumsqlog", "transfer_cnt")


def _per_step_running(events: pl.LazyFrame, key: str) -> pl.LazyFrame:
    """Running totals per entity, one row per (entity, step) where it had events."""
    return (
        events.group_by(key, "step")
        .agg(
            pl.len().cast(pl.Float64).alias("cnt"),
            pl.col("amount").sum().alias("sum"),
            pl.col("amount").log1p().sum().alias("sumlog"),
            (pl.col("amount").log1p() ** 2).sum().alias("sumsqlog"),
            (pl.col("type") == "TRANSFER").sum().cast(pl.Float64).alias("transfer_cnt"),
        )
        .sort(key, "step")
        .with_columns([pl.col(c).cum_sum().over(key).alias(f"c_{c}") for c in _STATS])
        .select(key, pl.col("step").alias("at_step"), *[f"c_{c}" for c in _STATS])
        .sort("at_step")
    )


def _asof(txns: pl.LazyFrame, running: pl.LazyFrame, key: str, offset: int, suffix: str,
          cols: list[str]) -> pl.LazyFrame:
    """Attach running totals as of step (step - offset) for each transaction."""
    probe = txns.with_columns((pl.col("step") - offset).alias("_probe"))
    right = running.select(key, "at_step", *cols).rename(
        {c: f"{c}{suffix}" for c in [*cols, "at_step"]}
    )
    joined = probe.sort("_probe").join_asof(
        right, left_on="_probe", right_on=f"at_step{suffix}", by=key, strategy="backward",
        check_sortedness=False,
    )
    return joined.drop("_probe")


def compute_primitives(lf: pl.LazyFrame, windows: tuple[int, ...]) -> pl.LazyFrame:
    """Add every primitive column to the canonical transactions."""
    base = lf.with_columns(
        pl.col("dest_id").hash(seed=0).alias("_dest"),
        pl.col("orig_id").hash(seed=0).alias("_orig"),
    )
    dest_running = _per_step_running(base.select("_dest", "step", "amount", "type"), "_dest")
    orig_running = _per_step_running(base.select("_orig", "step", "amount", "type"), "_orig")
    cum_cols = [f"c_{c}" for c in _STATS]

    out = _asof(base, dest_running, "_dest", 1, "_now", cum_cols)
    for w in windows:
        out = _asof(out, dest_running, "_dest", w + 1, f"_m{w}", cum_cols)
    out = _asof(out, orig_running, "_orig", 1, "_onow", ["c_cnt"])

    prim: list[pl.Expr] = []
    for w in windows:
        names = window_primitive_names("dest", w)
        for stat, name in zip(_STATS, names, strict=True):
            now = pl.col(f"c_{stat}_now").fill_null(0.0)
            before = pl.col(f"c_{stat}_m{w}").fill_null(0.0)
            prim.append((now - before).clip(lower_bound=0.0).alias(name))
    prim += [
        pl.col("c_cnt_now").fill_null(0.0).alias("dest_p_cnt_all"),
        pl.col("at_step_now").alias("dest_p_last_step"),
        pl.col("c_cnt_onow").fill_null(0.0).alias("orig_p_cnt_all"),
        pl.col("at_step_onow").alias("orig_p_last_step"),
    ]
    # Counts are exact integers; differences of float running sums are rounded
    # back so the tiny cancellation error cannot turn 0 into 1e-12.
    rounded = [
        e.round(0) if any(e.meta.output_name().startswith(p) for p in ("dest_p_cnt", "dest_p_transfer"))
        else e for e in prim
    ]
    keep = [c for c in lf.collect_schema().names()]
    return out.with_columns(rounded).select(*keep, *primitive_columns(windows)).sort("step", "event_seq")


def build_feature_frame(lf: pl.LazyFrame, registry: FeatureRegistry,
                        extra_keep: tuple[str, ...] = ()) -> pl.LazyFrame:
    prims = compute_primitives(lf, registry.config.windows)
    return derive_features(
        prims, registry.all_names(), registry.config.windows, keep=META_COLUMNS + extra_keep
    )


def features_dir(dataset: str) -> Path:
    return resolve_path(dataset_config(dataset)["processed_dir"]) / "features" / f"v{get_registry().version}"


def run_build_features(dataset: str) -> dict:
    """Write feature tables per split.

    History comes from the full event stream (all splits), so validation rows
    see training-period behavior, exactly as a live system would. The training
    table is additionally written in an undersampled form for model fitting.
    """
    registry = get_registry()
    out_dir = features_dir(dataset)
    out_dir.mkdir(parents=True, exist_ok=True)
    bounds = SplitBoundaries.from_config(dataset)
    lf = load_interim(dataset)

    logger.info("Computing point-in-time features (version %s) for %s", registry.version, dataset)
    feats = assign_split(build_feature_frame(lf, registry), bounds)
    all_path = out_dir / "_all.parquet"
    feats.sink_parquet(all_path, compression="zstd")

    counts = {}
    scan = pl.scan_parquet(all_path)
    for name in ("train", "val", "test"):
        part = scan.filter(pl.col("split") == name).drop("split")
        part.sink_parquet(out_dir / f"{name}.parquet", compression="zstd")
        counts[name] = part.select(pl.len()).collect().item()

    cfg = registry.config
    train = pl.scan_parquet(out_dir / "train.parquet")
    keep = (pl.col("is_fraud") == 1) | (
        (pl.col("transaction_id").hash(seed=cfg.sampling_seed) % 1_000_000)
        < int(cfg.negative_sample_rate * 1_000_000)
    )
    sampled = train.filter(keep)
    sampled.sink_parquet(out_dir / "train_sampled.parquet", compression="zstd")
    sampled_stats = sampled.select(pl.len(), pl.col("is_fraud").sum()).collect().row(0)
    all_path.unlink()

    meta = {
        "dataset": dataset,
        "feature_version": registry.version,
        "schema_hash_full": registry.schema_hash("full"),
        "windows_hours": list(cfg.windows),
        "rows": counts,
        "train_sampled": {
            "rows": int(sampled_stats[0]),
            "fraud": int(sampled_stats[1]),
            "negative_sample_rate": cfg.negative_sample_rate,
            "sampling_seed": cfg.sampling_seed,
            "note": "negatives kept by a deterministic hash of transaction_id; all positives kept",
        },
        "source_raw_sha256": load_manifest(dataset)["raw_sha256"],
    }
    (out_dir / "feature_manifest.json").write_text(json.dumps(meta, indent=2))
    (out_dir / "feature_schema.json").write_text(
        json.dumps({fs: registry.schema_dict(fs) for fs in cfg.feature_sets}, indent=2)
    )
    logger.info("Feature tables written to %s: %s", out_dir, counts)
    return meta


def load_features(dataset: str, split: str) -> pl.DataFrame:
    path = features_dir(dataset) / f"{split}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run: python -m riskstream.cli build-features")
    return pl.read_parquet(path)

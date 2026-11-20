"""The single implementation of every feature formula.

Input: a frame with canonical transaction columns plus window *primitives*
(see ``primitive_columns``). Output: the registry's feature columns.

Primitives are plain sufficient statistics over earlier events of an entity:
counts, sums of amount, sums of log1p(amount) and its square, and the last
seen step. Offline and online code differ only in how they obtain primitives;
everything model-facing is computed here.
"""

from __future__ import annotations

import polars as pl

from riskstream.data.schema import TRANSACTION_TYPES

STD_EPS = 1e-3  # std below this is treated as "no spread" (z-score undefined)
DRAIN_TOL = 0.01


def window_primitive_names(entity: str, w: int) -> list[str]:
    return [
        f"{entity}_p_cnt_{w}",
        f"{entity}_p_sum_{w}",
        f"{entity}_p_sumlog_{w}",
        f"{entity}_p_sumsqlog_{w}",
        f"{entity}_p_transfer_cnt_{w}",
    ]


def primitive_columns(windows: tuple[int, ...]) -> list[str]:
    cols: list[str] = []
    for w in windows:
        cols += window_primitive_names("dest", w)
    cols += ["dest_p_cnt_all", "dest_p_last_step", "orig_p_cnt_all", "orig_p_last_step"]
    return cols


def transaction_features() -> list[pl.Expr]:
    amount = pl.col("amount")
    bal = pl.col("orig_balance_before")
    return [
        amount.alias("amount"),
        amount.log1p().alias("log_amount"),
        *[(pl.col("type") == t).cast(pl.Float32).alias(f"type_{t}") for t in TRANSACTION_TYPES],
        (pl.col("dest_type") == "MERCHANT").cast(pl.Float32).alias("dest_is_merchant"),
        ((pl.col("step") - 1) % 24).cast(pl.Float32).alias("hour_of_day"),
        ((amount > 0) & ((amount % 1000) == 0)).cast(pl.Float32).alias("amount_is_round_1000"),
        bal.alias("orig_balance_before"),
        (bal == 0).cast(pl.Float32).alias("orig_balance_is_zero"),
        (amount / (bal + 1.0)).alias("amount_to_orig_balance"),
        (amount > bal).cast(pl.Float32).alias("amount_exceeds_orig_balance"),
        (((amount - bal).abs() <= DRAIN_TOL) & (bal > 0)).cast(pl.Float32).alias(
            "amount_drains_orig_balance"
        ),
    ]


def _nan_unless(cond: pl.Expr, value: pl.Expr) -> pl.Expr:
    return pl.when(cond).then(value).otherwise(pl.lit(float("nan")))


def dest_window_features(w: int) -> list[pl.Expr]:
    cnt = pl.col(f"dest_p_cnt_{w}")
    s = pl.col(f"dest_p_sum_{w}")
    slog = pl.col(f"dest_p_sumlog_{w}")
    ssq = pl.col(f"dest_p_sumsqlog_{w}")
    tcnt = pl.col(f"dest_p_transfer_cnt_{w}")
    mean = _nan_unless(cnt > 0, s / cnt)
    log_mean = slog / cnt
    var = ((ssq - slog * slog / cnt) / (cnt - 1)).clip(lower_bound=0.0)
    std = _nan_unless(cnt > 1, var.sqrt())
    z = _nan_unless(
        (cnt > 1) & (var.sqrt() >= STD_EPS),
        (pl.col("amount").log1p() - log_mean) / var.sqrt(),
    )
    return [
        cnt.alias(f"dest_txn_count_{w}h"),
        s.alias(f"dest_amount_sum_{w}h"),
        mean.alias(f"dest_amount_mean_{w}h"),
        std.alias(f"dest_log_amount_std_{w}h"),
        z.alias(f"dest_log_amount_zscore_{w}h"),
        _nan_unless((cnt > 0) & (s > 0), pl.col("amount") / (s / cnt)).alias(
            f"dest_amount_ratio_to_mean_{w}h"
        ),
        _nan_unless(cnt > 0, tcnt / cnt).alias(f"dest_transfer_share_{w}h"),
    ]


def behavior_features(windows: tuple[int, ...]) -> list[pl.Expr]:
    exprs: list[pl.Expr] = []
    for w in windows:
        exprs += dest_window_features(w)
    if len(windows) >= 2:
        short, long = windows[0], windows[-1]
        c_s, c_l = pl.col(f"dest_p_cnt_{short}"), pl.col(f"dest_p_cnt_{long}")
        exprs.append(
            _nan_unless(c_l > 0, c_s / (c_l * (short / long))).alias(
                f"dest_activity_ratio_{short}h_{long}h"
            )
        )
    exprs += [
        pl.col("dest_p_cnt_all").alias("dest_txn_count_all"),
        _nan_unless(
            pl.col("dest_p_last_step").is_not_null(),
            (pl.col("step") - pl.col("dest_p_last_step")).cast(pl.Float64),
        ).alias("dest_hours_since_last_txn"),
        pl.col("orig_p_cnt_all").alias("orig_txn_count_all"),
        _nan_unless(
            pl.col("orig_p_last_step").is_not_null(),
            (pl.col("step") - pl.col("orig_p_last_step")).cast(pl.Float64),
        ).alias("orig_hours_since_last_txn"),
    ]
    return exprs


def derive_features(
    frame: pl.LazyFrame | pl.DataFrame,
    feature_names: list[str],
    windows: tuple[int, ...],
    keep: tuple[str, ...] = (),
) -> pl.LazyFrame | pl.DataFrame:
    """Return ``keep`` columns plus ``feature_names`` (in that order) as Float32."""
    exprs = {e.meta.output_name(): e for e in transaction_features() + behavior_features(windows)}
    missing = [n for n in feature_names if n not in exprs]
    if missing:
        raise KeyError(f"No derivation for features: {missing}")
    # A kept column that is also a feature (e.g. ``amount``) appears once, as the feature.
    return frame.select(
        *[pl.col(k) for k in keep if k not in feature_names],
        *[exprs[n].cast(pl.Float32).fill_null(float("nan")).alias(n) for n in feature_names],
    )

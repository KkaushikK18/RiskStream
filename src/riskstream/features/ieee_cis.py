"""IEEE-CIS features (offline), point-in-time correct.

Three kinds of features:

1. **Per-transaction columns** from the source: amount, time of day, card / address
   fields, Vesta's C (counts), D (time deltas), M (match flags), a de-duplicated
   subset of V, and the identity table. Nothing here looks at other rows.
2. **Encodings fitted on the training split only**: each string column becomes a
   frequency-rank code and a frequency; card / address ids get a frequency. They
   are learned from training rows and saved (``encoders.json``), so validation,
   test and later serving apply exactly the same mapping.
3. **Behaviour per entity** (``card1`` and ``uid``), from earlier transactions only:
   for a transaction at second ``t`` and window ``W`` days, the entity's
   transactions with time in ``[t - W*86400, t - 1]``. Transactions at the same
   second as ``t`` (including itself) are excluded. Labels are never read.

``uid`` = card1 + addr1 + (day - D1). D1 is "days since the card was first seen",
so ``day - D1`` is that first day and is constant for one card holder. It is an
approximation of one client used widely for this dataset; it is undefined (all
uid features NaN) when card1, addr1 or D1 is missing.

The V-column subset is chosen on a deterministic sample of training rows only:
columns are grouped by their missing-value count (Vesta's V columns come in blocks
that are missing together), and inside a group a column is dropped when it
correlates above the threshold with a column already kept.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import polars as pl

from riskstream.config import dataset_config, load_config, resolve_path
from riskstream.data.adapters.ieee_cis import (
    ID_NUMERIC_COLUMNS,
    MATCH_FLAG_COLUMNS,
    SECONDS_PER_DAY,
    V_COLUMNS,
)
from riskstream.features.registry import FeatureSpec
from riskstream.logging_config import get_logger

logger = get_logger(__name__)

DATASET = "ieee_cis"
META_COLUMNS: tuple[str, ...] = ("transaction_id", "event_seq", "event_time_s", "day", "amount", "is_fraud")
MISSING_TOKEN = "__MISSING__"
CARD_ADDRESS = ("card1", "card2", "card3", "card5", "addr1", "addr2", "dist1", "dist2")
C_COLUMNS = tuple(f"C{i}" for i in range(1, 15))
D_COLUMNS = tuple(f"D{i}" for i in range(1, 16))
_STATS = ("cnt", "sum", "sumlog", "sumsqlog")
_STD_EPS = 1e-6

EXCLUDED_COLUMNS: dict[str, str] = {
    "is_fraud": "target",
    "transaction_id": "identifier",
    "event_seq": "TransactionID; increases with time, so it would encode the absolute date",
    "event_time_s": "absolute time; would not generalize to future periods",
    "day": "absolute day index; same reason",
    "V (not selected)": "near-duplicate of a kept V column (see v_selection in the feature manifest)",
    "M4, id_12.. (raw strings)": "used through their _code and _freq encodings",
}


# ------------------------------------------------------------------ config

@dataclass(frozen=True)
class IEEEFeatureConfig:
    version: str
    windows_days: tuple[int, ...]
    entities: tuple[str, ...]
    novelty_entities: tuple[str, ...]
    novelty_columns: tuple[str, ...]
    categorical: tuple[str, ...]
    max_vocabulary: int
    min_count: int
    frequency_numeric: tuple[str, ...]
    v_sample_rows: int
    v_correlation_threshold: float
    v_seed: int
    feature_sets: dict[str, tuple[str, ...]]
    negative_sample_rate: float
    sampling_seed: int

    @classmethod
    def load(cls) -> IEEEFeatureConfig:
        c = load_config("ieee_cis_features")
        v, cat = c["v_selection"], c["categorical"]
        return cls(
            version=str(c["version"]), windows_days=tuple(int(w) for w in c["windows_days"]),
            entities=tuple(c["entities"]), categorical=tuple(cat["columns"]),
            novelty_entities=tuple(c.get("novelty", {}).get("entities", ())),
            novelty_columns=tuple(c.get("novelty", {}).get("columns", ())),
            max_vocabulary=int(cat["max_vocabulary"]), min_count=int(cat["min_count"]),
            frequency_numeric=tuple(c["frequency_numeric"]), v_sample_rows=int(v["sample_rows"]),
            v_correlation_threshold=float(v["correlation_threshold"]), v_seed=int(v["seed"]),
            feature_sets={k: tuple(g) for k, g in c["feature_sets"].items()},
            negative_sample_rate=float(c["training_sampling"]["negative_sample_rate"]),
            sampling_seed=int(c["training_sampling"]["seed"]),
        )


@lru_cache(maxsize=1)
def get_config() -> IEEEFeatureConfig:
    return IEEEFeatureConfig.load()


def features_dir(version: str | None = None) -> Path:
    v = version or get_config().version
    return resolve_path(dataset_config(DATASET)["processed_dir"]) / "features" / f"v{v}"


# ------------------------------------------------------------------ entities

def with_entity_keys(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add ``_key_card1`` and ``_key_uid`` (UInt64, null when undefined)."""
    first_day = (pl.col("day") - pl.col("D1")).floor().cast(pl.Int64)
    uid = pl.when(pl.any_horizontal(pl.col("card1").is_null(), pl.col("addr1").is_null(), pl.col("D1").is_null())) \
        .then(None).otherwise(pl.struct(pl.col("card1").cast(pl.Int64), pl.col("addr1").cast(pl.Int64),
                                        first_day).hash(seed=0))
    card = pl.when(pl.col("card1").is_null()).then(None).otherwise(pl.col("card1").cast(pl.Int64).hash(seed=0))
    return lf.with_columns(card.alias("_key_card1"), uid.alias("_key_uid"))


# ------------------------------------------------------------------ behaviour

def _running(events: pl.DataFrame, key: str) -> pl.DataFrame:
    """Running totals per entity, one row per (entity, second) with events."""
    return (
        events.filter(pl.col(key).is_not_null())
        .group_by(key, "event_time_s")
        .agg(pl.len().cast(pl.Float64).alias("cnt"), pl.col("amount").sum().alias("sum"),
             pl.col("amount").log1p().sum().alias("sumlog"), (pl.col("amount").log1p() ** 2).sum().alias("sumsqlog"))
        .sort(key, "event_time_s")
        .with_columns([pl.col(s).cum_sum().over(key).alias(f"c_{s}") for s in _STATS])
        .select(key, pl.col("event_time_s").alias("at_t"), *[f"c_{s}" for s in _STATS])
        .sort("at_t")
    )


def _asof(left: pl.DataFrame, running: pl.DataFrame, key: str, offset_s: int, suffix: str) -> pl.DataFrame:
    """Running totals of the entity's events with time <= t - offset_s."""
    probe = left.with_columns((pl.col("event_time_s") - offset_s).alias("_probe")).sort("_probe")
    right = running.rename({c: f"{c}{suffix}" for c in running.columns if c != key})
    out = probe.join_asof(right, left_on="_probe", right_on=f"at_t{suffix}", by=key, strategy="backward",
                          check_sortedness=False)
    return out.drop("_probe")


def behavior_feature_names(entity: str, windows: tuple[int, ...]) -> list[str]:
    names = []
    for w in windows:
        names += [f"{entity}_txn_count_{w}d", f"{entity}_amount_sum_{w}d", f"{entity}_amount_mean_{w}d",
                  f"{entity}_log_amount_std_{w}d", f"{entity}_log_amount_zscore_{w}d",
                  f"{entity}_amount_ratio_to_mean_{w}d"]
    names += [f"{entity}_txn_count_all", f"{entity}_hours_since_last_txn"]
    if len(windows) >= 2:
        names.append(f"{entity}_activity_ratio_{windows[0]}d_{windows[-1]}d")
    return names


def behavior_features(base: pl.DataFrame, entity: str, windows: tuple[int, ...]) -> pl.DataFrame:
    """Point-in-time behaviour of one entity. ``base`` holds transaction_id, event_time_s,
    amount and ``_key_<entity>``; returns transaction_id plus the feature columns."""
    key = f"_key_{entity}"
    running = _running(base, key)
    df = base.select("transaction_id", "event_time_s", "amount", key)
    df = _asof(df, running, key, 1, "_now")
    for w in windows:
        df = _asof(df, running, key, w * SECONDS_PER_DAY + 1, f"_m{w}")

    undefined = pl.col(key).is_null()
    logamt = pl.col("amount").log1p()
    exprs: list[pl.Expr] = []
    for w in windows:
        win = {s: (pl.col(f"c_{s}_now").fill_null(0.0) - pl.col(f"c_{s}_m{w}").fill_null(0.0)).clip(lower_bound=0.0)
               for s in _STATS}
        cnt = win["cnt"].round(0)
        mean = pl.when(cnt > 0).then(win["sum"] / cnt)
        meanlog = win["sumlog"] / cnt
        var = (win["sumsqlog"] - cnt * meanlog ** 2) / (cnt - 1)
        std = pl.when(cnt >= 2).then(var.clip(lower_bound=0.0).sqrt())
        exprs += [
            cnt.alias(f"{entity}_txn_count_{w}d"),
            win["sum"].alias(f"{entity}_amount_sum_{w}d"),
            mean.alias(f"{entity}_amount_mean_{w}d"),
            std.alias(f"{entity}_log_amount_std_{w}d"),
            pl.when(std > _STD_EPS).then((logamt - meanlog) / std).alias(f"{entity}_log_amount_zscore_{w}d"),
            pl.when(mean > 0).then(pl.col("amount") / mean).alias(f"{entity}_amount_ratio_to_mean_{w}d"),
        ]
    exprs += [
        pl.col("c_cnt_now").fill_null(0.0).round(0).alias(f"{entity}_txn_count_all"),
        ((pl.col("event_time_s") - pl.col("at_t_now")) / 3600.0).alias(f"{entity}_hours_since_last_txn"),
    ]
    out = df.with_columns(exprs)
    if len(windows) >= 2:
        s, lng = windows[0], windows[-1]
        short, long_ = pl.col(f"{entity}_txn_count_{s}d"), pl.col(f"{entity}_txn_count_{lng}d")
        out = out.with_columns(pl.when(long_ > 0).then(short / (long_ * s / lng))
                               .alias(f"{entity}_activity_ratio_{s}d_{lng}d"))
    names = behavior_feature_names(entity, windows)
    # Undefined entity: every feature is NaN, never a misleading 0.
    return out.select("transaction_id", *[pl.when(undefined).then(None).otherwise(pl.col(n)).cast(pl.Float32).alias(n)
                                          for n in names])


def novelty_feature_names(entity: str, columns: tuple[str, ...]) -> list[str]:
    return [f"{entity}_prior_share_same_{c}" for c in columns] + [f"{entity}_is_new_{c}" for c in columns]


def novelty_features(base: pl.DataFrame, entity: str, columns: tuple[str, ...]) -> pl.DataFrame:
    """Does the entity use a value (email domain, device, ...) it has used before?

    For each column c: among the entity's earlier transactions (time < t, same second
    excluded), the share that had the same value of c, and a flag for "never before".
    Missing is its own value. NaN when the entity is undefined or has no earlier
    transactions. ``base`` holds transaction_id, event_time_s, ``_key_<entity>`` and
    the value columns.
    """
    key = f"_key_{entity}"
    df = base.select("transaction_id", "event_time_s", key, *columns)

    def prior_count(frame: pl.DataFrame, k: str) -> pl.Series:
        run = (frame.filter(pl.col(k).is_not_null()).group_by(k, "event_time_s").agg(pl.len().cast(pl.Float64).alias("n"))
               .sort(k, "event_time_s").with_columns(pl.col("n").cum_sum().over(k).alias("c"))
               .select(k, pl.col("event_time_s").alias("at_t"), "c").sort("at_t"))
        probe = frame.select("transaction_id", k, (pl.col("event_time_s") - 1).alias("_p")).sort("_p")
        got = probe.join_asof(run, left_on="_p", right_on="at_t", by=k, strategy="backward", check_sortedness=False)
        return frame.select("transaction_id").join(got.select("transaction_id", "c"), on="transaction_id",
                                                   how="left", maintain_order="left")["c"].fill_null(0.0)

    out = df.select("transaction_id", pl.col(key).is_null().alias("_undef"),
                    prior_count(df, key).alias("_total"))
    defined = ~pl.col("_undef") & (pl.col("_total") > 0)
    for c in columns:
        pair = df.with_columns(pl.when(pl.col(key).is_null()).then(None).otherwise(
            pl.struct(pl.col(key), pl.col(c).cast(pl.String).fill_null(MISSING_TOKEN)).hash(seed=1)).alias("_pair"))
        same = pl.col(f"_same_{c}")
        out = out.with_columns(prior_count(pair, "_pair").alias(f"_same_{c}")).with_columns(
            pl.when(defined).then(same / pl.col("_total")).cast(pl.Float32).alias(f"{entity}_prior_share_same_{c}"),
            pl.when(defined).then((same == 0).cast(pl.Float32)).alias(f"{entity}_is_new_{c}"))
    return out.select("transaction_id", *novelty_feature_names(entity, columns))


# ------------------------------------------------------------------ encoders (training split only)

def _check_integer_valued(train: pl.LazyFrame, cols: tuple[str, ...]) -> None:
    bad = train.select([((pl.col(c) % 1) != 0).sum().alias(c) for c in cols]).collect().row(0, named=True)
    if any(bad.values()):
        raise ValueError(f"expected integer-valued id columns, found decimals: {bad}")


def fit_encoders(train: pl.LazyFrame, cfg: IEEEFeatureConfig) -> dict:
    """Frequency vocabularies from training rows only."""
    n = train.select(pl.len()).collect().item()
    out: dict = {"fitted_on": "train", "rows": n, "categorical": {}, "numeric_frequency": {}}
    for c in cfg.categorical:
        vc = (train.select(pl.col(c).fill_null(MISSING_TOKEN).alias("v")).group_by("v").agg(pl.len().alias("n"))
              .sort(["n", "v"], descending=[True, False]).collect())
        freq = {v: k / n for v, k in vc.iter_rows()}
        vocab = [v for v, k in vc.iter_rows() if k >= cfg.min_count][: cfg.max_vocabulary]
        out["categorical"][c] = {"vocabulary": vocab, "frequency": freq}
    _check_integer_valued(train, cfg.frequency_numeric)
    for c in cfg.frequency_numeric:
        vc = (train.select(pl.col(c).cast(pl.Int64).fill_null(-1).alias("v")).group_by("v").agg(pl.len().alias("n"))
              .collect())
        out["numeric_frequency"][c] = {str(v): k / n for v, k in vc.iter_rows()}
    return out


def encoding_exprs(enc: dict) -> list[pl.Expr]:
    """Apply saved encoders. Code: index in the vocabulary; rare or unseen values share
    the code len(vocabulary). Frequency: training share; unseen values get 0."""
    exprs = []
    for c, e in enc["categorical"].items():
        v = pl.col(c).fill_null(MISSING_TOKEN)
        codes = {val: float(i) for i, val in enumerate(e["vocabulary"])}
        exprs.append(v.replace_strict(codes, default=float(len(codes)), return_dtype=pl.Float32).alias(f"{c}_code"))
        exprs.append(v.replace_strict(e["frequency"], default=0.0, return_dtype=pl.Float32).alias(f"{c}_freq"))
    for c, f in enc["numeric_frequency"].items():
        keys = {int(k): val for k, val in f.items()}
        exprs.append(pl.col(c).cast(pl.Int64).fill_null(-1).replace_strict(keys, default=0.0, return_dtype=pl.Float32)
                     .alias(f"{c}_freq"))
    return exprs


# ------------------------------------------------------------------ V selection (training split only)

def select_v_columns(train: pl.LazyFrame, cfg: IEEEFeatureConfig) -> dict:
    sample = (train.filter((pl.col("transaction_id").hash(seed=cfg.v_seed) % 1_000_000)
                           < int(1_000_000 * min(1.0, cfg.v_sample_rows / max(1, train.select(pl.len()).collect().item()))))
              .select(V_COLUMNS).collect())
    nulls = {c: int(sample[c].null_count()) for c in V_COLUMNS}
    groups: dict[int, list[str]] = {}
    for c in V_COLUMNS:
        groups.setdefault(nulls[c], []).append(c)
    kept, dropped_constant, dropped_corr = [], [], {}
    for _, cols in sorted(groups.items()):
        X = sample.select(cols).to_numpy().astype(np.float64)
        rows = ~np.isnan(X).any(axis=1)
        X = X[rows]
        std = X.std(axis=0) if len(X) else np.zeros(len(cols))
        group_kept: list[int] = []
        if len(X) >= 2:
            Z = (X - X.mean(axis=0)) / np.where(std > 0, std, 1.0)
        for i, c in enumerate(cols):
            if len(X) < 2 or std[i] == 0:
                dropped_constant.append(c)
                continue
            if group_kept:
                r = np.abs(Z[:, group_kept].T @ Z[:, i]) / len(Z)
                j = int(np.argmax(r))
                if r[j] > cfg.v_correlation_threshold:
                    dropped_corr[c] = {"duplicate_of": cols[group_kept[j]], "abs_r": round(float(r[j]), 4)}
                    continue
            group_kept.append(i)
        kept += [cols[i] for i in group_kept]
    kept = [c for c in V_COLUMNS if c in set(kept)]  # source order
    return {"sample_rows": len(sample), "threshold": cfg.v_correlation_threshold, "kept": kept,
            "dropped_constant": dropped_constant, "dropped_correlated": dropped_corr, "null_groups": len(groups)}


# ------------------------------------------------------------------ registry

def build_specs(cfg: IEEEFeatureConfig, v_kept: list[str]) -> tuple[FeatureSpec, ...]:
    S = FeatureSpec
    specs = [
        S("amount", "transaction", "float32", "Transaction amount (USD)", ("TransactionAmt",), "amount", "never"),
        S("log_amount", "transaction", "float32", "log(1 + amount)", ("TransactionAmt",), "log1p(amount)", "never"),
        S("amount_cents", "transaction", "float32", "Cents part of the amount (0-99); non-zero often means a "
          "currency conversion", ("TransactionAmt",), "round((amount mod 1) * 100)", "never"),
        S("hour_of_day", "time", "float32", "Hour of day relative to the dataset's undisclosed reference "
          "(not a known timezone)", ("TransactionDT",), "(TransactionDT // 3600) mod 24", "never"),
        S("day_of_week", "time", "float32", "Day of week relative to the reference", ("TransactionDT",),
          "day mod 7", "never"),
    ]
    specs += [S(c, "card_address", "float32", f"Source column {c} (anonymized card / address / distance field)",
                (c,), c, "nan_undefined") for c in CARD_ADDRESS]
    specs += [S(c, "vesta_counts", "float32", f"Vesta count feature {c} (meaning not disclosed)", (c,), c,
                "nan_undefined", online_available=False) for c in C_COLUMNS]
    specs += [S(c, "vesta_timedeltas", "float32", f"Vesta time delta {c} in days (meaning partly disclosed)", (c,), c,
                "nan_undefined", online_available=False) for c in D_COLUMNS]
    specs += [S(c, "vesta_match", "float32", f"Match flag {c}: 1 = T, 0 = F, NaN = missing", (c,),
                f"{c} == 'T'", "nan_undefined", online_available=False) for c in MATCH_FLAG_COLUMNS]
    specs += [S(c, "vesta_v", "float32", f"Vesta engineered feature {c}", (c,), c, "nan_undefined",
                online_available=False) for c in v_kept]
    specs.append(S("has_identity", "identity", "float32", "Transaction has an identity record", ("TransactionID",),
                   "TransactionID in train_identity", "never"))
    specs += [S(c, "identity", "float32", f"Identity field {c} (anonymized)", (c,), c, "nan_undefined")
              for c in ID_NUMERIC_COLUMNS]
    for c in cfg.categorical:
        specs.append(S(f"{c}_code", "category_code", "float32", f"{c}: rank by training frequency; rare and unseen "
                       "values share one code; missing is its own value", (c,), "training vocabulary index", "never"))
        specs.append(S(f"{c}_freq", "category_freq", "float32", f"{c}: share of training rows with this value "
                       "(0 if unseen)", (c,), "training frequency", "never"))
    specs += [S(f"{c}_freq", "category_freq", "float32", f"{c}: share of training rows with this value",
                (c,), "training frequency", "never") for c in cfg.frequency_numeric]
    for e in cfg.entities:
        src = ("card1",) if e == "card1" else ("card1", "addr1", "D1", "TransactionDT")
        miss = "nan_undefined"
        for n in behavior_feature_names(e, cfg.windows_days):
            specs.append(S(n, f"{e}_behavior", "float32", f"Point-in-time behaviour of {e}: {n}", src + ("TransactionAmt",),
                           "entity transactions with time in [t - W days, t - 1 s]; labels never used", miss))
    for e in cfg.novelty_entities:
        for c in cfg.novelty_columns:
            specs.append(S(f"{e}_prior_share_same_{c}", f"{e}_novelty", "float32",
                           f"Share of the {e}'s earlier transactions with the same {c} (missing is a value)",
                           ("card1", "addr1", "D1", "TransactionDT", c),
                           "count(earlier, same value) / count(earlier); NaN if no earlier transactions",
                           "nan_undefined"))
            specs.append(S(f"{e}_is_new_{c}", f"{e}_novelty", "float32",
                           f"1 if the {e} has never used this {c} before", ("card1", "addr1", "D1", "TransactionDT", c),
                           "count(earlier, same value) == 0; NaN if no earlier transactions", "nan_undefined"))
    names = [s.name for s in specs]
    assert len(names) == len(set(names)), "duplicate feature names"
    return tuple(specs)


@dataclass(frozen=True)
class IEEERegistry:
    config: IEEEFeatureConfig
    specs: tuple[FeatureSpec, ...]
    encoders_sha256: str
    v_selection: dict

    @property
    def version(self) -> str:
        return self.config.version

    def names(self, feature_set: str = "full") -> list[str]:
        if feature_set not in self.config.feature_sets:
            raise ValueError(f"Unknown feature set {feature_set}")
        groups = set(self.config.feature_sets[feature_set])
        return [s.name for s in self.specs if s.group in groups]

    def all_names(self) -> list[str]:
        return [s.name for s in self.specs]

    def schema_dict(self, feature_set: str = "full") -> dict:
        wanted = set(self.names(feature_set))
        return {"dataset": DATASET, "feature_version": self.version, "feature_set": feature_set,
                "windows_days": list(self.config.windows_days), "entities": list(self.config.entities),
                "novelty": {"entities": list(self.config.novelty_entities), "columns": list(self.config.novelty_columns)},
                "encoders_sha256": self.encoders_sha256, "v_selected": self.v_selection["kept"],
                "features": [asdict(s) for s in self.specs if s.name in wanted],
                "excluded_columns": EXCLUDED_COLUMNS}

    def schema_hash(self, feature_set: str = "full") -> str:
        return hashlib.sha256(json.dumps(self.schema_dict(feature_set), sort_keys=True).encode()).hexdigest()[:16]

    @classmethod
    def load(cls, directory: Path | None = None) -> IEEERegistry:
        d = directory or features_dir()
        if not (d / "encoders.json").exists():
            raise FileNotFoundError(f"{d} has no encoders.json. Run: python -m riskstream.cli build-features "
                                    "--dataset ieee_cis")
        cfg = get_config()
        enc_bytes = (d / "encoders.json").read_bytes()
        v_sel = json.loads((d / "v_selection.json").read_text())
        return cls(cfg, build_specs(cfg, v_sel["kept"]), hashlib.sha256(enc_bytes).hexdigest()[:16], v_sel)


# ------------------------------------------------------------------ feature frame

def row_feature_exprs(enc: dict) -> list[pl.Expr]:
    amt = pl.col("amount")
    return [
        amt.cast(pl.Float32).alias("amount_f"),
        amt.log1p().cast(pl.Float32).alias("log_amount"),
        ((amt % 1) * 100).round(0).cast(pl.Float32).alias("amount_cents"),
        ((pl.col("event_time_s") // 3600) % 24).cast(pl.Float32).alias("hour_of_day"),
        (pl.col("day") % 7).cast(pl.Float32).alias("day_of_week"),
        *[pl.when(pl.col(c) == "T").then(1.0).when(pl.col(c) == "F").then(0.0).otherwise(None)
          .cast(pl.Float32).alias(f"{c}__flag") for c in MATCH_FLAG_COLUMNS],
        pl.col("has_identity").cast(pl.Float32).alias("has_identity_f"),
        *encoding_exprs(enc),
    ]


def feature_frame(lf: pl.LazyFrame, behavior: pl.DataFrame, enc: dict, registry: IEEERegistry) -> pl.LazyFrame:
    """Meta columns + every registry feature, float32, in registry order."""
    renamed = {"amount": "amount_f", "has_identity": "has_identity_f", **{c: f"{c}__flag" for c in MATCH_FLAG_COLUMNS}}
    out = lf.with_columns(row_feature_exprs(enc)).join(behavior.lazy(), on="transaction_id", how="left",
                                                       maintain_order="left")
    cols = []
    for n in registry.all_names():
        if n in META_COLUMNS:
            # "amount" is both a feature and the meta column used for costs. It stays
            # one float64 column (exact cents); models receive it as float32 like every feature.
            continue
        src = renamed.get(n, n)
        cols.append(pl.col(src).cast(pl.Float32).alias(n))
    return out.select(*[pl.col(m) for m in META_COLUMNS], *cols)


def build_behavior(lf: pl.LazyFrame, cfg: IEEEFeatureConfig) -> pl.DataFrame:
    base = with_entity_keys(lf).select("transaction_id", "event_time_s", "amount", "_key_card1", "_key_uid",
                                       *cfg.novelty_columns).collect()
    parts = [behavior_features(base, e, cfg.windows_days) for e in cfg.entities]
    parts += [novelty_features(base, e, cfg.novelty_columns) for e in cfg.novelty_entities]
    out = parts[0]
    for p in parts[1:]:
        out = out.join(p, on="transaction_id", how="left", maintain_order="left")
    return out


def run_build_features(splits_dir: Path | None = None, out_dir: Path | None = None) -> dict:
    """Fit encoders and V selection on train, then write feature tables per split."""
    from riskstream.data.ieee_cis import SPLITS, load_manifest, load_split
    from riskstream.monitoring.resources import peak_rss_mb

    t0 = time.perf_counter()
    cfg = get_config()
    out_dir = out_dir or features_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    scan = (lambda s: pl.scan_parquet(splits_dir / f"{s}.parquet")) if splits_dir else load_split
    train = scan("train")

    enc = fit_encoders(train, cfg)
    (out_dir / "encoders.json").write_text(json.dumps(enc, sort_keys=True))
    v_sel = select_v_columns(train, cfg)
    (out_dir / "v_selection.json").write_text(json.dumps(v_sel, indent=2))
    logger.info("V selection: kept %d of %d (constant %d, correlated %d)", len(v_sel["kept"]), len(V_COLUMNS),
                len(v_sel["dropped_constant"]), len(v_sel["dropped_correlated"]))
    registry = IEEERegistry.load(out_dir)

    # History for behaviour features comes from all splits in time order, as a live
    # system would see it; each transaction only looks at earlier seconds.
    everything = pl.concat([scan(s) for s in SPLITS], how="vertical")
    behavior = build_behavior(everything, cfg)
    logger.info("Behaviour features for %s rows (%.1fs)", f"{len(behavior):,}", time.perf_counter() - t0)

    counts = {}
    for s in SPLITS:
        feature_frame(scan(s), behavior, enc, registry).sink_parquet(out_dir / f"{s}.parquet", compression="zstd")
        counts[s] = pl.scan_parquet(out_dir / f"{s}.parquet").select(pl.len()).collect().item()
    keep = (pl.col("is_fraud") == 1) | ((pl.col("transaction_id").hash(seed=cfg.sampling_seed) % 1_000_000)
                                        < int(cfg.negative_sample_rate * 1_000_000))
    pl.scan_parquet(out_dir / "train.parquet").filter(keep).sink_parquet(out_dir / "train_sampled.parquet",
                                                                         compression="zstd")
    sampled = pl.scan_parquet(out_dir / "train_sampled.parquet").select(pl.len(), pl.col("is_fraud").sum()).collect().row(0)

    try:
        source_sha = load_manifest()["raw_sha256"]
    except FileNotFoundError:
        source_sha = "unknown"
    meta = {
        "dataset": DATASET, "feature_version": registry.version,
        "schema_hash_full": registry.schema_hash("full"), "n_features_full": len(registry.names("full")),
        "windows_days": list(cfg.windows_days), "entities": list(cfg.entities),
        "novelty": {"entities": list(cfg.novelty_entities), "columns": list(cfg.novelty_columns)}, "rows": counts,
        "train_sampled": {"rows": int(sampled[0]), "fraud": int(sampled[1]),
                          "negative_sample_rate": cfg.negative_sample_rate, "sampling_seed": cfg.sampling_seed,
                          "note": "negatives kept by a deterministic hash of transaction_id; all positives kept"},
        "encoders": {"fitted_on": "train", "sha256_16": registry.encoders_sha256},
        "v_selection": {k: v_sel[k] for k in ("sample_rows", "threshold", "null_groups")} | {
            "kept": len(v_sel["kept"]), "dropped_constant": len(v_sel["dropped_constant"]),
            "dropped_correlated": len(v_sel["dropped_correlated"])},
        "source_raw_sha256": source_sha,
        "seconds": round(time.perf_counter() - t0, 1), "peak_rss_mb": peak_rss_mb(),
    }
    (out_dir / "feature_manifest.json").write_text(json.dumps(meta, indent=2))
    (out_dir / "feature_schema.json").write_text(
        json.dumps({fs: registry.schema_dict(fs) for fs in cfg.feature_sets}, indent=2))
    logger.info("IEEE-CIS feature tables in %s: %s (%.1fs)", out_dir, counts, meta["seconds"])
    return meta

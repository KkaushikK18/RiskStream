"""Feature registry: what each feature is, where it comes from, how missing is handled."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from functools import lru_cache

from riskstream.config import load_config
from riskstream.data.schema import TRANSACTION_TYPES

# Missing-value semantics, stored with every feature:
#   "never"          - always defined
#   "zero_is_true"   - 0 means "no prior events", which is a real value, not a gap
#   "nan_undefined"  - NaN means the statistic is undefined (e.g. mean of 0 events);
#                      boosted trees and sklearn trees route NaN natively, logistic
#                      regression imputes it inside its serialized pipeline
#                      (median + missing-indicator column), identically at serving.
MISSING_POLICIES = ("never", "zero_is_true", "nan_undefined")


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    group: str
    dtype: str
    description: str
    source_fields: tuple[str, ...]
    calculation: str
    missing: str
    online_available: bool = True
    version_added: str = "1.0.0"


@dataclass(frozen=True)
class FeatureConfig:
    version: str
    windows: tuple[int, ...]
    feature_sets: dict[str, tuple[str, ...]]
    negative_sample_rate: float
    sampling_seed: int

    @classmethod
    def load(cls) -> FeatureConfig:
        cfg = load_config("features")
        return cls(
            version=str(cfg["version"]),
            windows=tuple(int(w) for w in cfg["windows_hours"]),
            feature_sets={k: tuple(v) for k, v in cfg["feature_sets"].items()},
            negative_sample_rate=float(cfg["training_sampling"]["negative_sample_rate"]),
            sampling_seed=int(cfg["training_sampling"]["seed"]),
        )


def _type_flag(t: str) -> FeatureSpec:
    return FeatureSpec(
        f"type_{t}", "transaction", "float32", f"1 if type == {t}", ("type",),
        "one-hot of transaction type", "never",
    )


def build_specs(windows: tuple[int, ...]) -> tuple[FeatureSpec, ...]:
    specs: list[FeatureSpec] = [
        FeatureSpec("amount", "transaction", "float32", "Transaction amount", ("amount",),
                    "amount", "never"),
        FeatureSpec("log_amount", "transaction", "float32", "log(1 + amount)", ("amount",),
                    "log1p(amount)", "never"),
        *[_type_flag(t) for t in TRANSACTION_TYPES],
        FeatureSpec("dest_is_merchant", "transaction", "float32", "Destination is a merchant",
                    ("dest_type",), "dest_type == MERCHANT", "never"),
        FeatureSpec("hour_of_day", "transaction", "float32",
                    "Simulated hour of day; PaySim's clock, not a real timezone",
                    ("step",), "(step - 1) mod 24", "never"),
        FeatureSpec("amount_is_round_1000", "transaction", "float32",
                    "Amount is a positive multiple of 1000", ("amount",),
                    "amount > 0 and amount mod 1000 == 0", "never"),
        FeatureSpec("orig_balance_before", "balance", "float32",
                    "Sender balance before the transaction (known to the sender's institution)",
                    ("orig_balance_before",), "orig_balance_before", "never"),
        FeatureSpec("orig_balance_is_zero", "balance", "float32", "Sender balance before == 0",
                    ("orig_balance_before",), "orig_balance_before == 0", "never"),
        FeatureSpec("amount_to_orig_balance", "balance", "float32",
                    "amount / (orig_balance_before + 1)", ("amount", "orig_balance_before"),
                    "amount / (orig_balance_before + 1)", "never"),
        FeatureSpec("amount_exceeds_orig_balance", "balance", "float32",
                    "amount > orig_balance_before", ("amount", "orig_balance_before"),
                    "amount > orig_balance_before", "never"),
        FeatureSpec("amount_drains_orig_balance", "balance", "float32",
                    "amount equals the full sender balance (within 0.01). Strong PaySim "
                    "simulator artifact; see the no_balance ablation.",
                    ("amount", "orig_balance_before"),
                    "|amount - orig_balance_before| <= 0.01 and orig_balance_before > 0", "never"),
    ]
    for w in windows:
        src = ("dest_id", "step", "amount", "type")
        win = f"destination events with step in [step-{w}, step-1]"
        specs += [
            FeatureSpec(f"dest_txn_count_{w}h", "dest_behavior", "float32",
                        f"Prior transactions into the destination, last {w}h", src,
                        f"count of {win}", "zero_is_true"),
            FeatureSpec(f"dest_amount_sum_{w}h", "dest_behavior", "float32",
                        f"Total prior amount into the destination, last {w}h", src,
                        f"sum(amount) over {win}", "zero_is_true"),
            FeatureSpec(f"dest_amount_mean_{w}h", "dest_behavior", "float32",
                        f"Mean prior amount into the destination, last {w}h", src,
                        f"sum / count over {win}; NaN if count == 0", "nan_undefined"),
            FeatureSpec(f"dest_log_amount_std_{w}h", "dest_behavior", "float32",
                        f"Sample std of log1p(amount) into the destination, last {w}h", src,
                        f"std of log1p(amount) over {win}; NaN if count < 2", "nan_undefined"),
            FeatureSpec(f"dest_log_amount_zscore_{w}h", "dest_behavior", "float32",
                        f"How unusual this amount is for the destination, last {w}h", src,
                        "(log1p(amount) - mean) / std; NaN if std undefined or ~0",
                        "nan_undefined"),
            FeatureSpec(f"dest_amount_ratio_to_mean_{w}h", "dest_behavior", "float32",
                        f"amount / mean prior amount into destination, last {w}h", src,
                        "amount / mean; NaN if count == 0 or mean == 0", "nan_undefined"),
            FeatureSpec(f"dest_transfer_share_{w}h", "dest_behavior", "float32",
                        f"Share of prior destination events that were TRANSFERs, last {w}h", src,
                        f"count(type == TRANSFER) / count over {win}; NaN if count == 0",
                        "nan_undefined"),
        ]
    if len(windows) >= 2:
        short, long = windows[0], windows[-1]
        specs.append(
            FeatureSpec(f"dest_activity_ratio_{short}h_{long}h", "dest_behavior", "float32",
                        "Recent activity relative to the longer-window hourly rate",
                        ("dest_id", "step"),
                        f"count_{short}h / (count_{long}h * {short}/{long}); NaN if count_{long}h == 0",
                        "nan_undefined")
        )
    specs += [
        FeatureSpec("dest_txn_count_all", "dest_behavior", "float32",
                    "All prior transactions into the destination since history start",
                    ("dest_id", "step"), "count of destination events with step < step",
                    "zero_is_true"),
        FeatureSpec("dest_hours_since_last_txn", "dest_behavior", "float32",
                    "Hours since the destination's previous transaction (earlier steps only)",
                    ("dest_id", "step"), "step - max(prior step); NaN if none", "nan_undefined"),
        FeatureSpec("orig_txn_count_all", "orig_behavior", "float32",
                    "All prior transactions from the origin (rare in PaySim: 99.85% of origins "
                    "appear once)", ("orig_id", "step"),
                    "count of origin events with step < step", "zero_is_true"),
        FeatureSpec("orig_hours_since_last_txn", "orig_behavior", "float32",
                    "Hours since the origin's previous transaction", ("orig_id", "step"),
                    "step - max(prior step); NaN if none", "nan_undefined"),
    ]
    names = [s.name for s in specs]
    assert len(names) == len(set(names)), "duplicate feature names"
    assert all(s.missing in MISSING_POLICIES for s in specs)
    return tuple(specs)


# Columns from the canonical schema deliberately NOT used as features.
EXCLUDED_COLUMNS: dict[str, str] = {
    "is_fraud": "target",
    "is_flagged_fraud": "output of the simulator's legacy rule (rule leakage)",
    "orig_balance_after": "post-event value, unknown when scoring",
    "dest_balance_before": "another institution's data, not available live",
    "dest_balance_after": "post-event value, unknown when scoring",
    "transaction_id": "identifier",
    "event_seq": "row position in the source file, not a real timestamp",
    "orig_id": "high-cardinality identifier; used only as a grouping key",
    "dest_id": "high-cardinality identifier; used only as a grouping key",
    "step": "absolute time index; would not generalize to future periods",
}


@dataclass(frozen=True)
class FeatureRegistry:
    config: FeatureConfig
    specs: tuple[FeatureSpec, ...]

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
        return {
            "feature_version": self.version,
            "feature_set": feature_set,
            "windows_hours": list(self.config.windows),
            "features": [asdict(s) for s in self.specs if s.name in wanted],
            "excluded_columns": EXCLUDED_COLUMNS,
        }

    def schema_hash(self, feature_set: str = "full") -> str:
        payload = json.dumps(self.schema_dict(feature_set), sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()[:16]


@lru_cache(maxsize=1)
def get_registry() -> FeatureRegistry:
    cfg = FeatureConfig.load()
    return FeatureRegistry(cfg, build_specs(cfg.windows))

"""Deterministic synthetic transactions in PaySim's raw column format.

Purpose: tests, CI, demos, load and failure testing, drift simulation.
It is NOT a source of model-quality results. Its fraud mechanism is a few
hand-written rules, so any model trained on it learns those rules back.

Because the output uses PaySim's raw columns, it flows through the same
adapter, validation, EDA and split code as the real dataset.

Fraud mechanism (only on TRANSFER and CASH_OUT, mirroring PaySim):
* the fraudster usually drains the origin balance (amount == balance), and
* usually sends the money to a small pool of "mule" destination accounts,
  which therefore accumulate many incoming transactions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

from riskstream.config import load_config
from riskstream.data.adapters.paysim import PAYSIM_RAW_SCHEMA


@dataclass
class SyntheticConfig:
    seed: int = 42
    num_transactions: int = 50_000
    num_steps: int = 240
    fraud_rate_in_risky_types: float = 0.01
    type_probabilities: dict[str, float] = field(
        default_factory=lambda: {
            "CASH_OUT": 0.35, "PAYMENT": 0.34, "CASH_IN": 0.22, "TRANSFER": 0.08, "DEBIT": 0.01,
        }
    )
    amount_lognormal: dict[str, tuple[float, float]] = field(
        default_factory=lambda: {
            "CASH_OUT": (11.5, 1.0), "PAYMENT": (8.5, 1.0), "CASH_IN": (11.3, 1.0),
            "TRANSFER": (12.0, 1.3), "DEBIT": (8.0, 1.0),
        }
    )
    balance_lognormal: tuple[float, float] = (10.5, 1.8)
    origin_pool_ratio: float = 0.9
    customer_dest_pool_ratio: float = 0.2
    merchant_dest_pool_ratio: float = 0.3
    mule_dest_pool_size: int = 60
    fraud_drains_balance_probability: float = 0.9
    fraud_uses_mule_probability: float = 0.7

    @classmethod
    def from_yaml(cls, **overrides: object) -> SyntheticConfig:
        cfg = dict(load_config("data")["synthetic_generator"])
        cfg.update({k: v for k, v in overrides.items() if v is not None})
        cfg["amount_lognormal"] = {k: tuple(v) for k, v in cfg["amount_lognormal"].items()}
        cfg["balance_lognormal"] = tuple(cfg["balance_lognormal"])
        return cls(**cfg)


def generate(cfg: SyntheticConfig) -> pl.DataFrame:
    """Generate one dataset. The same config always yields identical output."""
    rng = np.random.default_rng(cfg.seed)
    n = cfg.num_transactions
    if n <= 0 or cfg.num_steps <= 0:
        raise ValueError("num_transactions and num_steps must be positive")

    steps = np.sort(rng.integers(1, cfg.num_steps + 1, size=n))

    types_list = list(cfg.type_probabilities)
    probs = np.array([cfg.type_probabilities[t] for t in types_list], dtype=float)
    types = np.array(types_list)[rng.choice(len(types_list), size=n, p=probs / probs.sum())]

    amounts = np.empty(n)
    for t in types_list:
        mask = types == t
        mu, sigma = cfg.amount_lognormal[t]
        amounts[mask] = rng.lognormal(mu, sigma, size=mask.sum())
    amounts = np.round(amounts, 2)

    n_orig = max(1, int(n * cfg.origin_pool_ratio))
    n_cust = max(1, int(n * cfg.customer_dest_pool_ratio))
    n_merch = max(1, int(n * cfg.merchant_dest_pool_ratio))
    orig_idx = rng.integers(0, n_orig, size=n)
    cust_idx = rng.integers(0, n_cust, size=n)
    merch_idx = rng.integers(0, n_merch, size=n)
    to_merchant = np.isin(types, ["PAYMENT", "DEBIT"])
    dest = np.where(
        to_merchant,
        np.char.add("M", (merch_idx + 1_000_000).astype(str)),
        np.char.add("C", (cust_idx + 5_000_000).astype(str)),
    )
    orig = np.char.add("C", (orig_idx + 1_000_000).astype(str))

    risky = np.isin(types, ["TRANSFER", "CASH_OUT"])
    is_fraud = risky & (rng.random(n) < cfg.fraud_rate_in_risky_types)

    mu_b, sigma_b = cfg.balance_lognormal
    old_orig = np.round(rng.lognormal(mu_b, sigma_b, size=n), 2)
    drains = is_fraud & (rng.random(n) < cfg.fraud_drains_balance_probability)
    amounts = np.where(drains, old_orig, amounts)
    # Fraud never moves zero: if the balance was 0, keep the sampled amount.
    amounts = np.where(amounts <= 0, np.round(rng.lognormal(10, 1, size=n), 2), amounts)

    mule = is_fraud & (rng.random(n) < cfg.fraud_uses_mule_probability)
    mule_ids = np.char.add("C", (rng.integers(0, cfg.mule_dest_pool_size, size=n) + 9_000_000).astype(str))
    dest = np.where(mule, mule_ids, dest)

    incoming = types == "CASH_IN"
    new_orig = np.where(incoming, old_orig + amounts, np.maximum(old_orig - amounts, 0.0))
    old_dest = np.where(to_merchant, 0.0, np.round(rng.lognormal(mu_b, sigma_b, size=n), 2))
    new_dest = np.where(
        to_merchant, 0.0, np.where(incoming, np.maximum(old_dest - amounts, 0.0), old_dest + amounts)
    )

    df = pl.DataFrame(
        {
            "step": steps.astype(np.int64),
            "type": types,
            "amount": amounts,
            "nameOrig": orig,
            "oldbalanceOrg": old_orig,
            "newbalanceOrig": np.round(new_orig, 2),
            "nameDest": dest,
            "oldbalanceDest": old_dest,
            "newbalanceDest": np.round(new_dest, 2),
            "isFraud": is_fraud.astype(np.int64),
            "isFlaggedFraud": np.zeros(n, dtype=np.int64),
        }
    )
    return df.cast(PAYSIM_RAW_SCHEMA)


def write(df: pl.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd")
    return path

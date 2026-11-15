"""Deterministic synthetic transaction generator for testing and demos."""

from typing import Dict, List, Tuple

import numpy as np
import polars as pl

from riskstream.config import load_config
from riskstream.logging_config import get_logger

logger = get_logger(__name__)


class SyntheticTransactionGenerator:
    """Generate realistic synthetic financial transactions for testing."""

    def __init__(self, seed: int = 42, config: Dict = None):
        """
        Initialize generator with seed for reproducibility.

        Args:
            seed: Random seed
            config: Optional configuration dict, loads from data.yaml if None
        """
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        if config is None:
            data_config = load_config("data")
            self.config = data_config.get("synthetic", {})
        else:
            self.config = config

        # Configuration
        self.fraud_rate = self.config.get("fraud_rate", 0.001)
        self.transaction_types = self.config.get(
            "transaction_types", ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]
        )
        self.amount_ranges = self.config.get(
            "amount_ranges",
            {
                "PAYMENT": [1.0, 10000.0],
                "TRANSFER": [100.0, 500000.0],
                "CASH_OUT": [1.0, 100000.0],
                "DEBIT": [1.0, 5000.0],
                "CASH_IN": [1.0, 50000.0],
            },
        )

        logger.info(f"Initialized SyntheticTransactionGenerator with seed={seed}")

    def generate(self, num_transactions: int = 10000, num_steps: int = 100) -> pl.DataFrame:
        """
        Generate synthetic transaction dataset.

        Args:
            num_transactions: Number of transactions to generate
            num_steps: Number of time steps to distribute transactions across

        Returns:
            Polars DataFrame with synthetic transactions
        """
        logger.info(f"Generating {num_transactions} synthetic transactions over {num_steps} steps")

        # Generate transaction IDs
        transaction_ids = [f"txn_{i:08d}" for i in range(num_transactions)]

        # Generate temporal distribution
        steps = self.rng.integers(1, num_steps + 1, size=num_transactions)

        # Generate transaction types with realistic distribution
        type_weights = [0.4, 0.2, 0.2, 0.1, 0.1]  # PAYMENT, TRANSFER, CASH_OUT, DEBIT, CASH_IN
        types = self.rng.choice(self.transaction_types, size=num_transactions, p=type_weights)

        # Generate amounts based on transaction type
        amounts = np.zeros(num_transactions)
        for i, txn_type in enumerate(types):
            min_amt, max_amt = self.amount_ranges.get(txn_type, [1.0, 10000.0])
            # Log-normal distribution for more realistic amounts
            amounts[i] = self._sample_log_normal(min_amt, max_amt)

        # Generate account identifiers
        num_customers = max(1000, num_transactions // 10)
        num_merchants = max(500, num_transactions // 20)

        name_origs = [f"C{self.rng.integers(1000000, 9999999)}" for _ in range(num_transactions)]

        # Destinations: merchants for PAYMENT/DEBIT, customers for others
        name_dests = []
        for txn_type in types:
            if txn_type in ["PAYMENT", "DEBIT"]:
                name_dests.append(f"M{self.rng.integers(1000000, 9999999)}")
            else:
                name_dests.append(f"C{self.rng.integers(1000000, 9999999)}")

        # Generate realistic balances
        oldbalance_orgs = self.rng.exponential(scale=50000, size=num_transactions)
        newbalance_origs = np.maximum(0, oldbalance_orgs - amounts)

        oldbalance_dests = self.rng.exponential(scale=30000, size=num_transactions)
        newbalance_dests = oldbalance_dests + amounts

        # Generate fraud labels
        is_fraud = self._generate_fraud_labels(
            num_transactions, types, amounts, oldbalance_orgs, newbalance_origs
        )

        # Create DataFrame
        df = pl.DataFrame(
            {
                "transaction_id": transaction_ids,
                "step": steps,
                "type": types,
                "amount": amounts,
                "nameOrig": name_origs,
                "oldbalanceOrg": oldbalance_orgs,
                "newbalanceOrig": newbalance_origs,
                "nameDest": name_dests,
                "oldbalanceDest": oldbalance_dests,
                "newbalanceDest": newbalance_dests,
                "isFraud": is_fraud.astype(int),
            }
        )

        # Sort by step for chronological ordering
        df = df.sort("step")

        fraud_count = df["isFraud"].sum()
        fraud_rate = fraud_count / len(df)
        logger.info(
            f"Generated {num_transactions} transactions: "
            f"{fraud_count} fraudulent ({fraud_rate:.2%})"
        )

        return df

    def _sample_log_normal(self, min_val: float, max_val: float) -> float:
        """Sample from log-normal distribution bounded by min and max."""
        mean = (np.log(min_val) + np.log(max_val)) / 2
        std = (np.log(max_val) - np.log(min_val)) / 4
        value = self.rng.lognormal(mean, std)
        return np.clip(value, min_val, max_val)

    def _generate_fraud_labels(
        self,
        num_transactions: int,
        types: np.ndarray,
        amounts: np.ndarray,
        oldbalance_orgs: np.ndarray,
        newbalance_origs: np.ndarray,
    ) -> np.ndarray:
        """
        Generate fraud labels with realistic patterns.

        Fraud patterns:
        1. High-value transfers (>200k)
        2. Complete balance drain cash-outs
        3. Random fraud at base rate
        """
        is_fraud = np.zeros(num_transactions, dtype=bool)

        # Base random fraud
        num_fraud = int(num_transactions * self.fraud_rate)
        fraud_indices = self.rng.choice(num_transactions, size=num_fraud, replace=False)
        is_fraud[fraud_indices] = True

        # Pattern 1: High-value transfers are often fraud
        high_value_transfers = (types == "TRANSFER") & (amounts > 200000)
        high_value_indices = np.where(high_value_transfers)[0]
        fraud_high_value = self.rng.choice(
            high_value_indices, size=int(len(high_value_indices) * 0.7), replace=False
        )
        is_fraud[fraud_high_value] = True

        # Pattern 2: Complete balance drain cash-outs
        balance_drain = (types == "CASH_OUT") & (
            np.abs(newbalance_origs - 0) < 0.01
        )  # Near zero
        drain_indices = np.where(balance_drain)[0]
        fraud_drain = self.rng.choice(
            drain_indices, size=int(len(drain_indices) * 0.8), replace=False
        )
        is_fraud[fraud_drain] = True

        return is_fraud

    def generate_streaming_batch(
        self, batch_size: int = 100, start_step: int = 1
    ) -> pl.DataFrame:
        """
        Generate a small batch for streaming simulation.

        Args:
            batch_size: Number of transactions in batch
            start_step: Starting time step

        Returns:
            Small batch DataFrame
        """
        return self.generate(num_transactions=batch_size, num_steps=1).with_columns(
            pl.lit(start_step).alias("step")
        )

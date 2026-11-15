"""Tests for synthetic data generation."""

import pytest

from riskstream.data.schemas import SYNTHETIC_SCHEMA
from riskstream.data.synthetic_generator import SyntheticTransactionGenerator


class TestSyntheticTransactionGenerator:
    """Test synthetic transaction generation."""

    def test_generator_initialization(self):
        """Test generator initializes correctly."""
        generator = SyntheticTransactionGenerator(seed=42)
        assert generator.seed == 42
        assert generator.fraud_rate > 0

    def test_generate_basic(self):
        """Test basic generation."""
        generator = SyntheticTransactionGenerator(seed=42)
        df = generator.generate(num_transactions=100, num_steps=10)

        assert len(df) == 100
        assert "transaction_id" in df.columns
        assert "isFraud" in df.columns

    def test_reproducibility(self):
        """Test that same seed produces same data."""
        gen1 = SyntheticTransactionGenerator(seed=42)
        gen2 = SyntheticTransactionGenerator(seed=42)

        df1 = gen1.generate(num_transactions=100)
        df2 = gen2.generate(num_transactions=100)

        assert df1.equals(df2)

    def test_fraud_rate_approximately_correct(self):
        """Test that fraud rate is approximately as configured."""
        generator = SyntheticTransactionGenerator(seed=42)
        generator.fraud_rate = 0.01  # 1% fraud

        df = generator.generate(num_transactions=10000)
        actual_fraud_rate = df["isFraud"].sum() / len(df)

        # Should be within reasonable range (allowing for stochastic variation + patterns)
        assert 0.005 < actual_fraud_rate < 0.05

    def test_transaction_types_valid(self):
        """Test that all transaction types are valid."""
        generator = SyntheticTransactionGenerator(seed=42)
        df = generator.generate(num_transactions=100)

        unique_types = set(df["type"].unique().to_list())
        allowed_types = set(generator.transaction_types)

        assert unique_types.issubset(allowed_types)

    def test_amounts_positive(self):
        """Test that all amounts are positive."""
        generator = SyntheticTransactionGenerator(seed=42)
        df = generator.generate(num_transactions=100)

        assert (df["amount"] > 0).all()

    def test_balances_non_negative(self):
        """Test that balances are non-negative."""
        generator = SyntheticTransactionGenerator(seed=42)
        df = generator.generate(num_transactions=100)

        assert (df["oldbalanceOrg"] >= 0).all()
        assert (df["newbalanceOrig"] >= 0).all()
        assert (df["oldbalanceDest"] >= 0).all()
        assert (df["newbalanceDest"] >= 0).all()

    def test_step_within_range(self):
        """Test that steps are within specified range."""
        generator = SyntheticTransactionGenerator(seed=42)
        num_steps = 50
        df = generator.generate(num_transactions=100, num_steps=num_steps)

        assert df["step"].min() >= 1
        assert df["step"].max() <= num_steps

    def test_chronological_sorting(self):
        """Test that output is sorted chronologically."""
        generator = SyntheticTransactionGenerator(seed=42)
        df = generator.generate(num_transactions=100)

        steps = df["step"].to_list()
        assert steps == sorted(steps)

    def test_streaming_batch(self):
        """Test streaming batch generation."""
        generator = SyntheticTransactionGenerator(seed=42)
        batch = generator.generate_streaming_batch(batch_size=50, start_step=10)

        assert len(batch) == 50
        assert (batch["step"] == 10).all()

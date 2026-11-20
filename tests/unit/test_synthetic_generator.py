import polars as pl
import pytest

from riskstream.data.adapters.paysim import PAYSIM_RAW_SCHEMA
from riskstream.data.synthetic_generator import SyntheticConfig, generate


def test_same_seed_gives_identical_data():
    a = generate(SyntheticConfig(seed=1, num_transactions=2_000))
    b = generate(SyntheticConfig(seed=1, num_transactions=2_000))
    assert a.equals(b)


def test_different_seed_gives_different_data():
    a = generate(SyntheticConfig(seed=1, num_transactions=2_000))
    b = generate(SyntheticConfig(seed=2, num_transactions=2_000))
    assert not a.equals(b)


def test_output_matches_paysim_raw_schema(small_synthetic_raw):
    assert dict(small_synthetic_raw.schema) == PAYSIM_RAW_SCHEMA


def test_steps_sorted_and_in_range():
    df = generate(SyntheticConfig(seed=3, num_transactions=1_000, num_steps=20))
    assert df["step"].is_sorted()
    assert df["step"].min() >= 1 and df["step"].max() <= 20


def test_fraud_only_in_transfer_and_cash_out(small_synthetic_raw):
    fraud_types = set(small_synthetic_raw.filter(pl.col("isFraud") == 1)["type"].unique())
    assert fraud_types <= {"TRANSFER", "CASH_OUT"}
    assert small_synthetic_raw["isFraud"].sum() > 0


def test_amounts_positive_and_balances_non_negative(small_synthetic_raw):
    df = small_synthetic_raw
    assert (df["amount"] > 0).all()
    for col in ("oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"):
        assert (df[col] >= 0).all()


def test_merchant_destinations_only_for_payment_and_debit(small_synthetic_raw):
    merchants = small_synthetic_raw.filter(pl.col("nameDest").str.starts_with("M"))
    assert set(merchants["type"].unique()) <= {"PAYMENT", "DEBIT"}


def test_invalid_sizes_rejected():
    with pytest.raises(ValueError):
        generate(SyntheticConfig(num_transactions=0))

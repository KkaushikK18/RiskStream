"""Shared fixtures. Tests use small synthetic or hand-built data only."""

from __future__ import annotations

import polars as pl
import pytest

from riskstream.data.adapters.paysim import PAYSIM_RAW_SCHEMA, PaySimAdapter
from riskstream.data.synthetic_generator import SyntheticConfig, generate


def raw_rows(rows: list[dict]) -> pl.DataFrame:
    """Build a PaySim-format raw frame from partial row dicts."""
    defaults = {
        "step": 1, "type": "PAYMENT", "amount": 10.0, "nameOrig": "C1",
        "oldbalanceOrg": 100.0, "newbalanceOrig": 90.0, "nameDest": "M1",
        "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0, "isFlaggedFraud": 0,
    }
    return pl.DataFrame([defaults | r for r in rows]).cast(PAYSIM_RAW_SCHEMA)


def to_canonical(df: pl.DataFrame, tmp_path, name: str = "t") -> pl.LazyFrame:
    path = tmp_path / f"{name}.parquet"
    df.write_parquet(path)
    adapter = PaySimAdapter(path, id_prefix="t")
    return adapter.to_canonical(adapter.scan_raw())


@pytest.fixture
def small_synthetic_raw() -> pl.DataFrame:
    return generate(SyntheticConfig(seed=7, num_transactions=3_000, num_steps=48))


@pytest.fixture
def small_canonical(small_synthetic_raw, tmp_path) -> pl.LazyFrame:
    return to_canonical(small_synthetic_raw, tmp_path, "syn")

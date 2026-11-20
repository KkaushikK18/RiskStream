"""PaySim adapter.

Source columns (verified against PS_20174392719_1491204439457_log.csv):
step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest,
oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud.

PaySim has no transaction identifier, so one is derived from the row position
in the source file (``ps_0000000`` ...). The file is ordered by ``step``, and
the row position is kept as ``event_seq`` to give a stable order inside a step.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from riskstream.data.adapters.base import DatasetAdapter
from riskstream.data.schema import CANONICAL_SCHEMA

PAYSIM_RAW_SCHEMA: dict[str, pl.DataType] = {
    "step": pl.Int64(),
    "type": pl.String(),
    "amount": pl.Float64(),
    "nameOrig": pl.String(),
    "oldbalanceOrg": pl.Float64(),
    "newbalanceOrig": pl.Float64(),
    "nameDest": pl.String(),
    "oldbalanceDest": pl.Float64(),
    "newbalanceDest": pl.Float64(),
    "isFraud": pl.Int64(),
    "isFlaggedFraud": pl.Int64(),
}


class PaySimAdapter(DatasetAdapter):
    def __init__(
        self,
        path: Path,
        id_prefix: str = "ps",
        is_synthetic_data: bool = False,
        name: str = "paysim",
    ) -> None:
        self.path = Path(path)
        self.id_prefix = id_prefix
        self.is_synthetic_data = is_synthetic_data
        self.name = name

    @property
    def raw_schema(self) -> dict[str, pl.DataType]:
        return PAYSIM_RAW_SCHEMA

    def scan_raw(self) -> pl.LazyFrame:
        if not self.path.exists():
            raise FileNotFoundError(
                f"Dataset file not found: {self.path}. See README section 'Datasets'."
            )
        if self.path.suffix.lower() == ".csv":
            return pl.scan_csv(self.path, schema_overrides=PAYSIM_RAW_SCHEMA)
        if self.path.suffix.lower() == ".parquet":
            return pl.scan_parquet(self.path)
        raise ValueError(f"Unsupported file type: {self.path.suffix}")

    def to_canonical(self, raw: pl.LazyFrame) -> pl.LazyFrame:
        return (
            raw.with_row_index("event_seq")
            .select(
                pl.format(
                    "{}_{}",
                    pl.lit(self.id_prefix),
                    pl.col("event_seq").cast(pl.String).str.zfill(7),
                ).alias("transaction_id"),
                pl.col("event_seq").cast(pl.Int64),
                pl.col("step").cast(pl.Int32),
                pl.col("type"),
                pl.col("amount"),
                pl.col("nameOrig").alias("orig_id"),
                pl.col("oldbalanceOrg").alias("orig_balance_before"),
                pl.col("newbalanceOrig").alias("orig_balance_after"),
                pl.col("nameDest").alias("dest_id"),
                # PaySim encodes the account kind in the id prefix: C = customer,
                # M = merchant. This is PaySim-specific, so it lives here.
                pl.when(pl.col("nameDest").str.starts_with("M"))
                .then(pl.lit("MERCHANT"))
                .otherwise(pl.lit("CUSTOMER"))
                .alias("dest_type"),
                pl.col("oldbalanceDest").alias("dest_balance_before"),
                pl.col("newbalanceDest").alias("dest_balance_after"),
                pl.col("isFraud").cast(pl.Int8).alias("is_fraud"),
                pl.col("isFlaggedFraud").cast(pl.Int8).alias("is_flagged_fraud"),
            )
            .select(list(CANONICAL_SCHEMA))
        )

"""IEEE-CIS Fraud Detection adapter (Vesta / Kaggle 2019).

Source files (verified against the Kaggle download):

* ``train_transaction.csv``: 590,540 rows, 394 columns. TransactionID, isFraud,
  TransactionDT (seconds from an undisclosed reference; minimum 86400),
  TransactionAmt (USD), ProductCD, card1-6, addr1-2, dist1-2, P/R_emaildomain,
  C1-14 (counts), D1-15 (time deltas, days), M1-9 (match flags), V1-339 (Vesta features).
* ``train_identity.csv``: 144,233 rows, 41 columns. TransactionID, id_01-38,
  DeviceType, DeviceInfo. Only some transactions have identity data.

``test_*.csv`` has no labels and is never read.

IEEE-CIS shares nothing with PaySim's columns, so it has its own schema. The
core columns use the same names as the PaySim canonical schema
(``transaction_id``, ``event_seq``, ``amount``, ``is_fraud``), so the training,
evaluation and policy code work unchanged. Every other column keeps its source
name (``card1``, ``V258``, ...), because those names are what the documentation
and research about this dataset use.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from riskstream.data.adapters.base import DatasetAdapter

SECONDS_PER_DAY = 86_400

TX_STRING_COLUMNS: tuple[str, ...] = (
    "ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain",
    *(f"M{i}" for i in range(1, 10)),
)
ID_STRING_COLUMNS: tuple[str, ...] = (
    "id_12", "id_15", "id_16", "id_23", "id_27", "id_28", "id_29", "id_30", "id_31", "id_33",
    "id_34", "id_35", "id_36", "id_37", "id_38", "DeviceType", "DeviceInfo",
)
TX_NUMERIC_COLUMNS: tuple[str, ...] = (
    "card1", "card2", "card3", "card5", "addr1", "addr2", "dist1", "dist2",
    *(f"C{i}" for i in range(1, 15)), *(f"D{i}" for i in range(1, 16)), *(f"V{i}" for i in range(1, 340)),
)
ID_NUMERIC_COLUMNS: tuple[str, ...] = tuple(
    f"id_{i:02d}" for i in range(1, 39) if f"id_{i:02d}" not in ID_STRING_COLUMNS
)
V_COLUMNS: tuple[str, ...] = tuple(f"V{i}" for i in range(1, 340))
MATCH_FLAG_COLUMNS: tuple[str, ...] = tuple(f"M{i}" for i in range(1, 10) if i != 4)  # T / F

TX_RAW_SCHEMA: dict[str, pl.DataType] = {
    "TransactionID": pl.Int64(), "isFraud": pl.Int64(), "TransactionDT": pl.Int64(),
    "TransactionAmt": pl.Float64(),
    **{c: pl.String() for c in TX_STRING_COLUMNS},
    **{c: pl.Float64() for c in TX_NUMERIC_COLUMNS},
}
ID_RAW_SCHEMA: dict[str, pl.DataType] = {
    "TransactionID": pl.Int64(),
    **{c: pl.String() for c in ID_STRING_COLUMNS},
    **{c: pl.Float64() for c in ID_NUMERIC_COLUMNS},
}

# Core columns, shared in name and meaning with the PaySim canonical schema.
CORE_SCHEMA: dict[str, pl.DataType] = {
    "transaction_id": pl.String(),    # "ieee_<TransactionID>"
    "event_seq": pl.Int64(),          # TransactionID; increases with time, stable tie-breaker
    "event_time_s": pl.Int64(),       # TransactionDT
    "day": pl.Int32(),                # (TransactionDT - 86400) // 86400: 0..181
    "amount": pl.Float64(),           # TransactionAmt, kept in float64 so the cents are exact
    "is_fraud": pl.Int8(),
    "has_identity": pl.Int8(),        # 1 if the transaction has a train_identity row
}
# Everything else keeps the source name: numerics as float32 (exact for the integer
# ids such as card1 <= 18396; ~7 significant digits for the rest), strings as is.
ORDER_COLUMNS: tuple[str, str] = ("event_time_s", "event_seq")


def canonical_schema() -> dict[str, pl.DataType]:
    schema = dict(CORE_SCHEMA)
    for c in (*TX_NUMERIC_COLUMNS, *ID_NUMERIC_COLUMNS):
        schema[c] = pl.Float32()
    for c in (*TX_STRING_COLUMNS, *ID_STRING_COLUMNS):
        schema[c] = pl.String()
    return schema


class IEEECISAdapter(DatasetAdapter):
    name = "ieee_cis"
    is_synthetic_data = False

    def __init__(self, path: Path, identity_path: Path) -> None:
        self.path = Path(path)
        self.identity_path = Path(identity_path)

    @property
    def raw_schema(self) -> dict[str, pl.DataType]:
        return TX_RAW_SCHEMA

    def _scan(self, path: Path, schema: dict[str, pl.DataType]) -> pl.LazyFrame:
        if not path.exists():
            raise FileNotFoundError(f"Dataset file not found: {path}. See README section 'Datasets'.")
        if path.suffix.lower() == ".parquet":
            return pl.scan_parquet(path)
        # Explicit types: inference from the first rows could guess Int64 for a column
        # that has decimals further down.
        return pl.scan_csv(path, schema_overrides=schema, infer_schema_length=0)

    def scan_raw(self) -> pl.LazyFrame:
        return self._scan(self.path, TX_RAW_SCHEMA)

    def scan_identity(self) -> pl.LazyFrame:
        return self._scan(self.identity_path, ID_RAW_SCHEMA)

    def check_identity_columns(self) -> list[str]:
        actual = self.scan_identity().collect_schema()
        problems = []
        missing = [c for c in ID_RAW_SCHEMA if c not in actual]
        if missing:
            problems.append(f"missing identity columns: {missing}")
        extra = [c for c in actual if c not in ID_RAW_SCHEMA]
        if extra:
            problems.append(f"unexpected identity columns: {extra}")
        return problems

    def to_canonical(self, raw: pl.LazyFrame, identity: pl.LazyFrame | None = None) -> pl.LazyFrame:
        identity = identity if identity is not None else self.scan_identity()
        ident = identity.with_columns(pl.lit(1, dtype=pl.Int8).alias("has_identity"))
        schema = canonical_schema()
        # maintain_order: Polars does not keep the left row order by default, and the
        # file's time order is relied on (and validated) downstream.
        joined = raw.join(ident, on="TransactionID", how="left", maintain_order="left")
        return joined.select(
            pl.format("ieee_{}", pl.col("TransactionID")).alias("transaction_id"),
            pl.col("TransactionID").cast(pl.Int64).alias("event_seq"),
            pl.col("TransactionDT").cast(pl.Int64).alias("event_time_s"),
            ((pl.col("TransactionDT") - SECONDS_PER_DAY) // SECONDS_PER_DAY).cast(pl.Int32).alias("day"),
            pl.col("TransactionAmt").cast(pl.Float64).alias("amount"),
            pl.col("isFraud").cast(pl.Int8).alias("is_fraud"),
            pl.col("has_identity").fill_null(0).cast(pl.Int8),
            *[pl.col(c).cast(schema[c]) for c in (*TX_NUMERIC_COLUMNS, *ID_NUMERIC_COLUMNS)],
            *[pl.col(c) for c in (*TX_STRING_COLUMNS, *ID_STRING_COLUMNS)],
        ).select(list(schema))

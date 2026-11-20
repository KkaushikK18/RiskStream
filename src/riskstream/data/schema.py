"""Canonical transaction schema shared by every dataset adapter.

Dataset adapters translate source-specific columns into these names, so the
validation, EDA, splitting and feature code never depend on one dataset's
column naming.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

TRANSACTION_TYPES: tuple[str, ...] = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
DEST_TYPES: tuple[str, ...] = ("CUSTOMER", "MERCHANT")


@dataclass(frozen=True)
class CanonicalColumn:
    name: str
    dtype: pl.DataType
    description: str
    # Whether the value is known when a transaction must be scored, i.e. before
    # it is executed. Post-transaction values exist in the historical file but
    # would not exist in a live authorization request.
    available_at_inference: bool
    nullable: bool = False


CANONICAL_COLUMNS: tuple[CanonicalColumn, ...] = (
    CanonicalColumn("transaction_id", pl.String(), "Unique transaction identifier", True),
    CanonicalColumn(
        "event_seq",
        pl.Int64(),
        "0-based position in the source file; stable ordering key, never a model feature",
        True,
    ),
    CanonicalColumn("step", pl.Int32(), "Simulated hour index (1 step = 1 hour)", True),
    CanonicalColumn("type", pl.String(), "Transaction type", True),
    CanonicalColumn("amount", pl.Float64(), "Transaction amount (simulated currency units)", True),
    CanonicalColumn("orig_id", pl.String(), "Origin account identifier", True),
    CanonicalColumn(
        "orig_balance_before", pl.Float64(), "Origin balance before the transaction", True
    ),
    CanonicalColumn(
        "orig_balance_after", pl.Float64(), "Origin balance after the transaction (post-event)", False
    ),
    CanonicalColumn("dest_id", pl.String(), "Destination account identifier", True),
    CanonicalColumn("dest_type", pl.String(), "CUSTOMER or MERCHANT", True),
    CanonicalColumn(
        "dest_balance_before",
        pl.Float64(),
        "Destination balance before (other institution's data; not available live)",
        False,
    ),
    CanonicalColumn(
        "dest_balance_after", pl.Float64(), "Destination balance after (post-event)", False
    ),
    CanonicalColumn("is_fraud", pl.Int8(), "Target label (1 = fraud)", False),
    CanonicalColumn(
        "is_flagged_fraud",
        pl.Int8(),
        "Output of the simulator's legacy rule; excluded from features (rule leakage)",
        False,
    ),
)

CANONICAL_SCHEMA: dict[str, pl.DataType] = {c.name: c.dtype for c in CANONICAL_COLUMNS}
TARGET_COLUMN = "is_fraud"
TEMPORAL_COLUMN = "step"
ID_COLUMN = "transaction_id"
ORDER_COLUMNS: tuple[str, str] = ("step", "event_seq")

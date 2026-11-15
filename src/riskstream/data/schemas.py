"""Data schemas and validation rules for RiskStream datasets."""

from dataclasses import dataclass
from typing import Dict, List, Optional

import polars as pl


@dataclass
class ColumnSchema:
    """Schema definition for a single column."""

    name: str
    dtype: pl.DataType
    nullable: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[str]] = None


@dataclass
class DatasetSchema:
    """Complete schema definition for a dataset."""

    name: str
    columns: List[ColumnSchema]
    primary_key: Optional[str] = None
    target_column: Optional[str] = None
    temporal_column: Optional[str] = None

    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]

    def get_dtype_dict(self) -> Dict[str, pl.DataType]:
        """Get dictionary mapping column names to data types."""
        return {col.name: col.dtype for col in self.columns}


# PaySim dataset schema
PAYSIM_SCHEMA = DatasetSchema(
    name="paysim",
    columns=[
        ColumnSchema(name="step", dtype=pl.Int64, min_value=1),
        ColumnSchema(
            name="type",
            dtype=pl.Utf8,
            allowed_values=["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"],
        ),
        ColumnSchema(name="amount", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="nameOrig", dtype=pl.Utf8),
        ColumnSchema(name="oldbalanceOrg", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="newbalanceOrig", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="nameDest", dtype=pl.Utf8),
        ColumnSchema(name="oldbalanceDest", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="newbalanceDest", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="isFraud", dtype=pl.Int64, allowed_values=["0", "1"]),
        ColumnSchema(name="isFlaggedFraud", dtype=pl.Int64, allowed_values=["0", "1"]),
    ],
    target_column="isFraud",
    temporal_column="step",
)


# Synthetic transaction schema
SYNTHETIC_SCHEMA = DatasetSchema(
    name="synthetic",
    columns=[
        ColumnSchema(name="transaction_id", dtype=pl.Utf8),
        ColumnSchema(name="step", dtype=pl.Int64, min_value=1),
        ColumnSchema(
            name="type",
            dtype=pl.Utf8,
            allowed_values=["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"],
        ),
        ColumnSchema(name="amount", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="nameOrig", dtype=pl.Utf8),
        ColumnSchema(name="oldbalanceOrg", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="newbalanceOrig", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="nameDest", dtype=pl.Utf8),
        ColumnSchema(name="oldbalanceDest", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="newbalanceDest", dtype=pl.Float64, min_value=0.0),
        ColumnSchema(name="isFraud", dtype=pl.Int64, allowed_values=["0", "1"]),
    ],
    primary_key="transaction_id",
    target_column="isFraud",
    temporal_column="step",
)

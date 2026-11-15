"""Data validation utilities."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl

from riskstream.data.schemas import ColumnSchema, DatasetSchema
from riskstream.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of data validation."""

    is_valid: bool
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, any]

    def __str__(self) -> str:
        """String representation of validation result."""
        status = "PASSED" if self.is_valid else "FAILED"
        return (
            f"Validation {status}\n"
            f"Errors: {len(self.errors)}\n"
            f"Warnings: {len(self.warnings)}\n"
            f"Metrics: {self.metrics}"
        )


class DataValidator:
    """Validator for dataset compliance with schema."""

    def __init__(self, schema: DatasetSchema):
        """
        Initialize validator with a schema.

        Args:
            schema: Dataset schema to validate against
        """
        self.schema = schema

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """
        Validate a dataframe against the schema.

        Args:
            df: Polars DataFrame to validate

        Returns:
            ValidationResult containing errors, warnings and metrics
        """
        errors: List[str] = []
        warnings: List[str] = []
        metrics: Dict[str, any] = {
            "num_rows": len(df),
            "num_columns": len(df.columns),
        }

        logger.info(f"Validating dataset: {self.schema.name}")
        logger.info(f"Shape: {len(df)} rows, {len(df.columns)} columns")

        # Check for missing columns
        expected_cols = set(self.schema.get_column_names())
        actual_cols = set(df.columns)
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols

        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        if extra_cols:
            warnings.append(f"Extra columns found: {extra_cols}")

        # Validate each column
        for col_schema in self.schema.columns:
            if col_schema.name not in df.columns:
                continue  # Already reported as missing

            col_errors, col_warnings, col_metrics = self._validate_column(df, col_schema)
            errors.extend(col_errors)
            warnings.extend(col_warnings)
            metrics[col_schema.name] = col_metrics

        # Check for duplicates if primary key exists
        if self.schema.primary_key and self.schema.primary_key in df.columns:
            num_duplicates = df.select(self.schema.primary_key).n_unique()
            if num_duplicates < len(df):
                dup_count = len(df) - num_duplicates
                errors.append(
                    f"Found {dup_count} duplicate values in primary key '{self.schema.primary_key}'"
                )
                metrics["duplicate_primary_keys"] = dup_count

        # Target distribution analysis
        if self.schema.target_column and self.schema.target_column in df.columns:
            target_dist = df.select(self.schema.target_column).to_series().value_counts()
            metrics["target_distribution"] = target_dist.to_dict()

            # Check class balance
            if len(target_dist) == 2:
                fraud_count = target_dist.filter(pl.col(self.schema.target_column) == 1)
                if len(fraud_count) > 0:
                    fraud_rate = fraud_count["counts"][0] / len(df)
                    metrics["fraud_rate"] = fraud_rate
                    logger.info(f"Fraud rate: {fraud_rate:.4%}")

        is_valid = len(errors) == 0

        return ValidationResult(
            is_valid=is_valid, errors=errors, warnings=warnings, metrics=metrics
        )

    def _validate_column(
        self, df: pl.DataFrame, col_schema: ColumnSchema
    ) -> tuple[List[str], List[str], Dict]:
        """Validate a single column."""
        errors = []
        warnings = []
        metrics = {}

        col = df.select(col_schema.name).to_series()

        # Missing values
        null_count = col.null_count()
        null_rate = null_count / len(col) if len(col) > 0 else 0
        metrics["null_count"] = null_count
        metrics["null_rate"] = null_rate

        if null_count > 0 and not col_schema.nullable:
            warnings.append(
                f"Column '{col_schema.name}' has {null_count} null values ({null_rate:.2%})"
            )

        # Data type check
        if col.dtype != col_schema.dtype:
            warnings.append(
                f"Column '{col_schema.name}' has dtype {col.dtype}, expected {col_schema.dtype}"
            )

        # Numeric range validation
        if col_schema.min_value is not None or col_schema.max_value is not None:
            if col.dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
                col_min = col.min()
                col_max = col.max()
                metrics["min"] = col_min
                metrics["max"] = col_max

                if col_schema.min_value is not None and col_min < col_schema.min_value:
                    errors.append(
                        f"Column '{col_schema.name}' has values below minimum: "
                        f"{col_min} < {col_schema.min_value}"
                    )

                if col_schema.max_value is not None and col_max > col_schema.max_value:
                    warnings.append(
                        f"Column '{col_schema.name}' has values above maximum: "
                        f"{col_max} > {col_schema.max_value}"
                    )

        # Categorical validation
        if col_schema.allowed_values:
            unique_values = set(col.drop_nulls().unique().to_list())
            allowed_set = set(col_schema.allowed_values)
            invalid_values = unique_values - allowed_set

            metrics["unique_values"] = len(unique_values)
            metrics["value_distribution"] = col.value_counts().to_dict()

            if invalid_values:
                errors.append(
                    f"Column '{col_schema.name}' has invalid values: {invalid_values}"
                )

        return errors, warnings, metrics


def validate_file_exists(file_path: Path) -> None:
    """
    Check if a file exists and raise error if not.

    Args:
        file_path: Path to check

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    logger.info(f"File found: {file_path}")


def save_validation_report(result: ValidationResult, output_path: Path) -> None:
    """
    Save validation results to a file.

    Args:
        result: ValidationResult to save
        output_path: Path to save the report
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write(str(result))

    logger.info(f"Validation report saved to: {output_path}")

"""Tests for data validation."""

import polars as pl
import pytest

from riskstream.data.schemas import PAYSIM_SCHEMA, ColumnSchema, DatasetSchema
from riskstream.data.validation import DataValidator


class TestDataValidator:
    """Test data validation functionality."""

    def test_validator_initialization(self):
        """Test validator initializes with schema."""
        validator = DataValidator(PAYSIM_SCHEMA)
        assert validator.schema == PAYSIM_SCHEMA

    def test_valid_dataframe_passes(self):
        """Test that valid data passes validation."""
        df = pl.DataFrame(
            {
                "step": [1, 2, 3],
                "type": ["PAYMENT", "TRANSFER", "CASH_OUT"],
                "amount": [100.0, 200.0, 300.0],
                "nameOrig": ["C1", "C2", "C3"],
                "oldbalanceOrg": [1000.0, 2000.0, 3000.0],
                "newbalanceOrig": [900.0, 1800.0, 2700.0],
                "nameDest": ["M1", "M2", "M3"],
                "oldbalanceDest": [0.0, 0.0, 0.0],
                "newbalanceDest": [100.0, 200.0, 300.0],
                "isFraud": [0, 0, 1],
                "isFlaggedFraud": [0, 0, 0],
            }
        )

        validator = DataValidator(PAYSIM_SCHEMA)
        result = validator.validate(df)

        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_required_column_fails(self):
        """Test that missing required columns cause errors."""
        df = pl.DataFrame(
            {
                "step": [1, 2, 3],
                "amount": [100.0, 200.0, 300.0],
                # Missing other required columns
            }
        )

        validator = DataValidator(PAYSIM_SCHEMA)
        result = validator.validate(df)

        assert not result.is_valid
        assert len(result.errors) > 0

    def test_invalid_categorical_value_fails(self):
        """Test that invalid categorical values cause errors."""
        df = pl.DataFrame(
            {
                "step": [1, 2, 3],
                "type": ["PAYMENT", "INVALID_TYPE", "CASH_OUT"],
                "amount": [100.0, 200.0, 300.0],
                "nameOrig": ["C1", "C2", "C3"],
                "oldbalanceOrg": [1000.0, 2000.0, 3000.0],
                "newbalanceOrig": [900.0, 1800.0, 2700.0],
                "nameDest": ["M1", "M2", "M3"],
                "oldbalanceDest": [0.0, 0.0, 0.0],
                "newbalanceDest": [100.0, 200.0, 300.0],
                "isFraud": [0, 0, 1],
                "isFlaggedFraud": [0, 0, 0],
            }
        )

        validator = DataValidator(PAYSIM_SCHEMA)
        result = validator.validate(df)

        assert not result.is_valid
        assert any("invalid values" in error.lower() for error in result.errors)

    def test_null_values_detected(self):
        """Test that null values are detected."""
        df = pl.DataFrame(
            {
                "step": [1, None, 3],
                "type": ["PAYMENT", "TRANSFER", "CASH_OUT"],
                "amount": [100.0, 200.0, 300.0],
                "nameOrig": ["C1", "C2", "C3"],
                "oldbalanceOrg": [1000.0, 2000.0, 3000.0],
                "newbalanceOrig": [900.0, 1800.0, 2700.0],
                "nameDest": ["M1", "M2", "M3"],
                "oldbalanceDest": [0.0, 0.0, 0.0],
                "newbalanceDest": [100.0, 200.0, 300.0],
                "isFraud": [0, 0, 1],
                "isFlaggedFraud": [0, 0, 0],
            }
        )

        validator = DataValidator(PAYSIM_SCHEMA)
        result = validator.validate(df)

        # Should have warnings about null values
        assert len(result.warnings) > 0

    def test_target_distribution_calculated(self):
        """Test that target distribution is calculated."""
        df = pl.DataFrame(
            {
                "step": [1, 2, 3, 4],
                "type": ["PAYMENT", "TRANSFER", "CASH_OUT", "PAYMENT"],
                "amount": [100.0, 200.0, 300.0, 400.0],
                "nameOrig": ["C1", "C2", "C3", "C4"],
                "oldbalanceOrg": [1000.0, 2000.0, 3000.0, 4000.0],
                "newbalanceOrig": [900.0, 1800.0, 2700.0, 3600.0],
                "nameDest": ["M1", "M2", "M3", "M4"],
                "oldbalanceDest": [0.0, 0.0, 0.0, 0.0],
                "newbalanceDest": [100.0, 200.0, 300.0, 400.0],
                "isFraud": [0, 0, 1, 0],
                "isFlaggedFraud": [0, 0, 0, 0],
            }
        )

        validator = DataValidator(PAYSIM_SCHEMA)
        result = validator.validate(df)

        assert "fraud_rate" in result.metrics
        assert result.metrics["fraud_rate"] == 0.25  # 1 out of 4

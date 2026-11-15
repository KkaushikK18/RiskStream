"""Tests for data preprocessing and splitting."""

import polars as pl
import pytest

from riskstream.data.preprocessing import ChronologicalSplitter, create_processed_dataset


class TestChronologicalSplitter:
    """Test chronological data splitting."""

    def test_splitter_initialization(self):
        """Test splitter initializes correctly."""
        splitter = ChronologicalSplitter(train_end_step=100, val_end_step=150)
        assert splitter.train_end_step == 100
        assert splitter.val_end_step == 150

    def test_split_creates_three_sets(self):
        """Test that split creates train, val, and test sets."""
        df = pl.DataFrame(
            {
                "step": list(range(1, 201)),
                "amount": [100.0] * 200,
                "isFraud": [0] * 200,
            }
        )

        splitter = ChronologicalSplitter(train_end_step=100, val_end_step=150)
        train_df, val_df, test_df, split_info = splitter.split(df)

        assert len(train_df) == 100
        assert len(val_df) == 50
        assert len(test_df) == 50

    def test_split_maintains_chronological_order(self):
        """Test that splits maintain chronological boundaries."""
        df = pl.DataFrame(
            {
                "step": list(range(1, 201)),
                "amount": [100.0] * 200,
                "isFraud": [0] * 200,
            }
        )

        splitter = ChronologicalSplitter(train_end_step=100, val_end_step=150)
        train_df, val_df, test_df, _ = splitter.split(df)

        # Train should have steps 1-100
        assert train_df["step"].min() == 1
        assert train_df["step"].max() == 100

        # Val should have steps 101-150
        assert val_df["step"].min() == 101
        assert val_df["step"].max() == 150

        # Test should have steps 151-200
        assert test_df["step"].min() == 151
        assert test_df["step"].max() == 200

    def test_split_no_temporal_leakage(self):
        """Test that there's no temporal leakage between splits."""
        df = pl.DataFrame(
            {
                "step": list(range(1, 201)),
                "amount": [100.0] * 200,
                "isFraud": [0] * 200,
            }
        )

        splitter = ChronologicalSplitter(train_end_step=100, val_end_step=150)
        train_df, val_df, test_df, _ = splitter.split(df)

        # Max train step should be less than min val step
        assert train_df["step"].max() < val_df["step"].min()

        # Max val step should be less than min test step
        assert val_df["step"].max() < test_df["step"].min()

    def test_split_info_contains_metadata(self):
        """Test that split_info contains correct metadata."""
        df = pl.DataFrame(
            {
                "step": list(range(1, 201)),
                "amount": [100.0] * 200,
                "isFraud": [0] * 180 + [1] * 20,
            }
        )

        splitter = ChronologicalSplitter(train_end_step=100, val_end_step=150)
        _, _, _, split_info = splitter.split(df)

        assert "total_rows" in split_info
        assert "train" in split_info
        assert "val" in split_info
        assert "test" in split_info

        assert split_info["total_rows"] == 200
        assert "fraud_count" in split_info["train"]
        assert "fraud_rate" in split_info["train"]


class TestCreateProcessedDataset:
    """Test basic preprocessing."""

    def test_removes_duplicates(self):
        """Test that exact duplicates are removed."""
        df = pl.DataFrame(
            {
                "step": [1, 1, 2, 3],  # Duplicate row
                "amount": [100.0, 100.0, 200.0, 300.0],
                "isFraud": [0, 0, 1, 0],
            }
        )

        processed_df = create_processed_dataset(df)
        assert len(processed_df) == 3  # One duplicate removed

    def test_sorts_by_step(self):
        """Test that data is sorted chronologically."""
        df = pl.DataFrame(
            {
                "step": [3, 1, 2],
                "amount": [300.0, 100.0, 200.0],
                "isFraud": [0, 1, 0],
            }
        )

        processed_df = create_processed_dataset(df)
        steps = processed_df["step"].to_list()
        assert steps == [1, 2, 3]

    def test_preserves_all_columns(self):
        """Test that all columns are preserved."""
        df = pl.DataFrame(
            {
                "step": [1, 2, 3],
                "amount": [100.0, 200.0, 300.0],
                "isFraud": [0, 1, 0],
                "extra_col": ["a", "b", "c"],
            }
        )

        processed_df = create_processed_dataset(df)
        assert set(processed_df.columns) == set(df.columns)

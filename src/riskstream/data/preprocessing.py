"""Data preprocessing and splitting utilities."""

from pathlib import Path
from typing import Dict, Tuple

import polars as pl

from riskstream.config import load_config, settings
from riskstream.logging_config import get_logger

logger = get_logger(__name__)


class ChronologicalSplitter:
    """Split data chronologically to prevent temporal leakage."""

    def __init__(
        self,
        train_end_step: int = None,
        val_end_step: int = None,
        temporal_column: str = "step",
    ):
        """
        Initialize chronological splitter.

        Args:
            train_end_step: Last step included in training set
            val_end_step: Last step included in validation set
            temporal_column: Name of temporal column
        """
        self.train_end_step = train_end_step or settings.train_end_step
        self.val_end_step = val_end_step or settings.val_end_step
        self.temporal_column = temporal_column

        logger.info(
            f"Initialized ChronologicalSplitter: "
            f"train=[1-{self.train_end_step}], "
            f"val=[{self.train_end_step+1}-{self.val_end_step}], "
            f"test=[{self.val_end_step+1}+]"
        )

    def split(
        self, df: pl.DataFrame
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, Dict[str, any]]:
        """
        Split DataFrame chronologically into train/val/test.

        Args:
            df: DataFrame to split

        Returns:
            Tuple of (train_df, val_df, test_df, split_info)
        """
        if self.temporal_column not in df.columns:
            raise ValueError(f"Temporal column '{self.temporal_column}' not found in DataFrame")

        logger.info(f"Splitting data chronologically on column '{self.temporal_column}'")

        # Sort by temporal column
        df = df.sort(self.temporal_column)

        # Split
        train_df = df.filter(pl.col(self.temporal_column) <= self.train_end_step)
        val_df = df.filter(
            (pl.col(self.temporal_column) > self.train_end_step)
            & (pl.col(self.temporal_column) <= self.val_end_step)
        )
        test_df = df.filter(pl.col(self.temporal_column) > self.val_end_step)

        # Calculate split info
        split_info = {
            "total_rows": len(df),
            "train": {
                "rows": len(train_df),
                "percentage": len(train_df) / len(df) * 100,
                "step_range": [int(train_df[self.temporal_column].min()), self.train_end_step],
                "fraud_count": int(train_df["isFraud"].sum()) if "isFraud" in train_df.columns else None,
                "fraud_rate": float(train_df["isFraud"].mean()) if "isFraud" in train_df.columns else None,
            },
            "val": {
                "rows": len(val_df),
                "percentage": len(val_df) / len(df) * 100,
                "step_range": [self.train_end_step + 1, self.val_end_step],
                "fraud_count": int(val_df["isFraud"].sum()) if "isFraud" in val_df.columns else None,
                "fraud_rate": float(val_df["isFraud"].mean()) if "isFraud" in val_df.columns else None,
            },
            "test": {
                "rows": len(test_df),
                "percentage": len(test_df) / len(df) * 100,
                "step_range": [
                    self.val_end_step + 1,
                    int(test_df[self.temporal_column].max()) if len(test_df) > 0 else self.val_end_step + 1,
                ],
                "fraud_count": int(test_df["isFraud"].sum()) if "isFraud" in test_df.columns else None,
                "fraud_rate": float(test_df["isFraud"].mean()) if "isFraud" in test_df.columns else None,
            },
        }

        # Log split info
        logger.info(f"Split completed:")
        logger.info(f"  Train: {split_info['train']['rows']} rows ({split_info['train']['percentage']:.1f}%)")
        logger.info(f"  Val:   {split_info['val']['rows']} rows ({split_info['val']['percentage']:.1f}%)")
        logger.info(f"  Test:  {split_info['test']['rows']} rows ({split_info['test']['percentage']:.1f}%)")

        if "isFraud" in df.columns:
            logger.info(f"  Train fraud rate: {split_info['train']['fraud_rate']:.4%}")
            logger.info(f"  Val fraud rate:   {split_info['val']['fraud_rate']:.4%}")
            logger.info(f"  Test fraud rate:  {split_info['test']['fraud_rate']:.4%}")

        return train_df, val_df, test_df, split_info

    def save_splits(
        self,
        train_df: pl.DataFrame,
        val_df: pl.DataFrame,
        test_df: pl.DataFrame,
        output_dir: Path = None,
        dataset_name: str = "paysim",
    ) -> None:
        """
        Save train/val/test splits to Parquet files.

        Args:
            train_df: Training DataFrame
            val_df: Validation DataFrame
            test_df: Test DataFrame
            output_dir: Output directory for splits
            dataset_name: Name for file prefixing
        """
        if output_dir is None:
            output_dir = settings.data_processed_dir

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save splits
        train_path = output_dir / f"{dataset_name}_train.parquet"
        val_path = output_dir / f"{dataset_name}_val.parquet"
        test_path = output_dir / f"{dataset_name}_test.parquet"

        train_df.write_parquet(train_path, compression="snappy")
        val_df.write_parquet(val_path, compression="snappy")
        test_df.write_parquet(test_path, compression="snappy")

        logger.info(f"Saved train split to: {train_path}")
        logger.info(f"Saved val split to: {val_path}")
        logger.info(f"Saved test split to: {test_path}")


def load_splits(
    dataset_name: str = "paysim", data_dir: Path = None
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Load previously saved train/val/test splits.

    Args:
        dataset_name: Name of dataset
        data_dir: Directory containing splits

    Returns:
        Tuple of (train_df, val_df, test_df)
    """
    if data_dir is None:
        data_dir = settings.data_processed_dir

    data_dir = Path(data_dir)

    train_path = data_dir / f"{dataset_name}_train.parquet"
    val_path = data_dir / f"{dataset_name}_val.parquet"
    test_path = data_dir / f"{dataset_name}_test.parquet"

    logger.info(f"Loading splits from {data_dir}")

    train_df = pl.read_parquet(train_path)
    val_df = pl.read_parquet(val_path)
    test_df = pl.read_parquet(test_path)

    logger.info(f"Loaded: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")

    return train_df, val_df, test_df


def create_processed_dataset(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply basic preprocessing to create processed dataset.

    Phase 1: Minimal preprocessing, just cleaning.
    Advanced feature engineering will be in Phase 2.

    Args:
        df: Raw validated DataFrame

    Returns:
        Processed DataFrame
    """
    logger.info("Applying basic preprocessing")

    # Make a copy
    processed_df = df.clone()

    # Remove any exact duplicates
    initial_rows = len(processed_df)
    processed_df = processed_df.unique()
    removed_duplicates = initial_rows - len(processed_df)
    if removed_duplicates > 0:
        logger.info(f"Removed {removed_duplicates} duplicate rows")

    # Sort by step for chronological consistency
    if "step" in processed_df.columns:
        processed_df = processed_df.sort("step")

    logger.info(f"Preprocessing complete: {len(processed_df)} rows")

    return processed_df

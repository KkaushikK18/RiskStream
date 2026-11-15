"""Data ingestion for various datasets."""

from pathlib import Path
from typing import Optional

import polars as pl

from riskstream.config import load_config, settings
from riskstream.data.schemas import PAYSIM_SCHEMA, SYNTHETIC_SCHEMA
from riskstream.data.validation import DataValidator, validate_file_exists
from riskstream.logging_config import get_logger

logger = get_logger(__name__)


class PaySimIngestion:
    """Ingest and validate PaySim dataset."""

    def __init__(self):
        """Initialize PaySim ingestion."""
        self.schema = PAYSIM_SCHEMA
        self.validator = DataValidator(self.schema)
        self.config = load_config("data")["paysim"]

    def load_raw(self, file_path: Optional[Path] = None) -> pl.DataFrame:
        """
        Load raw PaySim CSV file.

        Args:
            file_path: Path to CSV file, uses config default if None

        Returns:
            Polars DataFrame with raw data
        """
        if file_path is None:
            file_path = settings.paysim_path

        logger.info(f"Loading PaySim dataset from: {file_path}")
        validate_file_exists(file_path)

        # Load with Polars lazy API for efficiency
        df = pl.read_csv(
            file_path,
            infer_schema_length=10000,  # Sample for schema inference
        )

        logger.info(f"Loaded {len(df)} transactions with {len(df.columns)} columns")
        return df

    def validate_and_save(
        self, df: pl.DataFrame, output_path: Optional[Path] = None
    ) -> pl.DataFrame:
        """
        Validate data and save to Parquet.

        Args:
            df: DataFrame to validate
            output_path: Output path for validated Parquet file

        Returns:
            Validated DataFrame

        Raises:
            ValueError: If validation fails
        """
        if output_path is None:
            output_path = Path(self.config["interim_path"])

        logger.info("Validating PaySim dataset...")
        validation_result = self.validator.validate(df)

        logger.info(f"Validation result:\n{validation_result}")

        if not validation_result.is_valid:
            error_msg = f"Validation failed with {len(validation_result.errors)} errors"
            logger.error(error_msg)
            for error in validation_result.errors:
                logger.error(f"  - {error}")
            raise ValueError(error_msg)

        if validation_result.warnings:
            logger.warning(f"Found {len(validation_result.warnings)} warnings:")
            for warning in validation_result.warnings:
                logger.warning(f"  - {warning}")

        # Save validated data to Parquet
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path, compression="snappy")
        logger.info(f"Saved validated data to: {output_path}")

        # Save validation report
        report_path = Path("reports/eda/paysim_validation.txt")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            f.write(str(validation_result))
        logger.info(f"Saved validation report to: {report_path}")

        return df

    def load_validated(self, file_path: Optional[Path] = None) -> pl.DataFrame:
        """
        Load previously validated Parquet file.

        Args:
            file_path: Path to Parquet file

        Returns:
            Validated DataFrame
        """
        if file_path is None:
            file_path = Path(self.config["interim_path"])

        logger.info(f"Loading validated data from: {file_path}")
        validate_file_exists(file_path)

        df = pl.read_parquet(file_path)
        logger.info(f"Loaded {len(df)} validated transactions")
        return df

    def ingest(self, force_reload: bool = False) -> pl.DataFrame:
        """
        Complete ingestion pipeline: load, validate, save.

        Args:
            force_reload: If True, reload from CSV even if validated file exists

        Returns:
            Validated DataFrame
        """
        interim_path = Path(self.config["interim_path"])

        if not force_reload and interim_path.exists():
            logger.info("Loading existing validated data")
            return self.load_validated(interim_path)

        logger.info("Running full ingestion pipeline")
        df = self.load_raw()
        df = self.validate_and_save(df)
        return df


class SyntheticDataIngestion:
    """Handle synthetic data loading and validation."""

    def __init__(self):
        """Initialize synthetic data ingestion."""
        self.schema = SYNTHETIC_SCHEMA
        self.validator = DataValidator(self.schema)

    def load(self, file_path: Path) -> pl.DataFrame:
        """
        Load synthetic data from Parquet.

        Args:
            file_path: Path to synthetic data file

        Returns:
            DataFrame with synthetic transactions
        """
        logger.info(f"Loading synthetic data from: {file_path}")
        validate_file_exists(file_path)

        df = pl.read_parquet(file_path)
        logger.info(f"Loaded {len(df)} synthetic transactions")

        # Validate
        validation_result = self.validator.validate(df)
        if not validation_result.is_valid:
            logger.error(f"Synthetic data validation failed: {validation_result.errors}")
            raise ValueError("Synthetic data validation failed")

        return df


def load_dataset(dataset_name: str = "paysim", force_reload: bool = False) -> pl.DataFrame:
    """
    Convenience function to load any dataset.

    Args:
        dataset_name: Name of dataset ('paysim' or 'synthetic')
        force_reload: Force reload from source

    Returns:
        Loaded and validated DataFrame
    """
    if dataset_name == "paysim":
        ingestion = PaySimIngestion()
        return ingestion.ingest(force_reload=force_reload)
    elif dataset_name == "synthetic":
        config = load_config("data")["synthetic"]
        file_path = Path(config["output_path"])
        ingestion = SyntheticDataIngestion()
        return ingestion.load(file_path)
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")

"""
Data preparation script for RiskStream.

This script orchestrates the complete data pipeline:
1. Load and validate PaySim dataset
2. Run EDA and generate reports
3. Split data chronologically
4. Save processed datasets
"""

import argparse
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from riskstream.config import ensure_directories, settings
from riskstream.data.eda import EDAReport
from riskstream.data.ingestion import PaySimIngestion
from riskstream.data.preprocessing import ChronologicalSplitter, create_processed_dataset
from riskstream.logging_config import get_logger, setup_logging


def main():
    """Run data preparation pipeline."""
    parser = argparse.ArgumentParser(description="Prepare data for RiskStream")
    parser.add_argument(
        "--force-reload", action="store_true", help="Force reload from raw CSV"
    )
    parser.add_argument("--skip-eda", action="store_true", help="Skip EDA report generation")
    args = parser.parse_args()

    # Setup
    setup_logging()
    logger = get_logger(__name__)
    ensure_directories()

    logger.info("=" * 80)
    logger.info("RiskStream Data Preparation Pipeline")
    logger.info("=" * 80)

    try:
        # Step 1: Ingest and validate PaySim data
        logger.info("\n[1/4] Loading and validating PaySim dataset...")
        ingestion = PaySimIngestion()
        df = ingestion.ingest(force_reload=args.force_reload)
        logger.info(f"✓ Loaded {len(df)} transactions")

        # Step 2: Run EDA
        if not args.skip_eda:
            logger.info("\n[2/4] Running exploratory data analysis...")
            eda = EDAReport(df, dataset_name="paysim")
            eda_report = eda.generate_full_report()
            logger.info("✓ EDA report generated")
            logger.info(f"  - Fraud rate: {eda_report['target_distribution']['fraud_rate']:.4%}")
            logger.info(
                f"  - Imbalance ratio: {eda_report['target_distribution']['imbalance_ratio']:.1f}:1"
            )
        else:
            logger.info("\n[2/4] Skipping EDA (--skip-eda flag)")

        # Step 3: Basic preprocessing
        logger.info("\n[3/4] Applying basic preprocessing...")
        processed_df = create_processed_dataset(df)
        logger.info(f"✓ Processed {len(processed_df)} transactions")

        # Step 4: Chronological split
        logger.info("\n[4/4] Creating chronological train/val/test splits...")
        splitter = ChronologicalSplitter()
        train_df, val_df, test_df, split_info = splitter.split(processed_df)

        # Save splits
        splitter.save_splits(train_df, val_df, test_df, dataset_name="paysim")
        logger.info("✓ Splits saved to data/processed/")

        # Save split info
        split_info_path = Path("reports/eda/split_info.json")
        with open(split_info_path, "w") as f:
            json.dump(split_info, f, indent=2)
        logger.info(f"✓ Split info saved to: {split_info_path}")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("Data Preparation Complete!")
        logger.info("=" * 80)
        logger.info(f"Training set:   {len(train_df):,} rows ({split_info['train']['percentage']:.1f}%)")
        logger.info(f"Validation set: {len(val_df):,} rows ({split_info['val']['percentage']:.1f}%)")
        logger.info(f"Test set:       {len(test_df):,} rows ({split_info['test']['percentage']:.1f}%)")
        logger.info("\nNext steps:")
        logger.info("  1. Review EDA reports in reports/eda/")
        logger.info("  2. Examine data quality and fraud patterns")
        logger.info("  3. Proceed to Phase 2: Model Development")

        return 0

    except Exception as e:
        logger.error(f"Data preparation failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

"""
Generate synthetic demo data for testing and development.
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from riskstream.config import ensure_directories, load_config
from riskstream.data.eda import EDAReport
from riskstream.data.synthetic_generator import SyntheticTransactionGenerator
from riskstream.logging_config import get_logger, setup_logging


def main():
    """Generate synthetic transaction data."""
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data")
    parser.add_argument(
        "--num-transactions", type=int, default=10000, help="Number of transactions to generate"
    )
    parser.add_argument("--num-steps", type=int, default=100, help="Number of time steps")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--run-eda", action="store_true", help="Run EDA on generated data")
    args = parser.parse_args()

    # Setup
    setup_logging()
    logger = get_logger(__name__)
    ensure_directories()

    logger.info("=" * 80)
    logger.info("Synthetic Data Generation")
    logger.info("=" * 80)

    try:
        # Load config
        config = load_config("data")
        output_path = Path(config["synthetic"]["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Generate data
        logger.info(f"Generating {args.num_transactions} synthetic transactions...")
        generator = SyntheticTransactionGenerator(seed=args.seed)
        df = generator.generate(
            num_transactions=args.num_transactions, num_steps=args.num_steps
        )

        # Save
        df.write_parquet(output_path, compression="snappy")
        logger.info(f"✓ Saved synthetic data to: {output_path}")

        # Statistics
        fraud_count = df["isFraud"].sum()
        fraud_rate = fraud_count / len(df)
        logger.info(f"Generated {len(df)} transactions:")
        logger.info(f"  - Fraudulent: {fraud_count} ({fraud_rate:.2%})")
        logger.info(f"  - Legitimate: {len(df) - fraud_count} ({(1-fraud_rate):.2%})")

        # Run EDA if requested
        if args.run_eda:
            logger.info("\nRunning EDA on synthetic data...")
            eda = EDAReport(df, dataset_name="synthetic")
            eda.generate_full_report()
            logger.info("✓ EDA report generated")

        logger.info("\n" + "=" * 80)
        logger.info("Synthetic Data Generation Complete!")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error(f"Synthetic data generation failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

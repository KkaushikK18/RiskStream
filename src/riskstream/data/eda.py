"""Exploratory Data Analysis pipeline."""

import json
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

from riskstream.logging_config import get_logger

logger = get_logger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)


class EDAReport:
    """Generate comprehensive EDA reports."""

    def __init__(self, df: pl.DataFrame, dataset_name: str = "dataset"):
        """
        Initialize EDA report generator.

        Args:
            df: DataFrame to analyze
            dataset_name: Name for report labeling
        """
        self.df = df
        self.dataset_name = dataset_name
        self.report_dir = Path("reports/eda")
        self.report_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized EDA for {dataset_name}: {len(df)} rows, {len(df.columns)} cols")

    def generate_full_report(self) -> Dict:
        """
        Generate complete EDA report with visualizations and statistics.

        Returns:
            Dictionary containing all analysis results
        """
        logger.info("Starting full EDA report generation")

        report = {
            "dataset_name": self.dataset_name,
            "basic_stats": self._basic_statistics(),
            "missing_values": self._analyze_missing_values(),
            "target_distribution": self._analyze_target(),
            "numerical_stats": self._analyze_numerical_features(),
            "categorical_stats": self._analyze_categorical_features(),
            "temporal_analysis": self._analyze_temporal_patterns(),
            "fraud_analysis": self._analyze_fraud_patterns(),
            "data_quality": self._assess_data_quality(),
        }

        # Save JSON report
        report_path = self.report_dir / f"{self.dataset_name}_eda_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"Saved EDA report to: {report_path}")

        # Generate visualizations
        self._create_visualizations()

        return report

    def _basic_statistics(self) -> Dict:
        """Calculate basic dataset statistics."""
        stats = {
            "num_rows": len(self.df),
            "num_columns": len(self.df.columns),
            "memory_usage_mb": self.df.estimated_size("mb"),
            "columns": list(self.df.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(self.df.columns, self.df.dtypes)},
        }
        return stats

    def _analyze_missing_values(self) -> Dict:
        """Analyze missing values."""
        null_counts = self.df.null_count()
        total_rows = len(self.df)

        missing_analysis = {}
        for col in self.df.columns:
            null_count = null_counts[col][0]
            missing_analysis[col] = {
                "null_count": null_count,
                "null_percentage": (null_count / total_rows * 100) if total_rows > 0 else 0,
            }

        return missing_analysis

    def _analyze_target(self) -> Dict:
        """Analyze target variable distribution."""
        if "isFraud" not in self.df.columns:
            return {"error": "Target column 'isFraud' not found"}

        target_dist = self.df.select("isFraud").to_series().value_counts().sort("isFraud")

        legitimate = target_dist.filter(pl.col("isFraud") == 0)["counts"][0]
        fraudulent = target_dist.filter(pl.col("isFraud") == 1)["counts"][0]
        total = legitimate + fraudulent

        return {
            "legitimate_count": int(legitimate),
            "fraudulent_count": int(fraudulent),
            "total_count": int(total),
            "fraud_rate": float(fraudulent / total),
            "imbalance_ratio": float(legitimate / fraudulent) if fraudulent > 0 else float("inf"),
        }

    def _analyze_numerical_features(self) -> Dict:
        """Analyze numerical features."""
        numerical_cols = [
            col
            for col in self.df.columns
            if self.df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
        ]

        stats = {}
        for col in numerical_cols:
            col_stats = self.df.select(col).to_series().describe()
            stats[col] = {
                "mean": float(self.df[col].mean()),
                "std": float(self.df[col].std()),
                "min": float(self.df[col].min()),
                "q25": float(self.df[col].quantile(0.25)),
                "median": float(self.df[col].median()),
                "q75": float(self.df[col].quantile(0.75)),
                "max": float(self.df[col].max()),
                "zeros_count": int((self.df[col] == 0).sum()),
            }

        return stats

    def _analyze_categorical_features(self) -> Dict:
        """Analyze categorical features."""
        categorical_cols = [col for col in self.df.columns if self.df[col].dtype == pl.Utf8]

        stats = {}
        for col in categorical_cols:
            value_counts = self.df.select(col).to_series().value_counts()
            unique_count = len(value_counts)

            # Get top 10 most frequent values
            top_values = value_counts.sort("counts", descending=True).head(10)

            stats[col] = {
                "unique_count": unique_count,
                "cardinality": "high" if unique_count > 1000 else "low",
                "top_values": {
                    str(row[col]): int(row["counts"]) for row in top_values.iter_rows(named=True)
                },
            }

        return stats

    def _analyze_temporal_patterns(self) -> Dict:
        """Analyze temporal patterns if step column exists."""
        if "step" not in self.df.columns:
            return {"error": "Temporal column 'step' not found"}

        step_stats = {
            "min_step": int(self.df["step"].min()),
            "max_step": int(self.df["step"].max()),
            "num_steps": int(self.df["step"].n_unique()),
            "transactions_per_step": {
                "mean": float(self.df.group_by("step").agg(pl.count()).select("count").mean()),
                "std": float(self.df.group_by("step").agg(pl.count()).select("count").std()),
            },
        }

        return step_stats

    def _analyze_fraud_patterns(self) -> Dict:
        """Analyze fraud patterns across different dimensions."""
        if "isFraud" not in self.df.columns:
            return {"error": "Target column not found"}

        fraud_df = self.df.filter(pl.col("isFraud") == 1)

        patterns = {"fraud_count": len(fraud_df)}

        # Fraud by transaction type
        if "type" in self.df.columns:
            fraud_by_type = (
                self.df.group_by("type")
                .agg(
                    [
                        pl.count().alias("total"),
                        pl.col("isFraud").sum().alias("fraud_count"),
                    ]
                )
                .with_columns((pl.col("fraud_count") / pl.col("total")).alias("fraud_rate"))
            )

            patterns["by_transaction_type"] = {
                row["type"]: {
                    "total": int(row["total"]),
                    "fraud_count": int(row["fraud_count"]),
                    "fraud_rate": float(row["fraud_rate"]),
                }
                for row in fraud_by_type.iter_rows(named=True)
            }

        # Fraud amount statistics
        if "amount" in fraud_df.columns and len(fraud_df) > 0:
            patterns["fraud_amounts"] = {
                "mean": float(fraud_df["amount"].mean()),
                "median": float(fraud_df["amount"].median()),
                "min": float(fraud_df["amount"].min()),
                "max": float(fraud_df["amount"].max()),
            }

        return patterns

    def _assess_data_quality(self) -> Dict:
        """Assess overall data quality."""
        quality = {
            "completeness": 1.0
            - (self.df.null_count().sum_horizontal()[0] / (len(self.df) * len(self.df.columns))),
            "duplicates": {"count": 0, "percentage": 0.0},
        }

        # Check for potential leakage columns
        leakage_warnings = []
        if "isFlaggedFraud" in self.df.columns:
            leakage_warnings.append(
                "isFlaggedFraud: Output of existing rule, may cause leakage if used as feature"
            )

        quality["leakage_warnings"] = leakage_warnings

        return quality

    def _create_visualizations(self) -> None:
        """Create and save visualization plots."""
        logger.info("Generating visualizations...")

        # 1. Target distribution
        self._plot_target_distribution()

        # 2. Transaction type distribution
        if "type" in self.df.columns:
            self._plot_transaction_types()

        # 3. Amount distribution
        if "amount" in self.df.columns:
            self._plot_amount_distribution()

        # 4. Fraud by transaction type
        if "type" in self.df.columns and "isFraud" in self.df.columns:
            self._plot_fraud_by_type()

        logger.info("Visualizations generated successfully")

    def _plot_target_distribution(self) -> None:
        """Plot target variable distribution."""
        if "isFraud" not in self.df.columns:
            return

        fig, ax = plt.subplots(figsize=(8, 6))

        target_counts = self.df.select("isFraud").to_series().value_counts().sort("isFraud")
        labels = ["Legitimate", "Fraudulent"]
        counts = [
            int(target_counts.filter(pl.col("isFraud") == 0)["counts"][0]),
            int(target_counts.filter(pl.col("isFraud") == 1)["counts"][0]),
        ]

        ax.bar(labels, counts, color=["#2ecc71", "#e74c3c"])
        ax.set_ylabel("Count")
        ax.set_title(f"{self.dataset_name}: Transaction Distribution")

        # Add percentage labels
        total = sum(counts)
        for i, count in enumerate(counts):
            percentage = (count / total) * 100
            ax.text(i, count, f"{count}\n({percentage:.2f}%)", ha="center", va="bottom")

        plt.tight_layout()
        plt.savefig(self.report_dir / f"{self.dataset_name}_target_distribution.png", dpi=150)
        plt.close()

    def _plot_transaction_types(self) -> None:
        """Plot transaction type distribution."""
        fig, ax = plt.subplots(figsize=(10, 6))

        type_counts = self.df.select("type").to_series().value_counts().sort("counts", descending=True)
        types = [str(row["type"]) for row in type_counts.iter_rows(named=True)]
        counts = [int(row["counts"]) for row in type_counts.iter_rows(named=True)]

        ax.bar(types, counts, color="#3498db")
        ax.set_xlabel("Transaction Type")
        ax.set_ylabel("Count")
        ax.set_title(f"{self.dataset_name}: Transaction Type Distribution")
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig(self.report_dir / f"{self.dataset_name}_transaction_types.png", dpi=150)
        plt.close()

    def _plot_amount_distribution(self) -> None:
        """Plot transaction amount distribution."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        amounts_pd = self.df.select("amount").to_pandas()

        # Regular scale
        ax1.hist(amounts_pd["amount"], bins=50, color="#9b59b6", edgecolor="black")
        ax1.set_xlabel("Amount")
        ax1.set_ylabel("Frequency")
        ax1.set_title("Amount Distribution")

        # Log scale
        ax2.hist(amounts_pd["amount"], bins=50, color="#9b59b6", edgecolor="black")
        ax2.set_xlabel("Amount")
        ax2.set_ylabel("Frequency")
        ax2.set_yscale("log")
        ax2.set_title("Amount Distribution (Log Scale)")

        plt.tight_layout()
        plt.savefig(self.report_dir / f"{self.dataset_name}_amount_distribution.png", dpi=150)
        plt.close()

    def _plot_fraud_by_type(self) -> None:
        """Plot fraud rate by transaction type."""
        fig, ax = plt.subplots(figsize=(10, 6))

        fraud_by_type = (
            self.df.group_by("type")
            .agg(
                [
                    pl.count().alias("total"),
                    pl.col("isFraud").sum().alias("fraud_count"),
                ]
            )
            .with_columns((pl.col("fraud_count") / pl.col("total") * 100).alias("fraud_rate"))
            .sort("fraud_rate", descending=True)
        )

        types = [str(row["type"]) for row in fraud_by_type.iter_rows(named=True)]
        rates = [float(row["fraud_rate"]) for row in fraud_by_type.iter_rows(named=True)]

        bars = ax.bar(types, rates, color="#e74c3c")
        ax.set_xlabel("Transaction Type")
        ax.set_ylabel("Fraud Rate (%)")
        ax.set_title(f"{self.dataset_name}: Fraud Rate by Transaction Type")
        plt.xticks(rotation=45)

        # Add value labels on bars
        for bar, rate in zip(bars, rates):
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{rate:.2f}%",
                ha="center",
                va="bottom",
            )

        plt.tight_layout()
        plt.savefig(self.report_dir / f"{self.dataset_name}_fraud_by_type.png", dpi=150)
        plt.close()

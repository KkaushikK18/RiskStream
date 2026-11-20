"""Ingestion: raw file -> canonical, validated Parquet in ``data/interim``.

The raw file is only ever read. Outputs:

* ``<interim_dir>/transactions.parquet``: valid canonical rows
* ``<interim_dir>/rejected.parquet``: quarantined rows with ``_reject_reason``
* ``<interim_dir>/manifest.json``: raw-file checksum, row counts, schema
* ``<eda_dir>/validation_report.{json,md}``
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from riskstream.config import dataset_config, load_config, resolve_path
from riskstream.data.adapters import get_adapter
from riskstream.data.schema import CANONICAL_SCHEMA, ORDER_COLUMNS
from riskstream.data.validation import ValidationReport, validate, with_rejection_reason
from riskstream.logging_config import get_logger

logger = get_logger(__name__)


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Streamed SHA-256 so a 470 MB file never sits in memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


class DataValidationError(RuntimeError):
    def __init__(self, report: ValidationReport) -> None:
        self.report = report
        details = "; ".join(f"{c.name} ({c.failed_count})" for c in report.failures())
        super().__init__(f"Validation failed for {report.dataset}: {details}")


@dataclass
class IngestionResult:
    dataset: str
    interim_path: Path
    rejected_path: Path
    manifest: dict
    report: ValidationReport


def interim_paths(dataset: str) -> tuple[Path, Path, Path]:
    d = resolve_path(dataset_config(dataset)["interim_dir"])
    return d / "transactions.parquet", d / "rejected.parquet", d / "manifest.json"


def ingest(dataset: str, raw_path: Path | None = None) -> IngestionResult:
    """Read, map, validate and persist one dataset."""
    cfg = dataset_config(dataset)
    quality = load_config("data")["quality"]
    adapter = get_adapter(dataset, raw_path)
    out_path, rejected_path, manifest_path = interim_paths(dataset)
    eda_dir = resolve_path(cfg["eda_dir"])

    logger.info("Ingesting %s from %s", dataset, adapter.path)
    raw_problems = adapter.check_raw_columns()
    if raw_problems:
        raise ValueError(f"Source schema mismatch for {dataset}: {raw_problems}")

    canonical = adapter.to_canonical(adapter.scan_raw())
    report = validate(canonical, dataset, quality["max_invalid_row_rate"])
    eda_dir.mkdir(parents=True, exist_ok=True)
    (eda_dir / "validation_report.json").write_text(json.dumps(report.to_dict(), indent=2))
    (eda_dir / "validation_report.md").write_text(report.to_markdown())
    logger.info(
        "Validation %s: %s valid, %s rejected",
        "passed" if report.passed else "FAILED",
        f"{report.valid_rows:,}",
        f"{report.rejected_rows:,}",
    )
    if not report.passed:
        raise DataValidationError(report)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    flagged = with_rejection_reason(canonical)
    (
        flagged.filter(pl.col("_reject_reason").is_null())
        .drop("_reject_reason")
        .sort(list(ORDER_COLUMNS))
        .sink_parquet(out_path, compression="zstd")
    )
    flagged.filter(pl.col("_reject_reason").is_not_null()).sink_parquet(
        rejected_path, compression="zstd"
    )

    manifest = {
        "dataset": dataset,
        "is_synthetic_data": adapter.is_synthetic_data,
        "raw_path": str(adapter.path),
        "raw_sha256": file_sha256(adapter.path),
        "raw_size_bytes": adapter.path.stat().st_size,
        "interim_path": str(out_path),
        "rows_total": report.total_rows,
        "rows_valid": report.valid_rows,
        "rows_rejected": report.rejected_rows,
        "canonical_schema": {k: str(v) for k, v in CANONICAL_SCHEMA.items()},
        "created_at": datetime.now(UTC).isoformat(),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    logger.info("Wrote %s (sha256 of raw: %s...)", out_path, manifest["raw_sha256"][:12])
    return IngestionResult(dataset, out_path, rejected_path, manifest, report)


def load_interim(dataset: str) -> pl.LazyFrame:
    """Lazily scan the validated canonical dataset."""
    path, _, _ = interim_paths(dataset)
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run: python -m riskstream.cli prepare-data --dataset {dataset}"
        )
    return pl.scan_parquet(path)


def load_manifest(dataset: str) -> dict:
    _, _, manifest_path = interim_paths(dataset)
    return json.loads(manifest_path.read_text())

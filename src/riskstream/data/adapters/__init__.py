"""Dataset adapters: one per source dataset, all producing the canonical schema."""

from __future__ import annotations

from pathlib import Path

from riskstream.config import dataset_config, resolve_path
from riskstream.data.adapters.base import DatasetAdapter
from riskstream.data.adapters.paysim import PaySimAdapter


def get_adapter(dataset: str, raw_path: Path | None = None) -> DatasetAdapter:
    """Build the adapter for a configured dataset name."""
    cfg = dataset_config(dataset)
    path = raw_path or resolve_path(cfg["raw_path"])
    if dataset == "paysim":
        return PaySimAdapter(path, id_prefix="ps", is_synthetic_data=False)
    if dataset == "synthetic":
        # The generator emits PaySim-format columns, so the same adapter applies.
        return PaySimAdapter(path, id_prefix="syn", is_synthetic_data=True, name="synthetic")
    # IEEE-CIS has its own schema and pipeline: riskstream.data.ieee_cis.
    raise ValueError(f"No PaySim-format adapter for dataset '{dataset}'")


__all__ = ["DatasetAdapter", "PaySimAdapter", "get_adapter"]

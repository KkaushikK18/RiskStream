"""Adapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl


class DatasetAdapter(ABC):
    """Reads one source dataset and maps it to the canonical schema."""

    name: str
    path: Path
    is_synthetic_data: bool

    @property
    @abstractmethod
    def raw_schema(self) -> dict[str, pl.DataType]:
        """Expected source columns and their types."""

    @abstractmethod
    def scan_raw(self) -> pl.LazyFrame:
        """Lazily read the source file with the expected types."""

    @abstractmethod
    def to_canonical(self, raw: pl.LazyFrame) -> pl.LazyFrame:
        """Rename/derive columns so the output matches CANONICAL_SCHEMA."""

    def check_raw_columns(self) -> list[str]:
        """Return a list of schema problems (empty when the source matches)."""
        actual = self.scan_raw().collect_schema()
        problems = []
        missing = [c for c in self.raw_schema if c not in actual]
        if missing:
            problems.append(f"missing source columns: {missing}")
        extra = [c for c in actual if c not in self.raw_schema]
        if extra:
            problems.append(f"unexpected source columns: {extra}")
        for col, dtype in self.raw_schema.items():
            if col in actual and actual[col] != dtype:
                problems.append(f"column {col}: expected {dtype}, found {actual[col]}")
        return problems

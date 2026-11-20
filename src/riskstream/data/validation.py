"""Data validation on canonical transactions.

Checks are split by consequence:

* ``structural`` checks (columns, dtypes, unique ids, ordering) fail the whole
  dataset, because downstream code cannot be trusted on a broken structure.
* ``row`` checks mark individual records invalid. Invalid rows are quarantined
  (written to ``rejected.parquet`` with a reason) instead of being silently
  dropped, and ingestion fails if their share exceeds the configured tolerance.
* ``info`` checks measure known data characteristics (e.g. PaySim balance
  inconsistencies) without rejecting anything.

All checks are Polars lazy expressions so they run on 6M+ rows without loading
the whole dataset into memory at once.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal

import polars as pl

from riskstream.data.schema import (
    CANONICAL_COLUMNS,
    CANONICAL_SCHEMA,
    DEST_TYPES,
    ID_COLUMN,
    TRANSACTION_TYPES,
)

Severity = Literal["structural", "row", "info"]

DUPLICATE_KEY_COLUMNS: list[str] = [
    c for c in CANONICAL_SCHEMA if c not in ("transaction_id", "event_seq")
]


@dataclass
class CheckResult:
    name: str
    severity: Severity
    description: str
    failed_count: int
    passed: bool


@dataclass
class ValidationReport:
    dataset: str
    total_rows: int
    valid_rows: int
    rejected_rows: int
    max_invalid_row_rate: float
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def invalid_row_rate(self) -> float:
        return self.rejected_rows / self.total_rows if self.total_rows else 0.0

    @property
    def passed(self) -> bool:
        structural_ok = all(c.passed for c in self.checks if c.severity == "structural")
        return structural_ok and self.invalid_row_rate <= self.max_invalid_row_rate

    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed and c.severity != "info"]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["invalid_row_rate"] = self.invalid_row_rate
        d["passed"] = self.passed
        return d

    def to_markdown(self) -> str:
        lines = [
            f"# Validation report: {self.dataset}",
            "",
            f"- Result: **{'PASSED' if self.passed else 'FAILED'}**",
            f"- Rows: {self.total_rows:,} total, {self.valid_rows:,} valid, "
            f"{self.rejected_rows:,} rejected ({self.invalid_row_rate:.4%}; "
            f"tolerance {self.max_invalid_row_rate:.4%})",
            "",
            "| Check | Severity | Failing rows | Result |",
            "|---|---|---:|---|",
        ]
        for c in self.checks:
            status = "measured" if c.severity == "info" else ("pass" if c.passed else "FAIL")
            lines.append(f"| `{c.name}`: {c.description} | {c.severity} | {c.failed_count:,} | {status} |")
        return "\n".join(lines) + "\n"


def structural_problems(lf: pl.LazyFrame) -> list[str]:
    """Column/dtype problems relative to the canonical schema."""
    actual = lf.collect_schema()
    problems = []
    for col in CANONICAL_COLUMNS:
        if col.name not in actual:
            problems.append(f"missing column {col.name}")
        elif actual[col.name] != col.dtype:
            problems.append(f"column {col.name}: expected {col.dtype}, found {actual[col.name]}")
    return problems


def row_rule_exprs() -> dict[str, tuple[str, pl.Expr]]:
    """Row-level rules. Each expression is True where the row VIOLATES the rule."""
    non_null = [c.name for c in CANONICAL_COLUMNS if not c.nullable]
    balances = [
        "orig_balance_before",
        "orig_balance_after",
        "dest_balance_before",
        "dest_balance_after",
    ]
    return {
        "null_required_field": (
            "required field is null",
            pl.any_horizontal([pl.col(c).is_null() for c in non_null]),
        ),
        "unknown_transaction_type": (
            f"type not in {list(TRANSACTION_TYPES)}",
            ~pl.col("type").is_in(list(TRANSACTION_TYPES)),
        ),
        "unknown_dest_type": (
            f"dest_type not in {list(DEST_TYPES)}",
            ~pl.col("dest_type").is_in(list(DEST_TYPES)),
        ),
        "negative_amount": ("amount < 0", pl.col("amount") < 0),
        "non_finite_numeric": (
            "NaN or infinite amount/balance",
            pl.any_horizontal([~pl.col(c).is_finite() for c in ["amount", *balances]]),
        ),
        "negative_balance": (
            "a balance is negative",
            pl.any_horizontal([pl.col(c) < 0 for c in balances]),
        ),
        "invalid_label": ("is_fraud not in {0, 1}", ~pl.col("is_fraud").is_in([0, 1])),
        "invalid_flag": (
            "is_flagged_fraud not in {0, 1}",
            ~pl.col("is_flagged_fraud").is_in([0, 1]),
        ),
        "invalid_step": ("step < 1", pl.col("step") < 1),
        "duplicate_record": (
            "exact repeat of an earlier record (all fields except id)",
            # 64-bit row hash; the first occurrence is kept, repeats are rejected.
            ~pl.struct(DUPLICATE_KEY_COLUMNS).hash(seed=0).is_first_distinct(),
        ),
    }


def info_exprs() -> dict[str, tuple[str, pl.Expr]]:
    """Measured characteristics that do not reject rows."""
    tol = 0.01
    return {
        "zero_amount": ("amount == 0", pl.col("amount") == 0),
        "orig_balance_mismatch": (
            "orig_balance_before - amount != orig_balance_after (beyond 0.01)",
            ((pl.col("orig_balance_before") - pl.col("amount")).clip(lower_bound=0)
             - pl.col("orig_balance_after")).abs() > tol,
        ),
        "dest_balance_mismatch": (
            "dest_balance_before + amount != dest_balance_after (beyond 0.01), customer dests",
            (pl.col("dest_type") == "CUSTOMER")
            & ((pl.col("dest_balance_before") + pl.col("amount") - pl.col("dest_balance_after")).abs() > tol),
        ),
        "amount_exceeds_orig_balance": (
            "amount > orig_balance_before",
            pl.col("amount") > pl.col("orig_balance_before"),
        ),
        "merchant_dest_zero_balances": (
            "merchant destination with both balances 0 (PaySim does not track merchant balances)",
            (pl.col("dest_type") == "MERCHANT")
            & (pl.col("dest_balance_before") == 0)
            & (pl.col("dest_balance_after") == 0),
        ),
    }


def with_rejection_reason(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add ``_reject_reason``: the first violated row rule, or null when valid."""
    rules = row_rule_exprs()
    reason: pl.Expr = pl.lit(None, dtype=pl.String)
    # Build the when/then chain in reverse so the first rule listed wins.
    for name, (_, expr) in reversed(list(rules.items())):
        reason = pl.when(expr.fill_null(True)).then(pl.lit(name)).otherwise(reason)
    return lf.with_columns(reason.alias("_reject_reason"))


def validate(lf: pl.LazyFrame, dataset: str, max_invalid_row_rate: float) -> ValidationReport:
    """Run every check and return a report. Does not write anything."""
    problems = structural_problems(lf)
    if problems:
        return ValidationReport(
            dataset=dataset,
            total_rows=0,
            valid_rows=0,
            rejected_rows=0,
            max_invalid_row_rate=max_invalid_row_rate,
            checks=[
                CheckResult("canonical_schema", "structural", p, 0, False) for p in problems
            ],
        )

    rules = row_rule_exprs()
    infos = info_exprs()
    counts = lf.select(
        pl.len().alias("_total"),
        (pl.len() - pl.col(ID_COLUMN).n_unique()).alias("_dup_ids"),
        # Source order must never go backwards in time.
        (pl.col("step").diff() < 0).sum().alias("_order_violations"),
        *[expr.fill_null(True).sum().alias(name) for name, (_, expr) in rules.items()],
        *[expr.fill_null(False).sum().alias(name) for name, (_, expr) in infos.items()],
    ).collect()
    rejected = (
        with_rejection_reason(lf)
        .select(pl.col("_reject_reason").is_not_null().sum())
        .collect()
        .item()
    )
    c = counts.row(0, named=True)
    total = int(c["_total"])

    checks = [
        CheckResult("canonical_schema", "structural", "columns and dtypes match", 0, True),
        CheckResult(
            "unique_transaction_id", "structural", "transaction_id is unique",
            int(c["_dup_ids"]), c["_dup_ids"] == 0,
        ),
        CheckResult(
            "temporal_order", "structural", "step never decreases in source order",
            int(c["_order_violations"]), c["_order_violations"] == 0,
        ),
        CheckResult("non_empty", "structural", "dataset has rows", 0 if total else 1, total > 0),
    ]
    for name, (desc, _) in rules.items():
        checks.append(CheckResult(name, "row", desc, int(c[name]), c[name] == 0))
    for name, (desc, _) in infos.items():
        checks.append(CheckResult(name, "info", desc, int(c[name]), True))

    return ValidationReport(
        dataset=dataset,
        total_rows=total,
        valid_rows=total - int(rejected),
        rejected_rows=int(rejected),
        max_invalid_row_rate=max_invalid_row_rate,
        checks=checks,
    )

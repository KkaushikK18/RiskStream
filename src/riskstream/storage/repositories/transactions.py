"""Transactions and predictions."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from riskstream.storage.models import LabelRow, PredictionRow, TransactionRow


def get_transaction(session: Session, transaction_id: str) -> TransactionRow | None:
    return session.execute(
        select(TransactionRow).where(TransactionRow.transaction_id == transaction_id)
    ).scalar_one_or_none()


def get_prediction(session: Session, transaction_id: str, role: str = "champion") -> PredictionRow | None:
    return session.execute(
        select(PredictionRow).where(
            PredictionRow.transaction_id == transaction_id, PredictionRow.role == role
        )
    ).scalar_one_or_none()


def get_label(session: Session, transaction_id: str) -> LabelRow | None:
    return session.execute(
        select(LabelRow).where(LabelRow.transaction_id == transaction_id)
    ).scalar_one_or_none()


def list_predictions(session: Session, *, decision: str | None, before_id: int | None,
                     limit: int) -> list[tuple[PredictionRow, TransactionRow]]:
    """Newest first, keyset-paginated on ``predictions.id`` (no OFFSET scans)."""
    q = (
        select(PredictionRow, TransactionRow)
        .join(TransactionRow, TransactionRow.transaction_id == PredictionRow.transaction_id)
        .where(PredictionRow.role == "champion")
        .order_by(PredictionRow.id.desc())
        .limit(limit)
    )
    if decision:
        q = q.where(PredictionRow.decision == decision)
    if before_id is not None:
        q = q.where(PredictionRow.id < before_id)
    return [(p, t) for p, t in session.execute(q).all()]

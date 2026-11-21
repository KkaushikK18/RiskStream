"""ORM tables. The Alembic migrations in ``storage/migrations`` create the same schema
(a test compares them).

Design notes:
* Money columns are NUMERIC(18, 2); features read them as floats.
* ``transactions.log_amount`` is stored so feature SQL needs no math functions
  (portable to SQLite for tests, cheap on PostgreSQL).
* Account identifiers are the simulator's synthetic ids. No names, card numbers
  or other personal data are stored.
* ``predictions`` is unique per (transaction, role) so shadow predictions
  (Phase 8) can never overwrite the champion's.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from riskstream.data.schema import DEST_TYPES, TRANSACTION_TYPES

PK = BigInteger().with_variant(Integer, "sqlite")
JSONType = JSON().with_variant(JSONB(), "postgresql")
Money = Numeric(18, 2, asdecimal=True)

DECISIONS = ("APPROVE", "REVIEW", "DECLINE")
RISK_LEVELS = ("LOW", "MEDIUM", "HIGH")
LABEL_SOURCES = ("chargeback", "analyst_review", "customer_report", "simulation")


def _in(col: str, values: tuple[str, ...]) -> str:
    return f"{col} IN ({', '.join(repr(v) for v in values)})"


class Base(DeclarativeBase):
    pass


class TransactionRow(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(String(64))
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    step: Mapped[int] = mapped_column(Integer)
    type: Mapped[str] = mapped_column(String(16))
    amount: Mapped[Decimal] = mapped_column(Money)
    log_amount: Mapped[float] = mapped_column(Float)
    orig_id: Mapped[str] = mapped_column(String(64))
    orig_balance_before: Mapped[Decimal] = mapped_column(Money)
    dest_id: Mapped[str] = mapped_column(String(64))
    dest_type: Mapped[str] = mapped_column(String(16))
    # api | historical_backfill
    source: Mapped[str] = mapped_column(String(32))
    request_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("transaction_id", name="uq_transactions_transaction_id"),
        CheckConstraint("amount >= 0", name="ck_transactions_amount_non_negative"),
        CheckConstraint("orig_balance_before >= 0", name="ck_transactions_balance_non_negative"),
        CheckConstraint("step >= 1", name="ck_transactions_step_positive"),
        CheckConstraint(_in("type", TRANSACTION_TYPES), name="ck_transactions_type"),
        CheckConstraint(_in("dest_type", DEST_TYPES), name="ck_transactions_dest_type"),
        Index("ix_transactions_dest_step", "dest_id", "step"),
        Index("ix_transactions_orig_step", "orig_id", "step"),
        Index("ix_transactions_step", "step"),
    )


class PredictionRow(Base):
    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("transactions.transaction_id", ondelete="RESTRICT", name="fk_predictions_transaction")
    )
    # champion now; shadow / canary in Phase 8
    role: Mapped[str] = mapped_column(String(16), default="champion")
    model_version: Mapped[str] = mapped_column(String(128))
    policy_version: Mapped[str] = mapped_column(String(32))
    feature_version: Mapped[str] = mapped_column(String(16))
    fraud_probability: Mapped[float] = mapped_column(Float)
    risk_level: Mapped[str] = mapped_column(String(8))
    decision: Mapped[str] = mapped_column(String(8))
    # Feature vector used for the decision (NaN stored as null), for monitoring and audits.
    features: Mapped[dict[str, Any]] = mapped_column(JSONType)
    feature_latency_ms: Mapped[float] = mapped_column(Float)
    inference_latency_ms: Mapped[float] = mapped_column(Float)
    instance_id: Mapped[str] = mapped_column(String(128))
    scored_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("transaction_id", "role", name="uq_predictions_transaction_role"),
        CheckConstraint(_in("decision", DECISIONS), name="ck_predictions_decision"),
        CheckConstraint(_in("risk_level", RISK_LEVELS), name="ck_predictions_risk_level"),
        CheckConstraint(
            "fraud_probability >= 0 AND fraud_probability <= 1", name="ck_predictions_probability_range"
        ),
        Index("ix_predictions_decision_id", "decision", "id"),
        Index("ix_predictions_scored_at", "scored_at"),
        Index("ix_predictions_model_version", "model_version"),
    )


class LabelRow(Base):
    __tablename__ = "labels"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("transactions.transaction_id", ondelete="RESTRICT", name="fk_labels_transaction")
    )
    is_fraud: Mapped[bool] = mapped_column(Boolean)
    source: Mapped[str] = mapped_column(String(32))
    # When the outcome was determined (event time, supplied by the labeler).
    label_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # When the platform received it; delayed-label analysis uses this (Phase 7).
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    submitted_by: Mapped[str] = mapped_column(String(64))
    request_sha256: Mapped[str] = mapped_column(String(64))

    __table_args__ = (
        UniqueConstraint("transaction_id", name="uq_labels_transaction_id"),
        CheckConstraint(_in("source", LABEL_SOURCES), name="ck_labels_source"),
        Index("ix_labels_available_at", "available_at"),
    )


class ModelDeploymentRow(Base):
    __tablename__ = "model_deployments"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    model_version: Mapped[str] = mapped_column(String(128))
    policy_version: Mapped[str] = mapped_column(String(32))
    feature_version: Mapped[str] = mapped_column(String(16))
    bundle_path: Mapped[str] = mapped_column(Text)
    policy_path: Mapped[str] = mapped_column(Text)
    # active | inactive
    status: Mapped[str] = mapped_column(String(16))
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    activated_by: Mapped[str] = mapped_column(String(128))

    __table_args__ = (
        CheckConstraint("status IN ('active', 'inactive')", name="ck_model_deployments_status"),
        Index("ix_model_deployments_status", "status"),
    )


class ModelEvaluationRow(Base):
    """Champion vs challenger results (written from Phase 7)."""

    __tablename__ = "model_evaluations"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    champion_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    challenger_version: Mapped[str] = mapped_column(String(128))
    dataset_ref: Mapped[str] = mapped_column(Text)
    passed: Mapped[bool] = mapped_column(Boolean)
    result: Mapped[dict[str, Any]] = mapped_column(JSONType)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OutboxEventRow(Base):
    """Transactional outbox: events written in the same DB transaction as the
    state change, published to Redpanda by a separate relay (Phase 4)."""

    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36))
    topic: Mapped[str] = mapped_column(String(64))
    event_key: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_outbox_events_event_id"),
        Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where="published_at IS NULL",
            sqlite_where="published_at IS NULL",
        ),
    )


class AuditEventRow(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64))
    actor: Mapped[str] = mapped_column(String(128))
    subject: Mapped[str] = mapped_column(String(128))
    details: Mapped[dict[str, Any]] = mapped_column(JSONType)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("ix_audit_events_type_created", "event_type", "created_at"),)

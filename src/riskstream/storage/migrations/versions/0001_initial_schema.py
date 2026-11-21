"""Initial schema: transactions, predictions, labels, deployments, evaluations, outbox, audit.

Revision ID: 0001
Revises:
Create Date: 2026-11-20
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

PK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
JSONType = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
MONEY = sa.Numeric(18, 2)
TS = sa.DateTime(timezone=True)

TYPES = "'CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER'"


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("id", PK, primary_key=True, autoincrement=True),
        sa.Column("transaction_id", sa.String(64), nullable=False),
        sa.Column("event_time", TS, nullable=False),
        sa.Column("step", sa.Integer(), nullable=False),
        sa.Column("type", sa.String(16), nullable=False),
        sa.Column("amount", MONEY, nullable=False),
        sa.Column("log_amount", sa.Float(), nullable=False),
        sa.Column("orig_id", sa.String(64), nullable=False),
        sa.Column("orig_balance_before", MONEY, nullable=False),
        sa.Column("dest_id", sa.String(64), nullable=False),
        sa.Column("dest_type", sa.String(16), nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("request_sha256", sa.String(64), nullable=True),
        sa.Column("created_at", TS, server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("transaction_id", name="uq_transactions_transaction_id"),
        sa.CheckConstraint("amount >= 0", name="ck_transactions_amount_non_negative"),
        sa.CheckConstraint("orig_balance_before >= 0", name="ck_transactions_balance_non_negative"),
        sa.CheckConstraint("step >= 1", name="ck_transactions_step_positive"),
        sa.CheckConstraint(f"type IN ({TYPES})", name="ck_transactions_type"),
        sa.CheckConstraint("dest_type IN ('CUSTOMER', 'MERCHANT')", name="ck_transactions_dest_type"),
    )
    op.create_index("ix_transactions_dest_step", "transactions", ["dest_id", "step"])
    op.create_index("ix_transactions_orig_step", "transactions", ["orig_id", "step"])

    op.create_table(
        "predictions",
        sa.Column("id", PK, primary_key=True, autoincrement=True),
        sa.Column("transaction_id", sa.String(64),
                  sa.ForeignKey("transactions.transaction_id", ondelete="RESTRICT",
                                name="fk_predictions_transaction"), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("model_version", sa.String(128), nullable=False),
        sa.Column("policy_version", sa.String(32), nullable=False),
        sa.Column("feature_version", sa.String(16), nullable=False),
        sa.Column("fraud_probability", sa.Float(), nullable=False),
        sa.Column("risk_level", sa.String(8), nullable=False),
        sa.Column("decision", sa.String(8), nullable=False),
        sa.Column("features", JSONType, nullable=False),
        sa.Column("feature_latency_ms", sa.Float(), nullable=False),
        sa.Column("inference_latency_ms", sa.Float(), nullable=False),
        sa.Column("instance_id", sa.String(128), nullable=False),
        sa.Column("scored_at", TS, nullable=False),
        sa.UniqueConstraint("transaction_id", "role", name="uq_predictions_transaction_role"),
        sa.CheckConstraint("decision IN ('APPROVE', 'REVIEW', 'DECLINE')", name="ck_predictions_decision"),
        sa.CheckConstraint("risk_level IN ('LOW', 'MEDIUM', 'HIGH')", name="ck_predictions_risk_level"),
        sa.CheckConstraint("fraud_probability >= 0 AND fraud_probability <= 1",
                           name="ck_predictions_probability_range"),
    )
    op.create_index("ix_predictions_decision_id", "predictions", ["decision", "id"])
    op.create_index("ix_predictions_scored_at", "predictions", ["scored_at"])
    op.create_index("ix_predictions_model_version", "predictions", ["model_version"])

    op.create_table(
        "labels",
        sa.Column("id", PK, primary_key=True, autoincrement=True),
        sa.Column("transaction_id", sa.String(64),
                  sa.ForeignKey("transactions.transaction_id", ondelete="RESTRICT", name="fk_labels_transaction"),
                  nullable=False),
        sa.Column("is_fraud", sa.Boolean(), nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("label_time", TS, nullable=False),
        sa.Column("available_at", TS, nullable=False),
        sa.Column("submitted_by", sa.String(64), nullable=False),
        sa.Column("request_sha256", sa.String(64), nullable=False),
        sa.UniqueConstraint("transaction_id", name="uq_labels_transaction_id"),
        sa.CheckConstraint("source IN ('chargeback', 'analyst_review', 'customer_report', 'simulation')",
                           name="ck_labels_source"),
    )
    op.create_index("ix_labels_available_at", "labels", ["available_at"])

    op.create_table(
        "model_deployments",
        sa.Column("id", PK, primary_key=True, autoincrement=True),
        sa.Column("model_version", sa.String(128), nullable=False),
        sa.Column("policy_version", sa.String(32), nullable=False),
        sa.Column("feature_version", sa.String(16), nullable=False),
        sa.Column("bundle_path", sa.Text(), nullable=False),
        sa.Column("policy_path", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("activated_at", TS, nullable=False),
        sa.Column("deactivated_at", TS, nullable=True),
        sa.Column("activated_by", sa.String(128), nullable=False),
        sa.CheckConstraint("status IN ('active', 'inactive')", name="ck_model_deployments_status"),
    )
    op.create_index("ix_model_deployments_status", "model_deployments", ["status"])

    op.create_table(
        "model_evaluations",
        sa.Column("id", PK, primary_key=True, autoincrement=True),
        sa.Column("champion_version", sa.String(128), nullable=True),
        sa.Column("challenger_version", sa.String(128), nullable=False),
        sa.Column("dataset_ref", sa.Text(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("result", JSONType, nullable=False),
        sa.Column("created_at", TS, server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", PK, primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(36), nullable=False),
        sa.Column("topic", sa.String(64), nullable=False),
        sa.Column("event_key", sa.String(64), nullable=False),
        sa.Column("payload", JSONType, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("published_at", TS, nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.UniqueConstraint("event_id", name="uq_outbox_events_event_id"),
    )
    op.create_index("ix_outbox_unpublished", "outbox_events", ["id"],
                    postgresql_where=sa.text("published_at IS NULL"), sqlite_where=sa.text("published_at IS NULL"))

    op.create_table(
        "audit_events",
        sa.Column("id", PK, primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("actor", sa.String(128), nullable=False),
        sa.Column("subject", sa.String(128), nullable=False),
        sa.Column("details", JSONType, nullable=False),
        sa.Column("created_at", TS, nullable=False),
    )
    op.create_index("ix_audit_events_type_created", "audit_events", ["event_type", "created_at"])


def downgrade() -> None:
    for table in ("audit_events", "outbox_events", "model_evaluations", "model_deployments", "labels",
                  "predictions", "transactions"):
        op.drop_table(table)

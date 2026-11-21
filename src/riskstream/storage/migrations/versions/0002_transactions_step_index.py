"""Index transactions.step.

Needed for time-range queries that do not filter by account: latest stored
step, "anything stored at or after step N", and future retention by step.
Without it these scan the whole table (6.2M rows after the PaySim backfill).

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-21
"""

from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_transactions_step", "transactions", ["step"])


def downgrade() -> None:
    op.drop_index("ix_transactions_step", table_name="transactions")

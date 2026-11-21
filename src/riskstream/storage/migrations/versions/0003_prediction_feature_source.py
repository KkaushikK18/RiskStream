"""Record where each prediction's feature primitives came from.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing rows were all scored from PostgreSQL history (Phase 3).
    with op.batch_alter_table("predictions") as batch:
        batch.add_column(sa.Column("feature_source", sa.String(24), nullable=False, server_default="postgres"))
        batch.add_column(sa.Column("feature_fallback_reason", sa.String(48), nullable=True))
        batch.create_check_constraint(
            "ck_predictions_feature_source", "feature_source IN ('postgres', 'redis', 'postgres_fallback')")


def downgrade() -> None:
    with op.batch_alter_table("predictions") as batch:
        batch.drop_constraint("ck_predictions_feature_source", type_="check")
        batch.drop_column("feature_fallback_reason")
        batch.drop_column("feature_source")

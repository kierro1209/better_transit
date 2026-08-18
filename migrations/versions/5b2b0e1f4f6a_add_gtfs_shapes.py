"""add GTFS shape points

Revision ID: 5b2b0e1f4f6a
Revises: 20fae1e49eee
Create Date: 2026-08-17 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5b2b0e1f4f6a"
down_revision: str | Sequence[str] | None = "20fae1e49eee"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "shape_points",
        sa.Column("agency_key", sa.String(length=32), nullable=False),
        sa.Column("shape_id", sa.String(length=64), nullable=False),
        sa.Column("shape_pt_sequence", sa.Integer(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("distance_traveled", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("agency_key", "shape_id", "shape_pt_sequence"),
    )
    op.create_index(
        "ix_shape_points_shape",
        "shape_points",
        ["agency_key", "shape_id", "shape_pt_sequence"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_shape_points_shape", table_name="shape_points")
    op.drop_table("shape_points")

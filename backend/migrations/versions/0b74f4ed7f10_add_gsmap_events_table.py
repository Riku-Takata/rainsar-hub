"""add gsmap_events table

Revision ID: 0b74f4ed7f10
Revises: fde62d2349f8
Create Date: 2025-11-16 12:06:33.301441

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0b74f4ed7f10'
down_revision: Union[str, Sequence[str], None] = 'fde62d2349f8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "gsmap_events",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lon", sa.Float(), nullable=False),
        sa.Column("region", sa.String(length=50), nullable=True),
        sa.Column("start_ts_utc", sa.DateTime(), nullable=False),
        sa.Column("end_ts_utc", sa.DateTime(), nullable=False),
        sa.Column("hit_hours", sa.Integer(), nullable=False),
        sa.Column("max_gauge_mm_h", sa.Float(), nullable=False),
        sa.Column("sum_gauge_mm_h", sa.Float(), nullable=False),
        sa.Column("mean_gauge_mm_h", sa.Float(), nullable=False),
        sa.Column("threshold_mm_h", sa.Float(), nullable=False),
        sa.Column("repr_source_file", sa.String(length=255), nullable=True),
    )

    op.create_index(
        "ix_gsmap_events_lat_lon",
        "gsmap_events",
        ["lat", "lon"],
    )
    op.create_index(
        "ix_gsmap_events_start_ts_utc",
        "gsmap_events",
        ["start_ts_utc"],
    )
    op.create_index(
        "ix_gsmap_events_region",
        "gsmap_events",
        ["region"],
    )


def downgrade() -> None:
    op.drop_index("ix_gsmap_events_region", table_name="gsmap_events")
    op.drop_index("ix_gsmap_events_start_ts_utc", table_name="gsmap_events")
    op.drop_index("ix_gsmap_events_lat_lon", table_name="gsmap_events")
    op.drop_table("gsmap_events")
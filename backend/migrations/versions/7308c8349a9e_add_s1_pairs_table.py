"""add s1_pairs table

Revision ID: 7308c8349a9e
Revises: 0b74f4ed7f10
Create Date: 2025-11-16 05:34:06.367690

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7308c8349a9e"
down_revision: Union[str, Sequence[str], None] = "0b74f4ed7f10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.create_table(
        "s1_pairs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("grid_id", sa.String(length=32), nullable=False),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lon", sa.Float(), nullable=False),
        sa.Column("event_start_ts_utc", sa.DateTime(), nullable=False),
        sa.Column("event_end_ts_utc", sa.DateTime(), nullable=False),
        sa.Column("threshold_mm_h", sa.Float(), nullable=False),
        sa.Column("hit_hours", sa.Integer(), nullable=False),
        sa.Column("max_gauge_mm_h", sa.Float(), nullable=False),
        sa.Column("after_scene_id", sa.String(length=128), nullable=False),
        sa.Column("after_platform", sa.String(length=16), nullable=True),
        sa.Column("after_mission", sa.String(length=8), nullable=True),
        sa.Column("after_pass_direction", sa.String(length=8), nullable=True),
        sa.Column("after_relative_orbit", sa.Integer(), nullable=True),
        sa.Column("after_start_ts_utc", sa.DateTime(), nullable=False),
        sa.Column("after_end_ts_utc", sa.DateTime(), nullable=False),
        sa.Column("before_scene_id", sa.String(length=128), nullable=True),
        sa.Column("before_start_ts_utc", sa.DateTime(), nullable=True),
        sa.Column("before_end_ts_utc", sa.DateTime(), nullable=True),
        sa.Column("before_relative_orbit", sa.Integer(), nullable=True),
        sa.Column("delay_h", sa.Float(), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="cdse"),
    )
    op.create_index(
        "idx_s1_pairs_grid_start",
        "s1_pairs",
        ["grid_id", "event_start_ts_utc"],
    )
    op.create_index(
        "idx_s1_pairs_after_scene",
        "s1_pairs",
        ["after_scene_id"],
    )
    op.create_index(
        "idx_s1_pairs_before_scene",
        "s1_pairs",
        ["before_scene_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_s1_pairs_before_scene", table_name="s1_pairs")
    op.drop_index("idx_s1_pairs_after_scene", table_name="s1_pairs")
    op.drop_index("idx_s1_pairs_grid_start", table_name="s1_pairs")
    op.drop_table("s1_pairs")

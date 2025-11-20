"""add indexes for gsmap_points performance

Revision ID: add_gsmap_indexes_20241119
Revises: 0b74f4ed7f10
Create Date: 2025-11-19 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "add_gsmap_indexes_20241119"
down_revision: Union[str, Sequence[str], None] = "7308c8349a9e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_gsmap_points_grid_id",
        "gsmap_points",
        ["grid_id"],
        unique=False,
    )
    op.create_index(
        "ix_gsmap_points_lat_lon",
        "gsmap_points",
        ["lat", "lon"],
        unique=False,
    )
    op.create_index(
        "ix_gsmap_points_grid_ts",
        "gsmap_points",
        ["grid_id", "ts_utc"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_gsmap_points_grid_ts", table_name="gsmap_points")
    op.drop_index("ix_gsmap_points_lat_lon", table_name="gsmap_points")
    op.drop_index("ix_gsmap_points_grid_id", table_name="gsmap_points")



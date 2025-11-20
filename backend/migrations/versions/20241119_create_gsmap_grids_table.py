"""create gsmap_grids table

Revision ID: create_gsmap_grids_table_20241119
Revises: add_gsmap_indexes_20241119
Create Date: 2025-11-19 15:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "create_gsmap_grids_table_20241119"
down_revision: Union[str, Sequence[str], None] = "fa1a2b3c4d5e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = inspector.get_table_names()
    if "gsmap_grids" not in table_names:
        op.create_table(
            "gsmap_grids",
            sa.Column("grid_id", sa.String(length=32), primary_key=True),
            sa.Column("lat", sa.Float(), nullable=False),
            sa.Column("lon", sa.Float(), nullable=False),
            sa.Column("is_japan_land", sa.Boolean(), nullable=False, server_default=sa.text("0")),
            sa.Column("region", sa.String(length=32), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now(), onupdate=sa.func.now()),
        )

    existing_indexes = {idx["name"] for idx in inspector.get_indexes("gsmap_grids")} if "gsmap_grids" in table_names else set()
    if "ix_gsmap_grids_is_japan_land" not in existing_indexes:
        op.create_index(
            "ix_gsmap_grids_is_japan_land",
            "gsmap_grids",
            ["is_japan_land"],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index("ix_gsmap_grids_is_japan_land", table_name="gsmap_grids")
    op.drop_table("gsmap_grids")



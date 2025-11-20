"""expand alembic_version column length

Revision ID: fa1a2b3c4d5e
Revises: add_gsmap_indexes_20241119
Create Date: 2025-11-19 16:40:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "fa1a2b3c4d5e"
down_revision: Union[str, Sequence[str], None] = "add_gsmap_indexes_20241119"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE alembic_version MODIFY version_num VARCHAR(64)")


def downgrade() -> None:
    op.execute("ALTER TABLE alembic_version MODIFY version_num VARCHAR(32)")



"""add realtime connection epoch

Revision ID: 4d8f7a1c9e2b
Revises: 6b2e7f1c4a8d
Create Date: 2026-08-06 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "4d8f7a1c9e2b"
down_revision: Union[str, Sequence[str], None] = "6b2e7f1c4a8d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sessions",
        sa.Column(
            "realtime_connection_epoch",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("sessions", "realtime_connection_epoch")

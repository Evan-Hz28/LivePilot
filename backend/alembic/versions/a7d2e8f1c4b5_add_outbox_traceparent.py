"""add outbox trace context

Revision ID: a7d2e8f1c4b5
Revises: 9c6d4e1a2b3f
Create Date: 2026-08-07 15:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a7d2e8f1c4b5"
down_revision: Union[str, Sequence[str], None] = "9c6d4e1a2b3f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "event_outbox",
        sa.Column("traceparent", sa.String(length=128), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("event_outbox", "traceparent")

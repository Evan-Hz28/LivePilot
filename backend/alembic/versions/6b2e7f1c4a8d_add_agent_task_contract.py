"""add versioned agent task contract

Revision ID: 6b2e7f1c4a8d
Revises: 2a3f708ad2c5
Create Date: 2026-08-06 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "6b2e7f1c4a8d"
down_revision: Union[str, Sequence[str], None] = "2a3f708ad2c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column(
            "turn_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.add_column(
        "tasks",
        sa.Column("context_version", sa.Integer(), nullable=True),
    )
    op.add_column(
        "tasks",
        sa.Column("target_preference_version", sa.Integer(), nullable=True),
    )
    op.add_column(
        "tasks",
        sa.Column("idempotency_key", sa.String(length=160), nullable=True),
    )
    op.add_column(
        "tasks",
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_tasks_turn_id_turns",
        "tasks",
        "turns",
        ["turn_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_tasks_session_context_status",
        "tasks",
        ["session_id", "context_version", "status"],
        unique=False,
    )
    op.create_unique_constraint(
        "uq_tasks_session_idempotency_key",
        "tasks",
        ["session_id", "idempotency_key"],
    )
    op.add_column(
        "turns",
        sa.Column(
            "content",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("turns", "content")
    op.drop_constraint(
        "uq_tasks_session_idempotency_key",
        "tasks",
        type_="unique",
    )
    op.drop_index("idx_tasks_session_context_status", table_name="tasks")
    op.drop_constraint("fk_tasks_turn_id_turns", "tasks", type_="foreignkey")
    op.drop_column("tasks", "deadline_at")
    op.drop_column("tasks", "idempotency_key")
    op.drop_column("tasks", "target_preference_version")
    op.drop_column("tasks", "context_version")
    op.drop_column("tasks", "turn_id")

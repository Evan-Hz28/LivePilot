"""add versioned tool calls and itineraries

Revision ID: 8f5a6b7c8d9e
Revises: 4d8f7a1c9e2b
Create Date: 2026-08-07 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "8f5a6b7c8d9e"
down_revision: Union[str, Sequence[str], None] = "4d8f7a1c9e2b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column("target_itinerary_version", sa.Integer(), nullable=True),
    )
    op.create_table(
        "tool_calls",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("context_version", sa.Integer(), nullable=False),
        sa.Column("target_preference_version", sa.Integer(), nullable=False),
        sa.Column("tool_name", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("input", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("output", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("provider_request_id", sa.String(length=128), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id", "tool_name", "request_hash"),
    )
    op.create_index("idx_tool_calls_session_id", "tool_calls", ["session_id"])
    op.create_index(
        "idx_tool_calls_session_context",
        "tool_calls",
        ["session_id", "context_version"],
    )
    op.create_index(
        "idx_tool_calls_task",
        "tool_calls",
        ["task_id", "created_at"],
    )
    op.create_table(
        "itineraries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("context_version", sa.Integer(), nullable=False),
        sa.Column("preference_version", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default="draft",
            nullable=False,
        ),
        sa.Column("content", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "budget_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "source_task_ids",
            postgresql.ARRAY(postgresql.UUID(as_uuid=True)),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", "version"),
    )
    op.create_index("idx_itineraries_session_id", "itineraries", ["session_id"])
    op.create_index(
        "uq_confirmed_itinerary_per_session",
        "itineraries",
        ["session_id"],
        unique=True,
        postgresql_where=sa.text("status = 'confirmed'"),
    )


def downgrade() -> None:
    op.drop_index("uq_confirmed_itinerary_per_session", table_name="itineraries")
    op.drop_index("idx_itineraries_session_id", table_name="itineraries")
    op.drop_table("itineraries")
    op.drop_index("idx_tool_calls_task", table_name="tool_calls")
    op.drop_index("idx_tool_calls_session_context", table_name="tool_calls")
    op.drop_index("idx_tool_calls_session_id", table_name="tool_calls")
    op.drop_table("tool_calls")
    op.drop_column("tasks", "target_itinerary_version")

"""add principals and session owners

Revision ID: 9c6d4e1a2b3f
Revises: 8f5a6b7c8d9e
Create Date: 2026-08-07 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "9c6d4e1a2b3f"
down_revision: Union[str, Sequence[str], None] = "8f5a6b7c8d9e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

LEGACY_ISSUER = "legacy://livepilot"


def upgrade() -> None:
    op.create_table(
        "principals",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("issuer", sa.String(length=512), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("issuer", "subject"),
    )
    op.add_column(
        "sessions",
        sa.Column("owner_principal_id", postgresql.UUID(as_uuid=True), nullable=True),
    )

    connection = op.get_bind()
    connection.execute(
        sa.text(
            """
            INSERT INTO principals (id, issuer, subject)
            SELECT DISTINCT user_id, :issuer, user_id::text
            FROM sessions
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"issuer": LEGACY_ISSUER},
    )
    connection.execute(
        sa.text(
            "UPDATE sessions SET owner_principal_id = user_id "
            "WHERE owner_principal_id IS NULL"
        )
    )

    op.alter_column("sessions", "owner_principal_id", nullable=False)
    op.create_foreign_key(
        "fk_sessions_owner_principal_id_principals",
        "sessions",
        "principals",
        ["owner_principal_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_sessions_owner_principal_id",
        "sessions",
        ["owner_principal_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_sessions_owner_principal_id", table_name="sessions")
    op.drop_constraint(
        "fk_sessions_owner_principal_id_principals",
        "sessions",
        type_="foreignkey",
    )
    op.drop_column("sessions", "owner_principal_id")
    op.drop_table("principals")

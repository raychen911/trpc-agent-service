"""Centralize model credentials under platform administration.

Revision ID: 20260831_0013
Revises: 20260831_0012
Create Date: 2026-08-31
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260831_0013"
down_revision: str | None = "20260831_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add write-only provider credentials and optional profile references."""

    op.create_table(
        "model_provider_credential",
        sa.Column("model_credential_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("secret_ref", sa.String(length=500), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("created_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('active', 'disabled')",
            name="model_provider_credential_status",
        ),
        sa.PrimaryKeyConstraint(
            "model_credential_id",
            name=op.f("pk_model_provider_credential"),
        ),
        sa.UniqueConstraint(
            "provider",
            "name",
            name="uq_model_provider_credential_name",
        ),
    )
    op.create_index(
        "ix_model_provider_credential_provider_status",
        "model_provider_credential",
        ["provider", "status"],
    )
    op.add_column("model_profile", sa.Column("model_credential_id", sa.Uuid()))
    op.create_foreign_key(
        "fk_model_profile_model_credential",
        "model_profile",
        "model_provider_credential",
        ["model_credential_id"],
        ["model_credential_id"],
        ondelete="RESTRICT",
    )
    op.create_table(
        "usage_ledger",
        sa.Column("usage_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("trace_id", sa.String(length=128), nullable=False),
        sa.Column("model_provider", sa.String(length=80), nullable=False),
        sa.Column("model_name", sa.String(length=120), nullable=False),
        sa.Column("input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("output_tokens", sa.BigInteger(), nullable=False),
        sa.Column("total_tokens", sa.BigInteger(), nullable=False),
        sa.Column("estimated_cost", sa.Numeric(20, 8), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.CheckConstraint(
            "input_tokens >= 0 AND output_tokens >= 0 AND total_tokens >= 0",
            name="usage_ledger_tokens_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_usage_ledger_tenant_agent",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("usage_id", name=op.f("pk_usage_ledger")),
        sa.UniqueConstraint(
            "tenant_id",
            "request_id",
            name="uq_usage_ledger_tenant_request",
        ),
    )
    op.create_index(
        "ix_usage_ledger_scope_occurred",
        "usage_ledger",
        ["tenant_id", "occurred_at"],
    )


def downgrade() -> None:
    """Restore the legacy catalog/profile credential shape."""

    op.drop_index("ix_usage_ledger_scope_occurred", table_name="usage_ledger")
    op.drop_table("usage_ledger")
    op.drop_constraint(
        "fk_model_profile_model_credential",
        "model_profile",
        type_="foreignkey",
    )
    op.drop_column("model_profile", "model_credential_id")
    op.drop_index(
        "ix_model_provider_credential_provider_status",
        table_name="model_provider_credential",
    )
    op.drop_table("model_provider_credential")

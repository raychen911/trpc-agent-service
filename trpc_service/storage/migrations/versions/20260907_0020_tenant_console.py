"""Add tenant console login sessions and encrypted tenant credentials.

Revision ID: 20260907_0020
Revises: 20260905_0019
Create Date: 2026-09-07
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260907_0020"
down_revision: str | None = "20260905_0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create password, browser-session, and encrypted tenant-secret tables."""

    op.create_table(
        "management_password_credential",
        sa.Column("password_credential_id", sa.Uuid(), nullable=False),
        sa.Column("management_principal_id", sa.Uuid(), nullable=False),
        sa.Column("username", sa.String(length=120), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("failed_attempts", sa.Integer(), nullable=False),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("password_changed_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.Column("created_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.CheckConstraint("failed_attempts >= 0", name="management_password_failed_nonnegative"),
        sa.ForeignKeyConstraint(
            ["management_principal_id"],
            ["management_principal.management_principal_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("password_credential_id"),
        sa.UniqueConstraint("management_principal_id", name="uq_management_password_principal"),
        sa.UniqueConstraint("username", name="uq_management_password_username"),
    )
    op.create_table(
        "management_web_session",
        sa.Column("web_session_id", sa.Uuid(), nullable=False),
        sa.Column("management_principal_id", sa.Uuid(), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("csrf_hash", sa.String(length=64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.Column("last_seen_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["management_principal_id"],
            ["management_principal.management_principal_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("web_session_id"),
        sa.UniqueConstraint("token_hash"),
    )
    op.create_index("ix_management_web_session_expiry", "management_web_session", ["expires_at"])
    op.create_index(
        "ix_management_web_session_principal",
        "management_web_session",
        ["management_principal_id"],
    )
    op.create_table(
        "tenant_secret",
        sa.Column("tenant_secret_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("ciphertext", sa.Text(), nullable=False),
        sa.Column("nonce", sa.String(length=32), nullable=False),
        sa.Column("key_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("created_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.CheckConstraint("key_version > 0", name="tenant_secret_key_version_positive"),
        sa.CheckConstraint("status IN ('active', 'disabled')", name="tenant_secret_status"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("tenant_secret_id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_tenant_secret_name"),
    )
    op.create_index(
        "ix_tenant_secret_scope_status",
        "tenant_secret",
        ["tenant_id", "status"],
    )


def downgrade() -> None:
    """Remove tenant console credentials and their encrypted secret storage."""

    op.drop_index("ix_tenant_secret_scope_status", table_name="tenant_secret")
    op.drop_table("tenant_secret")
    op.drop_index("ix_management_web_session_principal", table_name="management_web_session")
    op.drop_index("ix_management_web_session_expiry", table_name="management_web_session")
    op.drop_table("management_web_session")
    op.drop_table("management_password_credential")

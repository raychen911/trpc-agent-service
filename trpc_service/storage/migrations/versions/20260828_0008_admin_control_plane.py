"""Add authenticated administrator control-plane resources.

Revision ID: 20260828_0008
Revises: 20260828_0007
Create Date: 2026-08-28
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260828_0008"
down_revision: str | None = "20260828_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> tuple[sa.Column[object], sa.Column[object]]:
    """Return the common server-backed creation and update columns."""

    return (
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def upgrade() -> None:
    """Create identities, RBAC, catalogs, tenant model profiles and audit."""

    op.create_table(
        "management_principal",
        sa.Column("management_principal_id", sa.Uuid(), nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=False),
        sa.Column("principal_type", sa.String(length=20), nullable=False),
        sa.Column("external_subject", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        *_timestamps(),
        sa.CheckConstraint(
            "principal_type IN ('human', 'service')",
            name="management_principal_type",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'disabled')",
            name="management_principal_status",
        ),
        sa.PrimaryKeyConstraint("management_principal_id", name=op.f("pk_management_principal")),
        sa.UniqueConstraint("external_subject",
                            name=op.f("uq_management_principal_external_subject")),
    )
    op.create_table(
        "model_catalog_entry",
        sa.Column("model_catalog_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model_name", sa.String(length=120), nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=False),
        sa.Column("capabilities", sa.JSON(), nullable=False),
        sa.Column("default_limits", sa.JSON(), nullable=False),
        sa.Column("platform_secret_ref", sa.String(length=500), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        *_timestamps(),
        sa.CheckConstraint("status IN ('active', 'disabled')", name="model_catalog_status"),
        sa.PrimaryKeyConstraint("model_catalog_id", name=op.f("pk_model_catalog_entry")),
        sa.UniqueConstraint("provider", "model_name", name="uq_model_catalog_provider_model"),
    )
    op.create_table(
        "channel_adapter_type",
        sa.Column("channel_type", sa.String(length=40), nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=False),
        sa.Column("adapter_version", sa.String(length=40), nullable=False),
        sa.Column("config_schema", sa.JSON(), nullable=False),
        sa.Column("secret_schema", sa.JSON(), nullable=False),
        sa.Column("capabilities", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        *_timestamps(),
        sa.CheckConstraint("status IN ('active', 'disabled')", name="channel_adapter_status"),
        sa.PrimaryKeyConstraint("channel_type", name=op.f("pk_channel_adapter_type")),
    )
    op.create_table(
        "management_role_assignment",
        sa.Column("role_assignment_id", sa.Uuid(), nullable=False),
        sa.Column("management_principal_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=True),
        sa.Column("role", sa.String(length=40), nullable=False),
        *_timestamps(),
        sa.CheckConstraint(
            "(role = 'platform_admin' AND tenant_id IS NULL) OR "
            "(role <> 'platform_admin' AND tenant_id IS NOT NULL)",
            name="management_role_scope",
        ),
        sa.CheckConstraint(
            "role IN ('platform_admin', 'tenant_owner', 'tenant_admin', 'agent_manager', "
            "'channel_manager', 'auditor', 'viewer')",
            name="management_role_value",
        ),
        sa.ForeignKeyConstraint(
            ["management_principal_id"],
            ["management_principal.management_principal_id"],
            name=op.f("fk_management_role_assignment_management_principal_id_management_principal"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant.tenant_id"],
            name=op.f("fk_management_role_assignment_tenant_id_tenant"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("role_assignment_id", name=op.f("pk_management_role_assignment")),
        sa.UniqueConstraint(
            "management_principal_id",
            "tenant_id",
            "role",
            name="uq_management_role_assignment_scope",
        ),
    )
    op.create_index(
        "ix_management_role_assignment_tenant",
        "management_role_assignment",
        ["tenant_id", "role"],
    )
    op.create_index(
        "uq_management_role_assignment_platform",
        "management_role_assignment",
        ["management_principal_id", "role"],
        unique=True,
        postgresql_where=sa.text("tenant_id IS NULL"),
    )
    op.create_table(
        "management_credential",
        sa.Column("credential_id", sa.Uuid(), nullable=False),
        sa.Column("management_principal_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("token_prefix", sa.String(length=24), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.CheckConstraint(
            "status IN ('active', 'disabled')",
            name="management_credential_status",
        ),
        sa.ForeignKeyConstraint(
            ["management_principal_id"],
            ["management_principal.management_principal_id"],
            name=op.f("fk_management_credential_management_principal_id_management_principal"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("credential_id", name=op.f("pk_management_credential")),
        sa.UniqueConstraint("token_hash", name=op.f("uq_management_credential_token_hash")),
    )
    op.create_index(
        "ix_management_credential_principal",
        "management_credential",
        ["management_principal_id"],
    )
    op.create_table(
        "model_profile",
        sa.Column("model_profile_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("model_catalog_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("credential_mode", sa.String(length=30), nullable=False),
        sa.Column("secret_ref", sa.String(length=500), nullable=True),
        sa.Column("parameter_config", sa.JSON(), nullable=False),
        sa.Column("limits", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        *_timestamps(),
        sa.CheckConstraint(
            "credential_mode IN ('tenant_managed', 'platform_managed')",
            name="model_profile_credential_mode",
        ),
        sa.CheckConstraint(
            "(credential_mode = 'tenant_managed' AND secret_ref IS NOT NULL) OR "
            "(credential_mode = 'platform_managed' AND secret_ref IS NULL)",
            name="model_profile_secret_ownership",
        ),
        sa.CheckConstraint("status IN ('active', 'disabled')", name="model_profile_status"),
        sa.ForeignKeyConstraint(
            ["model_catalog_id"],
            ["model_catalog_entry.model_catalog_id"],
            name=op.f("fk_model_profile_model_catalog_id_model_catalog_entry"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant.tenant_id"],
            name=op.f("fk_model_profile_tenant_id_tenant"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("model_profile_id", name=op.f("pk_model_profile")),
        sa.UniqueConstraint("tenant_id", "model_profile_id", name="uq_model_profile_tenant_id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_model_profile_tenant_name"),
    )
    op.create_index(
        "ix_model_profile_tenant_status",
        "model_profile",
        ["tenant_id", "status"],
    )
    op.create_table(
        "management_audit_log",
        sa.Column("audit_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=True),
        sa.Column("actor_subject", sa.String(length=255), nullable=False),
        sa.Column("actor_roles", sa.JSON(), nullable=False),
        sa.Column("action", sa.String(length=120), nullable=False),
        sa.Column("resource_type", sa.String(length=80), nullable=False),
        sa.Column("resource_id", sa.String(length=255), nullable=False),
        sa.Column("decision", sa.String(length=30), nullable=False, server_default="allowed"),
        sa.Column("reason", sa.String(length=500), nullable=True),
        sa.Column("details_redacted", sa.JSON(), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("immutable_marker", sa.Text(), nullable=False, server_default="append-only"),
        sa.PrimaryKeyConstraint("audit_id", name=op.f("pk_management_audit_log")),
    )
    op.create_index(
        "ix_management_audit_tenant_occurred",
        "management_audit_log",
        ["tenant_id", "occurred_at"],
    )
    op.create_index(
        "ix_management_audit_actor_occurred",
        "management_audit_log",
        ["actor_subject", "occurred_at"],
    )

    op.add_column("agent_app", sa.Column("model_profile_id", sa.Uuid(), nullable=True))
    op.create_index(op.f("ix_agent_app_model_profile_id"), "agent_app", ["model_profile_id"])
    op.create_foreign_key(
        "fk_agent_app_tenant_model_profile",
        "agent_app",
        "model_profile",
        ["tenant_id", "model_profile_id"],
        ["tenant_id", "model_profile_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Remove administrator resources in reverse dependency order."""

    op.drop_constraint("fk_agent_app_tenant_model_profile", "agent_app", type_="foreignkey")
    op.drop_index(op.f("ix_agent_app_model_profile_id"), table_name="agent_app")
    op.drop_column("agent_app", "model_profile_id")
    op.drop_index("ix_management_audit_actor_occurred", table_name="management_audit_log")
    op.drop_index("ix_management_audit_tenant_occurred", table_name="management_audit_log")
    op.drop_table("management_audit_log")
    op.drop_index("ix_model_profile_tenant_status", table_name="model_profile")
    op.drop_table("model_profile")
    op.drop_index("ix_management_credential_principal", table_name="management_credential")
    op.drop_table("management_credential")
    op.drop_index(
        "uq_management_role_assignment_platform",
        table_name="management_role_assignment",
    )
    op.drop_index(
        "ix_management_role_assignment_tenant",
        table_name="management_role_assignment",
    )
    op.drop_table("management_role_assignment")
    op.drop_table("channel_adapter_type")
    op.drop_table("model_catalog_entry")
    op.drop_table("management_principal")

"""Persistence models for authenticated control-plane management."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    func,
    Index,
    Integer,
    JSON,
    String,
    Text,
    text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from trpc_service.storage.orm import Base, TimestampMixin, utc_now


class ManagementPrincipal(TimestampMixin, Base):
    """Human or service identity that can receive management roles."""

    __tablename__ = "management_principal"
    __table_args__ = (
        CheckConstraint("principal_type IN ('human', 'service')", name="management_principal_type"),
        CheckConstraint("status IN ('active', 'disabled')", name="management_principal_status"),
    )

    management_principal_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    principal_type: Mapped[str] = mapped_column(String(20), nullable=False)
    external_subject: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")


class RoleAssignment(TimestampMixin, Base):
    """One platform-wide or tenant-scoped role granted to a principal."""

    __tablename__ = "management_role_assignment"
    __table_args__ = (
        UniqueConstraint(
            "management_principal_id",
            "tenant_id",
            "role",
            name="uq_management_role_assignment_scope",
        ),
        CheckConstraint(
            "(role = 'platform_admin' AND tenant_id IS NULL) OR "
            "(role = 'tenant_admin' AND tenant_id IS NOT NULL)",
            name="management_role_scope",
        ),
        CheckConstraint(
            "role IN ('platform_admin', 'tenant_admin')",
            name="management_role_value",
        ),
        # SQL considers NULL values distinct in an ordinary unique constraint.
        # This partial index prevents duplicate platform-wide grants.
        Index(
            "uq_management_role_assignment_platform",
            "management_principal_id",
            "role",
            unique=True,
            postgresql_where=text("tenant_id IS NULL"),
            sqlite_where=text("tenant_id IS NULL"),
        ),
        # A tenant has one administrator identity. IM participants are channel
        # identities and never receive control-plane roles.
        Index(
            "uq_management_role_assignment_tenant_admin",
            "tenant_id",
            unique=True,
            postgresql_where=text("tenant_id IS NOT NULL"),
            sqlite_where=text("tenant_id IS NOT NULL"),
        ),
        Index(
            "uq_management_role_assignment_tenant_principal",
            "management_principal_id",
            unique=True,
            postgresql_where=text("tenant_id IS NOT NULL"),
            sqlite_where=text("tenant_id IS NOT NULL"),
        ),
        Index("ix_management_role_assignment_tenant", "tenant_id", "role"),
    )

    role_assignment_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    management_principal_id: Mapped[UUID] = mapped_column(
        ForeignKey("management_principal.management_principal_id", ondelete="CASCADE"),
        nullable=False,
    )
    tenant_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="CASCADE"), )
    role: Mapped[str] = mapped_column(String(40), nullable=False)


class ManagementCredential(TimestampMixin, Base):
    """Revocable high-entropy API credential stored only as a digest."""

    __tablename__ = "management_credential"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'disabled')", name="management_credential_status"),
        Index("ix_management_credential_principal", "management_principal_id"),
    )

    credential_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    management_principal_id: Mapped[UUID] = mapped_column(
        ForeignKey("management_principal.management_principal_id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    token_prefix: Mapped[str] = mapped_column(String(24), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ManagementPasswordCredential(TimestampMixin, Base):
    """Password login credential kept separate from the management identity."""

    __tablename__ = "management_password_credential"
    __table_args__ = (
        UniqueConstraint("management_principal_id", name="uq_management_password_principal"),
        UniqueConstraint("username", name="uq_management_password_username"),
        CheckConstraint("failed_attempts >= 0", name="password_failed_nonnegative"),
    )

    password_credential_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    management_principal_id: Mapped[UUID] = mapped_column(
        ForeignKey("management_principal.management_principal_id", ondelete="CASCADE"),
        nullable=False,
    )
    username: Mapped[str] = mapped_column(String(120), nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    failed_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    password_changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )


class ManagementWebSession(Base):
    """Revocable server-side browser session identified by a hashed cookie."""

    __tablename__ = "management_web_session"
    __table_args__ = (
        Index("ix_management_web_session_principal", "management_principal_id"),
        Index("ix_management_web_session_expiry", "expires_at"),
    )

    web_session_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    management_principal_id: Mapped[UUID] = mapped_column(
        ForeignKey("management_principal.management_principal_id", ondelete="CASCADE"),
        nullable=False,
    )
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    csrf_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )


class TenantSecret(TimestampMixin, Base):
    """Encrypted tenant credential whose plaintext never enters configuration."""

    __tablename__ = "tenant_secret"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_tenant_secret_name"),
        CheckConstraint("status IN ('active', 'disabled')", name="tenant_secret_status"),
        CheckConstraint("key_version > 0", name="tenant_secret_key_version_positive"),
        Index("ix_tenant_secret_scope_status", "tenant_id", "status"),
    )

    tenant_secret_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    ciphertext: Mapped[str] = mapped_column(Text, nullable=False)
    nonce: Mapped[str] = mapped_column(String(32), nullable=False)
    key_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")


class ModelCatalogEntry(TimestampMixin, Base):
    """Platform-approved model capability and its global policy boundary."""

    __tablename__ = "model_catalog_entry"
    __table_args__ = (
        UniqueConstraint("provider", "model_name", name="uq_model_catalog_provider_model"),
        CheckConstraint("status IN ('active', 'disabled')", name="model_catalog_status"),
    )

    model_catalog_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    provider: Mapped[str] = mapped_column(String(80), nullable=False)
    model_name: Mapped[str] = mapped_column(String(120), nullable=False)
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    default_limits: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    platform_secret_ref: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")


class ModelProviderCredential(TimestampMixin, Base):
    """Platform-owned model provider credential referenced without exposing its value."""

    __tablename__ = "model_provider_credential"
    __table_args__ = (
        UniqueConstraint("provider", "name", name="uq_model_provider_credential_name"),
        CheckConstraint("status IN ('active', 'disabled')",
                        name="model_provider_credential_status"),
        Index("ix_model_provider_credential_provider_status", "provider", "status"),
    )

    model_credential_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    provider: Mapped[str] = mapped_column(String(80), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    secret_ref: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")


class ModelProfile(TimestampMixin, Base):
    """Platform-managed model policy applied to one tenant."""

    __tablename__ = "model_profile"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_model_profile_tenant_name"),
        UniqueConstraint("tenant_id", "model_profile_id", name="uq_model_profile_tenant_id"),
        CheckConstraint(
            "credential_mode IN ('tenant_managed', 'platform_managed')",
            name="model_profile_credential_mode",
        ),
        CheckConstraint(
            "(credential_mode = 'tenant_managed' AND secret_ref IS NOT NULL) OR "
            "(credential_mode = 'platform_managed' AND secret_ref IS NULL)",
            name="model_profile_secret_ownership",
        ),
        CheckConstraint("status IN ('active', 'disabled')", name="model_profile_status"),
        Index("ix_model_profile_tenant_status", "tenant_id", "status"),
    )

    model_profile_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        nullable=False,
    )
    model_catalog_id: Mapped[UUID] = mapped_column(
        ForeignKey("model_catalog_entry.model_catalog_id", ondelete="RESTRICT"),
        nullable=False,
    )
    model_credential_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("model_provider_credential.model_credential_id", ondelete="RESTRICT"), )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    credential_mode: Mapped[str] = mapped_column(String(30), nullable=False)
    secret_ref: Mapped[str | None] = mapped_column(String(500))
    parameter_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    limits: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")


class ChannelAdapterType(TimestampMixin, Base):
    """Platform-installed IM adapter contract available for tenant bindings."""

    __tablename__ = "channel_adapter_type"
    __table_args__ = (CheckConstraint("status IN ('active', 'disabled')",
                                      name="channel_adapter_status"), )

    channel_type: Mapped[str] = mapped_column(String(40), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    adapter_version: Mapped[str] = mapped_column(String(40), nullable=False)
    config_schema: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    secret_schema: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")


class ManagementAuditLog(Base):
    """Append-only record of control-plane decisions and mutations."""

    __tablename__ = "management_audit_log"
    __table_args__ = (
        Index("ix_management_audit_tenant_occurred", "tenant_id", "occurred_at"),
        Index("ix_management_audit_actor_occurred", "actor_subject", "occurred_at"),
    )

    audit_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID | None] = mapped_column(Uuid)
    actor_subject: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_roles: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    action: Mapped[str] = mapped_column(String(120), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(80), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(255), nullable=False)
    decision: Mapped[str] = mapped_column(String(30), nullable=False, default="allowed")
    reason: Mapped[str | None] = mapped_column(String(500))
    details_redacted: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )
    immutable_marker: Mapped[str] = mapped_column(Text, nullable=False, default="append-only")

"""External channel binding persistence model."""

import secrets
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from trpc_service.channels.contracts import ChannelBindingConfig
from trpc_service.storage.orm import Base, TimestampMixin

JSON_VALUE = JSON().with_variant(JSONB(), "postgresql")


def generate_binding_public_id() -> str:
    """Generate an unguessable identifier safe to expose to channel callbacks."""

    return secrets.token_urlsafe(24)


class ChannelBinding(TimestampMixin, Base):
    """Bind one external messaging account to a tenant-owned Agent."""

    __tablename__ = "channel_binding"
    __table_args__ = (
        UniqueConstraint("tenant_id", "binding_id", name="uq_channel_binding_tenant_id"),
        # A provider account can be moved after a binding is disabled, but two
        # active tenants must never consume the same Bot connection.
        Index(
            "uq_channel_binding_active_external_account",
            "channel_type",
            "external_account_hash",
            unique=True,
            postgresql_where=text("status = 'active'"),
            sqlite_where=text("status = 'active'"),
        ),
        # Matching tenant_id in this foreign key prevents a binding
        # from referencing an Agent owned by another tenant.
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_channel_binding_tenant_agent",
            ondelete="RESTRICT",
        ))

    binding_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    binding_public_id: Mapped[str] = mapped_column(
        String(64),
        unique=True,
        nullable=False,
        default=generate_binding_public_id,
    )
    tenant_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    agent_app_id: Mapped[UUID] = mapped_column(
        Uuid,
        nullable=False,
        index=True,
    )
    channel_type: Mapped[str] = mapped_column(String(40), nullable=False)
    external_account_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    account_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    secret_ref_map: Mapped[dict[str, str]] = mapped_column(JSON, default=dict, nullable=False)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="active", nullable=False)

    def to_config(self) -> ChannelBindingConfig:
        """Project the persistence row into the provider-neutral adapter value."""

        return ChannelBindingConfig(
            binding_id=self.binding_id,
            tenant_id=self.tenant_id,
            agent_app_id=self.agent_app_id,
            channel_type=self.channel_type,
            account_config=self.account_config,
            secret_ref_map=self.secret_ref_map,
            capabilities=self.capabilities,
        )


class ChannelPrincipal(TimestampMixin, Base):
    """Tenant-owned human or service identity independent of one IM account."""

    __tablename__ = "channel_principal"
    __table_args__ = (
        CheckConstraint(
            "status IN ('ACTIVE', 'DISABLED', 'DELETED')",
            name="channel_principal_status",
        ),
        CheckConstraint(
            "principal_type IN ('USER', 'SERVICE', 'BOT')",
            name="channel_principal_type",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    principal_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    principal_type: Mapped[str] = mapped_column(String(30), nullable=False, default="USER")
    display_name: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)


class ChannelIdentity(TimestampMixin, Base):
    """Bind one provider-specific account identifier to a tenant principal."""

    __tablename__ = "channel_identity"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            name="fk_channel_identity_tenant_binding",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            ["channel_principal.tenant_id", "channel_principal.principal_id"],
            name="fk_channel_identity_tenant_principal",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "binding_id",
            "provider_principal_id",
            name="uq_channel_identity_provider",
        ),
        CheckConstraint(
            "status IN ('ACTIVE', 'DISABLED', 'DELETED')",
            name="channel_identity_status",
        ),
        Index("ix_channel_identity_principal", "tenant_id", "principal_id"),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    identity_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    binding_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    principal_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    provider_principal_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)


class ChannelConversation(TimestampMixin, Base):
    """Stable tenant conversation mapped from one provider chat identity."""

    __tablename__ = "channel_conversation"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            name="fk_channel_conversation_tenant_binding",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "binding_id",
            "provider_conversation_id",
            name="uq_channel_conversation_provider",
        ),
        CheckConstraint(
            "kind IN ('DIRECT', 'GROUP', 'THREAD')",
            name="channel_conversation_kind",
        ),
        CheckConstraint(
            "status IN ('ACTIVE', 'CLOSED', 'DELETED')",
            name="channel_conversation_status",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    binding_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    provider_conversation_id: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    title: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    last_message_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)


class ConversationMember(TimestampMixin, Base):
    """Record tenant principal membership in a normalized conversation."""

    __tablename__ = "conversation_member"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "conversation_id"],
            ["channel_conversation.tenant_id", "channel_conversation.conversation_id"],
            name="fk_conversation_member_tenant_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "role IN ('MEMBER', 'OWNER', 'ADMIN', 'BOT')",
            name="conversation_member_role",
        ),
        CheckConstraint(
            "status IN ('ACTIVE', 'LEFT', 'REMOVED')",
            name="conversation_member_status",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            ["channel_principal.tenant_id", "channel_principal.principal_id"],
            name="fk_conversation_member_tenant_principal",
            ondelete="RESTRICT",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    principal_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    role: Mapped[str] = mapped_column(String(30), nullable=False, default="MEMBER")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")

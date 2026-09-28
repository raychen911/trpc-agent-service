"""Agent application persistence model."""

from typing import Any
from uuid import UUID, uuid4

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    JSON,
    ForeignKey,
    func,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from trpc_service.storage.orm import Base, TimestampMixin


class AgentApp(TimestampMixin, Base):
    """Tenant-owned Agent configuration and backend selection."""

    __tablename__ = "agent_app"
    __table_args__ = (
        # Revision 20260825_0002 makes this key the target of ChannelBinding's
        # composite foreign key, enforcing tenant ownership in the database.
        UniqueConstraint("tenant_id", "agent_app_id", name="uq_agent_app_tenant_id"),
        UniqueConstraint("tenant_id", "name", name="uq_agent_app_tenant_name"),
        ForeignKeyConstraint(
            ["tenant_id", "model_profile_id"],
            ["model_profile.tenant_id", "model_profile.model_profile_id"],
            name="fk_agent_app_tenant_model_profile",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "canary_percent >= 0 AND canary_percent <= 99",
            name="agent_app_canary_percent",
        ),
        CheckConstraint(
            "(canary_config_version IS NULL AND canary_percent = 0) OR "
            "(canary_config_version IS NOT NULL AND canary_percent > 0)",
            name="agent_app_canary_pointer",
        ),
    )

    agent_app_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    model_profile_id: Mapped[UUID | None] = mapped_column(Uuid, index=True)
    status: Mapped[str] = mapped_column(String(20), default="active", nullable=False)
    application_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    model_settings: Mapped[dict[str, Any]] = mapped_column("model_config",
                                                           JSON,
                                                           default=dict,
                                                           nullable=False)
    tool_permissions: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    knowledge_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    backend_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    stable_config_version: Mapped[int] = mapped_column(
        BigInteger,
        default=1,
        server_default="1",
        nullable=False,
    )
    canary_config_version: Mapped[int | None] = mapped_column(BigInteger)
    canary_percent: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
    )


class AgentConfigVersion(Base):
    """Immutable execution configuration snapshot owned by one Agent App."""

    __tablename__ = "agent_config_version"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_agent_config_version_tenant_agent",
            ondelete="RESTRICT",
        ),
        CheckConstraint("version > 0", name="agent_config_version_positive"),
        CheckConstraint(
            "status IN ('draft', 'released', 'retired')",
            name="agent_config_version_status",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    version: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="draft")
    # JSONB keeps PostgreSQL snapshots queryable while plain JSON preserves the
    # lightweight SQLite contract used by unit tests.
    snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(postgresql.JSONB(), "postgresql"),
        nullable=False,
    )
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    reason: Mapped[str] = mapped_column(String(500), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

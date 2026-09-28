"""Persistence model for tenant-owned remote MCP connections."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from trpc_service.storage.orm import Base, TimestampMixin


class MCPConnection(TimestampMixin, Base):
    """Describe one remote MCP Server without storing its credential inline."""

    __tablename__ = "mcp_connection"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_mcp_connection_tenant_name"),
        UniqueConstraint(
            "tenant_id",
            "connection_id",
            name="uq_mcp_connection_tenant_id",
        ),
        CheckConstraint("status IN ('active', 'disabled')", name="mcp_connection_status"),
        CheckConstraint("auth_type IN ('none', 'bearer')", name="mcp_connection_auth_type"),
    )

    connection_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    endpoint_url: Mapped[str] = mapped_column(String(1000), nullable=False)
    auth_type: Mapped[str] = mapped_column(String(20), nullable=False, default="none")
    secret_ref: Mapped[str | None] = mapped_column(String(500))
    timeout_seconds: Mapped[int] = mapped_column(nullable=False, default=10)
    tool_catalog: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)
    catalog_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")

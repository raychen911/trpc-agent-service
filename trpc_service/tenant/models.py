"""Tenant persistence model for the control plane."""

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from trpc_service.storage.orm import Base, TimestampMixin


class Tenant(TimestampMixin, Base):
    """Top-level isolation boundary for all tenant-owned resources."""

    __tablename__ = "tenant"

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    status: Mapped[str] = mapped_column(String(20), default="active")
    isolation_mode: Mapped[str] = mapped_column(String(20), default="shared")
    audit_policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

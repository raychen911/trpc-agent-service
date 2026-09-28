"""Tenant API request and response schemas."""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.config.models import NonNullableUpdateModel


class TenantStatus(StrEnum):
    """Lifecycle states exposed by the tenant API."""

    ACTIVE = "active"
    DISABLED = "disabled"


class IsolationMode(StrEnum):
    """Supported physical data-isolation strategies."""

    SHARED = "shared"
    SCHEMA = "schema"
    DATABASE = "database"


class TenantCreate(BaseModel):
    """Fields accepted when provisioning a tenant."""

    name: str = Field(min_length=1, max_length=120)
    isolation_mode: IsolationMode = IsolationMode.SHARED
    audit_policy: dict[str, Any] = Field(default_factory=dict)


class TenantUpdate(NonNullableUpdateModel):
    """Mutable tenant fields; omitted values remain unchanged."""

    name: str | None = Field(default=None, min_length=1, max_length=120)
    status: TenantStatus | None = None
    isolation_mode: IsolationMode | None = None
    audit_policy: dict[str, Any] | None = None


class TenantRead(BaseModel):
    """Public tenant representation returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    tenant_id: UUID
    name: str
    status: TenantStatus
    isolation_mode: IsolationMode
    audit_policy: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class TenantList(BaseModel):
    """Paginated tenant collection."""

    items: list[TenantRead]
    total: int

"""Validated HTTP contracts for remote MCP connections."""

from datetime import datetime
from enum import IntEnum, StrEnum
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trpc_service.config.models import NonNullableUpdateModel, SecretRef


class MCPAuthType(StrEnum):
    NONE = "none"
    BEARER = "bearer"


class MCPConnectionStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"


class MCPToolRisk(IntEnum):
    """Interaction policy applied to tenant-granted MCP tools."""

    READ_ONLY_DIRECT = 0
    CONFIRMED_MUTATION = 2


def normalize_mcp_tool_risk(value: object) -> MCPToolRisk:
    """Fail closed when a persisted or remote risk value is malformed."""

    if value == MCPToolRisk.READ_ONLY_DIRECT and not isinstance(value, bool):
        return MCPToolRisk.READ_ONLY_DIRECT
    return MCPToolRisk.CONFIRMED_MUTATION


def _validate_endpoint(value: str) -> str:
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or parsed.hostname is None or parsed.username is not None
            or parsed.password is not None or parsed.fragment):
        raise ValueError("MCP endpoint must be an HTTPS URL without credentials or fragments")
    return value


def _validate_secret_ref(value: str | None) -> str | None:
    if value is not None:
        SecretRef(uri=value)
    return value


class MCPConnectionCreate(BaseModel):
    """Create a tenant MCP binding with a write-only optional credential."""

    name: str = Field(min_length=1, max_length=120)
    endpoint_url: str = Field(min_length=1, max_length=1000)
    auth_type: MCPAuthType = MCPAuthType.NONE
    secret_ref: str | None = None
    secret_value: str | None = Field(default=None, min_length=1, max_length=16_384, exclude=True)
    timeout_seconds: int = Field(default=10, ge=2, le=60)

    _endpoint = field_validator("endpoint_url")(_validate_endpoint)
    _secret_ref = field_validator("secret_ref")(_validate_secret_ref)

    @model_validator(mode="after")
    def validate_authentication(self) -> "MCPConnectionCreate":
        if self.secret_ref is not None and self.secret_value is not None:
            raise ValueError("provide secret_value or secret_ref, not both")
        if (self.auth_type is MCPAuthType.BEARER and self.secret_ref is None
                and self.secret_value is None):
            raise ValueError("bearer authentication requires a credential")
        if self.auth_type is MCPAuthType.NONE and (self.secret_ref is not None
                                                   or self.secret_value is not None):
            raise ValueError("unauthenticated MCP connections cannot store a credential")
        return self


class MCPConnectionUpdate(NonNullableUpdateModel):
    """Patch connection settings without ever reading the old credential."""

    name: str | None = Field(default=None, min_length=1, max_length=120)
    endpoint_url: str | None = Field(default=None, min_length=1, max_length=1000)
    auth_type: MCPAuthType | None = None
    secret_ref: str | None = None
    secret_value: str | None = Field(default=None, min_length=1, max_length=16_384, exclude=True)
    timeout_seconds: int | None = Field(default=None, ge=2, le=60)
    status: MCPConnectionStatus | None = None

    _endpoint = field_validator("endpoint_url")(_validate_endpoint)
    _secret_ref = field_validator("secret_ref")(_validate_secret_ref)

    @model_validator(mode="after")
    def reject_mixed_credentials(self) -> "MCPConnectionUpdate":
        if self.secret_ref is not None and self.secret_value is not None:
            raise ValueError("provide secret_value or secret_ref, not both")
        return self


class MCPConnectionRead(BaseModel):
    """Safe connection metadata exposed to tenant administrators."""

    model_config = ConfigDict(from_attributes=True)

    connection_id: UUID
    tenant_id: UUID
    name: str
    endpoint_url: str
    auth_type: MCPAuthType
    credential_configured: bool
    timeout_seconds: int
    tool_catalog: list[dict[str, Any]]
    catalog_refreshed_at: datetime | None
    last_error_code: str | None
    status: MCPConnectionStatus
    created_at: datetime
    updated_at: datetime


class MCPConnectionList(BaseModel):
    items: list[MCPConnectionRead]
    total: int

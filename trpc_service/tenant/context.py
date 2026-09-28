"""Immutable tenant identity propagated through an Agent execution."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class TenantContext(BaseModel):
    """Immutable tenant boundary propagated through one Agent execution."""

    model_config = ConfigDict(frozen=True)

    tenant_id: UUID
    agent_app_id: UUID
    config_version: int = Field(ge=1)
    request_id: str = Field(min_length=1, max_length=128)
    trace_id: str = Field(min_length=1, max_length=128)

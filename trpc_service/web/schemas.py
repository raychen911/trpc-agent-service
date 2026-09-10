"""Validated HTTP request and response bodies."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trpc_service.config.models import ChannelMode, ChannelType, TenantStorageConfig
from trpc_service.config.settings import validate_secret_ref


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TenantCreate(ApiModel):
    tenant_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1, max_length=200)
    audit_policy: dict[str, Any] = Field(default_factory=dict)
    storage_config: TenantStorageConfig = Field(default_factory=TenantStorageConfig)


class TenantStorageUpdate(ApiModel):
    storage_config: TenantStorageConfig


class AgentAppCreate(ApiModel):
    app_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1, max_length=200)
    system_prompt: str = Field(min_length=1, max_length=20_000)
    model_config_data: dict[str, Any] = Field(default_factory=dict)
    tool_policy: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def reject_literal_model_secrets(self) -> AgentAppCreate:
        if "api_key" in self.model_config_data:
            raise ValueError("use api_key_ref instead of a literal api_key")
        api_key_ref = self.model_config_data.get("api_key_ref")
        if api_key_ref is not None:
            if not isinstance(api_key_ref, str):
                raise ValueError("api_key_ref must be a string")
            validate_secret_ref(api_key_ref)
        return self


class ChannelBindingCreate(ApiModel):
    binding_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    app_id: str = Field(default="assistant", min_length=1, max_length=64)
    channel_type: ChannelType
    connection_mode: ChannelMode = ChannelMode.WEBHOOK
    account_id: str = Field(min_length=1, max_length=200)
    token_ref: str | None = None
    secret_ref: str | None = None
    aes_key_ref: str | None = None
    webhook_path: str = ""

    @field_validator("token_ref", "secret_ref", "aes_key_ref")
    @classmethod
    def validate_secret_references(cls, value: str | None) -> str | None:
        return validate_secret_ref(value)


class ChatRequest(ApiModel):
    app_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=200)
    message: str = Field(min_length=1, max_length=20_000)
    session_id: str | None = Field(default=None, min_length=1, max_length=200)


class ToolEventResponse(ApiModel):
    type: str
    name: str
    data: Any


class ChatResponse(ApiModel):
    tenant_id: str
    app_id: str
    user_id: str
    session_id: str
    trace_id: str
    reply: str
    tool_events: list[ToolEventResponse]


__all__ = [
    "AgentAppCreate",
    "ChannelBindingCreate",
    "ChatRequest",
    "ChatResponse",
    "TenantCreate",
    "TenantStorageUpdate",
    "ToolEventResponse",
]

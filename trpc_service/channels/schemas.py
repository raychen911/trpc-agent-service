"""Channel binding API schemas and secret-reference validation."""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator, model_validator

from trpc_service.config.models import (
    CHANNEL_NAME_MAX_LENGTH,
    CHANNEL_NAME_PATTERN,
    NonNullableUpdateModel,
    SecretRef,
    normalize_channel_type,
    validate_secret_bearing_config,
)

ChannelName = Annotated[
    str,
    BeforeValidator(normalize_channel_type),
    Field(min_length=1, max_length=CHANNEL_NAME_MAX_LENGTH, pattern=CHANNEL_NAME_PATTERN),
]


class ChannelBindingStatus(StrEnum):
    """Lifecycle states exposed by the channel binding API."""

    ACTIVE = "active"
    DISABLED = "disabled"


class ChannelBindingCreate(BaseModel):
    """Fields required to bind an external account to an Agent."""

    agent_app_id: UUID
    channel_type: ChannelName
    external_account_hash: str | None = Field(default=None, min_length=1, max_length=128)
    account_config: dict[str, Any] = Field(default_factory=dict)
    secret_ref_map: dict[str, str] = Field(default_factory=dict)
    secret_values: dict[str, str] = Field(default_factory=dict, exclude=True)
    capabilities: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def reject_mixed_secret_inputs(self) -> "ChannelBindingCreate":
        """Accept write-only values or portable references, never both in one request."""

        if self.secret_ref_map and self.secret_values:
            raise ValueError("provide secret_values or secret_ref_map, not both")
        return self

    @field_validator("secret_ref_map")
    @classmethod
    def validate_secret_refs(cls, value: dict[str, str]) -> dict[str, str]:
        """Require every named channel credential to be an external reference."""

        for reference in value.values():
            SecretRef(uri=reference)
        return value

    @field_validator("secret_values")
    @classmethod
    def validate_secret_values(cls, value: dict[str, str]) -> dict[str, str]:
        """Bound write-only provider values before they reach the SecretStore."""

        if any(not field.strip() or not secret.strip() or len(secret) > 16_384
               for field, secret in value.items()):
            raise ValueError("channel secret names and values must be non-empty and bounded")
        return value

    @field_validator("account_config", "capabilities")
    @classmethod
    def reject_plaintext_secrets(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Reject credentials hidden inside provider-specific JSON configuration."""

        return validate_secret_bearing_config(value)


class ChannelBindingUpdate(NonNullableUpdateModel):
    """Mutable binding fields; identity and channel type remain stable."""

    agent_app_id: UUID | None = None
    status: ChannelBindingStatus | None = None
    account_config: dict[str, Any] | None = None
    secret_ref_map: dict[str, str] | None = None
    secret_values: dict[str, str] | None = Field(default=None, exclude=True)
    capabilities: dict[str, Any] | None = None

    @field_validator("secret_ref_map")
    @classmethod
    def validate_secret_refs(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        """Apply SecretRef validation when the map is part of a PATCH request."""

        if value is not None:
            for reference in value.values():
                SecretRef(uri=reference)
        return value

    @field_validator("secret_values")
    @classmethod
    def validate_secret_values(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is not None and any(not field.strip() or not secret.strip() or len(secret) > 16_384
                                     for field, secret in value.items()):
            raise ValueError("channel secret names and values must be non-empty and bounded")
        return value

    @model_validator(mode="after")
    def reject_mixed_secret_inputs(self) -> "ChannelBindingUpdate":
        if self.secret_ref_map is not None and self.secret_values is not None:
            raise ValueError("provide secret_values or secret_ref_map, not both")
        return self

    @field_validator("account_config", "capabilities")
    @classmethod
    def reject_plaintext_secrets(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        """Apply recursive secret checks to partial provider configuration."""

        return validate_secret_bearing_config(value)


class ChannelBindingRead(BaseModel):
    """Public channel binding representation returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    binding_id: UUID
    binding_public_id: str
    tenant_id: UUID
    agent_app_id: UUID
    channel_type: ChannelName
    external_account_hash: str
    account_config: dict[str, Any]
    secret_fields: list[str]
    capabilities: dict[str, Any]
    status: ChannelBindingStatus
    created_at: datetime
    updated_at: datetime


class ChannelBindingList(BaseModel):
    """Paginated channel binding collection."""

    items: list[ChannelBindingRead]
    total: int


class DeliveryFailureStatus(StrEnum):
    """Operator-visible delivery states that require observation or action."""

    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    UNKNOWN = "UNKNOWN"
    DEAD_LETTER = "DEAD_LETTER"


class DeliveryFailureRead(BaseModel):
    """Safe delivery failure metadata; provider payloads are never exposed."""

    model_config = ConfigDict(from_attributes=True)

    tenant_id: UUID
    agent_app_id: UUID
    outbox_id: str
    binding_id: UUID | None
    request_id: str | None
    status: DeliveryFailureStatus
    attempt_count: int
    next_attempt_at: datetime | None
    last_error_code: str | None
    last_error_summary: str | None
    created_at: datetime
    updated_at: datetime


class DeliveryFailureList(BaseModel):
    items: list[DeliveryFailureRead]
    total: int


class DeliveryReplayRequest(BaseModel):
    """Explicit operator justification required before a terminal replay."""

    reason: str = Field(min_length=12, max_length=500)


class DeliveryReplayRead(BaseModel):
    outbox_id: str
    status: Literal["PENDING"]

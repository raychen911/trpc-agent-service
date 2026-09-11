"""Immutable, tenant-scoped bindings for authenticated IM accounts."""

from __future__ import annotations

import re
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StrictBool, StrictInt, field_validator

from trpc_service.tenant.context import validate_tenant_id

_ACCOUNT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_SECRET_REF = re.compile(r"^env:TRPC_[A-Z0-9_]+$")


class ChannelBinding(BaseModel):
    """One external IM account owned by exactly one tenant/application."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    binding_id: UUID
    tenant_id: str
    app_id: str
    channel: Literal["wecom", "feishu"]
    external_account_id: str
    secret_ref: str
    webhook_token_ref: str | None = None
    webhook_aes_key_ref: str | None = None
    enabled: StrictBool
    version: StrictInt

    @field_validator("tenant_id")
    @classmethod
    def _tenant(cls, value: str) -> str:
        validate_tenant_id(value)
        return value

    @field_validator("app_id", "external_account_id")
    @classmethod
    def _identifier(cls, value: str) -> str:
        if not isinstance(value, str) or _ACCOUNT.fullmatch(value.strip()) is None:
            raise ValueError("invalid channel binding identifier")
        return value.strip()

    @field_validator("secret_ref")
    @classmethod
    def _secret_ref(cls, value: str) -> str:
        if not isinstance(value, str) or _SECRET_REF.fullmatch(value) is None:
            raise ValueError("invalid channel binding secret reference")
        return value

    @field_validator("webhook_token_ref", "webhook_aes_key_ref")
    @classmethod
    def _optional_secret_ref(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or _SECRET_REF.fullmatch(value) is None:
            raise ValueError("invalid channel binding secret reference")
        return value

    @field_validator("version")
    @classmethod
    def _version(cls, value: int) -> int:
        if value < 1:
            raise ValueError("invalid channel binding version")
        return value


__all__ = ["ChannelBinding"]

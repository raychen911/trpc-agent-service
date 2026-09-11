"""Strict versioned protocol models for Gateway ↔ Worker communication."""

from __future__ import annotations

import uuid
from enum import StrEnum
from typing import Any, Literal

from pydantic import (
    BaseModel,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from trpc_service.tenant.context import validate_tenant_id

PROTOCOL_VERSION: int = 1
INTERNAL_TOKEN_HEADER: str = "X-TRPC-Internal-Token"
_MAX_MESSAGE_LENGTH: int = 8000


def _nonblank(v: str, field_name: str) -> str:
    stripped = v.strip()
    if not stripped:
        raise ValueError(f"{field_name} must be non-blank")
    return stripped


class WorkerErrorCode(StrEnum):
    TENANT_CONFIG_MISMATCH = "tenant_config_mismatch"
    TENANT_AGENT_CONFIGURATION = "tenant_agent_configuration"
    MODEL_CONFIGURATION = "model_configuration"
    MODEL_RUNTIME = "model_runtime"
    WORKER_UNAVAILABLE = "worker_unavailable"
    WORKER_TIMEOUT = "worker_timeout"
    INVALID_WORKER_RESPONSE = "invalid_worker_response"
    SESSION_BUSY = "session_busy"
    TENANT_REPOSITORY_UNAVAILABLE = "tenant_repository_unavailable"
    MESSAGE_IN_PROGRESS = "message_in_progress"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    APPROVAL_NOT_AVAILABLE = "approval_not_available"
    APPROVAL_IN_PROGRESS = "approval_in_progress"
    APPROVAL_CONFLICT = "approval_conflict"
    APPROVAL_CONFIG_STALE = "approval_config_stale"
    APPROVAL_REPOSITORY_UNAVAILABLE = "approval_repository_unavailable"
    APPROVAL_EXECUTION_FAILED = "approval_execution_failed"
    CONTENT_INPUT_BLOCKED = "content_input_blocked"
    USAGE_BUDGET_EXCEEDED = "usage_budget_exceeded"
    CHANNEL_DELIVERY_FAILED = "channel_delivery_failed"


class WorkerTask(BaseModel):
    model_config = {"extra": "forbid", "frozen": True}

    protocol_version: Literal[1]
    request_id: uuid.UUID
    tenant_id: StrictStr
    app_id: StrictStr
    config_version: StrictInt
    user_id: StrictStr
    channel: StrictStr
    session_id: StrictStr
    message_id: StrictStr
    message: StrictStr

    @model_validator(mode="before")
    @classmethod
    def _reject_bool_protocol(cls, data: Any) -> Any:
        if isinstance(data, dict):
            pv = data.get("protocol_version")
            if isinstance(pv, bool) or isinstance(pv, float):
                raise ValueError("protocol_version must be int")
        return data

    @field_validator("protocol_version")
    @classmethod
    def _check_protocol_version(cls, v: Any) -> int:
        if not isinstance(v, int) or isinstance(v, bool):
            raise ValueError("protocol_version must be int")
        if v != 1:
            raise ValueError("protocol_version must be 1")
        return v

    @field_validator("config_version")
    @classmethod
    def _check_config_version(cls, v: Any) -> int:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("config_version must be int")
        if v < 1:
            raise ValueError("config_version must be >= 1")
        return v

    @field_validator("tenant_id")
    @classmethod
    def _check_tenant_id(cls, v: str) -> str:
        v = _nonblank(v, "tenant_id")
        validate_tenant_id(v)
        return v

    @field_validator("app_id")
    @classmethod
    def _check_app_id(cls, v: str) -> str:
        return _nonblank(v, "app_id")

    @field_validator("user_id")
    @classmethod
    def _check_user_id(cls, v: str) -> str:
        return _nonblank(v, "user_id")

    @field_validator("channel")
    @classmethod
    def _check_channel(cls, v: str) -> str:
        return _nonblank(v, "channel")

    @field_validator("session_id")
    @classmethod
    def _check_session_id(cls, v: str) -> str:
        return _nonblank(v, "session_id")

    @field_validator("message_id")
    @classmethod
    def _check_message_id(cls, v: str) -> str:
        v = _nonblank(v, "message_id")
        if len(v) > 200:
            raise ValueError("message_id must be at most 200 characters")
        return v

    @field_validator("message")
    @classmethod
    def _check_message(cls, v: str) -> str:
        v = _nonblank(v, "message")
        if len(v) > _MAX_MESSAGE_LENGTH:
            raise ValueError(f"message must be at most {_MAX_MESSAGE_LENGTH} characters")
        return v


class WorkerApprovalData(BaseModel):
    """Public approval pointer — opaque id + tool name only, never args."""

    model_config = {"extra": "forbid", "frozen": True}

    approval_id: uuid.UUID
    tool_name: StrictStr

    @field_validator("tool_name")
    @classmethod
    def _check_tool_name(cls, v: str) -> str:
        v = _nonblank(v, "tool_name")
        if len(v) > 200:
            raise ValueError("tool_name too long")
        return v


class WorkerApprovalTask(BaseModel):
    """Decision task: who (internal identity) decides what, via which message."""

    model_config = {"extra": "forbid", "frozen": True}

    protocol_version: Literal[1]
    request_id: uuid.UUID
    tenant_id: StrictStr
    app_id: StrictStr
    config_version: StrictInt
    user_id: StrictStr
    channel: StrictStr
    session_id: StrictStr
    message_id: StrictStr
    approval_id: uuid.UUID
    decision: Literal["approve", "reject"]

    @model_validator(mode="before")
    @classmethod
    def _reject_bool_protocol(cls, data: Any) -> Any:
        if isinstance(data, dict):
            pv = data.get("protocol_version")
            if isinstance(pv, bool) or isinstance(pv, float):
                raise ValueError("protocol_version must be int")
        return data

    @field_validator("protocol_version")
    @classmethod
    def _check_protocol_version(cls, v: Any) -> int:
        if isinstance(v, bool) or not isinstance(v, int) or v != 1:
            raise ValueError("protocol_version must be 1")
        return v

    @field_validator("config_version")
    @classmethod
    def _check_config_version(cls, v: Any) -> int:
        if isinstance(v, bool) or not isinstance(v, int) or v < 1:
            raise ValueError("config_version must be >= 1")
        return v

    @field_validator("tenant_id")
    @classmethod
    def _check_tenant_id(cls, v: str) -> str:
        v = _nonblank(v, "tenant_id")
        validate_tenant_id(v)
        return v

    @field_validator("app_id", "user_id", "channel", "session_id")
    @classmethod
    def _check_nonblank(cls, v: str) -> str:
        return _nonblank(v, "field")

    @field_validator("message_id")
    @classmethod
    def _check_message_id(cls, v: str) -> str:
        v = _nonblank(v, "message_id")
        if len(v) > 200:
            raise ValueError("message_id must be at most 200 characters")
        return v


class WorkerApprovalResult(BaseModel):
    model_config = {"extra": "forbid", "frozen": True}

    protocol_version: Literal[1]
    request_id: uuid.UUID
    response: str
    error_code: WorkerErrorCode | None = None

    @field_validator("protocol_version")
    @classmethod
    def _check_protocol_version(cls, v: Any) -> int:
        if isinstance(v, bool) or not isinstance(v, int) or v != 1:
            raise ValueError("protocol_version must be 1")
        return v

    @model_validator(mode="after")
    def _check_success_error_combination(self) -> "WorkerApprovalResult":
        if self.error_code is not None and self.response:
            raise ValueError("error result must have empty response")
        return self


class WorkerToolCallData(BaseModel):
    model_config = {"extra": "forbid", "frozen": True}

    kind: Literal["call"]
    name: str
    args: dict[str, Any]


class WorkerToolResultData(BaseModel):
    model_config = {"extra": "forbid", "frozen": True}

    kind: Literal["result"]
    name: str
    response: Any


class WorkerChatResult(BaseModel):
    model_config = {"extra": "forbid", "frozen": True}

    protocol_version: Literal[1]
    request_id: uuid.UUID
    response: str
    error_code: WorkerErrorCode | None = None

    @field_validator("protocol_version")
    @classmethod
    def _check_protocol_version(cls, v: Any) -> int:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("protocol_version must be int")
        if v != 1:
            raise ValueError("protocol_version must be 1")
        return v

    @model_validator(mode="after")
    def _check_success_error_combination(self) -> "WorkerChatResult":
        if self.error_code is not None:
            if self.response:
                raise ValueError("error result must have empty response")
        return self


class WorkerEvent(BaseModel):
    model_config = {"extra": "forbid", "frozen": True}

    protocol_version: Literal[1]
    request_id: uuid.UUID
    type: Literal["delta", "tool", "done", "error", "approval"]
    data: str | WorkerToolCallData | WorkerToolResultData | WorkerApprovalData | None = None
    error_code: WorkerErrorCode | None = None

    @field_validator("protocol_version")
    @classmethod
    def _check_protocol_version(cls, v: Any) -> int:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("protocol_version must be int")
        if v != 1:
            raise ValueError("protocol_version must be 1")
        return v

    @model_validator(mode="after")
    def _check_type_data_combination(self) -> "WorkerEvent":
        t = self.type
        if t == "delta":
            if not isinstance(self.data, str):
                raise ValueError("delta event must have string data")
            if self.error_code is not None:
                raise ValueError("delta event must not have error_code")
        elif t == "tool":
            if not isinstance(self.data, (WorkerToolCallData, WorkerToolResultData)):
                raise ValueError("tool event must have structured tool data")
            if self.error_code is not None:
                raise ValueError("tool event must not have error_code")
        elif t == "approval":
            if not isinstance(self.data, WorkerApprovalData):
                raise ValueError("approval event must have structured approval data")
            if self.error_code is not None:
                raise ValueError("approval event must not have error_code")
        elif t == "done":
            if self.data is not None:
                raise ValueError("done event must have null data")
            if self.error_code is not None:
                raise ValueError("done event must not have error_code")
        elif t == "error":
            if self.data is not None:
                raise ValueError("error event must have null data")
            if self.error_code is None:
                raise ValueError("error event must have error_code")
        return self


__all__ = [
    "INTERNAL_TOKEN_HEADER",
    "PROTOCOL_VERSION",
    "WorkerApprovalData",
    "WorkerApprovalResult",
    "WorkerApprovalTask",
    "WorkerChatResult",
    "WorkerErrorCode",
    "WorkerEvent",
    "WorkerTask",
    "WorkerToolCallData",
    "WorkerToolResultData",
]

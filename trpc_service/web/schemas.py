"""Pydantic request/response models for the Stage 1 chat routes."""

from __future__ import annotations

from pydantic import BaseModel, Field, StrictStr, field_validator

MAX_INPUT_LENGTH = 8000


def _nonblank(v: str, field_name: str) -> str:
    stripped = v.strip()
    if not stripped:
        raise ValueError(f"{field_name} must be non-blank")
    return stripped


class ChatRequest(BaseModel):
    """Inbound chat payload used by both sync and SSE routes."""

    session_id: StrictStr = Field(min_length=1, max_length=200)
    message_id: StrictStr
    message: StrictStr = Field(min_length=1, max_length=MAX_INPUT_LENGTH)

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
        if len(v) > MAX_INPUT_LENGTH:
            raise ValueError(f"message must be at most {MAX_INPUT_LENGTH} characters")
        return v


class ChatResponse(BaseModel):
    """Synchronous chat reply."""

    session_id: str
    response: str


class HealthResponse(BaseModel):
    """Stage 0 health response kept intact."""

    status: str
    service: str
    version: str


__all__ = ["MAX_INPUT_LENGTH", "ChatRequest", "ChatResponse", "HealthResponse"]

# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Public HTTP request schemas."""

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1, max_length=64)
    app_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=256)
    session_id: str = Field(min_length=1, max_length=256)
    message: str = Field(min_length=1, max_length=32768)
    idempotency_key: str = Field(default="", max_length=256)
    approval_id: str = ""
    approval_token: str = ""
    approval_tool_name: str = ""
    approval_arguments_json: str = ""


class RollbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)


class ArtifactUploadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    app_id: str
    name: str = Field(min_length=1, max_length=256)
    mime_type: str = "application/octet-stream"
    content_base64: str = Field(min_length=1)


class KnowledgeWriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    app_id: str
    title: str
    text: str = Field(min_length=1)


class ApprovalCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    user_id: str
    session_id: str
    tool_name: str
    arguments_json: str


class ApprovalDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approve: bool
    actor: str = "admin"


class MigrationCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    resource_type: str
    source_backend: str
    target_backend: str
    batch_size: int = Field(default=100, ge=1, le=1000)
    shadow_sample_rate: float = Field(default=0.1, ge=0, le=1)
    rollback_window_seconds: int = Field(default=3600, ge=0)

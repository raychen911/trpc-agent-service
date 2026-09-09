# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Safe service entry points for SDK workspace runtimes."""

from __future__ import annotations

from typing import Any

from trpc_agent_sdk.code_executors import BaseWorkspaceRuntime
from trpc_agent_sdk.code_executors import ContainerWorkspaceRuntime
from trpc_agent_sdk.code_executors import LocalWorkspaceRuntime
from trpc_agent_sdk.code_executors import create_container_workspace_runtime
from trpc_agent_sdk.code_executors import create_local_workspace_runtime


def create_workspace_runtime(mode: str, *, environment: str, **kwargs: Any) -> BaseWorkspaceRuntime:
    """Create an SDK workspace while preventing local execution in production."""
    normalized = mode.strip().lower()
    if normalized == "local":
        if environment != "development":
            raise ValueError("local workspace is allowed only in development")
        return create_local_workspace_runtime(**kwargs)
    if normalized == "container":
        return create_container_workspace_runtime(**kwargs)
    raise ValueError(f"unsupported workspace mode: {mode!r}")


__all__ = [
    "BaseWorkspaceRuntime",
    "ContainerWorkspaceRuntime",
    "LocalWorkspaceRuntime",
    "create_workspace_runtime",
]

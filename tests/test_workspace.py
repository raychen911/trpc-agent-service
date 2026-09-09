# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import pytest
from trpc_agent_sdk.code_executors import LocalWorkspaceRuntime

from trpc_service.workspace import create_workspace_runtime


def test_development_can_reuse_sdk_local_workspace(tmp_path):
    runtime = create_workspace_runtime("local", environment="development", work_root=str(tmp_path))
    assert isinstance(runtime, LocalWorkspaceRuntime)
    assert runtime.describe().isolation == "local"


def test_production_rejects_unsafe_local_workspace():
    with pytest.raises(ValueError, match="only in development"):
        create_workspace_runtime("local", environment="production")


def test_unknown_workspace_mode_fails_closed():
    with pytest.raises(ValueError, match="unsupported workspace mode"):
        create_workspace_runtime("host-shell", environment="development")

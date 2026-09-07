# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Governance-specific exceptions."""

from __future__ import annotations


class BudgetExceededError(PermissionError):
    """Raised when a tenant exceeds its configured budget ceiling."""


class ToolConfirmationRequired(PermissionError):
    """Raised when a dangerous tool requires an explicit human confirmation.

    Carries a short-lived confirmation token that the user must echo back
    (typically via IM) to authorize the operation.
    """

    def __init__(self, token: str, tool_name: str, message: str | None = None) -> None:
        self.token = token
        self.tool_name = tool_name
        super().__init__(message or f"tool '{tool_name}' requires confirmation (token: {token})")

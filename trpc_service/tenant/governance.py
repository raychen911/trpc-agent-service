# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Tenant boundary checks and lightweight request budget enforcement."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import date

from trpc_service.config import ChannelBindingConfig
from trpc_service.config import TenantConfig
from trpc_service.gateway.models import NormalizedInboundMessage


class AccessDeniedError(PermissionError):
    """Raised when a channel identity is not allowed by the tenant binding."""


class BudgetExceededError(RuntimeError):
    """Raised before execution when a tenant request quota is exhausted."""


class TenantPolicyEnforcer:
    """Validate message size, attachment count and optional binding ACL."""

    def validate_inbound(self, tenant: TenantConfig, binding: ChannelBindingConfig,
                         message: NormalizedInboundMessage) -> None:
        max_text_chars = int(binding.options.get("max_text_chars", 32768))
        max_attachments = int(binding.options.get("max_attachments", 10))
        allowed_users = set(binding.options.get("allowed_user_ids", []))
        if len(message.text) > max_text_chars:
            raise AccessDeniedError("message exceeds the binding text limit")
        if len(message.attachments) > max_attachments:
            raise AccessDeniedError("message exceeds the binding attachment limit")
        if allowed_users and message.external_user_id not in allowed_users:
            raise AccessDeniedError(f"user is not allowed by tenant {tenant.tenant_id}")


class InMemoryBudgetLedger:
    """Single-node daily request reservation; replace with Redis in production."""

    def __init__(self) -> None:
        self._requests: dict[tuple[str, date], int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def reserve_request(self, tenant: TenantConfig) -> None:
        key = (tenant.tenant_id, date.today())
        async with self._lock:
            if self._requests[key] >= tenant.budget.daily_requests:
                raise BudgetExceededError(f"daily request quota exceeded for tenant {tenant.tenant_id}")
            self._requests[key] += 1

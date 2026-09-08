# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-level IM identity authorization Agent filter."""

from __future__ import annotations

from typing import Optional

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.abc import FilterType
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.filter import BaseFilter

from trpc_service.tenant import Tenant
from ._filters import TenantResolver


class ChannelUserAuthorizationFilter(BaseFilter):
    """Reject external users/groups that are not allowed by tenant policy."""

    def __init__(self, *, tenant: Optional[Tenant] = None, resolver: Optional[TenantResolver] = None) -> None:
        super().__init__()
        self._type = FilterType.AGENT
        self._name = "tenant_channel_user_authorization"
        self._tenant = tenant
        self._resolver = resolver

    async def _before(self, ctx: AgentContext, req, rsp: FilterResult):
        tenant_id = ctx.get_metadata("tenant_id")
        tenant = self._resolver(tenant_id) if self._resolver is not None else self._tenant
        if tenant is None:
            return None
        policy = tenant.im_access_policy
        user_id = ctx.get_metadata("channel_user_id")
        group_id = ctx.get_metadata("channel_chat_id")
        verified = bool(ctx.get_metadata("channel_user_verified"))
        denied = user_id in policy.denied_users
        denied = denied or (policy.require_verified_identity and not verified)
        denied = denied or (bool(policy.allowed_users) and user_id not in policy.allowed_users)
        denied = denied or (bool(policy.allowed_groups) and group_id not in policy.allowed_groups)
        if denied:
            rsp.error = PermissionError("IM user is not authorized for this tenant")
            rsp.is_continue = False
        return None

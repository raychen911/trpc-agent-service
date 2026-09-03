"""
Unit tests for Filter Pipeline, Tool RBAC, Quotas, and Security Masking.
"""

import asyncio
import pytest
from trpc_service.config.models import (
    TenantConfig,
    AgentAppConfig,
    InboundMessage,
    ChannelType,
    AuditDecision,
)
from trpc_service.agent.filter import (
    FilterChain,
    AgentExecutionContext,
    TenantQuotaFilter,
    ToolPermissionFilter,
    SensitiveDataMaskFilter,
    HumanInTheLoopFilter,
)
from trpc_service.tenant.manager import tenant_manager
from trpc_service.log.logger import mask_sensitive_text


def test_sensitive_data_masking():
    raw_text = "My API Key is sk-abcdef1234567890abcdef and phone is 13812345678, id is 110101199003072345."
    masked = mask_sensitive_text(raw_text)

    assert "sk-abcdef1234567890abcdef" not in masked
    assert "sk-***REDACTED***" in masked
    assert "138****5678" in masked
    assert "110101********2345" in masked


def test_tool_permission_filter():
    async def _test():
        app = AgentAppConfig(
            app_id="app_1",
            tenant_id="tenant_sec",
            name="SecBot",
            allowed_tools=["calculator"],
        )
        tenant = TenantConfig(tenant_id="tenant_sec", name="Sec Tenant")

        inbound = InboundMessage(
            trace_id="tr_1",
            tenant_id="tenant_sec",
            channel_type=ChannelType.CUSTOM,
            raw_user_id="u1",
            message_id="m1",
            content="Calculate 1+1",
        )
        ctx = AgentExecutionContext(
            tenant=tenant,
            app=app,
            inbound=inbound,
            session_id="s1",
            trace_id="tr_1",
            user_id="u1",
        )

        filter_perm = ToolPermissionFilter()
        # Calculator is allowed
        res1 = await filter_perm.before_tool_call(ctx, "calculator", {})
        assert res1.passed is True

        # DB Query is not in whitelist
        res2 = await filter_perm.before_tool_call(ctx, "database_query", {})
        assert res2.passed is False
        assert res2.decision == AuditDecision.BLOCKED

    asyncio.run(_test())


def test_human_in_the_loop_filter():
    async def _test():
        app = AgentAppConfig(
            app_id="app_finance",
            tenant_id="tenant_fin",
            name="FinBot",
            allowed_tools=["fund_transfer"],
            require_confirmation_tools=["fund_transfer"],
        )
        tenant = TenantConfig(tenant_id="tenant_fin", name="Fin Tenant")
        inbound = InboundMessage(
            trace_id="tr_2",
            tenant_id="tenant_fin",
            channel_type=ChannelType.CUSTOM,
            raw_user_id="u2",
            message_id="m2",
            content="Transfer $500",
        )
        ctx = AgentExecutionContext(
            tenant=tenant,
            app=app,
            inbound=inbound,
            session_id="s2",
            trace_id="tr_2",
            user_id="u2",
        )

        hitl = HumanInTheLoopFilter()
        res = await hitl.before_tool_call(ctx, "fund_transfer", {"amount": 500})
        assert res.decision == AuditDecision.CONFIRM_REQUIRED

    asyncio.run(_test())

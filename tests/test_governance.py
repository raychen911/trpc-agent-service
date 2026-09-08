import asyncio
from types import SimpleNamespace

import pytest
from prometheus_client import generate_latest
from sqlalchemy import func, select

from trpc_service.gateway import AgentReply, NormalizedMessage
from trpc_service.governance import (
    GovernanceDeniedError,
    GovernanceService,
    ToolGovernanceCallbacks,
    ToolPolicy,
)
from trpc_service.metrics import PlatformMetrics
from trpc_service.storage import Database
from trpc_service.storage.models import AgentApp, AuditLog, Tenant, TenantBudgetUsage
from trpc_service.storage.sql_backend import SqlDataPlane


def incoming(user_id: str = "user-1") -> NormalizedMessage:
    return NormalizedMessage(
        "tenant-1",
        "app-1",
        "telegram",
        "bot",
        "update-1",
        user_id,
        "chat-1",
        "direct",
        "联系 me@example.com 或 13800138000",
        "trace-1",
    )


def test_acl_pii_budget_and_audit() -> None:
    database = Database("sqlite+pysqlite:///:memory:")
    database.create_schema()
    with database.session_factory.begin() as session:
        tenant = Tenant(
            id="tenant-1",
            slug="governance",
            name="Governance",
            key_namespace="tenant/governance",
            audit_policy={
                "governance": {
                    "im_acl": {"allow_users": ["user-1"]},
                    "pii": {"redact_input": True, "redact_output": True},
                    "budget": {"daily_requests": 1, "daily_tokens": 1000},
                }
            },
        )
        session.add(tenant)
        session.flush()
        session.add(
            AgentApp(
                id="app-1",
                tenant_id="tenant-1",
                slug="agent",
                name="Agent",
            )
        )

    async def scenario() -> None:
        service = GovernanceService(
            database.session_factory, SqlDataPlane(database.session_factory)
        )
        governed = await service.authorize(incoming())
        assert governed.message.text == "联系 [EMAIL] 或 [CN_PHONE]"
        reply = await service.settle(
            governed,
            AgentReply("回复到 second@example.com", input_tokens=10, output_tokens=5),
        )
        assert reply.text == "回复到 [EMAIL]"

        with pytest.raises(GovernanceDeniedError, match="daily_request_budget_exceeded"):
            await service.authorize(incoming())
        denied = incoming("stranger")
        with pytest.raises(GovernanceDeniedError, match="user_not_allowed"):
            await service.authorize(denied)

    try:
        asyncio.run(scenario())
        with database.session_factory() as session:
            usage = session.scalar(select(TenantBudgetUsage))
            assert usage is not None
            assert usage.request_count == 1
            assert usage.token_count == 15
            assert session.scalar(select(func.count()).select_from(AuditLog)) == 3
    finally:
        database.dispose()


def test_tool_confirmation_callback() -> None:
    async def scenario() -> None:
        metrics = PlatformMetrics()
        callbacks = ToolGovernanceCallbacks((ToolPolicy("dangerous_tool", True),), None, metrics)
        context = SimpleNamespace(
            run_config=SimpleNamespace(
                custom_data={"confirmed_tools": [], "tenant_id": "tenant-1"}
            ),
            invocation_id="invocation-1",
            agent=SimpleNamespace(name="agent"),
        )
        tool = SimpleNamespace(name="dangerous_tool")
        blocked = await callbacks.before_tool(context, tool, {"target": "x"}, None)
        assert blocked == {
            "error": "tool_confirmation_required",
            "tool_name": "dangerous_tool",
            "message": "Tool dangerous_tool requires explicit user confirmation.",
        }
        context.run_config.custom_data["confirmed_tools"] = ["dangerous_tool"]
        assert await callbacks.before_tool(context, tool, {}, None) is None
        assert await callbacks.after_tool(context, tool, {}, {"value": "ok"}) is None
        exported = generate_latest(metrics.registry)
        assert b"trpc_tool_duration_seconds_count" in exported
        assert b'tenant_id="tenant-1"' in exported

    asyncio.run(scenario())

"""Tenant-scoped wrappers around tRPC public service interfaces."""

import pytest
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.sessions import InMemorySessionService

from trpc_service.storage import GuardedSessionService
from trpc_service.storage import TenantBoundaryError
from trpc_service.storage import execution_scope


@pytest.mark.asyncio
async def test_guarded_session_delegates_public_interface_with_exact_scope() -> None:
    raw = InMemorySessionService()
    guarded = GuardedSessionService(raw, "tenant_acme", "support_app")
    context = AgentContext()
    context.with_metadata("tenant_id", "tenant_acme")
    with execution_scope("tenant_acme", "execution-1", 4):
        created = await guarded.create_session(
            app_name="tenant_acme:support_app",
            user_id="usr_1",
            session_id="ses_1",
            agent_context=context,
        )
        loaded = await guarded.get_session(
            app_name="tenant_acme:support_app",
            user_id="usr_1",
            session_id="ses_1",
            agent_context=context,
        )
    assert loaded == created

    conflicting = AgentContext()
    conflicting.with_metadata("tenant_id", "tenant_other")
    with pytest.raises(TenantBoundaryError):
        await guarded.get_session(
            app_name="tenant_other:support_app",
            user_id="usr_1",
            session_id="ses_1",
        )
    with pytest.raises(TenantBoundaryError):
        await guarded.get_session(
            app_name="tenant_acme:support_app",
            user_id="usr_1",
            session_id="ses_1",
            agent_context=conflicting,
        )


def test_execution_scope_rejects_nested_cross_tenant_context() -> None:
    with execution_scope("tenant_acme", "execution-1", 1):
        with pytest.raises(TenantBoundaryError):
            with execution_scope("tenant_other", "execution-2", 2):
                pass

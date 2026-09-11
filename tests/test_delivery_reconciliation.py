from __future__ import annotations

import asyncio

from trpc_service.channels import CallbackRequest
from trpc_service.config import AppSettings
from trpc_service.runtime import TenantContext
from trpc_service.web.app import ServiceContainer
from trpc_service.web.schemas import AgentCreate, ChannelCreate, ReleaseCreate, TenantCreate


def test_queryable_delivery_is_reconciled_without_a_second_send() -> None:
    async def scenario() -> None:
        services = ServiceContainer(AppSettings(admin_api_key="admin-test-key"))
        services.create_tenant(TenantCreate(tenant_id="tenant-query", display_name="Query"))
        services.create_agent("tenant-query", AgentCreate(agent_id="support", name="Support"))
        services.create_release(
            "tenant-query",
            "support",
            ReleaseCreate(version=1, model_config={"mode": "mock"}),
        )
        services.activate_release("tenant-query", "support", 1)
        services.create_binding(
            "tenant-query",
            ChannelCreate(
                binding_id="mock-query",
                agent_id="support",
                provider="mock",
                external_account_id="query-account",
                webhook_key="query-webhook-key-123456",
                capabilities={
                    "callback_secret": "query-secret",
                    "delivery_capability": "queryable",
                },
            ),
        )
        accepted = await services.accept_callback(
            "mock",
            "query-webhook-key-123456",
            CallbackRequest(
                body={"message_id": "query-message", "user_id": "alice", "text": "hello"},
                headers={"x-mock-secret": "query-secret"},
            ),
            "query-request",
            "a" * 32,
        )
        services.dispatch("tenant-query")
        await services.process_published("tenant-query")
        services.dispatch("tenant-query")
        services.mock_channel.queue_delivery_outcome("mock-query", "unknown_accepted")

        initial = await services.deliver_replies("tenant-query")
        assert initial[0]["status"] == "unknown"
        reconciled = await services.deliver_replies("tenant-query")

        assert reconciled[0]["status"] == "accepted"
        assert len(services.mock_channel.deliveries) == 1
        outbox = services.runtime.snapshot(TenantContext("tenant-query"))["outbox"]
        reply = next(row for row in outbox if row["event_type"] == "reply.dispatch")
        assert reply["status"] == "delivered"
        assert accepted["duplicate"] is False

    asyncio.run(scenario())

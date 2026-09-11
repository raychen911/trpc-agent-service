"""Actor identity must not accidentally expose personal memory in other chats."""

import pytest
from trpc_agent_sdk.context import AgentContext

from trpc_service.tenant import ChannelBindingRegistry, InboundRouteRequest, MessageRouter, SessionIdentityFactory
from trpc_service.storage import GuardedMemoryService, TenantBoundaryError


def routes(tenant, mode="per_user"):
    data = tenant.model_dump()
    data["channel_bindings"][0]["group_mode"] = mode
    tenant = type(tenant).model_validate(data)
    router = MessageRouter(ChannelBindingRegistry([tenant]), SessionIdentityFactory(lambda _: b"k" * 32))

    def route(actor="alice", chat="group1", topic="topic1", kind="thread"):
        return router.route(InboundRouteRequest(
            channel="wecom", webhook_public_id="callback_acme", external_user_id=actor,
            external_chat_id=chat, thread_id=topic, conversation_type=kind))
    return route


def test_personal_group_memory_is_not_private_or_other_group_memory(tenant):
    route = routes(tenant)
    scopes = [route(), route(chat="group2"), route(topic="topic2"),
              route(chat=None, topic=None, kind="direct"), route(actor="bob")]
    assert len({x.session_id for x in scopes}) == 5
    assert len({x.internal_user_id for x in scopes}) == 5
    assert len({x.actor_id for x in scopes[:4]}) == 1
    assert scopes[0].actor_id != scopes[-1].actor_id


def test_shared_group_storage_is_shared_but_actor_is_not(tenant):
    route = routes(tenant, "shared")
    alice, bob = route(), route(actor="bob")
    assert alice.session_id == bob.session_id
    assert alice.internal_user_id == bob.internal_user_id
    assert alice.actor_id != bob.actor_id
    assert alice.internal_user_id != route(chat="group2").internal_user_id


def test_identity_encoding_cannot_confuse_delimiter_in_external_values(tenant):
    route = routes(tenant)
    assert route(chat="a\x1fb", topic="c").session_id != route(chat="a", topic="b\x1fc").session_id


@pytest.mark.asyncio
async def test_memory_cannot_escape_trusted_conversation_scope():
    class Raw:
        enabled = True

        async def search_memory(self, key, query, **kwargs):
            return [key]

    memory = GuardedMemoryService(Raw(), "tenant_acme", "support")
    context = AgentContext()
    context.with_metadata("tenant_id", "tenant_acme")
    context.with_metadata("memory_scope_id", "group_alice")
    assert await memory.search_memory("tenant_acme:support/group_alice", "query", agent_context=context)
    with pytest.raises(TenantBoundaryError):
        await memory.search_memory("tenant_acme:support/private_alice", "query", agent_context=context)

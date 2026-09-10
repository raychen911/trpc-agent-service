from __future__ import annotations

import asyncio
from uuid import UUID

from tests.support import FIXED_UTC, inbound_message_data
from trpc_service.channels.contracts import InboundMessage
from trpc_service.web.app import build_runtime
from trpc_service.worker.service import AgentExecution
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.tenant.models import AgentApplication
from trpc_service.tenant.session_identity import derive_session_identity


async def test_scale_acceptance_for_two_tenants_repeats_and_parallel_sessions(runtime_secret_env: dict[str, str]) -> None:
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    for index in range(20):
        token_a, token_b = f"A{index}", f"B{index}"
        common = {"external_user_id": "shared", "external_conversation_id": f"conversation-{index}"}
        for binding, token, number in (("binding-alpha", token_a, 1), ("binding-beta", token_b, 2)):
            stored = await runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(binding_id=binding, external_message_id=f"scale-{index}-{number}-store", text=f"Remember validation token {token}.", **common)))
            recalled = await runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(binding_id=binding, external_message_id=f"scale-{index}-{number}-recall", text="Recall the validation token.", **common)))
            assert recalled.text == f"recalled:{token}"
            if binding == "binding-alpha":
                alpha_session = stored.platform_session_id
            else:
                assert stored.platform_session_id != alpha_session

    first = await runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(external_message_id="scale-repeat")))
    repeats = [await runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(external_message_id="scale-repeat", trace_id=UUID(int=20000 + i)))) for i in range(100)]
    assert all(item.status.value == "duplicate" and item.original_trace_id == first.trace_id for item in repeats)

    before = runtime.worker.call_count
    for group in range(20):
        replies = await asyncio.gather(*[
            runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(external_message_id=f"concurrent-{group}", external_conversation_id=f"parallel-{group}", trace_id=UUID(int=30000 + group * 20 + i))))
            for i in range(20)
        ])
        assert sum(item.status.value == "succeeded" for item in replies) == 1
    assert runtime.worker.call_count - before == 20
    await runtime.close()


async def test_same_session_serializes_while_distinct_sessions_run_in_parallel(runtime_secret_env, monkeypatch) -> None:
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    active = 0
    maximum = 0

    class Prepared:
        async def execute(self, **_kwargs):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.02)
            active -= 1
            return AgentExecution("ok", 1, 1)

    async def prepare(*_args): return Prepared()
    monkeypatch.setattr(runtime.worker, "prepare", prepare)

    different = [
        InboundMessage(**inbound_message_data(external_message_id=f"parallel-{index}", external_conversation_id=f"conversation-{index}", trace_id=UUID(int=50000 + index)))
        for index in range(2)
    ]
    await asyncio.gather(*(runtime.gateway.handle_verified_message_for_test(item) for item in different))
    assert maximum == 2

    maximum = 0
    same = [
        InboundMessage(**inbound_message_data(external_message_id=f"serial-{index}", external_conversation_id="one-conversation", trace_id=UUID(int=51000 + index)))
        for index in range(2)
    ]
    await asyncio.gather(*(runtime.gateway.handle_verified_message_for_test(item) for item in same))
    assert maximum == 1
    await runtime.close()


def test_agent_rebinding_changes_platform_session_identity() -> None:
    settings = build_demo_settings()
    base = InMemoryPlatformAdapters(settings)
    trace = UUID(int=52000)
    original_context = base.context_for_test("binding-alpha", "shared-user", trace)
    original = derive_session_identity(original_context, "direct", "shared-conversation")

    replacement_agent = AgentApplication(
        tenant_id="tenant-alpha", agent_id="agent-alpha-v2", agent_name="Alpha Agent V2",
        status="active", model_profile="deterministic-offline", instruction="Offline validation.", config_version=2,
    )
    replacement_binding = settings.bindings[0].model_copy(update={"agent_id": "agent-alpha-v2"})
    rebound_settings = settings.model_copy(update={"agents": settings.agents + (replacement_agent,), "bindings": (replacement_binding,) + settings.bindings[1:]})
    rebound = InMemoryPlatformAdapters(rebound_settings)
    rebound_context = rebound.context_for_test("binding-alpha", "shared-user", trace)
    changed = derive_session_identity(rebound_context, "direct", "shared-conversation")
    assert changed.platform_session_id != original.platform_session_id

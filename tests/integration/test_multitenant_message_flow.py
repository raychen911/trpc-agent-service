from __future__ import annotations

from uuid import UUID

from tests.support import FIXED_UTC, inbound_message_data
from trpc_service.channels.contracts import InboundMessage
from trpc_service.config.settings import build_demo_settings
from trpc_service.web.app import build_runtime


async def test_first_tenant_message_reaches_runner_audit_and_metrics(runtime_secret_env: dict[str, str]) -> None:
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    message = InboundMessage(**inbound_message_data())
    reply = await runtime.gateway.handle_verified_message_for_test(message)

    assert reply.status.value == "succeeded"
    assert reply.text == "stored:ALPHA"
    scope = runtime.tenant_scope("tenant-alpha")
    decisions = [item.decision.value for item in await runtime.adapters.audit.list_by_trace(scope, UUID(str(message.trace_id)))]
    assert decisions == ["authorized", "execution_started", "succeeded"]
    assert runtime.metrics.snapshot(scope).request_count == 1
    assert "state_backend" in runtime.metrics.snapshot(scope).stage_latency_ms
    await runtime.close()


async def test_two_tenants_with_same_external_ids_keep_distinct_two_turn_context(runtime_secret_env: dict[str, str]) -> None:
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    common = {
        "external_user_id": "shared-user",
        "external_conversation_id": "shared-conversation",
    }
    alpha_store = InboundMessage(**inbound_message_data(binding_id="binding-alpha", external_message_id="alpha-1", text="Remember validation token ALPHA.", **common))
    beta_store = InboundMessage(**inbound_message_data(binding_id="binding-beta", external_message_id="beta-1", text="Remember validation token BRAVO.", **common))
    alpha_recall = InboundMessage(**inbound_message_data(binding_id="binding-alpha", external_message_id="alpha-2", text="Recall the validation token.", **common))
    beta_recall = InboundMessage(**inbound_message_data(binding_id="binding-beta", external_message_id="beta-2", text="Recall the validation token.", **common))

    replies = [
        await runtime.gateway.handle_verified_message_for_test(alpha_store),
        await runtime.gateway.handle_verified_message_for_test(beta_store),
        await runtime.gateway.handle_verified_message_for_test(alpha_recall),
        await runtime.gateway.handle_verified_message_for_test(beta_recall),
    ]
    assert [item.text for item in replies] == ["stored:ALPHA", "stored:BRAVO", "recalled:ALPHA", "recalled:BRAVO"]
    assert replies[0].platform_session_id == replies[2].platform_session_id
    assert replies[1].platform_session_id == replies[3].platform_session_id
    assert replies[0].platform_session_id != replies[1].platform_session_id
    alpha_records = await runtime.adapters.audit.list_by_tenant(runtime.tenant_scope("tenant-alpha"))
    beta_records = await runtime.adapters.audit.list_by_tenant(runtime.tenant_scope("tenant-beta"))
    assert all(record.tenant_id == "tenant-alpha" for record in alpha_records)
    assert all(record.tenant_id == "tenant-beta" for record in beta_records)
    alpha_user = alpha_records[0].user_id
    beta_user = beta_records[0].user_id
    assert alpha_user != beta_user
    await runtime.close()


async def test_sequential_and_concurrent_duplicates_execute_once_and_conflict(runtime_secret_env: dict[str, str]) -> None:
    import asyncio

    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    first = InboundMessage(**inbound_message_data(external_message_id="duplicate-001"))
    initial = await runtime.gateway.handle_verified_message_for_test(first)
    repeated = [
        await runtime.gateway.handle_verified_message_for_test(
            InboundMessage(**inbound_message_data(external_message_id="duplicate-001", trace_id=UUID(int=index + 10)))
        )
        for index in range(100)
    ]
    concurrent = await asyncio.gather(*[
        runtime.gateway.handle_verified_message_for_test(
            InboundMessage(**inbound_message_data(external_message_id="concurrent-001", trace_id=UUID(int=index + 1000)))
        )
        for index in range(20)
    ])
    conflict = await runtime.gateway.handle_verified_message_for_test(
        InboundMessage(**inbound_message_data(external_message_id="duplicate-001", text="Recall the validation token.", trace_id=UUID(int=9999)))
    )

    assert all(reply.status.value == "duplicate" and reply.delivery_action.value == "suppress" for reply in repeated)
    assert sum(reply.status.value == "succeeded" for reply in concurrent) == 1
    assert sum(reply.status.value == "duplicate" for reply in concurrent) == 19
    assert conflict.status.value == "conflict"
    assert runtime.worker.call_count == 2
    assert all(reply.original_trace_id == initial.trace_id for reply in repeated)
    duplicate_audit = await runtime.adapters.audit.list_by_trace(
        runtime.tenant_scope("tenant-alpha"), repeated[0].trace_id
    )
    assert duplicate_audit[0].original_trace_id == initial.trace_id
    await runtime.close()


async def test_audit_prewrite_failure_is_safe_retryable_and_has_no_agent_side_effect(runtime_secret_env: dict[str, str]) -> None:
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    runtime.adapters.audit.fail_append = True
    reply = await runtime.gateway.handle_verified_message_for_test(
        InboundMessage(**inbound_message_data(external_message_id="audit-failure"))
    )
    assert reply.status.value == "failed"
    assert reply.error.code == "audit_unavailable"
    assert reply.error.execution_started is False and reply.error.retryable is True
    assert runtime.worker.call_count == 0
    await runtime.close()


async def test_preparation_failure_can_reclaim_but_post_start_failure_is_cached(runtime_secret_env: dict[str, str], monkeypatch) -> None:
    from trpc_service.storage.contracts import AgentExecutionFailed, AgentPreparationFailed

    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    original_prepare = runtime.worker.prepare

    async def unavailable(*_args, **_kwargs):
        raise AgentPreparationFailed("private detail")

    monkeypatch.setattr(runtime.worker, "prepare", unavailable)
    message = InboundMessage(**inbound_message_data(external_message_id="prepare-retry"))
    failed = await runtime.gateway.handle_verified_message_for_test(message)
    assert failed.error.code == "agent_unavailable" and failed.error.retryable is True
    monkeypatch.setattr(runtime.worker, "prepare", original_prepare)
    recovered = await runtime.gateway.handle_verified_message_for_test(message)
    assert recovered.status.value == "succeeded"

    class FailingPrepared:
        async def execute(self, timeout_seconds=30):
            raise AgentExecutionFailed("private vendor detail")

    async def failing_prepare(*_args, **_kwargs):
        return FailingPrepared()

    monkeypatch.setattr(runtime.worker, "prepare", failing_prepare)
    post_message = InboundMessage(**inbound_message_data(external_message_id="post-start-failure", trace_id=UUID(int=50001)))
    post = await runtime.gateway.handle_verified_message_for_test(post_message)
    cached = await runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(external_message_id="post-start-failure", trace_id=UUID(int=50002))))
    assert post.error.code == "agent_failed" and post.error.execution_started is True
    assert cached.error.code == "agent_failed" and cached.delivery_action.value == "suppress"
    assert cached.original_trace_id == post_message.trace_id
    await runtime.close()


async def test_final_audit_failure_becomes_nonretryable_cached_terminal(runtime_secret_env: dict[str, str]) -> None:
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    runtime.adapters.audit.fail_on_append_number = 3
    message = InboundMessage(**inbound_message_data(external_message_id="final-audit", trace_id=UUID(int=51001)))
    first = await runtime.gateway.handle_verified_message_for_test(message)
    cached = await runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(external_message_id="final-audit", trace_id=UUID(int=51002))))
    assert first.error.code == "audit_incomplete" and first.error.retryable is False
    assert cached.error.code == "audit_incomplete" and cached.delivery_action.value == "suppress"
    assert runtime.worker.call_count == 1
    await runtime.close()

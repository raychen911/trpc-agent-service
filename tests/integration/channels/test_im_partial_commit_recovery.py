from __future__ import annotations

import pytest

from tests.integration.channels.multinode_support import TwoNodeIMHarness
from trpc_service.audit.models import TenantScope
from trpc_service.channels.base import ProviderOutcomeUnknown
from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.recovery.reconciler import RecoveryReconciler
from trpc_service.storage.models import IdempotencyKey


class CapturingDeliveryRepository:
    def __init__(self, delegate) -> None:
        self.delegate = delegate
        self.executions = []

    async def create_or_get(
        self, tenant_scope, binding_scope, execution_result, reply_context, adapter_fence
    ):
        self.executions.append(execution_result)
        return await self.delegate.create_or_get(
            tenant_scope,
            binding_scope,
            execution_result,
            reply_context,
            adapter_fence,
        )

    def __getattr__(self, name):
        return getattr(self.delegate, name)


@pytest.mark.asyncio
async def test_durable_agent_result_is_reused_by_replay_and_delivery_recovery() -> None:
    from trpc_service.channels.delivery import InMemoryDeliveryRepository

    delegate = InMemoryDeliveryRepository()
    repository = CapturingDeliveryRepository(delegate)
    harness = await TwoNodeIMHarness.create(
        Channel.FEISHU, delivery_repository=repository
    )
    event = harness.event(
        message_id="partial-commit-message",
        conversation_id="partial-commit-conversation",
        sender_id="partial-commit-sender",
        text="Remember validation token RECOVERY.",
    )
    harness.providers[0].send_results.append(ProviderOutcomeUnknown())
    try:
        first = await harness.adapters[0].handle_provider_event(event)
        assert first.safe_code == "reply_delivery_unknown"
        assert harness.agent_calls == 1
        assert repository.executions
        # The delivery intent must use the durable Runner result, not a
        # synthetic reply-only result with zero event evidence.
        assert repository.executions[0].agent_event_count > 0

        replay = await harness.adapters[1].handle_provider_event(event)
        assert replay.safe_code == "duplicate_suppressed"
        assert harness.agent_calls == 1
        assert sum(len(provider.sent) for provider in harness.providers) == 1

        key = IdempotencyKey(
            tenant_id="tenant-alpha",
            channel=Channel.FEISHU,
            binding_id="binding-feishu-alpha",
            external_message_id="partial-commit-message",
        )
        durable = (await harness.platform.idempotency.get(key)).result
        assert durable is not None
        scope = VerifiedBindingScope._issue(
            binding_id="binding-feishu-alpha", channel=Channel.FEISHU
        )
        recovered = await harness.delivery_service.recover_execution_result(
            execution_result=durable,
            tenant_scope=TenantScope(tenant_id="tenant-alpha"),
            binding_scope=scope,
            reply_context=harness.adapters[0].parse_provider_event(event).reply_context,
            provider=harness.providers[1],
            adapter_fence=harness.adapters[1]._adapter_fence,
        )
        assert recovered.status == "delivery_unknown"
        assert harness.agent_calls == 1
        assert sum(len(provider.sent) for provider in harness.providers) == 1
        assert not hasattr(harness.delivery_service, "worker")
        assert not hasattr(harness.delivery_service, "runner")

        class DurableMarkers:
            reconciled = False

            async def get_pending(self, tenant_scope, limit):
                del tenant_scope, limit
                return [
                    {
                        "id": "im-recovery-marker",
                        "result": durable.model_dump(mode="json"),
                        "result_digest": "d" * 64,
                    }
                ]

            async def mark_reconciled(self, tenant_scope, marker_id, digest):
                del tenant_scope
                assert marker_id == "im-recovery-marker"
                assert digest == "d" * 64
                self.reconciled = True

        class TerminalState:
            marker = None

            async def complete_from_recovery(self, marker):
                self.marker = marker

        markers, terminal = DurableMarkers(), TerminalState()
        reconciler = RecoveryReconciler(markers, terminal)
        assert not hasattr(reconciler, "agent")
        assert await reconciler.run_once(TenantScope(tenant_id="tenant-alpha")) == 1
        assert terminal.marker["result"]["response_text"] == "stored:RECOVERY"
        assert markers.reconciled
        assert harness.agent_calls == 1
    finally:
        await harness.close()

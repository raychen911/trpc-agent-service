import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from trpc_service.agent.recovery import RetryableOperationError
from trpc_service.channels import (
    ChannelAdapter,
    ChannelAdapterRegistry,
    ChannelBindingConfig,
    DeliveryReceipt,
    OutgoingMessage,
)
from trpc_service.channels.delivery import DeliveryTaskClaim, DeliveryWorkerService
from trpc_service.config import LeasedWorkerConfig
from trpc_service.storage import OutboxMessage
from trpc_service.tenant import TenantContext


class RecordingAdapter(ChannelAdapter):
    channel_type = "test_im"

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def decode(self, envelope, binding):  # type: ignore[no-untyped-def]
        del envelope, binding
        raise NotImplementedError

    async def acknowledge(self, envelope, binding):  # type: ignore[no-untyped-def]
        del envelope, binding
        raise NotImplementedError

    async def deliver(
        self,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> DeliveryReceipt:
        del binding
        if self.error is not None:
            raise self.error
        return DeliveryReceipt(
            delivery_id=message.delivery_id,
            external_delivery_id="provider-receipt",
            accepted_at=datetime.now(timezone.utc),
        )


class BlockingAdapter(RecordingAdapter):
    """Represent an IM provider call that outlives its first lease."""

    def __init__(self) -> None:
        super().__init__()
        self.cancelled = asyncio.Event()

    async def deliver(
        self,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> DeliveryReceipt:
        del message, binding
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
        raise AssertionError("unreachable")


class RecordingDeliveryQueue:

    def __init__(self, claim: DeliveryTaskClaim) -> None:
        self._next_claim: DeliveryTaskClaim | None = claim
        self.completed = False
        self.failure: dict[str, object] | None = None
        self.renewed = True

    async def claim(self, worker_id: str, *, lease_until: datetime):  # type: ignore[no-untyped-def]
        del worker_id, lease_until
        claim, self._next_claim = self._next_claim, None
        return claim

    async def complete(self, claim, *, worker_id, receipt):  # type: ignore[no-untyped-def]
        del claim, worker_id, receipt
        self.completed = True

    async def renew(self, claim, *, worker_id, lease_until):  # type: ignore[no-untyped-def]
        del claim, worker_id, lease_until
        return self.renewed

    async def fail(self, claim, *, worker_id, decision,
                   next_attempt_at):  # type: ignore[no-untyped-def]
        del claim, worker_id
        self.failure = {
            "decision": decision,
            "next_attempt_at": next_attempt_at,
        }


def _claim() -> DeliveryTaskClaim:
    tenant_id = uuid4()
    agent_app_id = uuid4()
    binding_id = uuid4()
    return DeliveryTaskClaim(
        context=TenantContext(
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            config_version=1,
            request_id="request-1",
            trace_id="trace-1",
        ),
        binding=ChannelBindingConfig(
            binding_id=binding_id,
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            channel_type="test_im",
        ),
        message=OutboxMessage(
            outbox_id="outbox-1",
            category="IM_REPLY",
            idempotency_key="reply-1",
            destination="test_im",
            binding_id=binding_id,
            attempt_count=1,
            payload={
                "delivery_id": "delivery-1",
                "conversation_id": "conversation-1",
                "kind": "text",
                "text": "hello",
            },
        ),
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("error", "expected", "has_retry"),
    [
        (RetryableOperationError("limited", retry_after_seconds=120), "retry", True),
        (ValueError("invalid recipient"), "permanent", False),
        (TimeoutError("provider response lost"), "unknown", False),
    ],
)
async def test_delivery_worker_persists_safe_recovery_outcome(
    error: Exception,
    expected: str,
    has_retry: bool,
) -> None:
    """Provider failures become retry, DLQ, or reconciliation facts."""

    queue = RecordingDeliveryQueue(_claim())
    registry = ChannelAdapterRegistry()
    registry.register(RecordingAdapter(error))
    worker = DeliveryWorkerService(
        queue=queue,
        channels=registry,
        node_id="delivery-node-a",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=30,
            poll_interval_seconds=0.01,
            retry_base_seconds=1,
            retry_max_seconds=60,
            retry_jitter_ratio=0,
            max_attempts=5,
        ),
    )

    started = datetime.now(timezone.utc)
    assert await worker.run_once(0)
    assert not queue.completed
    assert queue.failure is not None
    assert queue.failure["decision"].disposition.value == expected  # type: ignore[union-attr]
    assert (queue.failure["next_attempt_at"] is not None) is has_retry
    if isinstance(error, RetryableOperationError):
        # A provider Retry-After is an earliest-safe time, even when it exceeds
        # the locally configured exponential-backoff ceiling.
        next_attempt = queue.failure["next_attempt_at"]
        assert isinstance(next_attempt, datetime)
        assert next_attempt >= started + timedelta(seconds=119)


@pytest.mark.anyio
async def test_delivery_worker_completes_a_provider_receipt() -> None:
    queue = RecordingDeliveryQueue(_claim())
    registry = ChannelAdapterRegistry()
    registry.register(RecordingAdapter())
    worker = DeliveryWorkerService(
        queue=queue,
        channels=registry,
        node_id="delivery-node-a",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=30,
            poll_interval_seconds=0.01,
            retry_base_seconds=1,
            retry_max_seconds=60,
            retry_jitter_ratio=0,
            max_attempts=5,
        ),
    )

    assert await worker.run_once(0)
    assert queue.completed
    assert queue.failure is None


@pytest.mark.anyio
async def test_delivery_worker_cancels_provider_call_after_lease_loss() -> None:
    """A stale node must not commit success, retry, or DLQ state."""

    queue = RecordingDeliveryQueue(_claim())
    queue.renewed = False
    adapter = BlockingAdapter()
    registry = ChannelAdapterRegistry()
    registry.register(adapter)
    worker = DeliveryWorkerService(
        queue=queue,
        channels=registry,
        node_id="delivery-node-a",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=3,
            poll_interval_seconds=0.01,
            retry_base_seconds=1,
            retry_max_seconds=60,
            retry_jitter_ratio=0,
            max_attempts=5,
        ),
    )

    assert await worker.run_once(0)
    assert adapter.cancelled.is_set()
    assert not queue.completed
    assert queue.failure is None

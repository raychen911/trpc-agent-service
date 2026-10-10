from __future__ import annotations

from typing import Any

import pytest

from trpc_service.channels.envelopes import InboundEnvelope, PayloadKind
from trpc_service.config.settings import SchedulerVersion
from trpc_service.queue.emergency import EmergencyMessage, EmergencyQueueDrainer
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import Acceptance, PreparedInbound
from trpc_service.tenant.models import Channel, ConversationKind, TenantContext


def prepared() -> PreparedInbound:
    context = TenantContext(
        tenant_id="tenant-a",
        app_id="app-a",
        config_version=1,
        channel_binding_id="binding-a",
        principal_id="principal-a",
        session_id="session-a",
        request_id="request-a",
        trace_id="trace-a",
    )
    envelope = InboundEnvelope(
        channel=Channel.FEISHU,
        account_id="account-a",
        external_message_id="message-a",
        external_user_id="user-a",
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text="offline scheduler test",
    )
    return PreparedInbound(context=context, envelope=envelope, trace_headers={})


class RecordingRepository:
    def __init__(self) -> None:
        self.legacy_calls: list[dict[str, Any]] = []
        self.v2_calls: list[dict[str, Any]] = []
        self.config_calls: list[tuple[str, str, int]] = []

    async def get_config(self, tenant_id: str, app_id: str, version: int) -> object:
        self.config_calls.append((tenant_id, app_id, version))
        return object()

    async def accept_inbound(self, **kwargs: Any) -> Acceptance:
        self.legacy_calls.append(kwargs)
        return Acceptance(
            inbound_id="accepted-v1",
            context=kwargs["context"],
            envelope=kwargs["envelope"],
        )

    async def accept_inbound_v2(self, **kwargs: Any) -> Acceptance:
        self.v2_calls.append(kwargs)
        return Acceptance(
            inbound_id="accepted-v2",
            context=kwargs["context"],
            envelope=kwargs["envelope"],
        )


class RecordingEmergencyQueue:
    def __init__(self, message: EmergencyMessage) -> None:
        self.messages = [message]
        self.acked: list[EmergencyMessage] = []
        self.consume_calls: list[dict[str, Any]] = []

    async def consume(self, **kwargs: Any) -> tuple[EmergencyMessage, ...]:
        self.consume_calls.append(kwargs)
        messages, self.messages = self.messages, []
        return tuple(messages)

    async def ack(self, message: EmergencyMessage) -> None:
        self.acked.append(message)


@pytest.mark.asyncio
async def test_tenant_runtime_default_is_v1_and_never_dual_writes() -> None:
    repository = RecordingRepository()
    value = prepared()

    accepted = await TenantRuntime(repository, routing_key=b"r" * 32).accept_prepared(value)

    assert accepted.inbound_id == "accepted-v1"
    assert len(repository.legacy_calls) == 1
    assert repository.v2_calls == []
    assert len(repository.legacy_calls) + len(repository.v2_calls) == 1


@pytest.mark.asyncio
async def test_tenant_runtime_v2_only_calls_v2_acceptance() -> None:
    repository = RecordingRepository()
    value = prepared()

    accepted = await TenantRuntime(
        repository,
        routing_key=b"r" * 32,
        scheduler_version=SchedulerVersion.V2,
    ).accept_prepared(value)

    assert accepted.inbound_id == "accepted-v2"
    assert repository.legacy_calls == []
    assert len(repository.v2_calls) == 1
    assert len(repository.legacy_calls) + len(repository.v2_calls) == 1


@pytest.mark.asyncio
async def test_emergency_drainer_default_is_v1_and_never_dual_writes() -> None:
    value = prepared()
    message = EmergencyMessage(message_id="emergency-1", prepared=value)
    repository = RecordingRepository()
    queue = RecordingEmergencyQueue(message)
    drainer = EmergencyQueueDrainer(repository, queue, consumer_id="drainer-a")  # type: ignore[arg-type]

    assert await drainer.drain_once() == 1

    assert len(repository.legacy_calls) == 1
    assert repository.v2_calls == []
    assert len(repository.legacy_calls) + len(repository.v2_calls) == 1
    assert queue.acked == [message]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scheduler_version", "expected_inbound_id"),
    [
        (SchedulerVersion.V1, "accepted-v1"),
        (SchedulerVersion.V2, "accepted-v2"),
    ],
)
async def test_emergency_drainer_routes_one_message_to_selected_version(
    scheduler_version: SchedulerVersion,
    expected_inbound_id: str,
) -> None:
    value = prepared()
    message = EmergencyMessage(message_id="emergency-1", prepared=value)
    repository = RecordingRepository()
    queue = RecordingEmergencyQueue(message)
    drainer = EmergencyQueueDrainer(
        repository,
        queue,  # type: ignore[arg-type]
        consumer_id="drainer-a",
        scheduler_version=scheduler_version,
    )

    assert await drainer.drain_once() == 1

    selected = (
        repository.v2_calls if scheduler_version == SchedulerVersion.V2 else repository.legacy_calls
    )
    unselected = (
        repository.legacy_calls if scheduler_version == SchedulerVersion.V2 else repository.v2_calls
    )
    assert len(selected) == 1
    assert unselected == []
    assert len(selected) + len(unselected) == 1
    assert queue.acked == [message]
    assert selected[0]["context"] == value.context
    assert expected_inbound_id == (
        "accepted-v2" if scheduler_version == SchedulerVersion.V2 else "accepted-v1"
    )

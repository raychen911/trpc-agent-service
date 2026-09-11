"""R2C delivery retry and terminal-audit contracts."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_unsent_send_is_retried_at_most_three_times_with_fixed_backoff(monkeypatch):
    from trpc_service.channels.delivery import ChannelSendError, send_with_retry

    attempts = 0
    delays: list[float] = []

    async def send():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ChannelSendError(sent=False)

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    assert await send_with_retry(send) is True
    assert attempts == 3
    assert delays == [0.1, 0.2]


@pytest.mark.asyncio
async def test_sent_failure_is_never_retried():
    from trpc_service.channels.delivery import ChannelSendError, send_with_retry

    attempts = 0

    async def send():
        nonlocal attempts
        attempts += 1
        raise ChannelSendError(sent=True)

    assert await send_with_retry(send) is False
    assert attempts == 1


@pytest.mark.asyncio
async def test_cancellation_is_not_swallowed():
    from trpc_service.channels.delivery import send_with_retry

    async def send():
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await send_with_retry(send)


@pytest.mark.asyncio
async def test_success_is_suppressed_but_delivery_failure_is_audited_for_failures_policy():
    from trpc_service.channels.delivery import ChannelExecutionStream
    from trpc_service.gateway.channel_service import ChannelIngressService
    from trpc_service.transport.models import WorkerErrorCode, WorkerTask

    async def empty():
        if False:
            yield None

    repository = MagicMock()
    repository.append = AsyncMock()
    service = ChannelIngressService(MagicMock(), MagicMock(), execution_repository=repository)
    execution = ChannelExecutionStream(empty())
    execution.task = WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id="tenant_default",
        app_id="app_default",
        config_version=1,
        user_id="usr_v1_" + "a" * 48,
        channel="wecom",
        session_id="ses_v1_" + "a" * 48,
        message_id="message-1",
        message="hello",
    )
    execution.delivery_events = "failures"

    await service.record_external_delivery(execution, None)
    repository.append.assert_not_awaited()

    await service.record_external_delivery(execution, WorkerErrorCode.CHANNEL_DELIVERY_FAILED)
    repository.append.assert_awaited_once()
    event = repository.append.await_args.args[0]
    assert event.outcome == "failed"
    assert event.error_code == "channel_delivery_failed"


class TestSdkReplySender:
    """First-write retry policy + exception normalization shared by IM chains."""

    @pytest.mark.asyncio
    async def test_first_write_retries_then_later_writes_fire_once(self, monkeypatch):
        from trpc_service.channels.delivery import SdkReplySender

        attempts = 0

        async def operation(text, *, finished):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("sdk glitch")

        sender = SdkReplySender()
        assert await sender.write(operation, "a", finished=False) is True
        assert attempts == 2  # normalized error retried once more
        assert sender.sent is True
        assert await sender.write(operation, "b", finished=True) is True
        assert attempts == 3  # no retry after the first visible write

    @pytest.mark.asyncio
    async def test_unsent_failure_exhausts_retries_and_reports_not_written(self, monkeypatch):
        from trpc_service.channels.delivery import ChannelSendError, SdkReplySender

        attempts = 0

        async def operation():
            nonlocal attempts
            attempts += 1
            raise ChannelSendError(sent=False)

        sender = SdkReplySender()
        assert await sender.write(operation) is False
        assert attempts == 3
        assert sender.sent is False

    @pytest.mark.asyncio
    async def test_cancellation_passes_through_unnormalized(self):
        from trpc_service.channels.delivery import SdkReplySender

        async def operation():
            raise asyncio.CancelledError

        sender = SdkReplySender()
        with pytest.raises(asyncio.CancelledError):
            await sender.write(operation)


@pytest.mark.asyncio
async def test_record_terminal_delivery_maps_platform_failure_categories():
    from trpc_service.channels.delivery import ChannelExecutionStream, record_terminal_delivery
    from trpc_service.transport.models import WorkerErrorCode

    async def empty():
        if False:
            yield None

    recorded = []

    class Ingress:

        async def stream(self, inbound):
            return empty()

        async def record_external_delivery(self, execution, code):
            recorded.append(code)

    ingress = Ingress()
    execution = ChannelExecutionStream(empty())
    failures = frozenset({"append_failed", "finish_failed"})

    await record_terminal_delivery(ingress, execution, "append_failed", failure_categories=failures)
    await record_terminal_delivery(ingress, execution, "done", failure_categories=failures)
    await record_terminal_delivery(ingress, execution, "missing", failure_categories=failures)
    assert recorded == [WorkerErrorCode.CHANNEL_DELIVERY_FAILED, None]


@pytest.mark.asyncio
async def test_record_terminal_delivery_tolerates_ingress_without_recorder():
    from trpc_service.channels.delivery import ChannelExecutionStream, record_terminal_delivery

    async def empty():
        if False:
            yield None

    class Ingress:

        async def stream(self, inbound):
            return empty()

    execution = ChannelExecutionStream(empty())
    # No crash, no audit: the capability is optional.
    await record_terminal_delivery(Ingress(), execution, "done", failure_categories=frozenset())

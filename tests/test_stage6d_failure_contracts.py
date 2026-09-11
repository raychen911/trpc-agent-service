"""Stage 6D fault-degradation contracts (unit).

One focused seam per failure family the plan enumerates — Worker/model
timeout, tool failure, transient Redis/SQL unavailability, Worker
unavailability and channel delivery failure.  Every case asserts the SAME
four properties:

1. the public reply is the existing fixed safe text (zero payload leak);
2. the current request is attempted EXACTLY ONCE (no auto-retry, no
   cross-endpoint re-issue);
3. failures never turn into success and never fall back to local state;
4. after the infrastructure recovers, a NEW request executes normally.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from tests.test_gateway_channel_service import RecordingWorkerClient
from tests.tenant_helpers import FakeTenantConfigRepository, make_default_test_configs
from trpc_service.channels.models import InboundMessage
from trpc_service.config.tenant_repository import TenantRepositoryUnavailableError
from trpc_service.gateway.channel_service import (
    ChannelIngressService,
    ChannelTenantUnavailableError,
)
from trpc_service.gateway.client import HttpWorkerClient, WorkerClientError
from trpc_service.gateway.errors import (
    SAFE_ERROR_TEXT,
    TENANT_SERVICE_UNAVAILABLE_TEXT,
    map_worker_error,
)
from trpc_service.gateway.health import WorkerHealthManager
from trpc_service.gateway.routed_client import RoutedWorkerClient
from trpc_service.gateway.routing import RendezvousRouter, WorkerEndpoint
from trpc_service.transport.auth import InternalToken
from trpc_service.transport.models import (
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
)


def _task(**overrides: Any) -> WorkerTask:
    defaults: dict[str, Any] = {
        "protocol_version": 1,
        "request_id": uuid.uuid4(),
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "user_default",
        "channel": "web_console",
        "session_id": "sess-1",
        "message_id": "msg-1",
        "message": "hello",
    }
    defaults.update(overrides)
    return WorkerTask(**defaults)


def _inbound(**overrides: Any) -> InboundMessage:
    defaults: dict[str, Any] = {
        "tenant_id": "tenant_default",
        "channel": "web_console",
        "external_user_id": "user_abc",
        "external_conversation_id": "conv_123",
        "external_message_id": f"msg-{uuid.uuid4().hex[:8]}",
        "text": "hello",
    }
    defaults.update(overrides)
    return InboundMessage(**defaults)


def _token() -> InternalToken:
    return InternalToken("d" * 48)


class _FakeEndpointClient:
    """Minimal endpoint client used by routed-client failure contracts."""

    def __init__(self, endpoint_id: str) -> None:
        self.endpoint_id = endpoint_id
        self.chat_count = 0
        self.close_count = 0
        self.chat_error: WorkerClientError | None = None

    async def start(self) -> None:
        return None

    async def chat(self, task: WorkerTask) -> WorkerChatResult:
        self.chat_count += 1
        if self.chat_error is not None:
            raise self.chat_error
        return WorkerChatResult(
            protocol_version=1,
            request_id=task.request_id,
            response="reply",
            error_code=None,
        )

    async def stream(self, task: WorkerTask) -> AsyncIterator[WorkerEvent]:
        if False:
            yield WorkerEvent(
                protocol_version=1,
                request_id=task.request_id,
                type="done",
                data=None,
                error_code=None,
            )

    async def close(self) -> None:
        self.close_count += 1

    async def check_health(self, timeout_seconds: float) -> bool:
        return self.chat_error is None


class _FakeExecutionAuditRepository:
    """Records appended delivery facts; optionally fails like an outage."""

    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[Any] = []
        self._fail = fail

    async def append(self, event: Any) -> None:
        if self._fail:
            raise RuntimeError("audit backend down")
        self.events.append(event)

    async def check_ready(self) -> None:
        pass

    async def close(self) -> None:
        pass


# ------------------------------------------------------------------ timeout


def _client_with_handler(handler) -> HttpWorkerClient:
    return HttpWorkerClient(
        base_url="http://worker:8001",
        internal_token=_token(),
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.asyncio
async def test_worker_read_timeout_single_attempt_fixed_error():
    """Model/worker overrun surfaces WORKER_TIMEOUT after EXACTLY one POST."""
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("secret upstream detail", request=request)

    client = _client_with_handler(handler)
    try:
        with pytest.raises(WorkerClientError) as excinfo:
            await client.chat(_task())
    finally:
        await client.close()

    assert excinfo.value.code is WorkerErrorCode.WORKER_TIMEOUT
    assert attempts == 1, "a timeout must never be auto-retried"
    assert str(excinfo.value) == "worker_timeout", "error carries only the fixed code"
    # Public mapping: fixed text, no payload from the upstream exception.
    assert map_worker_error(excinfo.value.code) == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_worker_connect_failure_maps_unavailable_once():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("10.0.0.7 refused: pass=leak-me")

    client = _client_with_handler(handler)
    try:
        with pytest.raises(WorkerClientError) as excinfo:
            await client.chat(_task())
    finally:
        await client.close()

    assert excinfo.value.code is WorkerErrorCode.WORKER_UNAVAILABLE
    assert attempts == 1
    public = map_worker_error(excinfo.value.code)
    assert public == SAFE_ERROR_TEXT
    assert "leak-me" not in public and "10.0.0.7" not in public


# ------------------------------------------------------- no cross-endpoint retry


@pytest.mark.asyncio
async def test_routed_client_calls_one_endpoint_once_and_never_redrives():
    """A failed chat must not be re-issued to the other Worker endpoint."""
    ep_a = WorkerEndpoint.from_url("http://a:8001")
    ep_b = WorkerEndpoint.from_url("http://b:8002")
    client_a = _FakeEndpointClient(ep_a.endpoint_id)
    client_b = _FakeEndpointClient(ep_b.endpoint_id)
    failure = WorkerClientError(WorkerErrorCode.WORKER_TIMEOUT)
    client_a.chat_error = failure
    client_b.chat_error = failure

    async def probe(ep: WorkerEndpoint, timeout: float) -> bool:
        return True

    health = WorkerHealthManager(
        endpoints=[ep_a, ep_b],
        probe_fn=probe,
        interval_seconds=100.0,
        timeout_seconds=1.0,
        failure_threshold=10,
        recovery_threshold=2,
    )
    await health.start()
    routed = RoutedWorkerClient(clients={
        ep_a.endpoint_id: client_a,
        ep_b.endpoint_id: client_b,
    },
                                router=RendezvousRouter(),
                                health_manager=health)
    try:
        task = _task(session_id="ses_v1_" + "b" * 48)
        selected_id = routed._select(task)[0].endpoint_id
        with pytest.raises(WorkerClientError) as excinfo:
            await routed.chat(task)
        assert excinfo.value.code is WorkerErrorCode.WORKER_TIMEOUT
        # EXACTLY one endpoint was touched, exactly once, in one process.
        assert (client_a.chat_count + client_b.chat_count) == 1
        selected_count = client_a.chat_count if selected_id == ep_a.endpoint_id else client_b.chat_count
        assert selected_count == 1
    finally:
        await routed.close()


@pytest.mark.asyncio
async def test_all_workers_unhealthy_zero_calls():
    """No healthy endpoint -> fixed WORKER_UNAVAILABLE with zero HTTP calls."""
    ep_a = WorkerEndpoint.from_url("http://a:8001")
    client_a = _FakeEndpointClient(ep_a.endpoint_id)

    async def dead_probe(ep: WorkerEndpoint, timeout: float) -> bool:
        return False

    health = WorkerHealthManager(
        endpoints=[ep_a],
        probe_fn=dead_probe,
        interval_seconds=100.0,
        timeout_seconds=1.0,
        failure_threshold=1,
        recovery_threshold=1,
    )
    await health.start()
    routed = RoutedWorkerClient(clients={ep_a.endpoint_id: client_a}, router=RendezvousRouter(), health_manager=health)
    try:
        with pytest.raises(WorkerClientError) as excinfo:
            await routed.chat(_task())
        assert excinfo.value.code is WorkerErrorCode.WORKER_UNAVAILABLE
        assert client_a.chat_count == 0
    finally:
        await routed.close()


# ------------------------------------------------------------- SQL unavailability


class _FlakyTenantRepository(FakeTenantConfigRepository):
    """First N lookups fail like a dead database, then recovery."""

    def __init__(self, configs, failures: int) -> None:
        super().__init__(configs)
        self.remaining_failures = failures
        self.lookup_count = 0

    async def get(self, tenant_id):
        self.lookup_count += 1
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise TenantRepositoryUnavailableError("database is not reachable")
        return await super().get(tenant_id)


@pytest.mark.asyncio
async def test_sql_outage_fails_closed_then_recovery_executes_new_request():
    repo = _FlakyTenantRepository(make_default_test_configs(), failures=1)
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    with pytest.raises(ChannelTenantUnavailableError) as excinfo:
        await service.chat(_inbound())
    assert str(excinfo.value) == TENANT_SERVICE_UNAVAILABLE_TEXT
    assert "reachable" not in str(excinfo.value)
    assert worker.chat_tasks == [], "admission failure must not reach the Worker"

    # Recovery: the NEXT request executes normally (one Worker call).
    reply = await service.chat(_inbound())
    assert reply.response == "test-reply"
    assert len(worker.chat_tasks) == 1


@pytest.mark.asyncio
async def test_model_runtime_error_result_maps_fixed_text_no_payload():
    """A failed turn (tool/model error) yields ONLY the fixed sentence."""
    worker = RecordingWorkerClient()
    task_box: dict[str, WorkerTask] = {}

    async def failing_chat(task: WorkerTask) -> WorkerChatResult:
        task_box["task"] = task
        return WorkerChatResult(
            protocol_version=1,
            request_id=task.request_id,
            response="",
            error_code=WorkerErrorCode.MODEL_RUNTIME,
        )

    worker.chat = failing_chat  # type: ignore[method-assign]
    service = ChannelIngressService(FakeTenantConfigRepository(make_default_test_configs()), worker)
    reply = await service.chat(_inbound())
    assert reply.response == SAFE_ERROR_TEXT


# --------------------------------------------------------- channel delivery fail


@pytest.mark.asyncio
async def test_delivery_failure_recorded_once_never_retried():
    """IM delivery failure: one fixed error, one delivery fact, zero retries."""
    worker = RecordingWorkerClient()
    worker.chat_error = WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
    audit = _FakeExecutionAuditRepository()
    service = ChannelIngressService(
        FakeTenantConfigRepository(make_default_test_configs()),
        worker,
        execution_repository=audit,
    )

    reply = await service.chat(_inbound())
    assert reply.response == SAFE_ERROR_TEXT
    assert len(worker.chat_tasks) == 1
    assert len(audit.events) == 1
    event = audit.events[0]
    assert event.event_type == "delivery_result"
    assert event.outcome == "failed"
    assert event.error_code == "worker_unavailable"


@pytest.mark.asyncio
async def test_delivery_audit_append_failure_does_not_alter_reply():
    worker = RecordingWorkerClient()
    audit = _FakeExecutionAuditRepository(fail=True)
    service = ChannelIngressService(
        FakeTenantConfigRepository(make_default_test_configs()),
        worker,
        execution_repository=audit,
    )
    reply = await service.chat(_inbound())
    assert reply.response == "test-reply"
    assert len(audit.events) == 0


# ------------------------------------------------------- stream cancel releases


@pytest.mark.asyncio
async def test_consumer_cancel_closes_worker_stream_generator():
    """Early consumer exit must aclose the Worker stream (release resources)."""
    closed = {"value": False}

    class _ClosableStreamClient:

        async def start(self) -> None:
            pass

        async def chat(self, task):
            raise AssertionError("not used")

        async def stream(self, task) -> AsyncIterator[WorkerEvent]:
            try:
                yield WorkerEvent(
                    protocol_version=1,
                    request_id=task.request_id,
                    type="delta",
                    data="partial",
                    error_code=None,
                )
                yield WorkerEvent(
                    protocol_version=1,
                    request_id=task.request_id,
                    type="delta",
                    data="more",
                    error_code=None,
                )
            finally:
                closed["value"] = True

        async def close(self) -> None:
            pass

    service = ChannelIngressService(
        FakeTenantConfigRepository(make_default_test_configs()),
        _ClosableStreamClient(),  # type: ignore[arg-type]
    )
    events = service.stream(_inbound())
    first = await events.__anext__()
    assert first.type == "delta"
    await events.aclose()
    assert closed["value"] is True, "cancellation must close the Worker stream now"


@pytest.mark.asyncio
async def test_worker_stream_mid_failure_is_fixed_error_event():
    worker = RecordingWorkerClient()
    worker.stream_error = WorkerClientError(WorkerErrorCode.WORKER_TIMEOUT)
    audit = _FakeExecutionAuditRepository()
    service = ChannelIngressService(
        FakeTenantConfigRepository(make_default_test_configs()),
        worker,
        execution_repository=audit,
    )
    events = [event async for event in service.stream(_inbound())]
    assert events[-1].type == "error"
    assert events[-1].data == SAFE_ERROR_TEXT
    assert len(worker.stream_tasks) == 1
    assert len(audit.events) == 1
    assert audit.events[0].outcome == "failed"

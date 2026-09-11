"""Tests for Stage 5A Gateway channel ingress service."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import pytest

from tests.tenant_helpers import FakeTenantConfigRepository, make_default_test_configs
from trpc_service.channels.models import InboundMessage
from trpc_service.gateway.client import WorkerClientError
from trpc_service.transport.models import (
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
)


class RecordingWorkerClient:
    """Test double that records tasks and returns scripted results."""

    def __init__(self) -> None:
        self.chat_tasks: list[WorkerTask] = []
        self.stream_tasks: list[WorkerTask] = []
        self.chat_result: WorkerChatResult | None = None
        self.chat_error: WorkerClientError | None = None
        self.stream_events: list[WorkerEvent] = []
        self.stream_error: WorkerClientError | None = None

    async def start(self) -> None:
        pass

    async def chat(self, task: WorkerTask) -> WorkerChatResult:
        self.chat_tasks.append(task)
        if self.chat_error is not None:
            raise self.chat_error
        if self.chat_result is not None:
            return self.chat_result
        return WorkerChatResult(
            protocol_version=1,
            request_id=task.request_id,
            response="test-reply",
            error_code=None,
        )

    async def stream(self, task: WorkerTask) -> AsyncIterator[WorkerEvent]:
        self.stream_tasks.append(task)
        if self.stream_error is not None:
            raise self.stream_error
        for event in self.stream_events:
            yield event

    async def close(self) -> None:
        pass


def _make_inbound(**overrides) -> InboundMessage:
    defaults = {
        "tenant_id": "tenant_default",
        "channel": "web_console",
        "external_user_id": "user_abc",
        "external_conversation_id": "conv_123",
        "external_message_id": "msg_456",
        "text": "hello",
    }
    defaults.update(overrides)
    return InboundMessage(**defaults)


@pytest.mark.asyncio
async def test_chat_builds_worker_task_with_projected_identity():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    inbound = _make_inbound()
    await service.chat(inbound)

    assert len(worker.chat_tasks) == 1
    task = worker.chat_tasks[0]
    assert task.tenant_id == "tenant_default"
    assert task.app_id == "app_demo"
    assert task.channel == "web_console"
    assert task.user_id.startswith("usr_v1_")
    assert task.session_id.startswith("ses_v1_")
    assert task.message_id == "msg_456"
    assert task.message == "hello"
    assert task.config_version >= 1


@pytest.mark.asyncio
async def test_chat_calls_worker_once():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    await service.chat(_make_inbound())
    assert len(worker.chat_tasks) == 1


@pytest.mark.asyncio
async def test_rollout_selects_one_exact_snapshot_before_worker_call():
    from trpc_service.config.rollout import TenantConfigRollout
    from trpc_service.gateway.channel_service import ChannelIngressService

    active = make_default_test_configs()["tenant_default"]
    candidate = active.model_copy(update={
        "version": 2,
        "app": active.app.model_copy(update={"instruction": "candidate"})
    })

    class _Repository:

        async def get(self, tenant_id):
            return active if tenant_id == active.tenant_id else None

        async def get_version(self, tenant_id, version):
            return {1: active, 2: candidate}.get(version) if tenant_id == active.tenant_id else None

    class _Rollout:

        async def get_running(self, tenant_id):
            return TenantConfigRollout(tenant_id, 1, 2, 50,
                                       __import__("datetime").datetime.now(__import__("datetime").timezone.utc))

    worker = RecordingWorkerClient()
    service = ChannelIngressService(_Repository(), worker, rollout_repository=_Rollout())
    await service.chat(_make_inbound())
    task = worker.chat_tasks[0]
    assert task.config_version in {1, 2}
    # Same immutable platform message must select the same snapshot anywhere.
    await service.chat(_make_inbound())
    assert worker.chat_tasks[1].config_version == task.config_version


@pytest.mark.asyncio
async def test_chat_returns_reply_on_success():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    worker.chat_result = WorkerChatResult(
        protocol_version=1,
        request_id=uuid.uuid4(),
        response="model reply",
        error_code=None,
    )
    service = ChannelIngressService(repo, worker)

    reply = await service.chat(_make_inbound())
    assert reply.response == "model reply"


@pytest.mark.asyncio
async def test_chat_maps_tenant_unknown_error():
    from trpc_service.gateway.channel_service import ChannelIngressService, ChannelTenantNotFoundError

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    inbound = _make_inbound(tenant_id="unknown_tenant")
    with pytest.raises(ChannelTenantNotFoundError):
        await service.chat(inbound)


@pytest.mark.asyncio
async def test_chat_maps_unknown_repository_exception_to_unavailable():
    """P1: Unknown repository/admission exceptions must be 503, not 403."""
    from trpc_service.gateway.channel_service import (
        ChannelIngressService,
        ChannelTenantUnavailableError,
    )

    class ExplodingRepository:

        async def get(self, tenant_id):
            raise RuntimeError("Database connection lost")

        async def check_ready(self):
            pass

        async def close(self):
            pass

    worker = RecordingWorkerClient()
    service = ChannelIngressService(ExplodingRepository(), worker)

    inbound = _make_inbound()
    with pytest.raises(ChannelTenantUnavailableError):
        await service.chat(inbound)

    assert len(worker.chat_tasks) == 0


@pytest.mark.asyncio
async def test_stream_maps_unknown_repository_exception_to_unavailable():
    """P1: Unknown repository/admission exceptions in stream must yield 503 error event."""
    from trpc_service.gateway.channel_service import ChannelIngressService
    from trpc_service.gateway.errors import TENANT_SERVICE_UNAVAILABLE_TEXT

    class ExplodingRepository:

        async def get(self, tenant_id):
            raise RuntimeError("Database connection lost")

        async def check_ready(self):
            pass

        async def close(self):
            pass

    worker = RecordingWorkerClient()
    service = ChannelIngressService(ExplodingRepository(), worker)

    inbound = _make_inbound()
    events = []
    async for event in service.stream(inbound):
        events.append(event)

    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].data == TENANT_SERVICE_UNAVAILABLE_TEXT
    assert len(worker.stream_tasks) == 0


@pytest.mark.asyncio
async def test_chat_maps_worker_error_to_fixed_text():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    worker.chat_error = WorkerClientError(WorkerErrorCode.MODEL_CONFIGURATION)
    service = ChannelIngressService(repo, worker)

    reply = await service.chat(_make_inbound())
    assert "TRPC_MODEL" in reply.response or "configured" in reply.response.lower()


@pytest.mark.asyncio
async def test_chat_maps_session_busy_error():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    worker.chat_error = WorkerClientError(WorkerErrorCode.SESSION_BUSY)
    service = ChannelIngressService(repo, worker)

    reply = await service.chat(_make_inbound())
    assert "busy" in reply.response.lower()


@pytest.mark.asyncio
async def test_stream_yields_public_events():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="delta",
            data="partial",
        ),
        WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="done",
            data=None,
        ),
    ]
    service = ChannelIngressService(repo, worker)

    inbound = _make_inbound()
    events = []
    async for event in service.stream(inbound):
        events.append(event)

    assert len(events) == 2
    assert events[0].type == "delta"
    assert events[0].data == "partial"
    assert events[1].type == "done"


@pytest.mark.asyncio
async def test_stream_builds_worker_task_once():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    inbound = _make_inbound()
    async for _ in service.stream(inbound):
        pass

    assert len(worker.stream_tasks) == 1
    task = worker.stream_tasks[0]
    assert task.channel == "web_console"
    assert task.user_id.startswith("usr_v1_")


@pytest.mark.asyncio
async def test_stream_maps_worker_error_to_safe_text():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = FakeTenantConfigRepository(make_default_test_configs())
    worker = RecordingWorkerClient()
    worker.stream_error = WorkerClientError(WorkerErrorCode.MODEL_CONFIGURATION)
    service = ChannelIngressService(repo, worker)

    events = []
    async for event in service.stream(_make_inbound()):
        events.append(event)

    assert len(events) == 1
    assert events[0].type == "error"
    assert "TRPC_MODEL" in events[0].data or "configured" in events[0].data.lower()


# ── Stage 6A1: governance admission (channel + projected user) ──────────────

from tests.tenant_helpers import make_app_config, make_governance, make_tenant_config  # noqa: E402
from trpc_service.channels.identity import project_identity  # noqa: E402


def _projected_user(channel: str = "web_console") -> str:
    return project_identity(channel, "user_abc", "conv_123").user_id


def _governance_repo(**gov_kwargs) -> FakeTenantConfigRepository:
    configs = {
        "tenant_default":
        make_tenant_config(
            "tenant_default",
            app=make_app_config(),
            governance=make_governance(**gov_kwargs),
        ),
    }
    return FakeTenantConfigRepository(configs)


def _ingress(repo, worker=None):
    from trpc_service.gateway.channel_service import ChannelIngressService

    return ChannelIngressService(repo, worker or RecordingWorkerClient())


@pytest.mark.asyncio
async def test_chat_denied_channel_raises_access_denied_without_worker_call():
    from trpc_service.gateway.channel_service import (
        ChannelAccessDeniedError,
        ChannelIngressService,
    )
    from trpc_service.gateway.errors import ACCESS_DENIED_TEXT

    repo = _governance_repo(allowed_channels=("wecom", ))
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    with pytest.raises(ChannelAccessDeniedError) as exc_info:
        await service.chat(_make_inbound())

    assert str(exc_info.value) == ACCESS_DENIED_TEXT == "Access is not allowed."
    assert worker.chat_tasks == []
    assert "user_abc" not in str(exc_info.value)
    assert "msg_456" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_stream_denied_channel_yields_fixed_error_and_no_worker_call():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = _governance_repo(allowed_channels=("feishu", ))
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    events = [event async for event in service.stream(_make_inbound())]

    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].data == "Access is not allowed."
    assert worker.stream_tasks == []


@pytest.mark.asyncio
async def test_denied_user_not_in_allowlist_zero_worker_calls():
    from trpc_service.gateway.channel_service import (
        ChannelAccessDeniedError,
        ChannelIngressService,
    )

    other_user = "usr_v1_" + "e" * 48
    repo = _governance_repo(allowed_channels=("web_console", ), allowed_user_ids=(other_user, ))
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    with pytest.raises(ChannelAccessDeniedError):
        await service.chat(_make_inbound())
    assert worker.chat_tasks == []


@pytest.mark.asyncio
async def test_allowed_user_in_allowlist_reaches_worker_once():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = _governance_repo(
        allowed_channels=("web_console", ),
        allowed_user_ids=(_projected_user(), ),
    )
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    await service.chat(_make_inbound())

    assert len(worker.chat_tasks) == 1
    assert worker.chat_tasks[0].user_id == _projected_user()


@pytest.mark.asyncio
async def test_empty_user_allowlist_allows_every_user_of_allowed_channel():
    from trpc_service.gateway.channel_service import ChannelIngressService

    repo = _governance_repo(allowed_channels=("web_console", ), allowed_user_ids=())
    worker = RecordingWorkerClient()
    service = ChannelIngressService(repo, worker)

    await service.chat(_make_inbound())
    assert len(worker.chat_tasks) == 1


@pytest.mark.asyncio
async def test_governance_uses_single_repository_query_allow_and_deny():
    from trpc_service.gateway.channel_service import (
        ChannelAccessDeniedError,
        ChannelIngressService,
    )

    allow_repo = _governance_repo(allowed_channels=("web_console", ))
    service = ChannelIngressService(allow_repo, RecordingWorkerClient())
    await service.chat(_make_inbound())
    assert allow_repo.query_count == 1

    deny_repo = _governance_repo(allowed_channels=("feishu", ))
    service = ChannelIngressService(deny_repo, RecordingWorkerClient())
    with pytest.raises(ChannelAccessDeniedError):
        await service.chat(_make_inbound())
    assert deny_repo.query_count == 1


@pytest.mark.asyncio
async def test_tenant_lookup_failure_still_maps_unavailable_not_denied():
    """Disabled/unknown tenant keeps the existing 403-tenant-mapping semantics;
    governance denial is a distinct fixed-text error."""
    from trpc_service.gateway.channel_service import (
        ChannelIngressService,
        ChannelTenantNotFoundError,
    )

    configs = make_default_test_configs()
    configs["tenant_missing"] = make_tenant_config("absent")
    repo = FakeTenantConfigRepository({"other": make_tenant_config("other")})
    service = ChannelIngressService(repo, RecordingWorkerClient())
    with pytest.raises(ChannelTenantNotFoundError):
        await service.chat(_make_inbound())

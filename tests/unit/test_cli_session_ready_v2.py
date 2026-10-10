from __future__ import annotations

from typing import Any, ClassVar

import pytest

import trpc_service._cli as cli
from trpc_service.config.settings import SchedulerVersion, ServiceSettings
from trpc_service.queue.session_ready_outbox import SessionReadyOutboxQueue


class FakeResource:
    pool = object()


class FakeSecrets:
    def resolve(self, _reference: object) -> str:
        return "s" * 32


class FakeWeb:
    def __init__(self) -> None:
        self.routers: list[object] = []

    def include_router(self, router: object) -> None:
        self.routers.append(router)


def spy_type(name: str) -> type[Any]:
    class Spy:
        instances: ClassVar[list[Any]] = []

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs
            type(self).instances.append(self)

        async def close(self) -> None:
            return None

        async def ensure_group(self) -> None:
            return None

        async def run(self, *args: Any, **kwargs: Any) -> None:
            return None

        async def serve(self) -> None:
            return None

        def ready_for_delivery(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

    Spy.__name__ = name
    return Spy


class UnexpectedConstruction:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError(f"unexpected construction of {type(self).__name__}")


def patch_worker_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    generic = spy_type("WorkerDependency")
    for target in (
        "trpc_service.agent.factory.DevelopmentAgentLoader",
        "trpc_service.agent.factory.ProductionAgentLoader",
        "trpc_service.agent.worker.AgentWorker",
        "trpc_service.channels.feishu.FeishuAdapter",
        "trpc_service.channels.media_locator.WeComMediaLocatorCipher",
        "trpc_service.channels.wecom.WeComMediaDownloader",
        "trpc_service.storage.services.PostgresTenantServiceFactory",
        "trpc_service.tool.confirmation.ConfirmationTokenService",
        "trpc_service.tool.governance.GovernancePipeline",
        "trpc_service.tool.governance.SdkToolSafetyScanner",
        "trpc_service.tool.postgres.PostgresBudgetLedger",
        "trpc_service.tool.postgres.PostgresConfirmationLedger",
        "trpc_service.tool.postgres.PostgresExecutionLedger",
        "trpc_service.tool.postgres.PostgresGovernanceAuditSink",
    ):
        monkeypatch.setattr(target, generic)
    monkeypatch.setattr("trpc_service.tool.execution.ToolExecutor", generic)
    monkeypatch.setattr(cli, "_s3_artifact_store", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        cli,
        "_worker_fault_stage_runtime",
        lambda _settings, _secrets, repository: (repository, None),
    )
    monkeypatch.setattr(
        "trpc_service.tool.test_tool.build_fault_stage_test_tools",
        lambda **_kwargs: {},
    )


@pytest.mark.asyncio
async def test_v2_worker_uses_session_ready_consumer_without_v1_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_worker_dependencies(monkeypatch)
    ready_queue = spy_type("SessionReadyQueue")
    reclaimer = spy_type("SessionReadyReclaimer")
    v2_consumer = spy_type("SessionWorkerConsumer")
    monkeypatch.setattr("trpc_service.queue.session_ready.SessionReadyQueue", ready_queue)
    monkeypatch.setattr("trpc_service.queue.session_ready.SessionReadyReclaimer", reclaimer)
    monkeypatch.setattr(
        "trpc_service.queue.session_worker_consumer.SessionWorkerConsumer", v2_consumer
    )
    monkeypatch.setattr("trpc_service.queue.worker_consumer.WorkerConsumer", UnexpectedConstruction)

    settings = ServiceSettings(
        scheduler_version=SchedulerVersion.V2,
        worker_concurrency=3,
        worker_poll_seconds=2.5,
    )
    repository = FakeResource()
    await cli._serve_worker(settings, FakeSecrets(), repository, object())

    assert len(ready_queue.instances) == 1
    assert ready_queue.instances[0].kwargs == {
        "stream": settings.redis_stream,
        "group": settings.redis_consumer_group,
    }
    assert len(reclaimer.instances) == 1
    assert reclaimer.instances[0].kwargs["count"] == 1
    assert reclaimer.instances[0].kwargs["min_idle_ms"] == settings.redis_reclaim_after_ms
    assert reclaimer.instances[0].kwargs["poll_seconds"] == settings.worker_poll_seconds
    assert len(v2_consumer.instances) == 1
    assert v2_consumer.instances[0].args[:2] == (
        ready_queue.instances[0],
        reclaimer.instances[0],
    )
    assert v2_consumer.instances[0].kwargs == {
        "consumer_id": v2_consumer.instances[0].kwargs["consumer_id"],
        "concurrency": 3,
        "receive_block_ms": 2_500,
        "reclaimer_poll_seconds": settings.worker_poll_seconds,
        "ack_timeout_seconds": settings.redis_ack_timeout_seconds,
        "shutdown_grace_seconds": settings.shutdown_grace_seconds,
        "fault_stages": None,
        "fault_injection_enabled": False,
        "test_environment": False,
    }


@pytest.mark.asyncio
async def test_explicit_v1_worker_uses_legacy_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_worker_dependencies(monkeypatch)
    legacy_queue = spy_type("RedisStreamQueue")
    legacy_consumer = spy_type("WorkerConsumer")
    monkeypatch.setattr("trpc_service.queue.redis_streams.RedisStreamQueue", legacy_queue)
    monkeypatch.setattr("trpc_service.queue.worker_consumer.WorkerConsumer", legacy_consumer)
    monkeypatch.setattr(
        "trpc_service.queue.session_ready.SessionReadyQueue", UnexpectedConstruction
    )
    monkeypatch.setattr(
        "trpc_service.queue.session_ready.SessionReadyReclaimer", UnexpectedConstruction
    )
    monkeypatch.setattr(
        "trpc_service.queue.session_worker_consumer.SessionWorkerConsumer",
        UnexpectedConstruction,
    )

    settings = ServiceSettings(scheduler_version=SchedulerVersion.V1, worker_concurrency=2)
    await cli._serve_worker(settings, FakeSecrets(), FakeResource(), object())

    assert len(legacy_queue.instances) == 1
    assert legacy_queue.instances[0].kwargs == {
        "stream": settings.redis_stream,
        "group": settings.redis_consumer_group,
        "reclaim_after_ms": settings.redis_reclaim_after_ms,
    }
    assert len(legacy_consumer.instances) == 1
    assert legacy_consumer.instances[0].kwargs["consumer_id"].startswith("worker-")
    assert legacy_consumer.instances[0].kwargs["concurrency"] == 2


@pytest.mark.asyncio
async def test_v2_outbox_uses_session_ready_adapter_and_event_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready_queue = spy_type("SessionReadyQueue")
    dispatcher = spy_type("OutboxDispatcher")
    emergency = spy_type("EmergencyQueue")
    drainer = spy_type("EmergencyQueueDrainer")
    monkeypatch.setattr("trpc_service.queue.session_ready.SessionReadyQueue", ready_queue)
    monkeypatch.setattr("trpc_service.queue.dispatcher.OutboxDispatcher", dispatcher)
    monkeypatch.setattr("trpc_service.queue.emergency.EmergencyQueue", emergency)
    monkeypatch.setattr("trpc_service.queue.emergency.EmergencyQueueDrainer", drainer)

    settings = ServiceSettings(scheduler_version=SchedulerVersion.V2)
    await cli._serve_outbox(settings, FakeSecrets(), FakeResource(), object())

    assert len(dispatcher.instances) == 1
    outbox_args = dispatcher.instances[0].args
    assert isinstance(outbox_args[1], SessionReadyOutboxQueue)
    assert isinstance(outbox_args[1]._queue, ready_queue)
    assert dispatcher.instances[0].kwargs["event_type"] == "session.ready.v2"
    assert dispatcher.instances[0].kwargs["owner_id"].startswith("outbox-")


@pytest.mark.asyncio
async def test_gateway_and_wecom_pass_same_scheduler_version_to_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = spy_type("TenantRuntime")
    generic = spy_type("ConnectorDependency")
    server = spy_type("UvicornServer")
    monkeypatch.setattr("trpc_service.runtime.TenantRuntime", runtime)
    monkeypatch.setattr("trpc_service.channels.feishu.FeishuAdapter", generic)
    monkeypatch.setattr("trpc_service.channels.wecom.WeComConnector", generic)
    monkeypatch.setattr("trpc_service.channels.media_locator.WeComMediaLocatorCipher", generic)
    monkeypatch.setattr("trpc_service.storage.postgres.PostgresBindingLease", generic)
    monkeypatch.setattr("trpc_service.queue.emergency.EmergencyQueue", generic)
    monkeypatch.setattr("trpc_service.web.feishu_gateway.FeishuGatewayService", generic)
    monkeypatch.setattr("trpc_service.agent.wecom_manager.WeComConnectionManager", generic)
    monkeypatch.setattr("trpc_service.channels.dispatcher.ChannelDispatcher", generic)
    monkeypatch.setattr("uvicorn.Config", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("uvicorn.Server", server)
    monkeypatch.setattr("trpc_service.web.app.create_base_app", lambda **_kwargs: FakeWeb())
    monkeypatch.setattr(
        "trpc_service.web.feishu_gateway.create_feishu_gateway_router",
        lambda _service: object(),
    )

    settings = ServiceSettings(scheduler_version=SchedulerVersion.V2)
    secrets = FakeSecrets()
    repository = FakeResource()
    await cli._serve_gateway(settings, secrets, repository, object())
    await cli._serve_wecom(settings, secrets, repository)

    assert len(runtime.instances) == 2
    assert [instance.kwargs["scheduler_version"] for instance in runtime.instances] == [
        SchedulerVersion.V2,
        SchedulerVersion.V2,
    ]

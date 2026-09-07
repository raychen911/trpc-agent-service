from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import yaml
from asgi_lifespan import LifespanManager

from tenant_agent.channels.base import DeliveryResult, RateLimited
from tenant_agent.channels.planning import plan_outbound
from tenant_agent.container import ApplicationContainer
from tenant_agent.main import create_app
from tenant_agent.models import (
    BackendKind,
    BackendRef,
    ChannelType,
    OutboundMessage,
    SecretRef,
    TenantStatus,
)
from tenant_agent.services.dispatcher import DispatchResult
from tenant_agent.settings import ServiceRole, Settings
from tenant_agent.storage.base import OutboxItem
from tests.helpers import make_envelope, make_tenant


class RecordingAdapter:
    def __init__(self, *, fail: bool = False) -> None:
        self.messages: list[object] = []
        self.fail = fail

    async def deliver(self, message: object, **kwargs: object) -> DeliveryResult:
        del kwargs
        self.messages.append(message)
        if self.fail:
            raise RateLimited(1)
        return DeliveryResult(("delivered",))


class RecordingRegistry:
    def __init__(self, adapter: RecordingAdapter) -> None:
        self.adapter = adapter

    def get(self, channel: object) -> RecordingAdapter:
        del channel
        return self.adapter


class FailSecondSegmentOnceAdapter(RecordingAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    async def deliver(self, message: object, **kwargs: object) -> DeliveryResult:
        del kwargs
        self.messages.append(message)
        if len(self.messages) == 2 and not self.failed:
            self.failed = True
            raise RateLimited(0.01)
        return DeliveryResult((f"delivered-{len(self.messages)}",))


def settings(**updates: object) -> Settings:
    values: dict[str, object] = {
        "control_database_url": "inmemory://",
        "bootstrap_config_path": None,
        "session_hmac_key": "a-long-enough-test-session-hmac-key",
        "admin_bearer_token": "admin-test-token",
        "internal_bearer_token": "internal-test-token",
        "worker_poll_ms": 20,
        "outbox_poll_seconds": 0.01,
    }
    values.update(updates)
    return Settings(**values)


@pytest.mark.asyncio
async def test_agent_worker_consumes_job_and_outbox_worker_delivers() -> None:
    container = ApplicationContainer.build(settings())
    await container.initialize()
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    await container.configs.create_version(tenant, actor="test", activate=True)
    routed = container.gateway.route(make_envelope(tenant, message_id="worker-message", text="work"), tenant)
    await container.broker.publish(routed)
    assert await container.worker.run_once()

    adapter = RecordingAdapter()
    container.outbox.channels = RecordingRegistry(adapter)  # type: ignore[assignment]
    assert await container.outbox.run_once() == 1
    assert len(adapter.messages) == 1
    plane = await container.storage.for_tenant(tenant)
    decisions = {row.decision for row in await plane.audit.query_audit(tenant.tenant_id)}
    assert {"allowed", "delivered"} <= decisions
    await container.close()


@pytest.mark.asyncio
async def test_im_delivery_does_not_initialize_unrelated_tenant_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_BROKEN_S3", "not-json")
    container = ApplicationContainer.build(settings())
    await container.initialize()
    base = make_tenant(channel=ChannelType.TELEGRAM)
    tenant = base.model_copy(
        update={
            "data_backends": base.data_backends.model_copy(
                update={
                    "artifact": BackendRef(
                        kind=BackendKind.S3,
                        dsn_ref=SecretRef(uri="env://TENANT_ALPHA_BROKEN_S3"),
                    )
                }
            )
        }
    )
    await container.configs.create_version(tenant, actor="test", activate=True)
    message = OutboundMessage(
        tenant_id=tenant.tenant_id,
        binding_id=tenant.channels[0].binding_id,
        channel=ChannelType.TELEGRAM,
        external_chat_id="chat",
        text="deliver without S3",
    )
    await container.control.enqueue_outbox(  # type: ignore[attr-defined]
        OutboxItem(
            outbox_id="unrelated-backend",
            tenant_id=tenant.tenant_id,
            kind="im-delivery",
            payload={
                "message": message.model_dump(mode="json"),
                "config_revision": tenant.revision,
                "app_id": "assistant",
                "trace_context": {},
            },
            status="pending",
            attempts=0,
            available_at=datetime.now(UTC),
        )
    )
    adapter = RecordingAdapter()
    container.outbox.channels = RecordingRegistry(adapter)  # type: ignore[assignment]
    assert await container.outbox.run_once() == 1
    assert len(adapter.messages) == 1
    assert container.control._outbox["unrelated-backend"].status == "completed"  # type: ignore[attr-defined]
    await container.close()


@pytest.mark.asyncio
async def test_agent_worker_honors_bounded_parallelism() -> None:
    container = ApplicationContainer.build(settings(worker_concurrency=2))
    await container.initialize()
    tenant = make_tenant()
    await container.configs.create_version(tenant, actor="test", activate=True)
    for index in range(2):
        routed = container.gateway.route(
            make_envelope(tenant, message_id=f"parallel-{index}"),
            tenant,
        )
        await container.broker.publish(routed)

    both_started = asyncio.Event()
    release = asyncio.Event()
    started = 0

    async def slow_process(**kwargs: object) -> DispatchResult:
        nonlocal started
        del kwargs
        started += 1
        if started == 2:
            both_started.set()
        await release.wait()
        return DispatchResult("processed", ())

    container.worker.dispatcher.process = slow_process  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(container.worker.run_forever(stop))
    await asyncio.wait_for(both_started.wait(), timeout=1)
    stop.set()
    release.set()
    await asyncio.wait_for(task, timeout=1)
    assert started == 2
    await container.close()


@pytest.mark.asyncio
async def test_duplicate_processing_job_is_deferred_instead_of_acknowledged() -> None:
    container = ApplicationContainer.build(settings(worker_duplicate_defer_seconds=0))
    await container.initialize()
    tenant = make_tenant()
    await container.configs.create_version(tenant, actor="test", activate=True)
    envelope = make_envelope(tenant, message_id="still-processing")
    routed = container.gateway.route(envelope, tenant)
    claim = await container.control.claim_receipt(  # type: ignore[attr-defined]
        tenant_id=tenant.tenant_id,
        dedupe_key=container.identities.dedupe_key(envelope),
        owner="original-worker",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    assert claim.acquired
    await container.broker.publish(routed)

    assert await container.worker.run_once()
    assert container.broker.queue.qsize() == 1  # type: ignore[attr-defined]
    assert not container.broker.dead_letters  # type: ignore[attr-defined]
    assert await container.worker.run_once()
    waiting = await container.broker.receive(timeout_ms=100)
    assert waiting is not None and waiting.attempts == 0
    await container.broker.ack(waiting)
    assert not container.broker.dead_letters  # type: ignore[attr-defined]
    await container.close()


@pytest.mark.asyncio
async def test_worker_defers_transient_failures_and_increments_attempts() -> None:
    container = ApplicationContainer.build(
        settings(
            worker_retry_initial_seconds=0,
            worker_duplicate_defer_seconds=0,
        )
    )
    await container.initialize()
    tenant = make_tenant()
    await container.configs.create_version(tenant, actor="test", activate=True)
    routed = container.gateway.route(
        make_envelope(tenant, message_id="transient-worker-error"),
        tenant,
    )
    await container.broker.publish(routed)

    async def fail_transiently(**kwargs: object) -> DispatchResult:
        del kwargs
        raise RuntimeError("temporary backend outage")

    container.worker.dispatcher.process = fail_transiently  # type: ignore[method-assign]
    assert await container.worker.run_once()
    deferred = await container.broker.receive(timeout_ms=100)
    assert deferred is not None and deferred.attempts == 1
    await container.broker.ack(deferred)
    assert not await container.worker.run_once()
    assert not container.broker.dead_letters  # type: ignore[attr-defined]
    await container.close()


@pytest.mark.asyncio
async def test_worker_dead_letters_persistent_transient_failures_at_attempt_limit() -> None:
    container = ApplicationContainer.build(
        settings(
            worker_retry_initial_seconds=0,
            worker_retry_max_seconds=0,
            worker_max_attempts=2,
        )
    )
    await container.initialize()
    tenant = make_tenant()
    await container.configs.create_version(tenant, actor="test", activate=True)
    routed = container.gateway.route(
        make_envelope(tenant, message_id="persistent-worker-error"),
        tenant,
    )
    await container.broker.publish(routed)

    async def always_fail(**kwargs: object) -> DispatchResult:
        del kwargs
        raise RuntimeError("persistent backend outage")

    container.worker.dispatcher.process = always_fail  # type: ignore[method-assign]
    assert await container.worker.run_once()
    assert await container.worker.run_once()
    assert len(container.broker.dead_letters) == 1  # type: ignore[attr-defined]
    assert container.broker.dead_letters[0].attempts == 2  # type: ignore[attr-defined]
    await container.close()


@pytest.mark.asyncio
async def test_current_tenant_suspension_stops_queued_execution_and_delivery() -> None:
    container = ApplicationContainer.build(settings(worker_duplicate_defer_seconds=0))
    await container.initialize()
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    await container.configs.create_version(tenant, actor="test", activate=True)

    queued = container.gateway.route(
        make_envelope(tenant, message_id="queued-before-suspend"),
        tenant,
    )
    await container.broker.publish(queued)
    suspended = tenant.model_copy(update={"revision": 2, "status": TenantStatus.SUSPENDED})
    await container.configs.create_version(suspended, actor="security", activate=True)
    assert await container.worker.run_once()
    assert len(container.broker.dead_letters) == 1  # type: ignore[attr-defined]

    await container.configs.rollback(tenant.tenant_id, tenant.revision)
    delivery_route = container.gateway.route(
        make_envelope(tenant, message_id="delivery-before-suspend"),
        tenant,
    )
    await container.dispatcher.process(tenant=tenant, routed=delivery_route)
    await container.configs.activate(tenant.tenant_id, suspended.revision)
    adapter = RecordingAdapter()
    container.outbox.channels = RecordingRegistry(adapter)  # type: ignore[assignment]
    assert await container.outbox.run_once() == 1
    outbox = next(iter(container.control._outbox.values()))  # type: ignore[attr-defined]
    assert outbox.status == "dead"
    assert adapter.messages == []
    await container.close()


@pytest.mark.asyncio
async def test_outbox_rate_limit_retries_then_dead_letters() -> None:
    container = ApplicationContainer.build(settings(outbox_max_attempts=1))
    await container.initialize()
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    await container.configs.create_version(tenant, actor="test", activate=True)
    routed = container.gateway.route(make_envelope(tenant, message_id="failed-delivery", text="work"), tenant)
    await container.dispatcher.process(tenant=tenant, routed=routed)
    adapter = RecordingAdapter(fail=True)
    container.outbox.channels = RecordingRegistry(adapter)  # type: ignore[assignment]
    assert await container.outbox.run_once() == 1
    states = list(container.control._outbox.values())  # type: ignore[attr-defined]
    assert states[0].status == "dead"
    assert states[0].last_error_type == "RateLimited"
    await container.close()


@pytest.mark.asyncio
async def test_outbox_resumes_from_checkpoint_without_repeating_first_segment() -> None:
    container = ApplicationContainer.build(settings())
    await container.initialize()
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    await container.configs.create_version(tenant, actor="test", activate=True)
    logical = OutboundMessage(
        tenant_id=tenant.tenant_id,
        binding_id=tenant.channels[0].binding_id,
        channel=ChannelType.TELEGRAM,
        external_chat_id="chat",
        text="x" * 4_500,
        metadata={
            "internal_session_id": "session",
            "internal_user_id": "user",
            "app_id": "assistant",
        },
    )
    segments = plan_outbound(logical)
    assert len(segments) == 2
    item = OutboxItem(
        outbox_id="segmented",
        tenant_id=tenant.tenant_id,
        kind="im-delivery",
        payload={
            "message": logical.model_dump(mode="json"),
            "segments": [segment.model_dump(mode="json") for segment in segments],
            "next_segment": 0,
            "config_revision": tenant.revision,
            "app_id": "assistant",
            "trace_context": {},
        },
        status="pending",
        attempts=0,
        available_at=datetime.now(UTC),
    )
    await container.control.enqueue_outbox(item)  # type: ignore[attr-defined]
    adapter = FailSecondSegmentOnceAdapter()
    container.outbox.channels = RecordingRegistry(adapter)  # type: ignore[assignment]

    assert await container.outbox.run_once() == 1
    stored = container.control._outbox["segmented"]  # type: ignore[attr-defined]
    assert stored.status == "retry"
    assert stored.payload["next_segment"] == 1
    container.control._outbox["segmented"] = replace(  # type: ignore[attr-defined]
        stored,
        available_at=datetime.now(UTC),
    )

    assert await container.outbox.run_once() == 1
    completed = container.control._outbox["segmented"]  # type: ignore[attr-defined]
    assert completed.status == "completed"
    first_text = segments[0].text
    assert sum(getattr(message, "text", None) == first_text for message in adapter.messages) == 1
    await container.close()


@pytest.mark.asyncio
async def test_delivery_audit_failure_does_not_requeue_completed_reply() -> None:
    container = ApplicationContainer.build(settings())
    await container.initialize()
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    await container.configs.create_version(tenant, actor="test", activate=True)
    routed = container.gateway.route(make_envelope(tenant, message_id="audit-failure", text="work"), tenant)
    await container.dispatcher.process(tenant=tenant, routed=routed)
    plane = await container.storage.for_tenant(tenant)

    async def fail_audit(record: object) -> None:
        del record
        raise RuntimeError("audit unavailable")

    plane.audit.append_audit = fail_audit  # type: ignore[method-assign]
    adapter = RecordingAdapter()
    container.outbox.channels = RecordingRegistry(adapter)  # type: ignore[assignment]
    assert await container.outbox.run_once() == 1
    stored = next(iter(container.control._outbox.values()))  # type: ignore[attr-defined]
    assert stored.status == "completed"
    assert len(adapter.messages) == 1
    await container.close()


@pytest.mark.asyncio
async def test_internal_gateway_authenticates_and_recomputes_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_WEBHOOK_TOKEN", "web-token")
    tenant = make_tenant()
    bootstrap = tmp_path / "tenants.yaml"
    bootstrap.write_text(
        yaml.safe_dump({"tenants": [tenant.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )
    app = create_app(
        settings(
            service_role=ServiceRole.GATEWAY,
            bootstrap_config_path=bootstrap,
        )
    )
    async with LifespanManager(app):
        container = app.state.container
        routed = container.gateway.route(make_envelope(tenant), tenant)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            denied = await client.post("/internal/v1/inbound", json=routed.model_dump(mode="json"))
            assert denied.status_code == 401
            accepted = await client.post(
                "/internal/v1/inbound",
                headers={"authorization": "Bearer internal-test-token"},
                json=routed.model_dump(mode="json"),
            )
            assert accepted.status_code == 202
            queued = await container.broker.receive(timeout_ms=100)
            assert queued is not None and queued.routed == routed
            await container.broker.ack(queued)

            tampered = routed.model_copy(update={"session_id": "s_tampered"})
            rejected = await client.post(
                "/internal/v1/inbound",
                headers={"authorization": "Bearer internal-test-token"},
                json=tampered.model_dump(mode="json"),
            )
            assert rejected.status_code == 422


@pytest.mark.asyncio
async def test_production_startup_rejects_development_or_incomplete_security() -> None:
    with pytest.raises(ValueError, match="environment"):
        Settings(environment="Production")

    weak = ApplicationContainer.build(
        Settings(
            environment="production",
            control_database_url="inmemory://",
            redis_url="redis://unused",
            broker_mode="redis-streams",
            bootstrap_config_path=None,
        )
    )
    with pytest.raises(RuntimeError, match="development secrets"):
        await weak.initialize()
    await weak.close()

    incomplete_channel = ApplicationContainer.build(
        Settings(
            environment="production",
            service_role=ServiceRole.CHANNEL,
            control_database_url="inmemory://",
            redis_url="redis://unused",
            broker_mode="redis-streams",
            bootstrap_config_path=None,
            session_hmac_key="strong-session-key-with-more-than-32-characters",
            admin_bearer_token="strong-admin-key-with-more-than-32-characters",
            internal_bearer_token="strong-internal-key-with-more-than-32-characters",
        )
    )
    with pytest.raises(RuntimeError, match="GATEWAY_INTERNAL_URL"):
        await incomplete_channel.initialize()
    await incomplete_channel.close()

    unsafe_schema = ApplicationContainer.build(
        Settings(
            environment="production",
            service_role=ServiceRole.GATEWAY,
            control_database_url="inmemory://",
            redis_url="redis://unused",
            broker_mode="redis-streams",
            bootstrap_config_path=None,
            session_hmac_key="strong-session-key-with-more-than-32-characters",
            admin_bearer_token="strong-admin-key-with-more-than-32-characters",
            internal_bearer_token="strong-internal-key-with-more-than-32-characters",
            auto_create_schema=True,
        )
    )
    with pytest.raises(RuntimeError, match="Alembic"):
        await unsafe_schema.initialize()
    await unsafe_schema.close()

    unsafe_control = ApplicationContainer.build(
        Settings(
            environment="production",
            service_role=ServiceRole.GATEWAY,
            control_database_url="inmemory://",
            redis_url="redis://unused",
            broker_mode="redis-streams",
            bootstrap_config_path=None,
            session_hmac_key="strong-session-key-with-more-than-32-characters",
            admin_bearer_token="strong-admin-key-with-more-than-32-characters",
            internal_bearer_token="strong-internal-key-with-more-than-32-characters",
            auto_create_schema=False,
        )
    )
    with pytest.raises(RuntimeError, match="shared SQL"):
        await unsafe_control.initialize()
    await unsafe_control.close()

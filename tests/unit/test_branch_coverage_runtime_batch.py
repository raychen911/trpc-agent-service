from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from cryptography.exceptions import InvalidTag
from fastapi import HTTPException
from fastapi.testclient import TestClient
from redis.exceptions import RedisError, ResponseError

from tests.conftest import envelope, repository
from trpc_service.config.secrets import (
    LocalSecretProvider,
    SecretRef,
    SecretResolutionError,
    _secret_path,
    validate_tenant_secret_ref,
)
from trpc_service.config.settings import SchedulerVersion
from trpc_service.lifecycle import (
    ProcessLifecycle,
    is_process_live,
    is_process_ready,
    request_drain,
)
from trpc_service.queue.dispatcher import OutboxDispatcher
from trpc_service.queue.emergency import EmergencyQueue, EmergencyQueueDrainer
from trpc_service.queue.worker_consumer import WorkerConsumer, _HeartbeatFailed, _OwnershipLost
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import OutboxRecord
from trpc_service.tenant.auth import Principal, Role
from trpc_service.tenant.models import Channel
from trpc_service.web.admin import (
    BindingRequest,
    _parse_etag,
    _tenant_env_name,
    _validate_json_shape,
    create_admin_router,
)
from trpc_service.web.app import _BodyLimitMiddleware, create_base_app
from trpc_service.workspace.manager import WorkspaceManager


class EmergencyRedis:
    def __init__(self) -> None:
        self.group_error: BaseException | None = None
        self.claimed: Any = (b"0-0", [], [])
        self.rows: Any = []
        self.added: list[tuple[str, dict[str, Any]]] = []
        self.acks: list[tuple[Any, ...]] = []
        self.deleted: list[tuple[Any, ...]] = []
        self.delete_error: BaseException | None = None
        self.xread_error: BaseException | None = None

    async def xadd(self, stream: str, fields: dict[str, Any]) -> bytes:
        self.added.append((stream, fields))
        return b"1-0"

    async def xgroup_create(self, *_args: Any, **_kwargs: Any) -> None:
        if self.group_error is not None:
            raise self.group_error

    async def xautoclaim(self, *_args: Any, **_kwargs: Any) -> Any:
        return self.claimed

    async def xreadgroup(self, *_args: Any, **_kwargs: Any) -> Any:
        if self.xread_error is not None:
            raise self.xread_error
        return self.rows

    async def xack(self, *args: Any) -> None:
        self.acks.append(args)

    async def xdel(self, *args: Any) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(args)


async def _prepared() -> Any:
    repo = repository()
    route = await repo.resolve_binding("binding-unpredictable-a")
    assert route is not None
    return TenantRuntime(repo, routing_key=b"e" * 32).prepare(route, envelope())


def _outbox(outbox_id: str = "outbox-1", *, attempts: int = 0) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=outbox_id,
        tenant_id="tenant-a",
        event_type="inbound.accepted",
        aggregate_id="inbound-1",
        payload={"inbound_id": "inbound-1"},
        trace_headers={},
        attempts=attempts,
    )


@pytest.mark.asyncio
async def test_emergency_queue_validation_decode_and_cleanup_matrix() -> None:
    redis = EmergencyRedis()
    with pytest.raises(ValueError, match="key version"):
        EmergencyQueue(redis, b"k" * 32, key_version="")
    with pytest.raises(ValueError, match="previous key"):
        EmergencyQueue(redis, b"k" * 32, previous_keys={"old": b"short"})

    queue = EmergencyQueue(redis, b"k" * 32, previous_keys={"old": b"o" * 32})
    prepared = await _prepared()
    assert await queue.enqueue(prepared) == "1-0"
    fields = redis.added[-1][1]
    decoded = queue.decrypt("binding-unpredictable-a", fields["nonce"], fields["payload"])
    assert decoded.context.channel_binding_id == "binding-unpredictable-a"
    with pytest.raises(InvalidTag):
        queue.decrypt("other-binding", fields["nonce"], fields["payload"])
    with pytest.raises(ValueError):
        queue.decrypt("binding-unpredictable-a", "not-base64", fields["payload"])

    await queue.ack(SimpleNamespace(message_id="1-0"))
    redis.delete_error = RuntimeError("xdel unavailable")
    await queue.ack(SimpleNamespace(message_id="2-0"))
    redis.delete_error = None
    no_delete = EmergencyQueue(SimpleNamespace(xack=redis.xack), b"k" * 32)
    await no_delete.ack(SimpleNamespace(message_id="3-0"))

    redis.group_error = ResponseError("BUSYGROUP already exists")
    await queue.ensure_group()
    redis.group_error = ResponseError("permission denied")
    with pytest.raises(ResponseError):
        await queue.ensure_group()
    redis.group_error = None

    redis.claimed = ()
    redis.rows = []
    assert await queue.consume(consumer="drainer", block_ms=0) == ()
    redis.claimed = (b"9-0", [], [])
    redis.rows = [(b"stream", [(b"4-0", fields)])]
    assert (await queue.consume(consumer="drainer", block_ms=0))[0].message_id == "4-0"

    # Exercise both string and byte field names, plus poison quarantine paths.
    byte_fields = {key.encode(): value for key, value in fields.items()}
    redis.claimed = (b"0-0", [(b"5-0", byte_fields)], [])
    assert (await queue.consume(consumer="drainer"))[0].message_id == "5-0"
    redis.claimed = (b"0-0", [(b"poison", {b"binding_id": b"x"})], [])
    assert await queue.consume(consumer="drainer") == ()
    redis.claimed = (b"0-0", [], [])
    redis.xread_error = RedisError("redis read failed")
    with pytest.raises(RedisError):
        await queue.consume(consumer="drainer")

    redis.xread_error = None
    redis.claimed = (b"0-0", [(b"poison-2", {b"binding_id": b"x"})], [])
    redis.delete_error = RuntimeError("delete failed")
    await queue.consume(consumer="drainer")


class DrainerQueue:
    def __init__(
        self,
        messages: tuple[Any, ...],
        *,
        quarantine: bool = True,
        consume_error: BaseException | None = None,
    ) -> None:
        self.messages = messages
        self.consume_error = consume_error
        self.acked: list[Any] = []
        self.quarantined: list[tuple[Any, str]] = []
        if not quarantine:
            self.quarantine = None  # type: ignore[assignment]

    async def consume(self, **_kwargs: Any) -> tuple[Any, ...]:
        if self.consume_error is not None:
            raise self.consume_error
        return self.messages

    async def ack(self, message: Any) -> None:
        self.acked.append(message)

    async def quarantine(self, message: Any, *, reason: str) -> None:
        self.quarantined.append((message, reason))


class DrainerRepository:
    def __init__(self, *, error: BaseException | None = None) -> None:
        self.error = error
        self.calls: list[str] = []

    async def get_config(self, *_args: Any) -> None:
        if self.error is not None:
            raise self.error

    async def accept_inbound(self, **_kwargs: Any) -> None:
        self.calls.append("v1")

    async def accept_inbound_v2(self, **_kwargs: Any) -> None:
        self.calls.append("v2")


@pytest.mark.asyncio
async def test_emergency_drainer_stop_quarantine_versions_and_retry(monkeypatch) -> None:
    prepared = await _prepared()
    message = SimpleNamespace(message_id="m-1", prepared=prepared)
    stop = asyncio.Event()
    stop.set()
    queue = DrainerQueue((message,))
    drainer = EmergencyQueueDrainer(DrainerRepository(), queue, consumer_id="d")
    assert await drainer.drain_once(stop) == 0
    assert queue.acked == []

    repository_v1 = DrainerRepository()
    queue = DrainerQueue((message,))
    assert await EmergencyQueueDrainer(repository_v1, queue, consumer_id="d").drain_once() == 1
    assert repository_v1.calls == ["v1"]

    repository_v2 = DrainerRepository()
    queue = DrainerQueue((message,))
    assert (
        await EmergencyQueueDrainer(
            repository_v2,
            queue,
            consumer_id="d",
            scheduler_version=SchedulerVersion.V2,
        ).drain_once()
        == 1
    )
    assert repository_v2.calls == ["v2"]

    queue = DrainerQueue((message,))
    bad = DrainerRepository(error=ValueError("bad config"))
    assert await EmergencyQueueDrainer(bad, queue, consumer_id="d").drain_once() == 0
    assert queue.quarantined

    queue = DrainerQueue((message,), quarantine=False)
    bad = DrainerRepository(error=TypeError("bad input"))
    assert await EmergencyQueueDrainer(bad, queue, consumer_id="d").drain_once() == 0
    assert queue.acked == [message]

    queue = DrainerQueue((message,))
    with pytest.raises(RedisError):
        await EmergencyQueueDrainer(
            DrainerRepository(error=RedisError("down")), queue, consumer_id="d"
        ).drain_once()

    # Both retry branches are exercised without sleeping: the replacement
    # wait/sleep immediately raises cancellation or sets the stop event.
    queue = DrainerQueue((message,), consume_error=ValueError("retry"))
    drainer = EmergencyQueueDrainer(
        DrainerRepository(error=ValueError("retry")), queue, consumer_id="d"
    )

    async def cancel_sleep(_seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("trpc_service.queue.emergency.asyncio.sleep", cancel_sleep)
    with pytest.raises(asyncio.CancelledError):
        await drainer.run()

    stop = asyncio.Event()

    async def stop_wait(_awaitable: Any, **_kwargs: Any) -> None:
        close = getattr(_awaitable, "close", None)
        if callable(close):
            close()
        stop.set()

    monkeypatch.setattr("trpc_service.queue.emergency.asyncio.wait_for", stop_wait)
    await drainer.run(stop)


class DispatchRepo:
    def __init__(self, records: tuple[OutboxRecord, ...]) -> None:
        self.records = records
        self.released: list[tuple[str, dict[str, Any]]] = []
        self.published: list[str] = []
        self.claim_error: BaseException | None = None

    async def claim_outbox(self, **_kwargs: Any) -> tuple[OutboxRecord, ...]:
        if self.claim_error is not None:
            raise self.claim_error
        return self.records

    async def release_outbox(self, tenant_id: str, outbox_id: str, **kwargs: Any) -> None:
        self.released.append((outbox_id, kwargs))

    async def mark_outbox_published(self, tenant_id: str, outbox_id: str, **_kwargs: Any) -> None:
        self.published.append(outbox_id)


class DispatchQueue:
    def __init__(self, *, error: BaseException | None = None) -> None:
        self.error = error
        self.groups = 0

    async def ensure_group(self) -> None:
        self.groups += 1

    async def publish(self, _record: OutboxRecord) -> None:
        if self.error is not None:
            raise self.error


@pytest.mark.asyncio
async def test_outbox_dispatcher_stop_failure_and_retry_branches(monkeypatch) -> None:
    records = (_outbox("a"), _outbox("b"))
    repo = DispatchRepo(records)
    queue = DispatchQueue()
    stop = asyncio.Event()
    stop.set()
    assert await OutboxDispatcher(repo, queue, owner_id="o").dispatch_once(stop) == 0
    assert [item[0] for item in repo.released] == ["a", "b"]

    repo = DispatchRepo((_outbox("fail", attempts=7),))
    assert (
        await OutboxDispatcher(
            repo, DispatchQueue(error=RuntimeError("down")), owner_id="o"
        ).dispatch_once()
        == 0
    )
    assert repo.released[0][1]["delay"].total_seconds() == 60

    stop = asyncio.Event()
    repo = DispatchRepo(())
    dispatcher = OutboxDispatcher(repo, DispatchQueue(), owner_id="o")

    async def set_stop(event: asyncio.Event, _seconds: float) -> None:
        event.set()

    monkeypatch.setattr("trpc_service.queue.dispatcher._wait_or_stop", set_stop)
    await dispatcher.run(stop_event=stop)

    stop = asyncio.Event()
    repo = DispatchRepo(())
    repo.claim_error = RuntimeError("database unavailable")
    dispatcher = OutboxDispatcher(repo, DispatchQueue(), owner_id="o")

    async def fail_then_stop(event: asyncio.Event, _seconds: float) -> None:
        event.set()

    monkeypatch.setattr("trpc_service.queue.dispatcher._wait_or_stop", fail_then_stop)
    await dispatcher.run(stop_event=stop, poll_seconds=0)
    assert stop.is_set()


class HeartbeatQueue:
    async def heartbeat(self, *_args: Any, **_kwargs: Any) -> bool:
        return True

    async def ensure_group(self) -> None:
        return None

    async def consume(self, **_kwargs: Any) -> tuple[Any, ...]:
        raise RuntimeError("consume failed")


@pytest.mark.asyncio
async def test_worker_consumer_internal_operation_and_shutdown_edges() -> None:
    consumer = WorkerConsumer(
        SimpleNamespace(), HeartbeatQueue(), SimpleNamespace(), consumer_id="w"
    )
    with pytest.raises(ValueError, match="shutdown grace"):
        WorkerConsumer(
            SimpleNamespace(),
            HeartbeatQueue(),
            SimpleNamespace(),
            consumer_id="w",
            shutdown_grace_seconds=0,
        )

    async def returns(value: Any) -> Any:
        return value

    heartbeat = asyncio.create_task(asyncio.Event().wait())
    result = await consumer._run_owned_operation(returns("ok"), heartbeat)
    assert result == "ok"
    heartbeat.cancel()
    await asyncio.gather(heartbeat, return_exceptions=True)

    async def failed_heartbeat_body() -> bool:
        raise RuntimeError("heartbeat")

    failed_heartbeat = asyncio.create_task(failed_heartbeat_body())
    await asyncio.gather(failed_heartbeat, return_exceptions=True)
    with pytest.raises(_HeartbeatFailed):
        await consumer._run_owned_operation(returns("unused"), failed_heartbeat)

    false_heartbeat = asyncio.create_task(returns(False))
    await false_heartbeat
    with pytest.raises(_OwnershipLost):
        await consumer._run_owned_operation(returns("unused"), false_heartbeat)

    task = asyncio.create_task(asyncio.sleep(0))
    await task
    await consumer._cancel_task_bounded(task)
    pending = asyncio.create_task(asyncio.Event().wait())
    await consumer._cancel_task_bounded(pending)
    assert pending.done()

    stop = asyncio.Event()
    heartbeat = asyncio.create_task(asyncio.Event().wait())
    ok, error = await consumer._stop_heartbeat(stop, heartbeat)
    assert not ok and isinstance(error, TimeoutError)

    stop = asyncio.Event()
    heartbeat = asyncio.create_task(returns(True))
    ok, error = await consumer._stop_heartbeat(stop, heartbeat)
    assert ok and error is None

    stop = asyncio.Event()
    consumer = WorkerConsumer(
        SimpleNamespace(), HeartbeatQueue(), SimpleNamespace(), consumer_id="w", concurrency=2
    )
    with pytest.raises(RuntimeError, match="consume failed"):
        await consumer.run(stop)


def test_secret_reference_and_provider_boundary_matrix(tmp_path: Path, monkeypatch) -> None:
    invalid_refs = ("http://x", "env://", "env://bad-name", "env://GOOD?x=1", "file:///tmp/x#frag")
    for value in invalid_refs:
        with pytest.raises(ValueError):
            SecretRef(uri=value)
    assert str(SecretRef(uri="env://GOOD")) == "SecretRef(env://***)"

    with pytest.raises(SecretResolutionError):
        _secret_path("env://NOPE")
    with pytest.raises(SecretResolutionError):
        _secret_path("file://host/path") if os.name != "nt" else _secret_path(
            "file:///tmp/path?x=1"
        )

    root = tmp_path / "secrets"
    root.mkdir()
    good = root / "good"
    good.write_text("secret\n", encoding="utf-8")
    provider = LocalSecretProvider(secret_root=root, allowed_env_names={"TRPC_TENANT_KEY"})
    monkeypatch.setenv("TRPC_TENANT_KEY", "env-secret")
    assert provider.resolve(SecretRef(uri="env://TRPC_TENANT_KEY")) == "env-secret"
    assert provider.resolve_tenant(SecretRef(uri="env://TRPC_TENANT_KEY")) == "env-secret"
    assert provider.resolve(SecretRef(uri=good.as_uri())) == "secret"
    assert provider.resolve_tenant(SecretRef(uri=good.as_uri())) == "secret"

    for ref in (
        SecretRef(uri="literal://x"),
        SecretRef(uri="env://OTHER"),
        SecretRef(uri="file:///relative/path"),
    ):
        with pytest.raises(SecretResolutionError):
            validate_tenant_secret_ref(ref, allowed_env_names={"TRPC_TENANT_KEY"}, secret_root=root)
    with pytest.raises(SecretResolutionError):
        validate_tenant_secret_ref(SecretRef(uri=good.as_uri()), secret_root=None)
    with pytest.raises(SecretResolutionError):
        validate_tenant_secret_ref(SecretRef(uri="file:///tmp/outside"), secret_root=root)

    missing = root / "missing"
    with pytest.raises(SecretResolutionError):
        provider.resolve(SecretRef(uri=missing.as_uri()))
    empty = root / "empty"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(SecretResolutionError):
        provider.resolve_tenant(SecretRef(uri=empty.as_uri()))
    with pytest.raises(SecretResolutionError):
        LocalSecretProvider().resolve(SecretRef(uri="env://MISSING_SECRET"))
    assert (
        LocalSecretProvider(allow_literal=True).resolve(SecretRef(uri="literal://hello%20world"))
        == "hello world"
    )
    with pytest.raises(SecretResolutionError):
        LocalSecretProvider().resolve(SecretRef(uri="literal://hello"))

    with pytest.raises(SecretResolutionError):
        validate_tenant_secret_ref(SecretRef(uri="file:///../escape"), secret_root=root)


@pytest.mark.asyncio
async def test_lifecycle_boundary_matrix(tmp_path: Path, monkeypatch) -> None:
    with pytest.raises(ValueError):
        ProcessLifecycle("", tmp_path)
    with pytest.raises(ValueError):
        ProcessLifecycle("worker", tmp_path, heartbeat_interval_seconds=0)
    assert not is_process_live("worker", tmp_path, max_age_seconds=0)
    assert not is_process_live("worker", tmp_path, max_age_seconds=1)
    heartbeat = tmp_path / "worker.heartbeat"
    heartbeat.write_text("x", encoding="ascii")
    future = time.time() + 10
    os.utime(heartbeat, (future, future))
    assert not is_process_live("worker", tmp_path, max_age_seconds=1)
    assert is_process_ready("worker", tmp_path)
    assert request_drain("worker", tmp_path).exists()
    assert not is_process_ready("worker", tmp_path)

    lifecycle = ProcessLifecycle("worker", tmp_path, heartbeat_interval_seconds=0.01)
    monkeypatch.setattr(lifecycle, "_install_signal_handlers", lambda: None)
    await lifecycle.start()
    assert lifecycle.heartbeat_path.exists()
    await lifecycle.close()
    await lifecycle.close()

    lifecycle = ProcessLifecycle("other", tmp_path, heartbeat_interval_seconds=0.01)
    monkeypatch.setattr(lifecycle, "_install_signal_handlers", lambda: None)
    await lifecycle.start()
    lifecycle.request_stop()
    await asyncio.wait_for(lifecycle.stop_event.wait(), timeout=1)
    await lifecycle.close()


@pytest.mark.asyncio
async def test_body_limit_middleware_scope_length_and_stream_edges() -> None:
    calls: list[str] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        del scope
        calls.append("app")
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = _BodyLimitMiddleware(app, max_bytes=3)

    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"ok", "more_body": False}

    await middleware({"type": "lifespan", "headers": []}, receive, send)
    assert calls == ["app"]
    await middleware({"type": "http", "headers": [(b"content-length", b"4")]}, receive, send)
    await middleware({"type": "http", "headers": [(b"content-length", b"bad")]}, receive, send)

    chunks = iter((b"ab", b"cd"))

    async def too_large_receive() -> dict[str, Any]:
        return {"type": "http.request", "body": next(chunks), "more_body": True}

    await middleware({"type": "http", "headers": []}, too_large_receive, send)
    assert any(item.get("status") == 413 for item in sent)


class AdminAuthorizer:
    async def authenticate(self, _token: str) -> Principal:
        return Principal(
            subject="admin",
            roles=frozenset({Role.PLATFORM_ADMIN}),
            tenant_ids=frozenset({"*"}),
        )


@pytest.mark.parametrize(
    "value",
    ["not-an-etag", '"0"', 'W/"0"'],
)
def test_admin_validation_shape_and_binding_edges(value: str) -> None:
    with pytest.raises(HTTPException):
        _parse_etag(value)
    assert _parse_etag('W/"3"') == 3
    assert _tenant_env_name("TRPC_TENANT_SECRET")
    assert not _tenant_env_name("BAD-NAME")

    with pytest.raises(ValueError):
        _validate_json_shape({"x": {"y": 1}}, depth=13)
    with pytest.raises(ValueError):
        _validate_json_shape({str(i): i for i in range(257)})
    with pytest.raises(ValueError):
        _validate_json_shape([0] * 257)
    with pytest.raises(ValueError):
        _validate_json_shape({"x": "a" * (16 * 1024 + 1)})
    with pytest.raises(ValueError):
        _validate_json_shape({1: "bad"})

    disabled_feishu = BindingRequest(
        app_id="app", channel=Channel.FEISHU, account_id="account", enabled=False
    )
    assert disabled_feishu.enabled is False
    bad_refs = (
        {"app_secret": "literal://secret"},
        {"app_secret": "env://BAD-NAME"},
        {"app_secret": "file:///tmp/not-allowed"},
        {"app_secret": "http://bad"},
    )
    for secret_refs in bad_refs:
        with pytest.raises(ValueError):
            BindingRequest(
                app_id="app",
                channel=Channel.WECOM_AI_BOT,
                account_id="account",
                secret_refs=secret_refs,
            )


class AdminRepository:
    async def get_tenant(self, _tenant_id: str) -> dict[str, Any]:
        return {"control_version": 1}


def test_admin_router_binding_and_base_app_validation_edges() -> None:
    router = create_admin_router(AdminRepository(), AdminAuthorizer())
    app = create_base_app(title="admin")
    app.include_router(router)
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/v1/tenants/t", headers={"Authorization": "Bearer token"})
    assert response.status_code in {200, 404}
    invalid = client.post("/v1/tenants", json={"unexpected": True})
    assert invalid.status_code == 401


def test_workspace_manager_edges(tmp_path: Path, monkeypatch) -> None:
    with pytest.raises(ValueError):
        WorkspaceManager(tmp_path, key=b"short")
    manager = WorkspaceManager(tmp_path, key=b"w" * 32)
    workspace = manager.for_context(SimpleNamespace(tenant_id="tenant-a", session_id="session-a"))
    assert workspace.path.exists()
    assert workspace.environment["TRPC_WORKSPACE_ROOT"] == str(workspace.path)
    assert workspace.metadata == {"workspace_root": str(workspace.path)}

    tenants = tmp_path / "tenants"
    tenants.mkdir(exist_ok=True)
    original_is_symlink = Path.is_symlink

    def fake_symlink(path: Path) -> bool:
        if path == tenants:
            return True
        return original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", fake_symlink)
    with pytest.raises(ValueError, match="symlinks"):
        manager.prepare("tenant-b", "session-b")

    monkeypatch.undo()
    original_resolve = Path.resolve

    def escape(path: Path, *args: Any, **kwargs: Any) -> Path:
        if path.name == "work":
            return Path("/outside-workspace")
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", escape)
    with pytest.raises(ValueError, match="escaped"):
        manager.prepare("tenant-c", "session-c")

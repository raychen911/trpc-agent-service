"""Gateway id fallback, tenant rate limiting and shutdown behavior."""

from __future__ import annotations

import asyncio
from threading import Event

from fastapi.testclient import TestClient

from trpc_service.channels import ChannelAdapter
from trpc_service.channels import InboundMessage
from trpc_service.channels import SendResult
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import WeComChannelConfig
from trpc_service.web.gateway import ChannelRegistry
from trpc_service.web.gateway import LocalIdempotencyStore
from trpc_service.web.gateway import LocalRateLimiter
from trpc_service.web.gateway import RedisRateLimiter
from trpc_service.web.gateway import build_rate_limiter
from trpc_service.web.gateway import create_gateway_app


class Adapter(ChannelAdapter):
    channel = "fake"

    async def verify_signature(self, payload, headers, query):
        return True

    async def parse_message(self, payload):
        return InboundMessage(
            channel="fake",
            chat_id="chat",
            sender_id="user",
            message_id=str(payload.get("message_id", "")),
            text=str(payload.get("text", "")),
        )

    async def send_message(self, outbound):
        return SendResult(ok=True)

    async def send_stream(self, chat_id, stream):
        return SendResult(ok=True)

    async def reply_text(self, inbound, text):
        return SendResult(ok=True)


class Worker:

    def __init__(self, manager, started: Event | None = None) -> None:
        self.manager = manager
        self.messages: list[InboundMessage] = []
        self.started = started

    def resolve_tenant(self, tenant_id, config_revision=None):
        del config_revision
        return self.manager.get(tenant_id)

    async def handle(self, tenant_id, channel, inbound):
        del tenant_id, channel
        self.messages.append(inbound)
        if self.started is not None:
            self.started.set()
            await asyncio.Event().wait()
        return "ok"


def _app(*, limit=None, worker=None, store=None, limiter=None, async_dispatch=False):
    manager = TenantConfigManager()
    tenant = Tenant(
        tenant_id="tenant",
        name="Tenant",
        model=ModelEndpoint(model_name="model"),
        channel_configs={"fake": WeComChannelConfig(token="token", aes_key="aes", corp_id="corp", agent_id="1")},
    )
    tenant.im_access_policy.callback_requests_per_minute = limit
    manager.register(tenant)
    worker = worker or Worker(manager)
    app = create_gateway_app(
        manager=manager,
        worker=worker,
        registry=ChannelRegistry({"fake": lambda _config: Adapter()}),
        idempotency_store=store,
        rate_limiter=limiter,
        async_dispatch=async_dispatch,
    )
    return app, worker


def test_idless_callbacks_get_stable_body_fingerprint_without_cross_message_collision():
    app, worker = _app()
    client = TestClient(app)

    first = client.post("/webhook/tenant/fake", json={"text": "one"})
    second = client.post("/webhook/tenant/fake", json={"text": "two"})
    duplicate = client.post("/webhook/tenant/fake", json={"text": "one"})

    assert first.status_code == second.status_code == duplicate.status_code == 200
    assert duplicate.json() == {"status": "duplicate"}
    assert [message.text for message in worker.messages] == ["one", "two"]
    assert all(message.message_id.startswith("callback-") for message in worker.messages)
    assert worker.messages[0].message_id != worker.messages[1].message_id
    assert worker.messages[0].metadata["message_id_synthesized"] is True


def test_tenant_callback_limit_rejects_distinct_messages_and_reports_retry_window():
    app, worker = _app(limit=1, limiter=LocalRateLimiter())
    client = TestClient(app)

    first = client.post("/webhook/tenant/fake", json={"message_id": "one"})
    limited = client.post("/webhook/tenant/fake", json={"message_id": "two"})

    assert first.status_code == 200
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "60"
    assert len(worker.messages) == 1


def test_rate_limit_backend_failure_is_retryable_and_releases_idempotency_key():

    class BrokenLimiter:
        calls = 0

        async def allow(self, key, limit, window_seconds=60):
            del key, limit, window_seconds
            self.calls += 1
            raise ConnectionError("redis unavailable")

    limiter = BrokenLimiter()
    app, worker = _app(limit=1, limiter=limiter)
    client = TestClient(app)
    payload = {"message_id": "retry"}

    assert client.post("/webhook/tenant/fake", json=payload).status_code == 503
    assert client.post("/webhook/tenant/fake", json=payload).status_code == 503
    assert limiter.calls == 2
    assert worker.messages == []


async def test_local_and_redis_rate_limiters_cover_window_atomicity_and_lifecycle():
    now = [10.0]
    local = LocalRateLimiter(clock=lambda: now[0])
    assert await local.allow("tenant", 2) is True
    assert await local.allow("tenant", 2) is True
    assert await local.allow("tenant", 2) is False
    assert await local.allow("disabled", 0) is True
    now[0] += 60
    assert await local.allow("tenant", 2) is True

    class Redis:

        def __init__(self) -> None:
            self.args = None
            self.closed = False

        async def eval(self, *args):
            self.args = args
            return 1

        async def aclose(self):
            self.closed = True

    redis = Redis()
    shared = RedisRateLimiter(client=redis, prefix="limit")
    assert await shared.allow("tenant:fake", 3, 10) is True
    assert redis.args[2:] == ("limit:tenant:fake", 10, 3)
    assert await shared.allow("disabled", 0) is True
    await shared.close()
    assert redis.closed is True
    assert isinstance(build_rate_limiter(), LocalRateLimiter)
    built = build_rate_limiter("redis://unused")
    assert isinstance(built, RedisRateLimiter)
    await built.close()


def test_gateway_shutdown_cancels_background_dispatch_releases_key_and_closes_resources():

    class ClosingStore(LocalIdempotencyStore):

        def __init__(self) -> None:
            super().__init__()
            self.released: list[str] = []
            self.closed = False

        async def release(self, key):
            self.released.append(key)
            await super().release(key)

        async def close(self):
            self.closed = True

    class ClosingLimiter(LocalRateLimiter):

        def __init__(self) -> None:
            super().__init__()
            self.closed = False

        async def close(self):
            self.closed = True

    manager = TenantConfigManager()
    tenant = Tenant(
        tenant_id="tenant",
        name="Tenant",
        model=ModelEndpoint(model_name="model"),
        channel_configs={"fake": WeComChannelConfig(token="token", aes_key="aes", corp_id="corp", agent_id="1")},
    )
    manager.register(tenant)
    started = Event()
    worker = Worker(manager, started)
    store = ClosingStore()
    limiter = ClosingLimiter()
    app, _ = _app(worker=worker, store=store, limiter=limiter, async_dispatch=True)

    with TestClient(app) as client:
        assert client.post("/webhook/tenant/fake", json={"message_id": "running"}).status_code == 200
        assert started.wait(timeout=1)

    assert store.released == ["tenant:fake:running"]
    assert store.closed is True
    assert limiter.closed is True


def test_redis_rate_limiter_requires_a_connection_source():
    try:
        RedisRateLimiter()
    except ValueError as exc:
        assert "redis_url or client" in str(exc)
    else:  # pragma: no cover - assertion helper
        raise AssertionError("missing Redis connection must fail")

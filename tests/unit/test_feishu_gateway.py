from __future__ import annotations

import asyncpg
from fastapi.testclient import TestClient

from tests.unit.test_feishu import APP_ID, feishu_binding
from trpc_service.channels.envelopes import InboundEnvelope, PayloadKind
from trpc_service.channels.feishu import FeishuCallback, FeishuVerificationError
from trpc_service.storage.models import BindingRoute
from trpc_service.tenant.models import Channel, ConversationKind
from trpc_service.web.app import create_base_app
from trpc_service.web.feishu_gateway import (
    FeishuGatewayService,
    create_feishu_gateway_router,
)


class Repository:
    def __init__(self) -> None:
        self.route = BindingRoute(binding=feishu_binding(), active_config_version=1)
        self.failure = False

    async def resolve_binding(self, binding_id):
        if self.failure:
            raise asyncpg.PostgresError("down")
        return self.route if binding_id == "feishu-binding" else None


class Runtime:
    def __init__(self) -> None:
        self.accepted = []
        self.failure = False

    def prepare(self, route, envelope):
        return route, envelope

    async def accept_prepared(self, prepared):
        if self.failure:
            raise asyncpg.PostgresError("down")
        self.accepted.append(prepared)


class Adapter:
    def __init__(self) -> None:
        self.challenge = False
        self.failure = False
        self.unexpected = False

    def verify_and_parse(self, request, binding):
        if self.unexpected:
            raise RuntimeError("unexpected adapter failure")
        if self.failure:
            raise FeishuVerificationError("bad")
        if self.challenge:
            return FeishuCallback(challenge="challenge")
        return FeishuCallback(
            envelope=InboundEnvelope(
                channel=Channel.FEISHU,
                account_id=APP_ID,
                external_message_id="om_message",
                external_user_id="ou_user",
                conversation_kind=ConversationKind.DIRECT,
                payload_kind=PayloadKind.TEXT,
                text="hello",
            )
        )


class EmergencyQueue:
    def __init__(self) -> None:
        self.values = []

    async def enqueue(self, value):
        self.values.append(value)


def client(service: FeishuGatewayService) -> TestClient:
    app = create_base_app(title="gateway")
    app.include_router(create_feishu_gateway_router(service))
    return TestClient(app, raise_server_exceptions=False)


def test_feishu_gateway_challenge_acceptance_and_rejections() -> None:
    repository = Repository()
    runtime = Runtime()
    adapter = Adapter()
    service = FeishuGatewayService(repository, runtime, adapter, max_body_bytes=9)
    http = client(service)

    adapter.challenge = True
    response = http.post("/v1/channels/feishu/feishu-binding/callback", content=b"challenge")
    assert response.status_code == 200
    assert response.json() == {"challenge": "challenge"}
    assert not runtime.accepted

    adapter.challenge = False
    response = http.post("/v1/channels/feishu/feishu-binding/callback", content=b"message")
    assert response.status_code == 200
    assert response.json() == {"msg": "success"}
    assert runtime.accepted
    assert (
        http.post("/v1/channels/feishu/feishu-binding/callback", content=b"too-large!").status_code
        == 413
    )
    adapter.failure = True
    assert (
        http.post("/v1/channels/feishu/feishu-binding/callback", content=b"bad").status_code == 403
    )
    adapter.failure = False
    repository.route = repository.route.model_copy(
        update={
            "binding": repository.route.binding.model_copy(update={"channel": Channel.WECOM_AI_BOT})
        }
    )
    assert (
        http.post("/v1/channels/feishu/feishu-binding/callback", content=b"bad").status_code == 403
    )


def test_feishu_gateway_cached_route_and_emergency_queue() -> None:
    repository = Repository()
    runtime = Runtime()
    emergency = EmergencyQueue()
    service = FeishuGatewayService(repository, runtime, Adapter(), emergency_queue=emergency)
    http = client(service)
    path = "/v1/channels/feishu/feishu-binding/callback"

    assert http.post(path, content=b"message").status_code == 200
    repository.failure = True
    runtime.failure = True
    assert http.post(path, content=b"message").status_code == 200
    assert emergency.values

    service._routes["feishu-binding"].expires_at = 0
    assert http.post(path, content=b"message").status_code == 503


def test_feishu_gateway_rejects_unknown_disabled_and_inactive_bindings() -> None:
    repository = Repository()
    service = FeishuGatewayService(repository, Runtime(), Adapter())
    http = client(service)
    path = "/v1/channels/feishu/feishu-binding/callback"

    assert http.post("/v1/channels/feishu/missing/callback", content=b"message").status_code == 403

    repository.route = repository.route.model_copy(
        update={
            "binding": repository.route.binding.model_copy(update={"enabled": False}),
        }
    )
    assert http.post(path, content=b"message").status_code == 403

    repository.route = repository.route.model_copy(
        update={
            "binding": repository.route.binding.model_copy(update={"enabled": True}),
            "tenant_active": False,
        }
    )
    assert http.post(path, content=b"message").status_code == 403


def test_feishu_gateway_returns_503_without_emergency_queue() -> None:
    repository = Repository()
    runtime = Runtime()
    service = FeishuGatewayService(repository, runtime, Adapter())
    http = client(service)
    runtime.failure = True
    assert (
        http.post("/v1/channels/feishu/feishu-binding/callback", content=b"message").status_code
        == 503
    )

    repository.failure = True
    assert http.post("/v1/channels/feishu/other/callback", content=b"message").status_code == 503


def test_feishu_gateway_converts_unexpected_adapter_failure_to_500() -> None:
    repository = Repository()
    adapter = Adapter()
    adapter.unexpected = True
    service = FeishuGatewayService(repository, Runtime(), adapter)
    response = client(service).post(
        "/v1/channels/feishu/feishu-binding/callback", content=b"message"
    )
    assert response.status_code == 500

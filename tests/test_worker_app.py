"""Tests for Worker FastAPI app: factory, auth, routes, lifespan."""

from __future__ import annotations

import json
import uuid

import httpx
import pytest

from trpc_service.agent.app import AgentApp
from trpc_service.transport.auth import InternalToken
from trpc_service.worker.app import create_worker_app
from trpc_service.worker.service import WorkerService

from tests.tenant_helpers import (
    FakeLLMModel,
    FakeModelProvider,
    FakeTenantConfigRepository,
    make_in_memory_state_backend,
    make_default_test_configs,
)


def _make_mock_state_backend():
    return make_in_memory_state_backend()


_VALID_TOKEN_VALUE = "a" * 48


def _token() -> InternalToken:
    return InternalToken(_VALID_TOKEN_VALUE)


def _make_worker_app(
    model: FakeLLMModel | None = None,
    token_value: str = _VALID_TOKEN_VALUE,
):
    """Build a Worker app with injected service and token."""
    if model is None:
        model = FakeLLMModel()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    provider = FakeModelProvider({"default": model})
    agent_app = AgentApp(model_provider=provider, state_backend=_make_mock_state_backend())
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    token = InternalToken(token_value)
    return create_worker_app(worker_service=service, internal_token=token), token


def _task_body(**overrides) -> dict:
    defaults = {
        "protocol_version": 1,
        "request_id": str(uuid.uuid4()),
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "user_default",
        "channel": "web",
        "session_id": "sess-1",
        "message_id": "msg-1",
        "message": "hello",
    }
    defaults.update(overrides)
    return defaults


def _worker_client(app, token_value: str = _VALID_TOKEN_VALUE):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-TRPC-Internal-Token": token_value},
    )


def _worker_client_no_token(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


def _collect_sse(response: httpx.Response) -> list[dict]:
    payloads = []
    for raw in response.text.splitlines():
        if not raw.startswith("data: "):
            continue
        payloads.append(json.loads(raw[len("data: "):]))
    return payloads


# ---------------------------------------------------------------------------
# Factory validation
# ---------------------------------------------------------------------------


def test_factory_rejects_both_service_and_repository() -> None:
    repo = FakeTenantConfigRepository(make_default_test_configs())
    provider = FakeModelProvider({"default": FakeLLMModel()})
    agent_app = AgentApp(model_provider=provider, state_backend=_make_mock_state_backend())
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    with pytest.raises(ValueError, match="not both"):
        create_worker_app(worker_service=service, internal_token=_token(), tenant_repository=repo)


def test_factory_rejects_service_without_token() -> None:
    repo = FakeTenantConfigRepository(make_default_test_configs())
    provider = FakeModelProvider({"default": FakeLLMModel()})
    agent_app = AgentApp(model_provider=provider, state_backend=_make_mock_state_backend())
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    with pytest.raises(ValueError, match="internal_token is required"):
        create_worker_app(worker_service=service)


# ---------------------------------------------------------------------------
# /health (unauthenticated)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_unauthenticated() -> None:
    app, _ = _make_worker_app()
    async with _worker_client_no_token(app) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# /internal/v1/chat — auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_without_token_returns_401() -> None:
    app, _ = _make_worker_app()
    async with _worker_client_no_token(app) as client:
        response = await client.post("/internal/v1/chat", json=_task_body())
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_chat_with_wrong_token_returns_401() -> None:
    app, _ = _make_worker_app()
    async with _worker_client(app, token_value="wrong-token-value-that-is-long-enough") as client:
        response = await client.post("/internal/v1/chat", json=_task_body())
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_chat_does_not_echo_token_in_error() -> None:
    app, _ = _make_worker_app()
    async with _worker_client_no_token(app) as client:
        response = await client.post("/internal/v1/chat", json=_task_body())
    assert _VALID_TOKEN_VALUE not in response.text


# ---------------------------------------------------------------------------
# /internal/v1/chat — validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_with_invalid_json_returns_422() -> None:
    app, _ = _make_worker_app()
    async with _worker_client(app) as client:
        response = await client.post(
            "/internal/v1/chat",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_chat_with_invalid_task_returns_422() -> None:
    app, _ = _make_worker_app()
    async with _worker_client(app) as client:
        response = await client.post("/internal/v1/chat", json={"protocol_version": 2})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# /internal/v1/chat — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_success_returns_worker_chat_result() -> None:
    model = FakeLLMModel()
    app, _ = _make_worker_app(model=model)
    body = _task_body()
    async with _worker_client(app) as client:
        response = await client.post("/internal/v1/chat", json=body)
    assert response.status_code == 200
    result = response.json()
    assert result["protocol_version"] == 1
    assert result["request_id"] == body["request_id"]
    assert result["response"] == "OK"
    assert result["error_code"] is None
    assert model.call_count == 1


@pytest.mark.asyncio
async def test_chat_tenant_mismatch_returns_error_code() -> None:
    app, _ = _make_worker_app()
    body = _task_body(tenant_id="nonexistent")
    async with _worker_client(app) as client:
        response = await client.post("/internal/v1/chat", json=body)
    assert response.status_code == 200
    result = response.json()
    assert result["error_code"] == "tenant_config_mismatch"
    assert result["response"] == ""


# ---------------------------------------------------------------------------
# /internal/v1/chat/stream — auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_without_token_returns_401() -> None:
    app, _ = _make_worker_app()
    async with _worker_client_no_token(app) as client:
        response = await client.post("/internal/v1/chat/stream", json=_task_body())
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_stream_with_wrong_token_returns_401() -> None:
    app, _ = _make_worker_app()
    async with _worker_client(app, token_value="wrong-token-value-that-is-long-enough") as client:
        response = await client.post("/internal/v1/chat/stream", json=_task_body())
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# /internal/v1/chat/stream — validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_with_invalid_json_returns_422() -> None:
    app, _ = _make_worker_app()
    async with _worker_client(app) as client:
        response = await client.post(
            "/internal/v1/chat/stream",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# /internal/v1/chat/stream — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_success_returns_sse() -> None:
    model = FakeLLMModel()
    app, _ = _make_worker_app(model=model)
    body = _task_body()
    async with _worker_client(app) as client:
        async with client.stream("POST", "/internal/v1/chat/stream", json=body) as response:
            raw_body = await response.aread()
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    payloads = _collect_sse(httpx.Response(200, content=raw_body))
    types = [p["type"] for p in payloads]
    assert "delta" in types
    assert types[-1] == "done"
    for p in payloads:
        assert p["request_id"] == body["request_id"]
        assert p["protocol_version"] == 1


@pytest.mark.asyncio
async def test_stream_tenant_mismatch_emits_error_event() -> None:
    app, _ = _make_worker_app()
    body = _task_body(tenant_id="nonexistent")
    async with _worker_client(app) as client:
        async with client.stream("POST", "/internal/v1/chat/stream", json=body) as response:
            raw_body = await response.aread()
    payloads = _collect_sse(httpx.Response(200, content=raw_body))
    assert len(payloads) == 1
    assert payloads[0]["type"] == "error"
    assert payloads[0]["error_code"] == "tenant_config_mismatch"


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lifespan_closes_worker_service() -> None:
    model = FakeLLMModel()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    provider = FakeModelProvider({"default": model})
    agent_app = AgentApp(model_provider=provider, state_backend=_make_mock_state_backend())
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    token = InternalToken(_VALID_TOKEN_VALUE)
    app = create_worker_app(worker_service=service, internal_token=token)

    closed = {"count": 0}
    original_close = service.close

    async def tracking_close():
        closed["count"] += 1
        await original_close()

    service.close = tracking_close  # type: ignore[assignment]
    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/health")
        assert response.status_code == 200
    finally:
        service.close = original_close  # type: ignore[assignment]

    assert closed["count"] == 1


@pytest.mark.asyncio
async def test_lifespan_closes_model_http_clients_after_agent_runtime(monkeypatch) -> None:
    import trpc_service.worker.app as worker_app_module

    events: list[str] = []

    async def tracked_cleanup() -> None:
        events.append("model_http_cleanup")

    monkeypatch.setattr(worker_app_module, "close_model_http_clients", tracked_cleanup, raising=False)

    repo = FakeTenantConfigRepository(make_default_test_configs())
    agent_app = AgentApp(
        model_provider=FakeModelProvider({"default": FakeLLMModel()}),
        state_backend=_make_mock_state_backend(),
    )
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    original_close = service.close

    async def tracking_close() -> None:
        events.append("service_close")
        await original_close()

    service.close = tracking_close  # type: ignore[assignment]
    app = create_worker_app(worker_service=service, internal_token=_token())
    async with app.router.lifespan_context(app):
        pass

    assert events == ["service_close", "model_http_cleanup"]


@pytest.mark.asyncio
async def test_lifespan_model_http_cleanup_failure_keeps_other_shutdown(monkeypatch, caplog) -> None:
    import logging

    import trpc_service.worker.app as worker_app_module

    cleanup_attempted = {"count": 0}

    async def exploding_cleanup() -> None:
        cleanup_attempted["count"] += 1
        raise RuntimeError("close https://model.example/v1 failed bearer=top-secret-value")

    monkeypatch.setattr(worker_app_module, "close_model_http_clients", exploding_cleanup, raising=False)

    class _TrackingTelemetry:
        enabled = False

        def __init__(self) -> None:
            self.close_calls = 0

        async def start(self) -> None:
            return None

        async def close(self) -> None:
            self.close_calls += 1

    closed = {"count": 0}
    repo = FakeTenantConfigRepository(make_default_test_configs())
    agent_app = AgentApp(
        model_provider=FakeModelProvider({"default": FakeLLMModel()}),
        state_backend=_make_mock_state_backend(),
    )
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    original_close = service.close

    async def tracking_close() -> None:
        closed["count"] += 1
        await original_close()

    service.close = tracking_close  # type: ignore[assignment]
    telemetry = _TrackingTelemetry()
    app = create_worker_app(worker_service=service, internal_token=_token(), telemetry=telemetry)

    with caplog.at_level(logging.WARNING):
        async with app.router.lifespan_context(app):
            pass

    assert cleanup_attempted["count"] == 1
    assert closed["count"] == 1
    assert telemetry.close_calls == 1
    assert "top-secret-value" not in caplog.text
    assert "model.example" not in caplog.text


# ── Stage 6A2: decide route wiring ───────────────────────────────────────────


def _decide_app(approval_service=None):
    repo = FakeTenantConfigRepository(make_default_test_configs())
    agent_app = AgentApp(
        model_provider=FakeModelProvider({"default": FakeLLMModel()}),
        state_backend=_make_mock_state_backend(),
    )
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    return create_worker_app(worker_service=service, internal_token=_token(), approval_service=approval_service)


class _StubApprovalService:

    def __init__(self, *, error_code=None, response="OK"):
        self.calls = []
        self.error_code = error_code
        self.response = response

    async def decide(self, task):
        self.calls.append(task)
        from trpc_service.transport.models import WorkerApprovalResult
        return WorkerApprovalResult(
            protocol_version=1,
            request_id=task.request_id,
            response="" if self.error_code else self.response,
            error_code=self.error_code,
        )


def _decide_body(**over):
    import uuid as _uuid
    body = {
        "protocol_version": 1,
        "request_id": str(_uuid.uuid4()),
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "usr_v1_" + "a" * 48,
        "channel": "web_console",
        "session_id": "ses_v1_" + "b" * 48,
        "message_id": "dm-1",
        "approval_id": str(_uuid.uuid4()),
        "decision": "approve",
    }
    body.update(over)
    return body


@pytest.mark.asyncio
async def test_decide_requires_internal_token():
    stub = _StubApprovalService()
    app = _decide_app(approval_service=stub)
    async with _worker_client_no_token(app) as client:
        resp = await client.post("/internal/v1/approvals/decide", json=_decide_body())
    assert resp.status_code == 401
    assert stub.calls == []


@pytest.mark.asyncio
async def test_decide_rejects_malformed_body():
    stub = _StubApprovalService()
    app = _decide_app(approval_service=stub)
    async with _worker_client(app) as client:
        resp = await client.post("/internal/v1/approvals/decide", json={"nope": 1})
    assert resp.status_code == 422
    assert stub.calls == []


@pytest.mark.asyncio
async def test_decide_returns_result_json():
    stub = _StubApprovalService()
    app = _decide_app(approval_service=stub)
    body = _decide_body()
    async with _worker_client(app) as client:
        resp = await client.post("/internal/v1/approvals/decide", json=body)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["response"] == "OK"
    assert payload["request_id"] == body["request_id"]


@pytest.mark.asyncio
async def test_decide_error_code_maps_to_fixed_shape():
    from trpc_service.transport.models import WorkerErrorCode
    stub = _StubApprovalService(error_code=WorkerErrorCode.APPROVAL_CONFLICT)
    app = _decide_app(approval_service=stub)
    async with _worker_client(app) as client:
        resp = await client.post("/internal/v1/approvals/decide", json=_decide_body())
    payload = resp.json()
    assert payload["error_code"] == "approval_conflict"
    assert payload["response"] == ""


@pytest.mark.asyncio
async def test_decide_unavailable_503_without_service():
    app = _decide_app()
    async with _worker_client(app) as client:
        resp = await client.post("/internal/v1/approvals/decide", json=_decide_body())
    assert resp.status_code == 503

"""Tests for Gateway FastAPI app: admission, routes, error mapping, lifespan."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from trpc_service.gateway.app import create_gateway_app
from trpc_service.gateway.client import WorkerClientError
from trpc_service.gateway.routes import register_gateway_routes
from trpc_service.transport.models import (
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
    WorkerToolCallData,
    WorkerToolResultData,
)

from tests.tenant_helpers import FakeTenantConfigRepository, make_default_test_configs

SAFE_ERROR_TEXT = "An internal error occurred while talking to the model."
CONFIG_ERROR_TEXT = "Service is not configured. Set TRPC_MODEL_* environment variables and restart."
TENANT_AGENT_CONFIG_ERROR_TEXT = "Tenant agent configuration is not available."
TENANT_UNAVAILABLE = "Tenant is not available."
TENANT_SERVICE_UNAVAILABLE_TEXT = "Tenant service is temporarily unavailable."
SESSION_BUSY_ERROR_TEXT = "The session is busy. Please try again shortly."


class FakeWorkerClient:
    """Test double for HttpWorkerClient. Returns scripted results."""

    def __init__(self) -> None:
        self.chat_result: WorkerChatResult | None = None
        self.chat_error: WorkerClientError | None = None
        self.chat_side_effect: Exception | None = None
        self.stream_events: list[WorkerEvent] = []
        self.stream_error: WorkerClientError | None = None
        self.stream_side_effect: Exception | None = None
        self.chat_tasks: list[WorkerTask] = []
        self.stream_tasks: list[WorkerTask] = []
        self.close_count = 0

    async def start(self) -> None:
        pass

    async def chat(self, task: WorkerTask) -> WorkerChatResult:
        self.chat_tasks.append(task)
        if self.chat_side_effect is not None:
            raise self.chat_side_effect
        if self.chat_error is not None:
            raise self.chat_error
        if self.chat_result is not None:
            return self.chat_result
        return WorkerChatResult(
            protocol_version=1,
            request_id=task.request_id,
            response="fake-reply",
            error_code=None,
        )

    async def stream(self, task: WorkerTask) -> AsyncIterator[WorkerEvent]:
        self.stream_tasks.append(task)
        if self.stream_side_effect is not None:
            raise self.stream_side_effect
        if self.stream_error is not None:
            raise self.stream_error
        for event in self.stream_events:
            yield event

    async def close(self) -> None:
        self.close_count += 1


def _client(app, *, headers: dict | None = None):
    default_headers = {"X-Tenant-ID": "tenant_default"}
    if headers is not None:
        default_headers.update(headers)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test", headers=default_headers)


def _client_no_tenant(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


def _collect_sse(response: httpx.Response) -> list[dict]:
    payloads = []
    for raw in response.text.splitlines():
        if not raw.startswith("data:"):
            continue
        payloads.append(json.loads(raw[len("data: "):]))
    return payloads


def _make_app(worker_client: FakeWorkerClient | None = None):
    if worker_client is None:
        worker_client = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker_client, tenant_repository=repo)
    return app, worker_client


def _success_result(task_rid: uuid.UUID, text: str = "reply-text") -> WorkerChatResult:
    return WorkerChatResult(protocol_version=1, request_id=task_rid, response=text, error_code=None)


def _error_result(task_rid: uuid.UUID, code: WorkerErrorCode) -> WorkerChatResult:
    return WorkerChatResult(protocol_version=1, request_id=task_rid, response="", error_code=code)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_returns_200_without_worker() -> None:
    app, worker = _make_app()
    async with _client(app) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "trpc-agent-service"


# ---------------------------------------------------------------------------
# Tenant admission — 422/403
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_tenant_header_returns_422() -> None:
    app, _ = _make_app()
    async with _client_no_tenant(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_empty_tenant_header_returns_422() -> None:
    app, _ = _make_app()
    async with _client(app, headers={"X-Tenant-ID": ""}) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_invalid_tenant_id_format_returns_422() -> None:
    app, _ = _make_app()
    async with _client(app, headers={"X-Tenant-ID": "INVALID!"}) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_unknown_tenant_returns_403() -> None:
    app, _ = _make_app()
    async with _client(app, headers={"X-Tenant-ID": "nonexistent"}) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 403
    assert response.json()["detail"] == TENANT_UNAVAILABLE


@pytest.mark.asyncio
async def test_disabled_tenant_returns_403() -> None:
    app, _ = _make_app()
    async with _client(app, headers={"X-Tenant-ID": "tenant_disabled"}) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_admission_does_not_call_worker_on_422() -> None:
    app, worker = _make_app()
    async with _client_no_tenant(app) as client:
        await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert len(worker.chat_tasks) == 0
    assert len(worker.stream_tasks) == 0


@pytest.mark.asyncio
async def test_admission_does_not_call_worker_on_403() -> None:
    app, worker = _make_app()
    async with _client(app, headers={"X-Tenant-ID": "nonexistent"}) as client:
        await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert len(worker.chat_tasks) == 0


@pytest.mark.asyncio
async def test_admission_returns_503_when_repository_unavailable() -> None:
    from trpc_service.config.tenant_repository import TenantRepositoryUnavailableError

    class UnavailableRepository:

        async def get(self, tenant_id: str):
            raise TenantRepositoryUnavailableError("database is not reachable")

        async def check_ready(self):
            raise TenantRepositoryUnavailableError("database is not reachable")

        async def close(self):
            pass

    worker_client = FakeWorkerClient()
    app = create_gateway_app(worker_client=worker_client, tenant_repository=UnavailableRepository())
    async with _client(app, headers={"X-Tenant-ID": "tenant_default"}) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 503
    body = response.json()
    assert body["detail"] == "Tenant service is temporarily unavailable."
    assert len(worker_client.chat_tasks) == 0


# ---------------------------------------------------------------------------
# Sync chat — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_chat_returns_worker_reply() -> None:
    app, worker = _make_app()
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "sess-1", "message": "hi"})
    assert response.status_code == 200
    body = response.json()
    assert body["session_id"] == "sess-1"
    assert body["response"] == "fake-reply"
    assert len(worker.chat_tasks) == 1
    task = worker.chat_tasks[0]
    assert task.tenant_id == "tenant_default"
    assert task.app_id == "app_demo"
    assert task.config_version == 1
    assert task.user_id == "user_default"
    assert task.channel == "web"
    assert task.session_id == "sess-1"
    assert task.message_id == "msg-1"
    assert task.message == "hi"
    assert task.protocol_version == 1
    assert isinstance(task.request_id, uuid.UUID)


@pytest.mark.asyncio
async def test_sync_chat_rejects_empty_message() -> None:
    app, _ = _make_app()
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": ""})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_sync_chat_rejects_empty_session_id() -> None:
    app, _ = _make_app()
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "", "message": "hi"})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_sync_chat_rejects_overlong_message() -> None:
    app, _ = _make_app()
    async with _client(app) as client:
        response = await client.post("/api/chat",
                                     json={
                                         "message_id": "msg-1",
                                         "session_id": "s",
                                         "message": "x" * 8001,
                                     })
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Sync chat — error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_chat_worker_unavailable_returns_safe_error() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_worker_timeout_returns_safe_error() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.WORKER_TIMEOUT)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_invalid_worker_response_returns_safe_error() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_tenant_repository_unavailable_maps_to_service_unavailable_text() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == TENANT_SERVICE_UNAVAILABLE_TEXT


@pytest.mark.asyncio
async def test_sync_chat_model_configuration_returns_config_error() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.MODEL_CONFIGURATION)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == CONFIG_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_tenant_agent_config_returns_agent_error() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.TENANT_AGENT_CONFIGURATION)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == TENANT_AGENT_CONFIG_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_tenant_config_mismatch_returns_agent_error() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.TENANT_CONFIG_MISMATCH)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == TENANT_AGENT_CONFIG_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_model_runtime_returns_safe_error() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.MODEL_RUNTIME)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_generic_exception_returns_safe_error() -> None:
    """Gateway sync chat catches generic exceptions and returns safe error text."""
    app, worker = _make_app()
    worker.chat_side_effect = RuntimeError("Unexpected internal error")
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_session_busy_returns_busy_text() -> None:
    app, worker = _make_app()
    worker.chat_error = WorkerClientError(WorkerErrorCode.SESSION_BUSY)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SESSION_BUSY_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_result_with_session_busy_error_code_returns_busy_text() -> None:
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.chat_result = _error_result(rid, WorkerErrorCode.SESSION_BUSY)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SESSION_BUSY_ERROR_TEXT


# ---------------------------------------------------------------------------
# Sync chat — error_code in WorkerChatResult (HTTP 200 + error_code)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_chat_result_with_model_runtime_error_code_returns_safe_error() -> None:
    """Worker returns HTTP 200 with error_code=MODEL_RUNTIME; Gateway maps to SAFE_ERROR_TEXT."""
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.chat_result = _error_result(rid, WorkerErrorCode.MODEL_RUNTIME)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_result_with_model_configuration_error_code_returns_config_error() -> None:
    """Worker returns HTTP 200 with error_code=MODEL_CONFIGURATION; Gateway maps to CONFIG_ERROR_TEXT."""
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.chat_result = _error_result(rid, WorkerErrorCode.MODEL_CONFIGURATION)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == CONFIG_ERROR_TEXT


@pytest.mark.asyncio
async def test_sync_chat_result_with_tenant_config_mismatch_error_code_returns_agent_error() -> None:
    """Worker returns HTTP 200 with error_code=TENANT_CONFIG_MISMATCH; Gateway maps to agent error text."""
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.chat_result = _error_result(rid, WorkerErrorCode.TENANT_CONFIG_MISMATCH)
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 200
    assert response.json()["response"] == TENANT_AGENT_CONFIG_ERROR_TEXT


# ---------------------------------------------------------------------------
# SSE stream — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_emits_delta_then_done() -> None:
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.stream_events = [
        WorkerEvent(protocol_version=1, request_id=rid, type="delta", data="Hello", error_code=None),
        WorkerEvent(protocol_version=1, request_id=rid, type="done", data=None, error_code=None),
    ]
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "sess-1",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers["content-type"]
    payloads = _collect_sse(httpx.Response(200, content=body))
    types = [p["type"] for p in payloads]
    assert "delta" in types
    assert types[-1] == "done"
    delta_text = "".join(p["data"] for p in payloads if p["type"] == "delta")
    assert delta_text == "Hello"
    for p in payloads:
        assert p["session_id"] == "sess-1"
        assert "protocol_version" not in p
        assert "request_id" not in p
        assert "error_code" not in p


@pytest.mark.asyncio
async def test_stream_emits_tool_events() -> None:
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="tool",
            data=WorkerToolCallData(kind="call", name="get_current_time", args={}),
            error_code=None,
        ),
        WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="tool",
            data=WorkerToolResultData(kind="result", name="get_current_time", response={"now": "12:00"}),
            error_code=None,
        ),
        WorkerEvent(protocol_version=1, request_id=rid, type="delta", data="done", error_code=None),
        WorkerEvent(protocol_version=1, request_id=rid, type="done", data=None, error_code=None),
    ]
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    types = [p["type"] for p in payloads]
    assert types == ["tool", "tool", "delta", "done"]
    assert payloads[0]["data"]["kind"] == "call"
    assert payloads[0]["data"]["name"] == "get_current_time"
    assert payloads[1]["data"]["kind"] == "result"
    assert payloads[1]["data"]["name"] == "get_current_time"


@pytest.mark.asyncio
async def test_stream_rejects_empty_message() -> None:
    app, _ = _make_app()
    async with _client(app) as client:
        response = await client.post("/api/chat/stream", json={"message_id": "msg-1", "session_id": "s", "message": ""})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# SSE stream — error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_worker_unavailable_returns_error_event() -> None:
    app, worker = _make_app()
    worker.stream_error = WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    assert len(payloads) == 1
    assert payloads[0]["type"] == "error"
    assert payloads[0]["data"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_stream_worker_error_code_maps_to_safe_text() -> None:
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="error",
            data=None,
            error_code=WorkerErrorCode.MODEL_RUNTIME,
        ),
    ]
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    assert len(payloads) == 1
    assert payloads[0]["type"] == "error"
    assert payloads[0]["data"] == SAFE_ERROR_TEXT


@pytest.mark.asyncio
async def test_stream_model_configuration_error_maps_to_config_text() -> None:
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="error",
            data=None,
            error_code=WorkerErrorCode.MODEL_CONFIGURATION,
        ),
    ]
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    assert payloads[0]["data"] == CONFIG_ERROR_TEXT


@pytest.mark.asyncio
async def test_stream_session_busy_error_maps_to_busy_text() -> None:
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="error",
            data=None,
            error_code=WorkerErrorCode.SESSION_BUSY,
        ),
    ]
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    assert len(payloads) == 1
    assert payloads[0]["type"] == "error"
    assert payloads[0]["data"] == SESSION_BUSY_ERROR_TEXT


@pytest.mark.asyncio
async def test_stream_session_busy_client_error_maps_to_busy_text() -> None:
    app, worker = _make_app()
    worker.stream_error = WorkerClientError(WorkerErrorCode.SESSION_BUSY)
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    assert len(payloads) == 1
    assert payloads[0]["type"] == "error"
    assert payloads[0]["data"] == SESSION_BUSY_ERROR_TEXT


@pytest.mark.asyncio
async def test_stream_tenant_agent_config_error_maps_to_agent_text() -> None:
    app, worker = _make_app()
    rid = uuid.uuid4()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="error",
            data=None,
            error_code=WorkerErrorCode.TENANT_AGENT_CONFIGURATION,
        ),
    ]
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    assert payloads[0]["data"] == TENANT_AGENT_CONFIG_ERROR_TEXT


@pytest.mark.asyncio
async def test_stream_generic_exception_returns_safe_error() -> None:
    """Gateway stream catches generic exceptions and returns safe error text."""
    app, worker = _make_app()
    worker.stream_side_effect = RuntimeError("Unexpected internal error")
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi",
                                 }) as resp:
            body = await resp.aread()
    payloads = _collect_sse(httpx.Response(200, content=body))
    assert len(payloads) == 1
    assert payloads[0]["type"] == "error"
    assert payloads[0]["data"] == SAFE_ERROR_TEXT


# ---------------------------------------------------------------------------
# Gateway does not import Agent/SDK
# ---------------------------------------------------------------------------


def test_gateway_does_not_import_agent_app() -> None:
    """Gateway module must not import AgentApp directly."""
    import trpc_service.gateway.app as gw
    import trpc_service.gateway.routes as gwr
    import trpc_service.gateway.client as gwc
    assert not hasattr(gw, "AgentApp")
    assert not hasattr(gwr, "AgentApp")
    assert not hasattr(gwc, "AgentApp")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lifespan_closes_worker_client() -> None:
    app, worker = _make_app()
    async with app.router.lifespan_context(app):
        pass
    assert worker.close_count == 1


@pytest.mark.asyncio
async def test_lifespan_closes_even_when_body_raises() -> None:
    app, worker = _make_app()
    try:
        async with app.router.lifespan_context(app):
            raise RuntimeError("simulated failure")
    except RuntimeError:
        pass
    assert worker.close_count == 1


# ---------------------------------------------------------------------------
# Gateway public UI routes and lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_returns_html() -> None:
    """GET / returns 200 with text/html content-type."""
    app, _ = _make_app()
    async with _client(app) as client:
        response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


@pytest.mark.asyncio
async def test_register_gateway_routes_idempotent() -> None:
    """Calling register_gateway_routes twice is idempotent (no duplicate routes)."""
    app, worker = _make_app()
    route_paths = ("/", "/api/chat", "/api/chat/stream")
    before = {path: sum(getattr(route, "path", None) == path for route in app.routes) for path in route_paths}
    assert before == {path: 1 for path in route_paths}

    register_gateway_routes(app, worker_client=worker)

    after = {path: sum(getattr(route, "path", None) == path for route in app.routes) for path in route_paths}
    assert after == before
    assert getattr(app.state, "_gateway_routes_registered", False) is True


# ── Stage 6A1: legacy /api/chat governance lockdown ─────────────────────────


def _make_gov_app(governance, worker_client=None):
    from tests.tenant_helpers import make_app_config, make_tenant_config

    if worker_client is None:
        worker_client = FakeWorkerClient()
    repo = FakeTenantConfigRepository({
        "tenant_default":
        make_tenant_config("tenant_default", app=make_app_config(), governance=governance),
    })
    app = create_gateway_app(worker_client=worker_client, tenant_repository=repo)
    return app, worker_client


@pytest.mark.asyncio
async def test_api_chat_denied_when_web_not_in_allowed_channels() -> None:
    from tests.tenant_helpers import make_governance

    app, worker = _make_gov_app(make_governance(allowed_channels=("web_console", )))
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 403
    assert response.json()["detail"] == "Access is not allowed."
    assert worker.chat_tasks == []


@pytest.mark.asyncio
async def test_api_chat_denied_when_user_allowlist_nonempty() -> None:
    """The anonymous legacy entry cannot prove a platform user identity."""
    from tests.tenant_helpers import make_governance

    app, worker = _make_gov_app(
        make_governance(
            allowed_channels=("web", "web_console", "wecom", "feishu"),
            allowed_user_ids=("usr_v1_" + "b" * 48, ),
        ))
    async with _client(app) as client:
        response = await client.post("/api/chat", json={"message_id": "msg-1", "session_id": "s", "message": "hi"})
    assert response.status_code == 403
    assert response.json()["detail"] == "Access is not allowed."
    assert worker.chat_tasks == []


@pytest.mark.asyncio
async def test_api_chat_stream_denied_before_any_worker_call() -> None:
    """The admission dependency denies while the request is still resolvable,
    so SSE never starts and the Worker is never contacted."""
    from tests.tenant_helpers import make_governance

    app, worker = _make_gov_app(make_governance(allowed_channels=("wecom", )))
    async with _client(app) as client:
        async with client.stream("POST",
                                 "/api/chat/stream",
                                 json={
                                     "message_id": "msg-1",
                                     "session_id": "s",
                                     "message": "hi"
                                 }) as resp:
            assert resp.status_code == 403
            body = (await resp.aread()).decode("utf-8")

    assert "Access is not allowed." in body
    assert worker.stream_tasks == []
    assert "user_default" not in body

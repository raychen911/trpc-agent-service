"""Tests for Stage 5A Gateway console HTTP routes."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from trpc_service.gateway.app import create_gateway_app
from trpc_service.gateway.client import WorkerClientError
from trpc_service.transport.models import (
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
)

from tests.tenant_helpers import FakeTenantConfigRepository, make_default_test_configs


class FakeWorkerClient:
    """Test double for console route tests."""

    def __init__(self) -> None:
        self.chat_result: WorkerChatResult | None = None
        self.chat_error: WorkerClientError | None = None
        self.stream_events: list[WorkerEvent] = []
        self.stream_error: WorkerClientError | None = None
        self.chat_tasks: list[WorkerTask] = []
        self.stream_tasks: list[WorkerTask] = []

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
            response="fake-reply",
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


def _client(app, *, headers: dict | None = None):
    default_headers = {"X-Tenant-ID": "tenant_default"}
    if headers:
        default_headers.update(headers)
    return httpx.ASGITransport(app=app), default_headers


def _console_payload(**overrides) -> dict:
    defaults = {
        "tenant_id": "tenant_default",
        "user_id": "user_abc",
        "conversation_id": "conv_123",
        "message_id": "msg_456",
        "message": "hello",
    }
    defaults.update(overrides)
    return defaults


@pytest.mark.asyncio
async def test_console_sync_returns_response():
    worker = FakeWorkerClient()
    worker.chat_result = WorkerChatResult(
        protocol_version=1,
        request_id=uuid.uuid4(),
        response="model reply",
        error_code=None,
    )
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages",
            json=_console_payload(),
            headers=headers,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["response"] == "model reply"


@pytest.mark.asyncio
async def test_console_sync_rejects_malformed_payload():
    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages",
            json={
                "tenant_id": "tenant_default",
                "message": "hi"
            },
            headers=headers,
        )

    assert resp.status_code == 422
    assert len(worker.chat_tasks) == 0


@pytest.mark.asyncio
async def test_console_sync_rejects_extra_fields():
    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        payload = _console_payload()
        payload["extra_field"] = "not allowed"
        resp = await client.post(
            "/api/console/messages",
            json=payload,
            headers=headers,
        )

    assert resp.status_code == 422
    assert len(worker.chat_tasks) == 0


@pytest.mark.asyncio
async def test_console_sync_422_does_not_leak_raw_input():
    """P0 Security: 422 responses must not echo raw input values."""
    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages",
            json={
                "tenant_id": "tenant_default",
                "user_id": ["SENTINEL_RAW_VALUE_12345"],
                "conversation_id": "conv_123",
                "message_id": "msg_456",
                "message": "hello",
            },
            headers=headers,
        )

    assert resp.status_code == 422
    response_text = resp.text
    assert "SENTINEL_RAW_VALUE_12345" not in response_text
    assert "input" not in response_text.lower()
    assert "payload" not in response_text.lower()
    assert len(worker.chat_tasks) == 0


@pytest.mark.asyncio
async def test_console_stream_422_does_not_leak_raw_input():
    """P0 Security: Stream endpoint 422 responses must not echo raw input values."""
    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages/stream",
            json={
                "tenant_id": "tenant_default",
                "user_id": {
                    "nested": "SENTINEL_STREAM_67890"
                },
                "conversation_id": "conv_123",
                "message_id": "msg_456",
                "message": "hello",
            },
            headers=headers,
        )

    assert resp.status_code == 422
    response_text = resp.text
    assert "SENTINEL_STREAM_67890" not in response_text
    assert "nested" not in response_text
    assert "input" not in response_text.lower()
    assert len(worker.stream_tasks) == 0


@pytest.mark.asyncio
async def test_console_sync_adapter_decode_error_does_not_leak():
    """P1: adapter.decode() ValueError must return fixed text, not str(exc)."""
    from typing import Any

    from trpc_service.channels.adapter import AdapterRegistry
    from trpc_service.channels.models import InboundMessage, PublicChannelEvent

    class MaliciousAdapter:
        channel = "web_console"

        def decode(self, payload: object) -> InboundMessage:
            raise ValueError("SENTINEL_DECODE_ERROR: leaked payload details")

        def encode_sync(self, response: str) -> dict[str, Any]:
            return {"response": response}

        def encode_event(self, event: PublicChannelEvent) -> dict[str, Any]:
            return {"type": event.type, "data": event.data}

    registry = AdapterRegistry()
    registry.register(MaliciousAdapter())

    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(
        worker_client=worker,
        tenant_repository=repo,
        adapter_registry=registry,
    )

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages",
            json=_console_payload(),
            headers=headers,
        )

    assert resp.status_code == 422
    response_text = resp.text
    assert "SENTINEL_DECODE_ERROR" not in response_text
    assert "leaked" not in response_text
    assert "Invalid request format" in response_text
    assert len(worker.chat_tasks) == 0


@pytest.mark.asyncio
async def test_console_stream_adapter_decode_error_does_not_leak():
    """P1: adapter.decode() ValueError in stream must return fixed text, not str(exc)."""
    from typing import Any

    from trpc_service.channels.adapter import AdapterRegistry
    from trpc_service.channels.models import InboundMessage, PublicChannelEvent

    class MaliciousAdapter:
        channel = "web_console"

        def decode(self, payload: object) -> InboundMessage:
            raise ValueError("SENTINEL_STREAM_DECODE: secret internal error details")

        def encode_sync(self, response: str) -> dict[str, Any]:
            return {"response": response}

        def encode_event(self, event: PublicChannelEvent) -> dict[str, Any]:
            return {"type": event.type, "data": event.data}

    registry = AdapterRegistry()
    registry.register(MaliciousAdapter())

    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(
        worker_client=worker,
        tenant_repository=repo,
        adapter_registry=registry,
    )

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages/stream",
            json=_console_payload(),
            headers=headers,
        )

    assert resp.status_code == 422
    response_text = resp.text
    assert "SENTINEL_STREAM_DECODE" not in response_text
    assert "secret" not in response_text
    assert "Invalid request format" in response_text
    assert len(worker.stream_tasks) == 0


@pytest.mark.asyncio
async def test_console_validation_handler_preserves_legacy_chat_422():
    """Console-only validation handling must not turn legacy validation into 500."""
    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/chat",
            headers={"X-Tenant-ID": "tenant_default"},
            json={
                "session_id": "session",
                "message_id": "message",
                "message": ["invalid-type"],
            },
        )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_console_sync_maps_worker_error():
    worker = FakeWorkerClient()
    worker.chat_error = WorkerClientError(WorkerErrorCode.SESSION_BUSY)
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages",
            json=_console_payload(),
            headers=headers,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert "busy" in data["response"].lower()


@pytest.mark.asyncio
async def test_console_stream_yields_sse_events():
    worker = FakeWorkerClient()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="delta",
            data="partial text",
        ),
        WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="done",
            data=None,
        ),
    ]
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
                "POST",
                "/api/console/messages/stream",
                json=_console_payload(),
                headers=headers,
        ) as resp:
            assert resp.status_code == 200
            lines = []
            async for line in resp.aiter_lines():
                if line:
                    lines.append(line)

    sse_events = [line for line in lines if line.startswith("data: ")]
    assert len(sse_events) >= 2

    first_event = json.loads(sse_events[0][6:])
    assert first_event["type"] == "delta"
    assert first_event["data"] == "partial text"


@pytest.mark.asyncio
async def test_console_stream_rejects_malformed_payload():
    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages/stream",
            json={"tenant_id": "tenant_default"},
            headers=headers,
        )

    assert resp.status_code == 422
    assert len(worker.stream_tasks) == 0


@pytest.mark.asyncio
async def test_console_stream_maps_worker_error():
    worker = FakeWorkerClient()
    worker.stream_error = WorkerClientError(WorkerErrorCode.MODEL_CONFIGURATION)
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
                "POST",
                "/api/console/messages/stream",
                json=_console_payload(),
                headers=headers,
        ) as resp:
            assert resp.status_code == 200
            lines = []
            async for line in resp.aiter_lines():
                if line:
                    lines.append(line)

    error_events = [line for line in lines if line.startswith("data: ")]
    assert len(error_events) >= 1
    event_data = json.loads(error_events[0][6:])
    assert event_data["type"] == "error"
    assert "TRPC_MODEL" in event_data["data"] or "configured" in event_data["data"].lower()


@pytest.mark.asyncio
async def test_console_routes_use_adapter():
    """Verify console endpoints invoke the WebConsoleChannelAdapter."""
    worker = FakeWorkerClient()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages",
            json=_console_payload(),
            headers=headers,
        )

    assert resp.status_code == 200
    assert len(worker.chat_tasks) == 1
    task = worker.chat_tasks[0]
    assert task.channel == "web_console"
    assert task.user_id.startswith("usr_v1_")
    assert task.session_id.startswith("ses_v1_")


@pytest.mark.asyncio
async def test_console_routes_use_adapter_from_registry():
    """Routes must call adapter.decode / encode_sync / encode_event from the
    injected AdapterRegistry, not a hardcoded WebConsoleChannelAdapter."""
    from typing import Any

    from trpc_service.channels.adapter import AdapterRegistry
    from trpc_service.channels.models import InboundMessage, PublicChannelEvent

    class RecordingAdapter:
        channel = "web_console"

        def __init__(self) -> None:
            self.decode_calls: list[object] = []
            self.encode_sync_calls: list[str] = []
            self.encode_event_calls: list[PublicChannelEvent] = []

        def decode(self, payload: object) -> InboundMessage:
            self.decode_calls.append(payload)
            if not isinstance(payload, dict):
                raise ValueError("Invalid payload.")
            return InboundMessage(
                tenant_id=payload["tenant_id"],
                channel="web_console",
                external_user_id=payload["user_id"],
                external_conversation_id=payload["conversation_id"],
                external_message_id=payload["message_id"],
                text=payload["message"],
            )

        def encode_sync(self, response: str) -> dict[str, Any]:
            self.encode_sync_calls.append(response)
            return {"response": response, "source": "recording"}

        def encode_event(self, event: PublicChannelEvent) -> dict[str, Any]:
            self.encode_event_calls.append(event)
            return {"type": event.type, "data": event.data, "source": "recording"}

    recorder = RecordingAdapter()
    registry = AdapterRegistry()
    registry.register(recorder)

    worker = FakeWorkerClient()
    worker.chat_result = WorkerChatResult(
        protocol_version=1,
        request_id=uuid.uuid4(),
        response="registry-reply",
        error_code=None,
    )
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(
        worker_client=worker,
        tenant_repository=repo,
        adapter_registry=registry,
    )

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/console/messages",
            json=_console_payload(),
            headers=headers,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["source"] == "recording"
    assert data["response"] == "registry-reply"

    assert len(recorder.decode_calls) == 1
    assert len(recorder.encode_sync_calls) == 1
    assert recorder.encode_sync_calls[0] == "registry-reply"


@pytest.mark.asyncio
async def test_console_stream_routes_use_adapter_from_registry():
    """Stream route must call adapter.encode_event from the injected registry."""
    from typing import Any

    from trpc_service.channels.adapter import AdapterRegistry
    from trpc_service.channels.models import InboundMessage, PublicChannelEvent

    class RecordingAdapter:
        channel = "web_console"

        def __init__(self) -> None:
            self.encode_event_calls: list[PublicChannelEvent] = []

        def decode(self, payload: object) -> InboundMessage:
            if not isinstance(payload, dict):
                raise ValueError("Invalid payload.")
            return InboundMessage(
                tenant_id=payload["tenant_id"],
                channel="web_console",
                external_user_id=payload["user_id"],
                external_conversation_id=payload["conversation_id"],
                external_message_id=payload["message_id"],
                text=payload["message"],
            )

        def encode_sync(self, response: str) -> dict[str, Any]:
            return {"response": response}

        def encode_event(self, event: PublicChannelEvent) -> dict[str, Any]:
            self.encode_event_calls.append(event)
            return {"type": event.type, "data": event.data, "source": "recording"}

    recorder = RecordingAdapter()
    registry = AdapterRegistry()
    registry.register(recorder)

    worker = FakeWorkerClient()
    worker.stream_events = [
        WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="delta",
            data="chunk-1",
        ),
        WorkerEvent(
            protocol_version=1,
            request_id=uuid.uuid4(),
            type="done",
            data=None,
        ),
    ]
    repo = FakeTenantConfigRepository(make_default_test_configs())
    app = create_gateway_app(
        worker_client=worker,
        tenant_repository=repo,
        adapter_registry=registry,
    )

    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
                "POST",
                "/api/console/messages/stream",
                json=_console_payload(),
                headers=headers,
        ) as resp:
            assert resp.status_code == 200
            lines = []
            async for line in resp.aiter_lines():
                if line:
                    lines.append(line)

    sse_events = [line for line in lines if line.startswith("data: ")]
    assert len(sse_events) >= 2

    first_event = json.loads(sse_events[0][6:])
    assert first_event["source"] == "recording"
    assert first_event["type"] == "delta"
    assert first_event["data"] == "chunk-1"

    assert len(recorder.encode_event_calls) >= 2


def test_index_html_uses_console_adapter():
    """Verify index.html calls /api/console/messages/stream, not /api/chat."""
    from pathlib import Path

    html_path = Path(__file__).parent.parent / "trpc_service" / "web" / "static" / "index.html"
    content = html_path.read_text()

    assert "/api/console/messages/stream" in content
    assert "/api/chat/stream" not in content
    assert "/api/chat" not in content.replace("/api/console/messages/stream", "")

    assert "tenant_id" in content
    assert "user_id" in content
    assert "conversation_id" in content
    assert "message_id" in content

    assert "Web Console Adapter" in content or "web_console" in content


# ── Stage 6A1: governance admission over console HTTP/SSE ───────────────────

from tests.tenant_helpers import make_app_config, make_governance, make_tenant_config  # noqa: E402


def _gov_repo(**gov_kwargs) -> FakeTenantConfigRepository:
    return FakeTenantConfigRepository({
        "tenant_default":
        make_tenant_config(
            "tenant_default",
            app=make_app_config(),
            governance=make_governance(**gov_kwargs),
        ),
    })


@pytest.mark.asyncio
async def test_console_sync_denied_channel_returns_403_fixed_text():
    worker = FakeWorkerClient()
    app = create_gateway_app(worker_client=worker, tenant_repository=_gov_repo(allowed_channels=("wecom", )))
    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/console/messages", json=_console_payload(), headers=headers)

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Access is not allowed."
    assert worker.chat_tasks == []
    assert "user_abc" not in resp.text


@pytest.mark.asyncio
async def test_console_sse_denied_channel_emits_fixed_error_and_stops():
    worker = FakeWorkerClient()
    app = create_gateway_app(worker_client=worker, tenant_repository=_gov_repo(allowed_channels=("feishu", )))
    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream("POST", "/api/console/messages/stream", json=_console_payload(),
                                 headers=headers) as resp:
            assert resp.status_code == 200
            body = (await resp.aread()).decode("utf-8")

    events = [json.loads(line[len("data: "):]) for line in body.splitlines() if line.startswith("data:")]
    assert [e["type"] for e in events] == ["error"]
    assert events[0]["data"] == "Access is not allowed."
    assert worker.stream_tasks == []
    assert "user_abc" not in body


@pytest.mark.asyncio
async def test_console_sync_denied_user_returns_403():
    worker = FakeWorkerClient()
    repo = _gov_repo(allowed_channels=("web_console", ), allowed_user_ids=("usr_v1_" + "a" * 48, ))
    app = create_gateway_app(worker_client=worker, tenant_repository=repo)
    transport, headers = _client(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/console/messages", json=_console_payload(), headers=headers)

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Access is not allowed."
    assert worker.chat_tasks == []

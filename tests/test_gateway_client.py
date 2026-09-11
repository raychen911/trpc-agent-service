"""Tests for HttpWorkerClient: HTTP/SSE protocol, error mapping, lifecycle."""

from __future__ import annotations

import json
import uuid

import httpx
import pytest

from trpc_service.gateway.client import HttpWorkerClient, WorkerClientError
from trpc_service.transport.auth import InternalToken
from trpc_service.transport.models import (
    WorkerChatResult,
    WorkerErrorCode,
    WorkerTask,
)

_TOKEN_VALUE = "b" * 48


def _token() -> InternalToken:
    return InternalToken(_TOKEN_VALUE)


def _task(**overrides) -> WorkerTask:
    defaults = {
        "protocol_version": 1,
        "request_id": uuid.uuid4(),
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
    return WorkerTask(**defaults)


def _chat_result_json(task: WorkerTask, response: str = "OK", error_code: str | None = None) -> dict:
    payload = {
        "protocol_version": 1,
        "request_id": str(task.request_id),
        "response": response,
        "error_code": error_code,
    }
    return payload


def _event_json(task: WorkerTask, type: str, data=None, error_code: str | None = None) -> dict:
    payload = {
        "protocol_version": 1,
        "request_id": str(task.request_id),
        "type": type,
        "data": data,
        "error_code": error_code,
    }
    return payload


class _MockTransport(httpx.AsyncBaseTransport):
    """Programmable mock transport for httpx."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._response_factory = None

    def set_response(self, response: httpx.Response) -> None:
        self._response_factory = lambda req: response

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self._response_factory is not None:
            return self._response_factory(request)
        return httpx.Response(200, json={"status": "ok"})


class _StreamingTransport(httpx.AsyncBaseTransport):
    """Transport that returns SSE byte stream for stream tests."""

    def __init__(self, lines: list[str], status_code: int = 200) -> None:
        self._lines = lines
        self._status_code = status_code
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        content = "\n".join(self._lines).encode()
        return httpx.Response(
            self._status_code,
            content=content,
            headers={"Content-Type": "text/event-stream"},
        )


class _ConnectErrorTransport(httpx.AsyncBaseTransport):
    """Always raises ConnectError."""

    async def handle_async_request(self, request):
        raise httpx.ConnectError("connection refused")


class _TimeoutTransport(httpx.AsyncBaseTransport):
    """Always raises ReadTimeout."""

    async def handle_async_request(self, request):
        raise httpx.ReadTimeout("read timed out")


class _HttpErrorTransport(httpx.AsyncBaseTransport):
    """Raises a non-connect, non-timeout HTTP transport error."""

    async def handle_async_request(self, request):
        raise httpx.RemoteProtocolError("upstream protocol error")


class _ReadTimeoutStreamTransport(httpx.AsyncBaseTransport):
    """Returns a successful response but raises timeout when reading body lines."""

    class _TimeoutIterator:

        def __init__(self, initial_lines: list[str]) -> None:
            self._lines = initial_lines
            self._index = 0

        def __aiter__(self) -> "_ReadTimeoutStreamTransport._TimeoutIterator":
            return self

        async def __anext__(self) -> str:
            if self._index < len(self._lines):
                line = self._lines[self._index]
                self._index += 1
                return line
            raise httpx.ReadTimeout("read timed out during streaming")

    def __init__(self, initial_lines: list[str], status_code: int = 200) -> None:
        self._initial_lines = initial_lines
        self._status_code = status_code
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        content = "\n".join(self._initial_lines).encode()
        response = httpx.Response(
            self._status_code,
            content=content,
            headers={"Content-Type": "text/event-stream"},
        )

        # Replace aiter_lines with a method that returns our custom iterator
        def make_aiter_lines():
            return self._TimeoutIterator(self._initial_lines)

        response.aiter_lines = make_aiter_lines
        return response


class _HttpErrorStreamTransport(httpx.AsyncBaseTransport):
    """Returns a successful response but raises HTTPError when reading body lines."""

    class _ErrorIterator:

        def __init__(self, initial_lines: list[str]) -> None:
            self._lines = initial_lines
            self._index = 0

        def __aiter__(self) -> "_HttpErrorStreamTransport._ErrorIterator":
            return self

        async def __anext__(self) -> str:
            if self._index < len(self._lines):
                line = self._lines[self._index]
                self._index += 1
                return line
            raise httpx.HTTPError("HTTP error during streaming")

    def __init__(self, initial_lines: list[str], status_code: int = 200) -> None:
        self._initial_lines = initial_lines
        self._status_code = status_code
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        content = "\n".join(self._initial_lines).encode()
        response = httpx.Response(
            self._status_code,
            content=content,
            headers={"Content-Type": "text/event-stream"},
        )

        # Replace aiter_lines with a method that returns our custom iterator
        def make_aiter_lines():
            return self._ErrorIterator(self._initial_lines)

        response.aiter_lines = make_aiter_lines
        return response


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_from_env_uses_default_base_url() -> None:
    client = HttpWorkerClient.from_env(_token(), environ={})
    assert client._base_url == "http://127.0.0.1:8001"  # noqa: SLF001


def test_from_env_uses_custom_base_url() -> None:
    client = HttpWorkerClient.from_env(
        _token(),
        environ={"TRPC_WORKER_BASE_URL": "http://worker:9000"},
    )
    assert client._base_url == "http://worker:9000"  # noqa: SLF001


def test_from_env_uses_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRPC_WORKER_BASE_URL", "http://worker-from-env:9001")

    client = HttpWorkerClient.from_env(_token())

    assert client._base_url == "http://worker-from-env:9001"  # noqa: SLF001


def test_from_env_blank_base_url_uses_default() -> None:
    client = HttpWorkerClient.from_env(_token(), environ={"TRPC_WORKER_BASE_URL": "   "})
    assert client._base_url == "http://127.0.0.1:8001"  # noqa: SLF001


def test_rejects_non_http_scheme() -> None:
    with pytest.raises(ValueError, match="scheme"):
        HttpWorkerClient(base_url="ftp://worker:8001", internal_token=_token())


def test_rejects_credentials_in_url() -> None:
    with pytest.raises(ValueError, match="credentials"):
        HttpWorkerClient(base_url="http://user:pass@worker:8001", internal_token=_token())


def test_rejects_query_in_url() -> None:
    with pytest.raises(ValueError, match="query"):
        HttpWorkerClient(base_url="http://worker:8001?foo=bar", internal_token=_token())


def test_rejects_fragment_in_url() -> None:
    with pytest.raises(ValueError, match="fragment"):
        HttpWorkerClient(base_url="http://worker:8001#frag", internal_token=_token())


def test_rejects_missing_hostname() -> None:
    with pytest.raises(ValueError, match="hostname"):
        HttpWorkerClient(base_url="http://", internal_token=_token())


def test_rejects_hostname_with_empty_path() -> None:
    with pytest.raises(ValueError, match="hostname"):
        HttpWorkerClient(base_url="http:///worker", internal_token=_token())


# ---------------------------------------------------------------------------
# chat() — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_success_returns_result() -> None:
    transport = _MockTransport()
    task = _task()
    result_body = _chat_result_json(task, response="hello world")
    transport.set_response(httpx.Response(200, json=result_body))

    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    result = await client.chat(task)

    assert isinstance(result, WorkerChatResult)
    assert result.response == "hello world"
    assert result.error_code is None
    assert result.request_id == task.request_id
    assert len(transport.requests) == 1
    req = transport.requests[0]
    assert req.url.path == "/internal/v1/chat"
    assert req.headers["x-trpc-internal-token"] == _TOKEN_VALUE


@pytest.mark.asyncio
async def test_chat_sends_correct_body() -> None:
    transport = _MockTransport()
    task = _task()
    transport.set_response(httpx.Response(200, json=_chat_result_json(task)))

    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    await client.chat(task)

    body = json.loads(transport.requests[0].content)
    assert body["protocol_version"] == 1
    assert body["request_id"] == str(task.request_id)
    assert body["tenant_id"] == task.tenant_id
    assert body["message"] == task.message


# ---------------------------------------------------------------------------
# chat() — error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_connect_error_maps_to_worker_unavailable() -> None:
    client = HttpWorkerClient(
        base_url="http://127.0.0.1:8001",
        internal_token=_token(),
        transport=_ConnectErrorTransport(),
    )
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.WORKER_UNAVAILABLE


@pytest.mark.asyncio
async def test_chat_timeout_maps_to_worker_timeout() -> None:
    client = HttpWorkerClient(
        base_url="http://127.0.0.1:8001",
        internal_token=_token(),
        transport=_TimeoutTransport(),
    )
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.WORKER_TIMEOUT


@pytest.mark.asyncio
async def test_chat_http_error_maps_to_worker_unavailable() -> None:
    client = HttpWorkerClient(
        base_url="http://127.0.0.1:8001",
        internal_token=_token(),
        transport=_HttpErrorTransport(),
    )
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.WORKER_UNAVAILABLE


@pytest.mark.asyncio
async def test_chat_non_200_maps_to_invalid_worker_response() -> None:
    transport = _MockTransport()
    transport.set_response(httpx.Response(500, text="Internal Server Error"))
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.INVALID_WORKER_RESPONSE


@pytest.mark.asyncio
async def test_chat_invalid_json_maps_to_invalid_worker_response() -> None:
    transport = _MockTransport()
    transport.set_response(httpx.Response(200, text="not json", headers={"Content-Type": "application/json"}))
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.INVALID_WORKER_RESPONSE


@pytest.mark.asyncio
async def test_chat_invalid_schema_maps_to_invalid_worker_response() -> None:
    transport = _MockTransport()
    transport.set_response(httpx.Response(200, json={"bad": "schema"}))
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.INVALID_WORKER_RESPONSE


@pytest.mark.asyncio
async def test_chat_request_id_mismatch_maps_to_invalid_worker_response() -> None:
    transport = _MockTransport()
    task = _task()
    wrong_rid = _chat_result_json(task, response="OK")
    wrong_rid["request_id"] = str(uuid.uuid4())
    transport.set_response(httpx.Response(200, json=wrong_rid))
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.INVALID_WORKER_RESPONSE


# ---------------------------------------------------------------------------
# stream() — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_success_yields_events() -> None:
    task = _task()
    lines = [
        f"data: {json.dumps(_event_json(task, 'delta', data='Hello'))}",
        f"data: {json.dumps(_event_json(task, 'done'))}",
    ]
    transport = _StreamingTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)

    events = []
    async for event in client.stream(task):
        events.append(event)

    assert len(events) == 2
    assert events[0].type == "delta"
    assert events[0].data == "Hello"
    assert events[1].type == "done"


@pytest.mark.asyncio
async def test_stream_sends_token_header() -> None:
    task = _task()
    lines = [f"data: {json.dumps(_event_json(task, 'done'))}"]
    transport = _StreamingTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)

    async for _ in client.stream(task):
        pass

    assert len(transport.requests) == 1
    assert transport.requests[0].headers["x-trpc-internal-token"] == _TOKEN_VALUE


@pytest.mark.asyncio
async def test_stream_stops_at_error_event() -> None:
    task = _task()
    lines = [
        f"data: {json.dumps(_event_json(task, 'error', error_code='model_runtime'))}",
    ]
    transport = _StreamingTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)

    events = []
    async for event in client.stream(task):
        events.append(event)

    assert len(events) == 1
    assert events[0].type == "error"
    assert events[0].error_code == WorkerErrorCode.MODEL_RUNTIME


# ---------------------------------------------------------------------------
# stream() — error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_connect_error_maps_to_worker_unavailable() -> None:
    client = HttpWorkerClient(
        base_url="http://127.0.0.1:8001",
        internal_token=_token(),
        transport=_ConnectErrorTransport(),
    )
    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(_task()):
            pass
    assert exc_info.value.code == WorkerErrorCode.WORKER_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_timeout_maps_to_worker_timeout() -> None:
    client = HttpWorkerClient(
        base_url="http://127.0.0.1:8001",
        internal_token=_token(),
        transport=_TimeoutTransport(),
    )
    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(_task()):
            pass
    assert exc_info.value.code == WorkerErrorCode.WORKER_TIMEOUT


@pytest.mark.asyncio
async def test_stream_send_http_error_maps_to_worker_unavailable() -> None:
    client = HttpWorkerClient(
        base_url="http://127.0.0.1:8001",
        internal_token=_token(),
        transport=_HttpErrorTransport(),
    )
    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(_task()):
            pass
    assert exc_info.value.code == WorkerErrorCode.WORKER_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_early_eof_maps_to_invalid_worker_response() -> None:
    """Stream ends without done or error event - should raise invalid_worker_response."""
    task = _task()
    delta_event = {
        "protocol_version": 1,
        "request_id": str(task.request_id),
        "type": "delta",
        "data": "partial",
        "error_code": None
    }
    lines = [f"data: {json.dumps(delta_event)}\n"]
    transport = _StreamingTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(task):
            pass
    assert exc_info.value.code == WorkerErrorCode.INVALID_WORKER_RESPONSE


@pytest.mark.asyncio
async def test_stream_non_200_maps_to_invalid_worker_response() -> None:
    transport = _StreamingTransport([], status_code=500)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(_task()):
            pass
    assert exc_info.value.code == WorkerErrorCode.INVALID_WORKER_RESPONSE


@pytest.mark.asyncio
async def test_stream_request_id_mismatch_maps_to_invalid_worker_response() -> None:
    task = _task()
    wrong_event = _event_json(task, "done")
    wrong_event["request_id"] = str(uuid.uuid4())
    lines = [f"data: {json.dumps(wrong_event)}"]
    transport = _StreamingTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)

    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(task):
            pass
    assert exc_info.value.code == WorkerErrorCode.INVALID_WORKER_RESPONSE


@pytest.mark.asyncio
async def test_stream_read_timeout_maps_to_worker_timeout() -> None:
    """Timeout during read phase (not connection) should map to WORKER_TIMEOUT."""
    task = _task()
    delta_event = _event_json(task, "delta", data="partial")
    lines = [f"data: {json.dumps(delta_event)}"]
    transport = _ReadTimeoutStreamTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)

    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(task):
            pass
    assert exc_info.value.code == WorkerErrorCode.WORKER_TIMEOUT


@pytest.mark.asyncio
async def test_stream_http_error_during_read_maps_to_worker_unavailable() -> None:
    """HTTP error during read phase should map to WORKER_UNAVAILABLE."""
    task = _task()
    delta_event = _event_json(task, "delta", data="partial")
    lines = [f"data: {json.dumps(delta_event)}"]
    transport = _HttpErrorStreamTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)

    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(task):
            pass
    assert exc_info.value.code == WorkerErrorCode.WORKER_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_ignores_non_data_lines() -> None:
    """Stream should skip lines that don't start with 'data: ' prefix."""
    task = _task()
    lines = [
        "",  # empty line
        ": comment line",  # SSE comment
        "event: some-event",  # event type line without data: prefix
        f"data: {json.dumps(_event_json(task, 'delta', data='Hello'))}",
        "   ",  # whitespace only
        f"data: {json.dumps(_event_json(task, 'done'))}",
    ]
    transport = _StreamingTransport(lines)
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)

    events = []
    async for event in client.stream(task):
        events.append(event)

    assert len(events) == 2
    assert events[0].type == "delta"
    assert events[0].data == "Hello"
    assert events[1].type == "done"


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    transport = _MockTransport()
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    await client.close()
    await client.close()


@pytest.mark.asyncio
async def test_chat_after_close_raises_unavailable() -> None:
    transport = _MockTransport()
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    await client.close()
    with pytest.raises(WorkerClientError) as exc_info:
        await client.chat(_task())
    assert exc_info.value.code == WorkerErrorCode.WORKER_UNAVAILABLE


@pytest.mark.asyncio
async def test_stream_after_close_raises_worker_unavailable() -> None:
    """Calling stream() after close() should raise WORKER_UNAVAILABLE."""
    transport = _MockTransport()
    client = HttpWorkerClient(base_url="http://127.0.0.1:8001", internal_token=_token(), transport=transport)
    await client.close()
    with pytest.raises(WorkerClientError) as exc_info:
        async for _ in client.stream(_task()):
            pass
    assert exc_info.value.code == WorkerErrorCode.WORKER_UNAVAILABLE

"""HttpWorkerClient: Gateway-side HTTP client for the Worker process."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Mapping
from typing import Protocol
from typing import runtime_checkable
from urllib.parse import urlsplit

import httpx
from opentelemetry.trace import SpanKind

from trpc_service.telemetry.propagation import inject_traceparent
from trpc_service.telemetry.runtime import SPAN_WORKER_REQUEST, safe_span
from trpc_service.transport.auth import InternalToken
from trpc_service.transport.models import (
    WorkerApprovalResult,
    WorkerApprovalTask,
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
)
from trpc_service.transport.sse import decode_worker_event

logger = logging.getLogger(__name__)

_WORKER_BASE_URL_ENV = "TRPC_WORKER_BASE_URL"
_DEFAULT_WORKER_BASE_URL = "http://127.0.0.1:8001"
_CONNECT_TIMEOUT = 5.0
_STREAM_READ_TIMEOUT = 180.0


class WorkerClientError(RuntimeError):
    """Raised when the Worker cannot be reached or returns an invalid response."""

    def __init__(self, code: WorkerErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


@runtime_checkable
class WorkerClient(Protocol):
    """Protocol defining the interface for Worker clients."""

    async def start(self) -> None:
        """Initialize the client (idempotent)."""
        ...

    async def chat(self, task: WorkerTask) -> WorkerChatResult:
        """Send a synchronous chat request to the Worker."""
        ...

    def stream(self, task: WorkerTask) -> AsyncIterator[WorkerEvent]:
        """Stream events from the Worker."""
        ...

    async def decide(self, task: WorkerApprovalTask) -> WorkerApprovalResult:
        """Submit one approval decision; routed to a single Worker, no retry."""
        ...

    async def close(self) -> None:
        """Close the client and release resources."""
        ...


class HttpWorkerClient:
    """Application-scoped httpx client that talks to the Worker over HTTP/SSE."""

    def __init__(
        self,
        base_url: str,
        internal_token: InternalToken,
        transport: httpx.AsyncBaseTransport | None = None,
        *,
        tracer: object | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"Worker base URL scheme must be http or https, got {parsed.scheme!r}")
        if not parsed.hostname:
            raise ValueError("Worker base URL must include a hostname")
        if parsed.username or parsed.password:
            raise ValueError("Worker base URL must not contain credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("Worker base URL must not contain query or fragment")

        self._base_url = base_url.rstrip("/")
        self._token = internal_token
        # Stage 6B1: optional CLIENT-span tracer.  With it, every real
        # Worker request gets one "worker.request" span and W3C
        # ``traceparent`` injection; health probes stay untraced.
        self._tracer = tracer
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-TRPC-Internal-Token": internal_token.header_value()},
            timeout=httpx.Timeout(connect=_CONNECT_TIMEOUT, read=_STREAM_READ_TIMEOUT, write=5.0, pool=5.0),
            transport=transport,
        )
        self._closed = False

    async def start(self) -> None:
        """No-op for single-endpoint client. Satisfies WorkerClient protocol."""

    async def check_health(self, timeout_seconds: float) -> bool:
        """Probe this Worker's /health endpoint. Returns False on any failure."""
        try:
            response = await self._client.get(
                "/health",
                timeout=httpx.Timeout(timeout_seconds),
            )
            if response.status_code != 200:
                return False
            data = response.json()
            return isinstance(data, dict) and data.get("status") == "ok"
        except Exception:
            return False

    @classmethod
    def from_env(
        cls,
        internal_token: InternalToken,
        environ: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> "HttpWorkerClient":
        if environ is None:
            import os
            environ = os.environ
        base_url = environ.get(_WORKER_BASE_URL_ENV, _DEFAULT_WORKER_BASE_URL).strip()
        if not base_url:
            base_url = _DEFAULT_WORKER_BASE_URL
        return cls(base_url=base_url, internal_token=internal_token, transport=transport)

    def _outbound_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        inject_traceparent(headers)
        return headers

    async def chat(self, task: WorkerTask) -> WorkerChatResult:
        if self._closed:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
        with safe_span(self._tracer, SPAN_WORKER_REQUEST, kind=SpanKind.CLIENT):
            try:
                response = await self._client.post(
                    "/internal/v1/chat",
                    json=task.model_dump(mode="json"),
                    headers=self._outbound_headers(),
                )
            except httpx.ConnectError:
                raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE) from None
            except httpx.TimeoutException:
                raise WorkerClientError(WorkerErrorCode.WORKER_TIMEOUT) from None
            except httpx.HTTPError:
                raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE) from None

            if response.status_code != 200:
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE)
            try:
                raw = response.json()
                result = WorkerChatResult.model_validate(raw)
            except (json.JSONDecodeError, ValueError, TypeError):
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE) from None

            if result.request_id != task.request_id:
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE)
            return result

    async def decide(self, task: WorkerApprovalTask) -> WorkerApprovalResult:
        if self._closed:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
        with safe_span(self._tracer, SPAN_WORKER_REQUEST, kind=SpanKind.CLIENT):
            try:
                response = await self._client.post(
                    "/internal/v1/approvals/decide",
                    json=task.model_dump(mode="json"),
                    headers=self._outbound_headers(),
                )
            except httpx.ConnectError:
                raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE) from None
            except httpx.TimeoutException:
                raise WorkerClientError(WorkerErrorCode.WORKER_TIMEOUT) from None
            except httpx.HTTPError:
                raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE) from None

            if response.status_code != 200:
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE)
            try:
                raw = response.json()
                result = WorkerApprovalResult.model_validate(raw)
            except (json.JSONDecodeError, ValueError, TypeError):
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE) from None

            if result.request_id != task.request_id:
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE) from None
            return result

    async def stream(self, task: WorkerTask) -> AsyncIterator[WorkerEvent]:
        if self._closed:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)

        # The CLIENT span lives exactly as long as the SSE consumption: it
        # ends on terminal events, on errors, and on consumer cancellation
        # (GeneratorExit / CancelledError) alike.
        with safe_span(self._tracer, SPAN_WORKER_REQUEST, kind=SpanKind.CLIENT):
            request = self._client.build_request(
                "POST",
                "/internal/v1/chat/stream",
                json=task.model_dump(mode="json"),
                headers=self._outbound_headers(),
            )
            try:
                response = await self._client.send(request, stream=True)
            except httpx.ConnectError:
                raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE) from None
            except httpx.TimeoutException:
                raise WorkerClientError(WorkerErrorCode.WORKER_TIMEOUT) from None
            except httpx.HTTPError:
                raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE) from None

            if response.status_code != 200:
                await response.aclose()
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE)

            received_terminal = False
            try:
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    try:
                        event = decode_worker_event(line, expected_request_id=task.request_id)
                    except (json.JSONDecodeError, ValueError, TypeError):
                        raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE) from None
                    yield event
                    if event.type in ("done", "error"):
                        received_terminal = True
                        return
            except WorkerClientError:
                raise
            except httpx.TimeoutException:
                raise WorkerClientError(WorkerErrorCode.WORKER_TIMEOUT) from None
            except httpx.HTTPError:
                raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE) from None
            finally:
                await response.aclose()

            if not received_terminal:
                raise WorkerClientError(WorkerErrorCode.INVALID_WORKER_RESPONSE)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._client.aclose()


__all__ = [
    "HttpWorkerClient",
    "WorkerClient",
    "WorkerClientError",
]
